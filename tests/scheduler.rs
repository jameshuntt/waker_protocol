//! The scheduler end to end. Tasks count their drops in a thread-local, so a task freed twice
//! or never fails here, and Miri reports any leak or bad read.

use std::cell::{Cell, RefCell};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Context;
use std::time::Duration;

use waker_protocol::{
    derive_protocol_task_vtable, scheduler_waker, typed_envelope, CapitulatePlan, CapitulateStrategy,
    CheckpointHandle, CheckpointReason, CheckpointStore, DefaultCapitulateStrategy,
    MemoryCheckpointStore, ProtocolTask, ProtocolTaskPtr, QoSClass, Resident, RetentionPolicy,
    SchedulerConfig, SchedulerMsg, TaskEntry, TaskId, TaskPoll, TaskPolicy, TaskSlab, TaskSnapshot,
    TaskState, WakeOrdering, WakeReason, WakerProtocol,
};
use waker_tables::VTableKey;

thread_local! {
    static DROPS: Cell<usize> = const { Cell::new(0) };
    static CLEANUPS: Cell<usize> = const { Cell::new(0) };
    static ORDER: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

fn drops() -> usize {
    DROPS.with(Cell::get)
}

/// Counts to `target`, one step per poll; remembers its wakes and messages.
struct Counter {
    name: &'static str,
    count: u32,
    target: u32,
    wakes: Vec<WakeReason>,
    notes: Vec<String>,
}

impl Counter {
    fn boxed(name: &'static str, target: u32) -> Box<Self> {
        Box::new(Self { name, count: 0, target, wakes: Vec::new(), notes: Vec::new() })
    }
}

impl Drop for Counter {
    fn drop(&mut self) {
        DROPS.with(|d| d.set(d.get() + 1));
    }
}

// SAFETY: the functions only touch `self`; `restore` panics on short bytes.
unsafe impl ProtocolTask for Counter {
    unsafe fn poll(&mut self) -> TaskPoll {
        ORDER.with(|o| o.borrow_mut().push(self.name));
        self.count += 1;
        if self.count >= self.target { TaskPoll::Ready } else { TaskPoll::Pending }
    }

    unsafe fn on_wake(&mut self, reason: WakeReason) {
        self.wakes.push(reason);
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        let mut bytes = self.count.to_le_bytes().to_vec();
        bytes.extend_from_slice(&self.target.to_le_bytes());
        bytes
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let target = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        Self { name: "restored", count, target, wakes: Vec::new(), notes: Vec::new() }
    }

    unsafe fn cleanup(&mut self) {
        CLEANUPS.with(|c| c.set(c.get() + 1));
    }
}

/// A second task type, for the mismatch checks.
struct Other(u64);

// SAFETY: the functions only touch `self`.
unsafe impl ProtocolTask for Other {
    unsafe fn poll(&mut self) -> TaskPoll {
        TaskPoll::Pending
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        self.0.to_le_bytes().to_vec()
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        Other(u64::from_le_bytes(bytes[0..8].try_into().unwrap()))
    }
}

derive_protocol_task_vtable!(Counter, COUNTER_VTABLE);
derive_protocol_task_vtable!(Other, OTHER_VTABLE);

const COUNTER: VTableKey = VTableKey { segment: "test", kind: "counter", version: 1 };
const OTHER: VTableKey = VTableKey { segment: "test", kind: "other", version: 1 };

fn scheduler() -> WakerProtocol<MemoryCheckpointStore> {
    scheduler_with(MemoryCheckpointStore::default(), Box::new(DefaultCapitulateStrategy::default()))
}

fn scheduler_with<S: CheckpointStore>(store: S, strategy: Box<dyn CapitulateStrategy>) -> WakerProtocol<S> {
    let sched = WakerProtocol::with_strategy(store, strategy);
    sched.register_vtable(COUNTER, &COUNTER_VTABLE);
    sched.register_vtable(OTHER, &OTHER_VTABLE);
    sched.register_vtable(SCRIPTED, &SCRIPTED_VTABLE);
    sched.register_vtable(PANICKY, &PANICKY_VTABLE);
    sched
}

fn row<S: CheckpointStore>(sched: &WakerProtocol<S>, id: TaskId) -> &TaskEntry<S::Handle> {
    sched.tasks.get(id).expect("the task's row is there")
}

fn is_live<S: CheckpointStore>(sched: &WakerProtocol<S>, id: TaskId) -> bool {
    matches!(row(sched, id).resident, Resident::Live(_))
}

fn clear_order() {
    ORDER.with(|o| o.borrow_mut().clear());
}

fn order() -> Vec<&'static str> {
    ORDER.with(|o| o.borrow().clone())
}

/// A message `deliver` that writes its text to ORDER.
fn note(_: &mut Counter, text: &'static str) -> Result<(), String> {
    ORDER.with(|o| o.borrow_mut().push(text));
    Ok(())
}

fn live_counter(sched: &WakerProtocol<MemoryCheckpointStore>, id: TaskId) -> &ProtocolTaskPtr {
    match &sched.tasks.get(id).unwrap().resident {
        Resident::Live(ptr) => ptr,
        _ => panic!("task not live"),
    }
}

#[test]
fn a_task_runs_to_ready_and_is_freed_once() {
    let mut sched = scheduler();
    let before = drops();
    let handle = sched.spawn_box(COUNTER, Counter::boxed("a", 3), TaskPolicy::default()).unwrap();
    let id = handle.id();

    for _ in 0..2 {
        sched.run_once().unwrap();
        assert!(sched.tasks.get(id).is_some());
        handle.wake(WakeReason::Manual).unwrap();
    }
    sched.run_once().unwrap();
    assert!(sched.tasks.get(id).is_none(), "removed when ready");
    assert_eq!(drops() - before, 1);
    assert!(CLEANUPS.with(Cell::get) >= 1);
}

#[test]
fn spawn_refuses_an_unregistered_key() {
    let mut sched = scheduler();
    let before = drops();
    let key = VTableKey { segment: "test", kind: "missing", version: 1 };
    let err = sched.spawn_box(key, Counter::boxed("x", 1), TaskPolicy::default()).err().unwrap();
    assert!(err.contains("no protocol vtable registered"), "{err}");
    assert_eq!(drops() - before, 1, "the refused task is freed");
}

#[test]
fn spawn_refuses_a_key_for_another_type() {
    let mut sched = scheduler();
    let before = drops();
    let err = sched.spawn_box(OTHER, Counter::boxed("x", 1), TaskPolicy::default()).err().unwrap();
    assert!(err.contains("is for scheduler::Other, not scheduler::Counter"), "{err}");
    assert_eq!(drops() - before, 1, "the refused task is freed");
    assert_eq!(sched.tasks.live_ids().len(), 0);
}

#[test]
fn messages_are_delivered_before_the_poll() {
    let mut sched = scheduler();
    let handle = sched.spawn_box(COUNTER, Counter::boxed("m", 10), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();

    ORDER.with(|o| o.borrow_mut().clear());
    handle
        .send("note", String::from("hello"), |task: &mut Counter, note| {
            assert_eq!(note, "hello");
            task.notes.push(note);
            ORDER.with(|o| o.borrow_mut().push("delivered"));
            Ok(())
        })
        .unwrap();
    sched.run_once().unwrap();

    ORDER.with(|o| assert_eq!(*o.borrow(), ["delivered", "m"], "message first, then the poll"));
    let ptr = live_counter(&sched, handle.id());
    assert_eq!(ptr.vtable().type_name(), "scheduler::Counter");
    assert_eq!(sched.tasks.get(handle.id()).unwrap().stats.last_wake, Some(WakeReason::Inbox));
}

#[test]
fn a_message_for_another_type_is_refused_not_delivered() {
    let mut sched = scheduler();
    let handle = sched.spawn_box(COUNTER, Counter::boxed("t", 10), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();

    // Written for `Other`, sent to a Counter's id through the plain scheduler channel.
    let wrong = typed_envelope::<Other, u64>("poke", 7, |task, n| {
        task.0 = n;
        Ok(())
    });
    let before = drops();
    let err = deliver_raw(&mut sched, SchedulerMsg::Deliver { id: handle.id(), envelope: wrong }).unwrap_err();
    assert!(err.contains("targets another task type than scheduler::Counter"), "{err}");
    assert_eq!(drops() - before, 0, "only the message is dropped, never the task");
    assert!(is_live(&sched, handle.id()), "the task stays live in its row");
    assert_eq!(row(&sched, handle.id()).stats.poll_count, 2, "and the round went on to poll it");

    clear_order();
    handle.send("note", "right type", note).unwrap();
    sched.run_once().unwrap();
    assert_eq!(order(), ["right type", "t"], "a message of the right type still arrives");
}

/// Push a hand-built message through the scheduler's own channel and run once.
fn deliver_raw(sched: &mut WakerProtocol<MemoryCheckpointStore>, msg: SchedulerMsg) -> Result<(), String> {
    sched.sender().send(msg).map_err(|e| e.to_string())?;
    sched.run_once()
}

#[test]
fn checkpoint_then_wake_restores_the_same_state() {
    let mut sched = scheduler();
    let before = drops();
    let handle = sched.spawn_box(COUNTER, Counter::boxed("c", 5), TaskPolicy::default()).unwrap();
    let id = handle.id();
    sched.run_once().unwrap();
    handle.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap();

    handle.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    let entry = sched.tasks.get(id).unwrap();
    assert!(matches!(entry.resident, Resident::Dormant));
    assert_eq!(entry.state, TaskState::Dormant);
    assert_eq!(entry.stats.checkpoint_count, 1);
    assert_eq!(drops() - before, 1, "the live copy is freed at checkpoint");
    assert_eq!(sched.reclaim_log.len(), 1);
    assert_eq!(sched.reclaim_log[0].reason, CheckpointReason::Manual);

    handle.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap();
    let entry = sched.tasks.get(id).unwrap();
    assert_eq!(entry.stats.restore_count, 1);
    // Two polls before the checkpoint, one after: the restored count carried on from 2.
    assert_eq!(entry.stats.poll_count, 3);

    for _ in 0..2 {
        handle.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap();
    }
    assert!(sched.tasks.get(id).is_none(), "ready at 5 after restore");
    assert_eq!(drops() - before, 2);
}

#[test]
fn completed_tasks_take_their_checkpoints_with_them() {
    let mut sched = scheduler();
    let handle = sched.spawn_box(COUNTER, Counter::boxed("r", 3), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    handle.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    let ckpt = sched.reclaim_log[0].handle.clone().unwrap();
    assert!(sched.store.load(&ckpt).is_ok());

    for _ in 0..3 {
        handle.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap();
    }
    assert!(sched.tasks.get(handle.id()).is_none());
    assert!(sched.store.load(&ckpt).is_err(), "retain_on_completion is false by default");
}

#[test]
fn cancel_removes_and_frees_the_task() {
    let mut sched = scheduler();
    let before = drops();
    let handle = sched.spawn_box(COUNTER, Counter::boxed("k", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    handle.cancel().unwrap();
    sched.run_once().unwrap();
    assert!(sched.tasks.get(handle.id()).is_none());
    assert_eq!(drops() - before, 1);
}

#[test]
fn capitulate_without_a_stub_removes_with_a_stub_goes_dormant() {
    let mut sched = scheduler();
    let before = drops();
    let gone = sched.spawn_box(COUNTER, Counter::boxed("g", 100), TaskPolicy::default()).unwrap();
    let kept = sched.spawn_box(COUNTER, Counter::boxed("s", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();

    gone.capitulate(CapitulatePlan { reason: CheckpointReason::QoS, retain_stub: false }).unwrap();
    kept.capitulate(CapitulatePlan { reason: CheckpointReason::QoS, retain_stub: true }).unwrap();
    sched.run_once().unwrap();

    assert!(sched.tasks.get(gone.id()).is_none());
    assert_eq!(sched.tasks.get(kept.id()).unwrap().state, TaskState::Dormant);
    assert_eq!(drops() - before, 2, "both live copies freed");
}

#[test]
fn shutdown_checkpoints_everything_and_stops_the_pump() {
    let mut sched = scheduler();
    let a = sched.spawn_box(COUNTER, Counter::boxed("a", 100), TaskPolicy::default()).unwrap();
    let b = sched.spawn_box(COUNTER, Counter::boxed("b", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    sched.handle().shutdown().unwrap();
    assert!(!sched.pump(std::time::Duration::from_millis(1)).unwrap());
    for id in [a.id(), b.id()] {
        assert_eq!(sched.tasks.get(id).unwrap().state, TaskState::Dormant);
    }
    assert!(sched.shutdown);
}

#[test]
fn biased_qos_polls_higher_classes_first() {
    let mut sched = scheduler();
    sched.config = SchedulerConfig { wake_ordering: WakeOrdering::BiasedQoS };
    ORDER.with(|o| o.borrow_mut().clear());
    let policy = |qos| TaskPolicy { qos, ..TaskPolicy::default() };
    sched.spawn_box(COUNTER, Counter::boxed("background", 9), policy(QoSClass::Background)).unwrap();
    sched.spawn_box(COUNTER, Counter::boxed("realtime", 9), policy(QoSClass::Realtime)).unwrap();
    sched.spawn_box(COUNTER, Counter::boxed("standard", 9), policy(QoSClass::Standard)).unwrap();
    sched.run_once().unwrap();
    ORDER.with(|o| assert_eq!(*o.borrow(), ["realtime", "standard", "background"]));
}

#[test]
fn dropping_the_scheduler_frees_live_and_queued_work() {
    let before = drops();
    {
        let mut sched = scheduler();
        let h = sched.spawn_box(COUNTER, Counter::boxed("d", 100), TaskPolicy::default()).unwrap();
        sched.run_once().unwrap();
        h.send("n", String::from("queued"), |t: &mut Counter, n| {
            t.notes.push(n);
            Ok(())
        })
        .unwrap();
    }
    assert_eq!(drops() - before, 1);
}

#[test]
fn a_task_is_told_why_it_woke() {
    let mut sched = scheduler();
    let h = sched.spawn_box(COUNTER, Counter::boxed("w", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    h.wake(WakeReason::Timer).unwrap();
    sched.run_once().unwrap();
    assert_eq!(sched.tasks.get(h.id()).unwrap().stats.last_wake, Some(WakeReason::Timer));
}

#[test]
fn scheduler_waker_sends_a_wake_and_gives_its_refs_back() {
    let mut sched = scheduler();
    let h = sched.spawn_box(COUNTER, Counter::boxed("sw", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    let polls = sched.tasks.get(h.id()).unwrap().stats.poll_count;

    let waker = scheduler_waker(h.id(), sched.sender(), WakeReason::Condition);
    let cx = Context::from_waker(&waker);
    cx.waker().wake_by_ref();
    let clone = waker.clone();
    drop(waker);
    clone.wake();
    sched.run_once().unwrap();
    let entry = sched.tasks.get(h.id()).unwrap();
    assert_eq!(entry.stats.poll_count, polls + 1, "two wakes, one poll");
    assert_eq!(entry.stats.last_wake, Some(WakeReason::Condition));
}

#[test]
fn ptr_from_box_refuses_another_types_vtable() {
    let before = drops();
    let result = std::panic::catch_unwind(|| ProtocolTaskPtr::from_box(Box::new(Other(1)), &COUNTER_VTABLE));
    assert!(result.is_err());
    assert_eq!(drops() - before, 0, "Other does not count drops; nothing else was freed");
    let ok = ProtocolTaskPtr::from_box(Counter::boxed("p", 1), &COUNTER_VTABLE);
    assert!(ok.vtable().is_for::<Counter>());
    drop(ok);
    assert_eq!(drops() - before, 1);
}

#[test]
fn slab_ids_go_stale_when_a_slot_is_reused() {
    let mut slab = TaskSlab::default();
    let a = slab.insert("a");
    assert_eq!(slab.get(a), Some(&"a"));
    assert_eq!(slab.remove(a), Some("a"));
    assert_eq!(slab.remove(a), None);
    let b = slab.insert("b");
    assert_eq!(b.slot, a.slot);
    assert_ne!(b.generation, a.generation);
    assert_eq!(slab.get(a), None, "the old id cannot reach the new value");
    assert_eq!(slab.get_mut(b), Some(&mut "b"));
    let c = slab.insert("c");
    assert_eq!(slab.live_ids(), vec![b, c]);
}

#[test]
fn memory_store_save_load_remove() {
    let store = MemoryCheckpointStore::default();
    let id = TaskId::new(0, 0);
    let h1 = store.save(id, vec![1, 2]).unwrap();
    let h2 = store.save(id, vec![3]).unwrap();
    assert_ne!(h1, h2);
    assert_eq!(store.load(&h1).unwrap(), vec![1, 2]);
    store.remove(&h1).unwrap();
    assert!(store.load(&h1).is_err());
    assert_eq!(store.load(&h2).unwrap(), vec![3]);
}

#[test]
fn retention_keep_latest_prunes_older_checkpoints() {
    let mut sched = scheduler();
    let policy = TaskPolicy { retention: RetentionPolicy { keep_latest: 1, ..RetentionPolicy::default() }, ..TaskPolicy::default() };
    let h = sched.spawn_box(COUNTER, Counter::boxed("k", 100), policy).unwrap();
    sched.run_once().unwrap();
    for _ in 0..3 {
        h.checkpoint(CheckpointReason::Manual).unwrap();
        sched.run_once().unwrap();
        h.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap();
    }
    let handles: Vec<_> = sched.reclaim_log.iter().filter_map(|r| r.handle.clone()).collect();
    assert_eq!(handles.len(), 3);
    assert!(sched.store.load(&handles[0]).is_err());
    assert!(sched.store.load(&handles[1]).is_err());
    assert!(sched.store.load(&handles[2]).is_ok());
    assert_eq!(sched.tasks.get(h.id()).unwrap().checkpoint_history.len(), 1);
}

// ---------------------------------------------------------------------------------------------
// Tasks, stores and strategies for the 0.3.1 tests below.

/// Poll results a [`Scripted`] task can be told to give, one byte each.
const READY: u8 = 0;
const PENDING: u8 = 1;
const YIELD: u8 = 2;
const CHECKPOINT: u8 = 3;
const CAPITULATE: u8 = 4;
const CANCELLED: u8 = 5;
const FAILED: u8 = 6;

/// Gives the polls in `script` in turn and then repeats the last one; reports `footprint` as its
/// size; keeps its place in the script across a checkpoint.
struct Scripted {
    step: u32,
    footprint: u32,
    script: Vec<u8>,
}

impl Scripted {
    fn boxed(footprint: u32, script: &[u8]) -> Box<Self> {
        Box::new(Self { step: 0, footprint, script: script.to_vec() })
    }
}

impl Drop for Scripted {
    fn drop(&mut self) {
        DROPS.with(|d| d.set(d.get() + 1));
    }
}

// SAFETY: the functions only touch `self`; `restore` panics on short bytes.
unsafe impl ProtocolTask for Scripted {
    unsafe fn poll(&mut self) -> TaskPoll {
        let code = self.script[(self.step as usize).min(self.script.len() - 1)];
        self.step += 1;
        match code {
            READY => TaskPoll::Ready,
            PENDING => TaskPoll::Pending,
            YIELD => TaskPoll::Yield,
            CHECKPOINT => TaskPoll::Checkpoint(CheckpointReason::Manual),
            CAPITULATE => TaskPoll::Capitulate(CapitulatePlan { reason: CheckpointReason::QoS, retain_stub: false }),
            CANCELLED => TaskPoll::Cancelled,
            _ => TaskPoll::Failed("scripted failure".into()),
        }
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        let mut bytes = self.step.to_le_bytes().to_vec();
        bytes.extend_from_slice(&self.footprint.to_le_bytes());
        bytes.extend_from_slice(&self.script);
        bytes
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        Self {
            step: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
            footprint: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            script: bytes[8..].to_vec(),
        }
    }

    fn footprint_estimate(&self) -> usize {
        self.footprint as usize
    }
}

derive_protocol_task_vtable!(Scripted, SCRIPTED_VTABLE);
const SCRIPTED: VTableKey = VTableKey { segment: "test", kind: "scripted", version: 1 };

/// A checkpoint store whose `save` fails, whose `save` panics, or whose `remove` fails, while
/// told to.
#[derive(Default)]
struct FlakyStore {
    inner: MemoryCheckpointStore,
    failing: AtomicBool,
    panicking: AtomicBool,
    failing_remove: AtomicBool,
}

impl FlakyStore {
    fn fail(&self, on: bool) {
        self.failing.store(on, Ordering::SeqCst);
    }

    fn panic_on_save(&self, on: bool) {
        self.panicking.store(on, Ordering::SeqCst);
    }

    fn fail_remove(&self, on: bool) {
        self.failing_remove.store(on, Ordering::SeqCst);
    }
}

impl CheckpointStore for FlakyStore {
    type Handle = CheckpointHandle;

    fn save(&self, task_id: TaskId, payload: Vec<u8>) -> Result<CheckpointHandle, String> {
        if self.panicking.load(Ordering::SeqCst) {
            panic!("store bug");
        }
        if self.failing.load(Ordering::SeqCst) {
            return Err("disk full".into());
        }
        self.inner.save(task_id, payload)
    }

    fn load(&self, handle: &CheckpointHandle) -> Result<Vec<u8>, String> {
        self.inner.load(handle)
    }

    fn remove(&self, handle: &CheckpointHandle) -> Result<(), String> {
        if self.failing_remove.load(Ordering::SeqCst) {
            return Err("remove failed".into());
        }
        self.inner.remove(handle)
    }
}

fn flaky() -> WakerProtocol<FlakyStore> {
    scheduler_with(FlakyStore::default(), Box::new(DefaultCapitulateStrategy::default()))
}

/// Capitulates with its plan whenever the task's newest wake is `QoS`.
struct OnQoS(CapitulatePlan);

impl CapitulateStrategy for OnQoS {
    fn plan(&self, _: &TaskSnapshot, last_wake: Option<&WakeReason>) -> Option<CapitulatePlan> {
        matches!(last_wake, Some(WakeReason::QoS)).then(|| self.0.clone())
    }
}

/// Writes down every snapshot it is shown, then asks the default strategy.
struct Recording {
    seen: Arc<Mutex<Vec<TaskSnapshot>>>,
    inner: DefaultCapitulateStrategy,
}

impl CapitulateStrategy for Recording {
    fn plan(&self, snapshot: &TaskSnapshot, last_wake: Option<&WakeReason>) -> Option<CapitulatePlan> {
        self.seen.lock().unwrap().push(snapshot.clone());
        self.inner.plan(snapshot, last_wake)
    }
}

/// The default strategy with every threshold at zero, so its rules fire without waiting.
fn eager_default_strategy() -> DefaultCapitulateStrategy {
    DefaultCapitulateStrategy {
        min_footprint_bytes: 0,
        idle_before_checkpoint: Duration::ZERO,
        protect_qos_at_or_above: QoSClass::Realtime,
    }
}

// ---------------------------------------------------------------------------------------------
// An error drops one message, or leaves one task as it was; it never destroys a task, and the
// round goes on.

#[test]
fn a_failed_delivery_drops_only_that_message() {
    let mut sched = scheduler();
    let before = drops();
    let h = sched.spawn_box(COUNTER, Counter::boxed("d", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    clear_order();

    h.send("bad", "bad", |_, _| Err("rejected".into())).unwrap();
    h.send("good", "good", note).unwrap();
    assert_eq!(sched.run_once().unwrap_err(), "rejected");

    assert_eq!(drops() - before, 0, "the task is not freed");
    assert!(is_live(&sched, h.id()));
    assert_eq!(order(), ["good", "d"], "the next message is delivered and the task is polled");
}

#[test]
fn an_error_never_skips_removing_tasks_that_finished_earlier_in_the_round() {
    let mut sched = scheduler();
    let realtime = TaskPolicy { qos: QoSClass::Realtime, ..TaskPolicy::default() };
    let done = sched.spawn_box(COUNTER, Counter::boxed("done", 2), realtime).unwrap();
    let victim = sched.spawn_box(COUNTER, Counter::boxed("victim", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    done.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    let ckpt = sched.reclaim_log[0].handle.clone().unwrap();

    // One round: `done` (Realtime, so polled first) finishes, then `victim`'s message fails.
    done.wake(WakeReason::Manual).unwrap();
    victim.send("bad", "bad", |_, _| Err("nope".into())).unwrap();
    assert_eq!(sched.run_once().unwrap_err(), "nope");

    assert!(sched.tasks.get(done.id()).is_none(), "the finished task is removed in the same round");
    assert!(sched.store.load(&ckpt).is_err(), "with its checkpoint (retain_on_completion is false)");
    assert!(is_live(&sched, victim.id()));
}

#[test]
fn a_checkpoint_that_cannot_be_saved_leaves_the_task_live_as_it_was() {
    let mut sched = flaky();
    let before = drops();
    let h = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[CHECKPOINT]), TaskPolicy::default()).unwrap();
    sched.store.fail(true);
    assert_eq!(sched.run_once().unwrap_err(), "disk full");

    assert_eq!(drops() - before, 0, "nothing was saved, so nothing is freed");
    let entry = row(&sched, h.id());
    assert!(matches!(entry.resident, Resident::Live(_)));
    assert_eq!(entry.state, TaskState::Runnable);
    assert_eq!(entry.stats.checkpoint_count, 0);

    sched.store.fail(false);
    sched.run_once().unwrap();
    let entry = row(&sched, h.id());
    assert!(matches!(entry.resident, Resident::Dormant), "polled again, and this save goes through");
    assert_eq!(entry.stats.checkpoint_count, 1);
    assert_eq!(drops() - before, 1);
}

#[test]
fn a_strategy_checkpoint_that_cannot_be_saved_leaves_the_task_live() {
    let plan = CapitulatePlan { reason: CheckpointReason::QoS, retain_stub: true };
    let mut sched = scheduler_with(FlakyStore::default(), Box::new(OnQoS(plan)));
    let before = drops();
    let h = sched.spawn_box(COUNTER, Counter::boxed("q", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    clear_order();

    sched.store.fail(true);
    h.send("note", "delivered", note).unwrap();
    h.wake(WakeReason::QoS).unwrap();
    assert_eq!(sched.run_once().unwrap_err(), "disk full");
    assert_eq!(drops() - before, 0);
    assert!(is_live(&sched, h.id()));
    assert_eq!(order(), ["delivered"], "the message reached the task; the plan stopped the poll");

    sched.store.fail(false);
    sched.run_once().unwrap();
    assert!(matches!(row(&sched, h.id()).resident, Resident::Dormant));
}

#[test]
fn a_checkpoint_or_capitulate_message_that_cannot_be_saved_leaves_the_task_live() {
    let mut sched = flaky();
    let before = drops();
    let a = sched.spawn_box(COUNTER, Counter::boxed("a", 100), TaskPolicy::default()).unwrap();
    let b = sched.spawn_box(COUNTER, Counter::boxed("b", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();

    sched.store.fail(true);
    a.checkpoint(CheckpointReason::Manual).unwrap();
    b.capitulate(CapitulatePlan { reason: CheckpointReason::QoS, retain_stub: true }).unwrap();
    let err = sched.run_once().unwrap_err();
    assert_eq!(drops() - before, 0);
    for id in [a.id(), b.id()] {
        assert!(is_live(&sched, id));
        assert_eq!(row(&sched, id).state, TaskState::Waiting, "as it was");
    }
    assert_eq!(err, "disk full; disk full", "both errors, after the round");

    sched.store.fail(false);
    a.checkpoint(CheckpointReason::Manual).unwrap();
    b.capitulate(CapitulatePlan { reason: CheckpointReason::QoS, retain_stub: true }).unwrap();
    sched.run_once().unwrap();
    for id in [a.id(), b.id()] {
        assert!(matches!(row(&sched, id).resident, Resident::Dormant));
    }
}

// ---------------------------------------------------------------------------------------------
// Shutdown checkpoints every live task and then does nothing more.

#[test]
fn shutdown_returns_every_failed_save_and_keeps_those_tasks_live() {
    let mut sched = flaky();
    let before = drops();
    let ids: Vec<TaskId> = (0..3)
        .map(|_| sched.spawn_box(COUNTER, Counter::boxed("s", 100), TaskPolicy::default()).unwrap().id())
        .collect();
    sched.run_once().unwrap();

    sched.store.fail(true);
    sched.handle().shutdown().unwrap();
    let result = sched.pump(Duration::from_millis(1));
    assert_eq!(drops() - before, 0, "no task is freed without its checkpoint");
    assert!(ids.iter().all(|id| is_live(&sched, *id)));
    assert_eq!(result, Err("disk full; disk full; disk full".to_string()), "every failed save is returned");
    assert!(sched.shutdown);

    // Once the store works, clear the flag and shut down again.
    sched.store.fail(false);
    sched.shutdown = false;
    sched.handle().shutdown().unwrap();
    assert!(!sched.pump(Duration::from_millis(1)).unwrap());
    assert!(ids.iter().all(|id| matches!(row(&sched, *id).resident, Resident::Dormant)));
}

#[test]
fn after_shutdown_nothing_is_restored_or_polled_and_later_work_stays_queued() {
    let mut sched = scheduler();
    let a = sched.spawn_box(COUNTER, Counter::boxed("a", 100), TaskPolicy::default()).unwrap();
    let b = sched.spawn_box(COUNTER, Counter::boxed("b", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    clear_order();

    a.send("note", "queued before shutdown", note).unwrap();
    sched.handle().shutdown().unwrap();
    b.wake(WakeReason::Manual).unwrap();
    assert!(!sched.pump(Duration::from_millis(1)).unwrap());
    a.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap();
    assert!(!sched.pump(Duration::from_millis(1)).unwrap());

    for id in [a.id(), b.id()] {
        let entry = row(&sched, id);
        assert!(matches!(entry.resident, Resident::Dormant));
        assert_eq!((entry.stats.poll_count, entry.stats.restore_count), (1, 0), "not restored, not polled");
    }
    assert!(order().is_empty(), "nothing was delivered or polled");
    assert_eq!(row(&sched, a.id()).mailbox.len(), 1, "the message queued before shutdown is kept");
    assert_eq!(sched.sender().len(), 2, "the two wakes sent after it wait in the channel");
}

#[test]
fn a_wake_queued_before_a_checkpoint_message_still_brings_the_task_back() {
    let mut sched = scheduler();
    let h = sched.spawn_box(COUNTER, Counter::boxed("w", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();

    scheduler_waker(h.id(), sched.sender(), WakeReason::Condition).wake();
    h.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();

    let entry = row(&sched, h.id());
    assert_eq!(entry.stats.checkpoint_count, 1);
    assert_eq!(entry.stats.restore_count, 1, "the wake restored it");
    assert_eq!(entry.stats.poll_count, 2, "and it was polled for the wake");
}

// ---------------------------------------------------------------------------------------------
// Yield, pump and the capitulation strategy do what their docs say.

#[test]
fn yield_runs_again_next_round_when_eager_and_waits_for_a_wake_when_not() {
    let mut sched = scheduler();
    let eager = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[YIELD]), TaskPolicy::default()).unwrap();
    let lazy_policy = TaskPolicy { eager_repoll_after_yield: false, ..TaskPolicy::default() };
    let lazy = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[YIELD]), lazy_policy).unwrap();

    for round in 1..=3 {
        sched.run_once().unwrap();
        assert_eq!(row(&sched, eager.id()).stats.poll_count, round, "one poll per run_once, never two");
        assert_eq!(row(&sched, lazy.id()).stats.poll_count, 1, "parked until a wake");
    }
    assert_eq!(row(&sched, lazy.id()).state, TaskState::Waiting);

    lazy.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap();
    sched.run_once().unwrap();
    assert_eq!(row(&sched, lazy.id()).stats.poll_count, 2, "one poll per wake");
}

#[test]
fn pump_keeps_returning_true_with_every_handle_dropped_until_shutdown() {
    let mut sched = scheduler();
    let h = sched.spawn_box(COUNTER, Counter::boxed("p", 100), TaskPolicy::default()).unwrap();
    drop(h);
    drop(sched.handle());
    for _ in 0..3 {
        assert!(sched.pump(Duration::from_millis(1)).unwrap(), "the scheduler holds its own sender");
    }
    sched.sender().send(SchedulerMsg::Shutdown).unwrap();
    assert!(!sched.pump(Duration::from_millis(1)).unwrap());
}

#[test]
fn every_capitulation_without_a_stub_ends_the_task_as_completed() {
    // retain_on_completion is on, so a task that ends Completed keeps its latest checkpoint.
    let policy = TaskPolicy {
        retention: RetentionPolicy { retain_on_completion: true, ..RetentionPolicy::default() },
        ..TaskPolicy::default()
    };
    let no_stub = CapitulatePlan { reason: CheckpointReason::QoS, retain_stub: false };
    let mut sched = scheduler_with(MemoryCheckpointStore::default(), Box::new(OnQoS(no_stub.clone())));
    let before = drops();
    let by_poll = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[CHECKPOINT, CAPITULATE]), policy.clone()).unwrap();
    let by_message = sched.spawn_box(COUNTER, Counter::boxed("m", 100), policy.clone()).unwrap();
    let by_strategy = sched.spawn_box(COUNTER, Counter::boxed("s", 100), policy).unwrap();
    sched.run_once().unwrap(); // by_poll checkpoints itself
    by_message.checkpoint(CheckpointReason::Manual).unwrap();
    by_strategy.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    let ckpts: Vec<_> = sched.reclaim_log.iter().map(|r| r.handle.clone().unwrap()).collect();
    assert_eq!(ckpts.len(), 3);

    for h in [by_message.id(), by_poll.id(), by_strategy.id()] {
        sched.handle().wake(h, WakeReason::Manual).unwrap();
    }
    sched.run_once().unwrap(); // all restored; by_poll capitulates in its poll
    by_message.capitulate(no_stub).unwrap();
    by_strategy.wake(WakeReason::QoS).unwrap();
    sched.run_once().unwrap();

    for id in [by_poll.id(), by_message.id(), by_strategy.id()] {
        assert!(sched.tasks.get(id).is_none(), "{id:?} is removed");
    }
    assert_eq!(drops() - before, 6, "three checkpointed copies and three restored copies, each freed once");
    for ckpt in &ckpts {
        assert!(sched.store.load(ckpt).is_ok(), "ended Completed, so retain_on_completion keeps {ckpt:?}");
    }
}

#[test]
fn the_strategy_sees_waiting_messages_and_wakes_before_they_are_delivered() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let strategy = Recording { seen: seen.clone(), inner: eager_default_strategy() };
    let mut sched = scheduler_with(MemoryCheckpointStore::default(), Box::new(strategy));
    let h = sched.spawn_box(COUNTER, Counter::boxed("i", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    for _ in 0..5 {
        h.send("note", "work", note).unwrap();
    }
    sched.run_once().unwrap();

    let counts: Vec<_> = seen.lock().unwrap().iter().map(|s| (s.mailbox_len, s.pending_wakes)).collect();
    assert_eq!(counts, [(0, 1), (5, 5)], "(messages, wakes) waiting when the strategy looked");
    let entry = row(&sched, h.id());
    assert!(sched.reclaim_log.is_empty(), "the idle rule spares a task with waiting work");
    assert_eq!(entry.stats.poll_count, 2);
    assert!(matches!(entry.resident, Resident::Live(_)));
}

#[test]
fn the_idle_rule_checkpoints_a_task_with_no_waiting_work_and_a_wake_brings_it_back() {
    let mut sched = scheduler_with(MemoryCheckpointStore::default(), Box::new(eager_default_strategy()));
    let h = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[YIELD]), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap(); // polled; Yield keeps it runnable
    sched.run_once().unwrap(); // runnable with nothing waiting: idle
    assert_eq!(sched.reclaim_log.len(), 1);
    assert_eq!(sched.reclaim_log[0].reason, CheckpointReason::Idle);
    assert!(matches!(row(&sched, h.id()).resident, Resident::Dormant));

    h.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap();
    let entry = row(&sched, h.id());
    assert_eq!(entry.stats.restore_count, 1);
    assert_eq!(entry.stats.poll_count, 2, "woken, restored and polled");
    assert_eq!(sched.reclaim_log.len(), 1);
}

// ---------------------------------------------------------------------------------------------
// The default strategy's rules, the poll order, the message order, and every way a task ends.

#[test]
fn memory_pressure_reclaims_below_interactive_but_never_interactive_or_above() {
    let mut sched = scheduler();
    let classes = [QoSClass::Realtime, QoSClass::Interactive, QoSClass::Standard, QoSClass::Batch, QoSClass::Background];
    let big: Vec<TaskId> = classes
        .iter()
        .map(|&qos| {
            let policy = TaskPolicy { qos, ..TaskPolicy::default() };
            sched.spawn_box(SCRIPTED, Scripted::boxed(8192, &[PENDING]), policy).unwrap().id()
        })
        .collect();
    let small_policy = TaskPolicy { qos: QoSClass::Background, ..TaskPolicy::default() };
    let small = sched.spawn_box(SCRIPTED, Scripted::boxed(100, &[PENDING]), small_policy).unwrap().id();
    sched.run_once().unwrap();
    for &id in big.iter().chain([&small]) {
        sched.handle().wake(id, WakeReason::MemoryPressure).unwrap();
    }
    sched.run_once().unwrap();

    let reclaimed: Vec<TaskId> = sched.reclaim_log.iter().map(|r| r.id).collect();
    assert_eq!(reclaimed, big[2..], "Standard, Batch and Background, in poll order");
    assert!(sched.reclaim_log.iter().all(|r| r.reason == CheckpointReason::MemoryPressure));
    for &id in big[..2].iter().chain([&small]) {
        assert!(is_live(&sched, id), "protected or too small: {id:?}");
        assert_eq!(row(&sched, id).stats.poll_count, 2);
    }
}

#[test]
fn the_budget_rule_reclaims_only_batch_and_below_over_their_budget() {
    let mut sched = scheduler();
    let budget = |qos| TaskPolicy { qos, memory_budget_hint: Some(1024), ..TaskPolicy::default() };
    let over_batch = sched.spawn_box(SCRIPTED, Scripted::boxed(2048, &[PENDING]), budget(QoSClass::Batch)).unwrap();
    let over_standard = sched.spawn_box(SCRIPTED, Scripted::boxed(2048, &[PENDING]), budget(QoSClass::Standard)).unwrap();
    let under_background = sched.spawn_box(SCRIPTED, Scripted::boxed(512, &[PENDING]), budget(QoSClass::Background)).unwrap();
    sched.run_once().unwrap();

    assert_eq!(sched.reclaim_log.len(), 1);
    assert_eq!(sched.reclaim_log[0].id, over_batch.id());
    assert_eq!(sched.reclaim_log[0].reason, CheckpointReason::YieldForSla);
    assert!(is_live(&sched, over_standard.id()));
    assert!(is_live(&sched, under_background.id()));
}

#[test]
fn the_default_order_polls_higher_classes_first() {
    let mut sched = scheduler();
    assert_eq!(sched.config.wake_ordering, WakeOrdering::BiasedQoSThenAge);
    clear_order();
    let policy = |qos| TaskPolicy { qos, ..TaskPolicy::default() };
    sched.spawn_box(COUNTER, Counter::boxed("background", 9), policy(QoSClass::Background)).unwrap();
    sched.spawn_box(COUNTER, Counter::boxed("realtime", 9), policy(QoSClass::Realtime)).unwrap();
    sched.spawn_box(COUNTER, Counter::boxed("standard", 9), policy(QoSClass::Standard)).unwrap();
    sched.run_once().unwrap();
    assert_eq!(order(), ["realtime", "standard", "background"]);
}

#[test]
fn the_default_order_polls_the_one_polled_longest_ago_first_within_a_class() {
    let mut sched = scheduler();
    let a = sched.spawn_box(COUNTER, Counter::boxed("a", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    std::thread::sleep(Duration::from_millis(1));
    let b = sched.spawn_box(COUNTER, Counter::boxed("b", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    clear_order();

    // b became runnable first, but a was polled longer ago.
    b.wake(WakeReason::Manual).unwrap();
    a.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap();
    assert_eq!(order(), ["a", "b"]);
}

#[test]
fn round_robin_starts_one_later_each_round() {
    let mut sched = scheduler();
    sched.config = SchedulerConfig { wake_ordering: WakeOrdering::UnbiasedRoundRobin };
    let handles: Vec<_> = ["a", "b", "c"]
        .map(|name| sched.spawn_box(COUNTER, Counter::boxed(name, 100), TaskPolicy::default()).unwrap())
        .into();
    let mut rounds = Vec::new();
    for _ in 0..3 {
        clear_order();
        for h in &handles {
            h.wake(WakeReason::Manual).unwrap();
        }
        sched.run_once().unwrap();
        rounds.push(order());
    }
    assert_eq!(rounds, [["a", "b", "c"], ["b", "c", "a"], ["c", "a", "b"]]);
}

#[test]
fn messages_arrive_in_the_order_they_were_sent() {
    let mut sched = scheduler();
    let h = sched.spawn_box(COUNTER, Counter::boxed("m", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    clear_order();
    for text in ["1", "2", "3"] {
        h.send("note", text, note).unwrap();
    }
    sched.run_once().unwrap();
    assert_eq!(order(), ["1", "2", "3", "m"]);
}

#[test]
fn every_terminal_poll_removes_and_frees_the_task_and_retention_decides_its_checkpoint() {
    let with = |change: fn(&mut RetentionPolicy)| {
        let mut retention = RetentionPolicy::default();
        change(&mut retention);
        retention
    };
    let cases = [
        (READY, with(|_| {}), false),
        (READY, with(|r| r.retain_on_completion = true), true),
        (CAPITULATE, with(|_| {}), false),
        (CAPITULATE, with(|r| r.retain_on_completion = true), true),
        (FAILED, with(|_| {}), true),
        (FAILED, with(|r| r.retain_on_failure = false), false),
        (CANCELLED, with(|_| {}), false),
        (CANCELLED, with(|r| r.retain_on_cancel = true), true),
    ];
    for (code, retention, kept) in cases {
        let mut sched = scheduler();
        let before = drops();
        let policy = TaskPolicy { retention, ..TaskPolicy::default() };
        let h = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[CHECKPOINT, code]), policy).unwrap();
        sched.run_once().unwrap(); // checkpoints itself
        let ckpt = sched.reclaim_log[0].handle.clone().unwrap();
        h.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap(); // restored, then ends with `code`

        assert!(sched.tasks.get(h.id()).is_none(), "code {code}: removed");
        assert_eq!(drops() - before, 2, "code {code}: both copies freed, once each");
        assert_eq!(sched.store.load(&ckpt).is_ok(), kept, "code {code}: checkpoint kept?");
    }
}

#[test]
fn a_dormant_task_keeps_its_checkpoint_even_with_keep_latest_zero() {
    let mut sched = scheduler();
    let policy = TaskPolicy {
        retention: RetentionPolicy { keep_latest: 0, ..RetentionPolicy::default() },
        ..TaskPolicy::default()
    };
    let h = sched.spawn_box(COUNTER, Counter::boxed("z", 100), policy).unwrap();
    sched.run_once().unwrap();
    h.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    assert_eq!(row(&sched, h.id()).checkpoint_history.len(), 1);

    h.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap();
    assert_eq!(row(&sched, h.id()).stats.restore_count, 1);
}

#[test]
fn checkpoints_a_task_asks_for_are_pruned_to_keep_latest() {
    let mut sched = scheduler();
    let h = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[CHECKPOINT]), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    for _ in 0..2 {
        h.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap();
    }
    let handles: Vec<_> = sched.reclaim_log.iter().map(|r| r.handle.clone().unwrap()).collect();
    assert_eq!(handles.len(), 3);
    assert!(sched.store.load(&handles[0]).is_err());
    assert!(sched.store.load(&handles[1]).is_err());
    assert!(sched.store.load(&handles[2]).is_ok());
    assert_eq!(row(&sched, h.id()).checkpoint_history.len(), 1);
}

#[test]
fn a_dormant_task_stays_dormant_when_auto_restore_is_off() {
    let mut sched = scheduler();
    let policy = TaskPolicy { auto_restore_on_wake: false, ..TaskPolicy::default() };
    let h = sched.spawn_box(COUNTER, Counter::boxed("n", 100), policy).unwrap();
    sched.run_once().unwrap();
    h.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    h.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap();

    let entry = row(&sched, h.id());
    assert!(matches!(entry.resident, Resident::Dormant));
    assert_eq!((entry.stats.restore_count, entry.stats.poll_count), (0, 1));
}

#[test]
fn a_restored_task_is_told_it_resumed() {
    let mut sched = scheduler();
    let h = sched.spawn_box(COUNTER, Counter::boxed("r", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    h.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    h.wake(WakeReason::Timer).unwrap();
    sched.run_once().unwrap();

    let entry = row(&sched, h.id());
    assert_eq!(entry.stats.restore_count, 1);
    assert_eq!(entry.stats.last_wake, Some(WakeReason::Resume), "Resume comes after the wake that restored it");
}

#[test]
fn a_reclaim_report_counts_the_bytes_freed() {
    let mut sched = scheduler();
    sched.spawn_box(SCRIPTED, Scripted::boxed(5000, &[CHECKPOINT]), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    let r = &sched.reclaim_log[0];
    let id_size = std::mem::size_of::<TaskId>();
    assert_eq!((r.before_bytes, r.after_bytes, r.reclaimed_bytes), (5000, id_size, 5000 - id_size));
}

/// The whole run of 2^32 reuses that the unit test in src/slab.rs fast-forwards. About 7 s in
/// release: `cargo test --release --test scheduler -- --ignored`.
#[test]
#[ignore = "2^32 slab reuses; run in release with --ignored"]
fn slab_ids_stay_stale_after_every_generation_of_a_slot_is_used() {
    let mut slab = TaskSlab::default();
    let old = slab.insert(0u64);
    slab.remove(old);
    for n in 1..=u64::from(u32::MAX) {
        let id = slab.insert(n);
        slab.remove(id);
    }
    let new = slab.insert(u64::MAX);
    assert_eq!(slab.get(old), None, "old {old:?}, new {new:?}");
}

// ---------------------------------------------------------------------------------------------
// Tasks and strategies for the 0.3.2 tests below.

/// Where a [`Panicky`] task panics.
const IN_POLL: u8 = 0;
const IN_ON_WAKE: u8 = 1;
const IN_CHECKPOINT: u8 = 2;

/// Runs calmly through its first poll and its first checkpoint, then panics in the call
/// `panic_in` names: its next `on_wake`, its second poll, or the checkpoint its second poll asks
/// for.
struct Panicky {
    panic_in: u8,
    polls: u8,
}

impl Panicky {
    fn boxed(panic_in: u8) -> Box<Self> {
        Box::new(Self { panic_in, polls: 0 })
    }
}

impl Drop for Panicky {
    fn drop(&mut self) {
        DROPS.with(|d| d.set(d.get() + 1));
    }
}

// SAFETY: the functions only touch `self`; `restore` panics on short bytes.
unsafe impl ProtocolTask for Panicky {
    unsafe fn poll(&mut self) -> TaskPoll {
        if self.panic_in == IN_POLL && self.polls >= 1 {
            panic!("boom in poll");
        }
        self.polls += 1;
        if self.panic_in == IN_CHECKPOINT && self.polls >= 2 {
            TaskPoll::Checkpoint(CheckpointReason::Manual)
        } else {
            TaskPoll::Pending
        }
    }

    unsafe fn on_wake(&mut self, _: WakeReason) {
        if self.panic_in == IN_ON_WAKE && self.polls >= 1 {
            panic!("boom in on_wake");
        }
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        if self.panic_in == IN_CHECKPOINT && self.polls >= 2 {
            panic!("boom in checkpoint");
        }
        vec![self.panic_in, self.polls]
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        Self { panic_in: bytes[0], polls: bytes[1] }
    }
}

derive_protocol_task_vtable!(Panicky, PANICKY_VTABLE);
const PANICKY: VTableKey = VTableKey { segment: "test", kind: "panicky", version: 1 };

/// Capitulates with its plan before every poll it is asked about.
struct Always(CapitulatePlan);

impl CapitulateStrategy for Always {
    fn plan(&self, _: &TaskSnapshot, _: Option<&WakeReason>) -> Option<CapitulatePlan> {
        Some(self.0.clone())
    }
}

/// Panics whenever the task's newest wake is `QoS`.
struct PanicOnQoS;

impl CapitulateStrategy for PanicOnQoS {
    fn plan(&self, _: &TaskSnapshot, last_wake: Option<&WakeReason>) -> Option<CapitulatePlan> {
        if matches!(last_wake, Some(WakeReason::QoS)) {
            panic!("strategy bug");
        }
        None
    }
}

/// Spawn a [`Panicky`] task, poll it once, checkpoint it, then wake it so it panics in
/// `panic_in`; its first checkpoint.
fn panic_after_a_checkpoint(
    sched: &mut WakerProtocol<MemoryCheckpointStore>,
    panic_in: u8,
    policy: TaskPolicy,
) -> (TaskId, CheckpointHandle) {
    let h = sched.spawn_box(PANICKY, Panicky::boxed(panic_in), policy).unwrap();
    sched.run_once().unwrap();
    h.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    let ckpt = sched.reclaim_log[0].handle.clone().unwrap();
    h.wake(WakeReason::Manual).unwrap();
    sched.run_once().expect("a panic in the task's own call does not reach the caller");
    (h.id(), ckpt)
}

// ---------------------------------------------------------------------------------------------
// 0.3.2: a restored task runs before the strategy sees it again; a dormant task without
// auto-restore stays dormant; a checkpoint the store cannot remove is retried; a panic never
// leaves a row empty.

#[test]
fn the_budget_rule_lets_a_restored_task_poll_before_it_checkpoints_it_again() {
    let mut sched = scheduler();
    let policy = TaskPolicy { qos: QoSClass::Batch, memory_budget_hint: Some(1024), ..TaskPolicy::default() };
    let h = sched.spawn_box(SCRIPTED, Scripted::boxed(2048, &[PENDING, READY]), policy).unwrap();
    sched.run_once().unwrap(); // over budget: checkpointed before its first poll
    assert!(matches!(row(&sched, h.id()).resident, Resident::Dormant));

    h.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap();
    let entry = row(&sched, h.id());
    assert_eq!((entry.stats.restore_count, entry.stats.poll_count), (1, 1), "restored, then polled");
    assert!(is_live(&sched, h.id()));

    // Still over budget: checkpointed again before its next poll, then restored and polled to Ready.
    for _ in 0..2 {
        h.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap();
    }
    assert!(sched.tasks.get(h.id()).is_none(), "it ran to the end");
    let reasons: Vec<_> = sched.reclaim_log.iter().map(|r| r.reason.clone()).collect();
    assert_eq!(reasons, [CheckpointReason::YieldForSla, CheckpointReason::YieldForSla]);
}

#[test]
fn every_strategy_lets_a_restored_task_poll_before_it_is_asked_about_it_again() {
    let plan = CapitulatePlan { reason: CheckpointReason::QoS, retain_stub: true };
    let mut sched = scheduler_with(MemoryCheckpointStore::default(), Box::new(Always(plan)));
    let before = drops();
    let h = sched.spawn_box(COUNTER, Counter::boxed("a", 3), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap(); // checkpointed before its first poll
    for _ in 0..5 {
        h.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap(); // restored and polled, or checkpointed again
    }
    assert!(sched.tasks.get(h.id()).is_none(), "three restores, three polls, and it counted to 3");
    assert_eq!(sched.reclaim_log.len(), 3, "the strategy checkpointed it before each restore");
    assert_eq!(drops() - before, 4, "the spawned copy and three restored copies, each freed once");
}

#[test]
fn a_wake_or_message_to_a_dormant_task_without_auto_restore_is_kept_and_it_stays_dormant() {
    let mut sched = scheduler();
    let policy = TaskPolicy { auto_restore_on_wake: false, ..TaskPolicy::default() };
    let h = sched.spawn_box(COUNTER, Counter::boxed("n", 100), policy).unwrap();
    sched.run_once().unwrap();
    h.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    clear_order();

    h.wake(WakeReason::Timer).unwrap();
    h.send("note", "kept", note).unwrap();
    for _ in 0..3 {
        sched.run_once().unwrap();
    }
    let entry = row(&sched, h.id());
    assert!(matches!(entry.resident, Resident::Dormant));
    assert_eq!(entry.state, TaskState::Dormant, "still Dormant, not Runnable");
    assert_eq!(entry.pending_wakes, [WakeReason::Timer, WakeReason::Inbox], "the wakes are kept");
    assert_eq!(entry.mailbox.len(), 1, "and so is the message");
    assert!(order().is_empty());

    // With auto-restore back on, the kept wakes restore it in the next round.
    sched.tasks.get_mut(h.id()).unwrap().policy.auto_restore_on_wake = true;
    sched.run_once().unwrap();
    assert_eq!(row(&sched, h.id()).stats.restore_count, 1);
    assert_eq!(order(), ["kept", "restored"], "the kept message arrives, then the poll");
}

#[test]
fn a_checkpoint_the_store_cannot_remove_stays_in_the_history_and_is_retried() {
    let mut sched = flaky();
    let h = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[CHECKPOINT]), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap(); // first checkpoint

    sched.store.fail_remove(true);
    h.wake(WakeReason::Manual).unwrap();
    assert_eq!(sched.run_once().unwrap_err(), "remove failed", "pruning the first checkpoint fails");
    let ckpts: Vec<_> = sched.reclaim_log.iter().map(|r| r.handle.clone().unwrap()).collect();
    assert_eq!(row(&sched, h.id()).checkpoint_history, ckpts, "so it stays in the history");
    assert!(sched.store.load(&ckpts[0]).is_ok());

    sched.store.fail_remove(false);
    h.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap(); // third checkpoint: the prune removes the first two
    let ckpts: Vec<_> = sched.reclaim_log.iter().map(|r| r.handle.clone().unwrap()).collect();
    assert_eq!(row(&sched, h.id()).checkpoint_history, [ckpts[2].clone()]);
    for old in &ckpts[..2] {
        assert!(sched.store.load(old).is_err(), "{old:?} is removed");
    }
}

#[test]
fn a_panic_in_a_tasks_poll_on_wake_or_checkpoint_ends_it_as_failed() {
    for panic_in in [IN_POLL, IN_ON_WAKE, IN_CHECKPOINT] {
        for retain_on_failure in [true, false] {
            let mut sched = scheduler();
            let before = drops();
            let retention = RetentionPolicy { retain_on_failure, ..RetentionPolicy::default() };
            let policy = TaskPolicy { retention, ..TaskPolicy::default() };
            let (id, ckpt) = panic_after_a_checkpoint(&mut sched, panic_in, policy);

            assert!(sched.tasks.get(id).is_none(), "{panic_in}: the row is removed");
            assert_eq!(drops() - before, 2, "{panic_in}: the checkpointed and restored copies, each freed once");
            assert_eq!(
                sched.store.load(&ckpt).is_ok(),
                retain_on_failure,
                "{panic_in}: the checkpoint follows retain_on_failure, as for any Failed task"
            );
        }
    }
}

#[test]
fn a_panic_from_the_store_reaches_the_caller_with_the_task_live_in_its_row() {
    let mut sched = flaky();
    let before = drops();
    let h = sched.spawn_box(COUNTER, Counter::boxed("s", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();

    sched.store.panic_on_save(true);
    h.checkpoint(CheckpointReason::Manual).unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| sched.run_once())).is_err(), "the store's panic reaches the caller");
    assert_eq!(drops() - before, 0, "the task is not freed");
    assert!(is_live(&sched, h.id()), "it is back in its row, live");

    sched.store.panic_on_save(false);
    h.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    assert!(matches!(row(&sched, h.id()).resident, Resident::Dormant), "the scheduler goes on");
    h.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap();
    assert_eq!(row(&sched, h.id()).stats.poll_count, 2);
}

#[test]
fn a_panic_from_the_strategy_reaches_the_caller_with_the_task_live_in_its_row() {
    let mut sched = scheduler_with(MemoryCheckpointStore::default(), Box::new(PanicOnQoS));
    let before = drops();
    let h = sched.spawn_box(COUNTER, Counter::boxed("q", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();

    h.wake(WakeReason::QoS).unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| sched.run_once())).is_err(), "the strategy's panic reaches the caller");
    assert_eq!(drops() - before, 0, "the task is not freed");
    assert!(is_live(&sched, h.id()), "it is back in its row, live");

    h.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap();
    assert_eq!(row(&sched, h.id()).stats.poll_count, 2, "the scheduler goes on");
}

#[test]
fn every_removed_task_leaves_its_final_state_in_finished() {
    let mut sched = scheduler();
    let failed = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[FAILED]), TaskPolicy::default()).unwrap();
    let done = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[READY]), TaskPolicy::default()).unwrap();
    let cancelled = sched.spawn_box(COUNTER, Counter::boxed("c", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    cancelled.cancel().unwrap();
    sched.run_once().unwrap();

    assert_eq!(
        sched.finished,
        [
            (failed.id(), TaskState::Failed("scripted failure".into())),
            (done.id(), TaskState::Completed),
            (cancelled.id(), TaskState::Cancelled),
        ],
        "in the order they were removed, the Failed message included"
    );
    assert!(sched.tasks.live_ids().is_empty());
}

#[test]
fn a_task_that_panics_is_recorded_as_failed_with_a_message_naming_the_panic() {
    for (panic_in, call) in [(IN_POLL, "poll"), (IN_ON_WAKE, "on_wake"), (IN_CHECKPOINT, "checkpoint")] {
        let mut sched = scheduler();
        let (id, _) = panic_after_a_checkpoint(&mut sched, panic_in, TaskPolicy::default());
        let message = format!("task panicked in {call}: boom in {call}");
        assert_eq!(sched.finished, [(id, TaskState::Failed(message))]);
    }
}
