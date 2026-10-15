//! The scheduler end to end. Tasks count their drops in a thread-local, so a task freed twice
//! or never fails here, and Miri reports any leak or bad read.

use std::any::TypeId;
use std::cell::{Cell, RefCell};
use std::panic::{catch_unwind, panic_any, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Context;
use std::time::Duration;

use waker_protocol::{
    derive_protocol_task_vtable, scheduler_waker, typed_envelope, CapitulatePlan, CapitulateStrategy,
    CheckpointHandle, Envelope, CheckpointReason, CheckpointStore, DefaultCapitulateStrategy,
    MemoryCheckpointStore, ProtocolTask, ProtocolTaskPtr, ProtocolVTable, QoSClass, Resident,
    RetentionPolicy, SchedulerConfig, SchedulerMsg, TaskEntry, TaskId, TaskPoll, TaskPolicy, TaskSlab,
    TaskSnapshot, TaskState, WakeOrdering, WakeReason, WakerProtocol,
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
    sched.register_vtable(FRAGILE, &FRAGILE_VTABLE);
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

/// A checkpoint store whose `save` fails or panics, whose `load` panics, or whose `remove` fails
/// or panics, while told to; and whose `remove` always fails for one handle it is told is stuck.
#[derive(Default)]
struct FlakyStore {
    inner: MemoryCheckpointStore,
    failing: AtomicBool,
    panicking: AtomicBool,
    panicking_load: AtomicBool,
    failing_remove: AtomicBool,
    panicking_remove: AtomicBool,
    stuck: Mutex<Option<CheckpointHandle>>,
}

impl FlakyStore {
    fn fail(&self, on: bool) {
        self.failing.store(on, Ordering::SeqCst);
    }

    fn panic_on_save(&self, on: bool) {
        self.panicking.store(on, Ordering::SeqCst);
    }

    fn panic_on_load(&self, on: bool) {
        self.panicking_load.store(on, Ordering::SeqCst);
    }

    fn fail_remove(&self, on: bool) {
        self.failing_remove.store(on, Ordering::SeqCst);
    }

    fn panic_on_remove(&self, on: bool) {
        self.panicking_remove.store(on, Ordering::SeqCst);
    }

    /// From now on `remove` fails for `handle`, and only for it.
    fn stick(&self, handle: CheckpointHandle) {
        *self.stuck.lock().unwrap() = Some(handle);
    }

    /// Whether the bytes behind `handle` are still in the store.
    fn holds(&self, handle: &CheckpointHandle) -> bool {
        self.inner.load(handle).is_ok()
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
        if self.panicking_load.load(Ordering::SeqCst) {
            panic!("store bug in load");
        }
        self.inner.load(handle)
    }

    fn remove(&self, handle: &CheckpointHandle) -> Result<(), String> {
        ORDER.with(|o| o.borrow_mut().push("store.remove"));
        if self.panicking_remove.load(Ordering::SeqCst) {
            panic!("store bug in remove");
        }
        if self.failing_remove.load(Ordering::SeqCst) || self.stuck.lock().unwrap().as_ref() == Some(handle) {
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
fn the_idle_rule_checkpoints_a_task_that_yielded_and_the_next_round_brings_it_back() {
    let mut sched = scheduler_with(MemoryCheckpointStore::default(), Box::new(eager_default_strategy()));
    let h = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[YIELD]), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap(); // polled; Yield keeps it runnable
    sched.run_once().unwrap(); // runnable with nothing waiting: idle
    assert_eq!(sched.reclaim_log.len(), 1);
    assert_eq!(sched.reclaim_log[0].reason, CheckpointReason::Idle);
    assert!(matches!(row(&sched, h.id()).resident, Resident::Dormant));

    // Its Yield asked for another poll, so it is still runnable: no wake is needed.
    sched.run_once().unwrap();
    let entry = row(&sched, h.id());
    assert_eq!(entry.stats.restore_count, 1);
    assert_eq!(entry.stats.poll_count, 2, "restored and polled");
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
const IN_RESTORE: u8 = 3;
const IN_FOOTPRINT: u8 = 4;

/// A panic payload whose own drop panics.
struct Bomb;

impl Drop for Bomb {
    fn drop(&mut self) {
        panic!("the payload's drop panicked");
    }
}

/// Panic in `call`: with text naming it, or with a [`Bomb`].
fn boom(call: &str, bomb: bool) -> ! {
    if bomb {
        panic_any(Bomb)
    } else {
        panic!("boom in {call}")
    }
}

/// Runs calmly through its first poll and its first checkpoint, then panics in the call
/// `panic_in` names: its next `on_wake`, its second poll, the checkpoint its second poll asks
/// for, any restore, or `footprint_estimate` once it has been polled. A `bomb` task panics with
/// a [`Bomb`] instead of text.
struct Panicky {
    panic_in: u8,
    polls: u8,
    bomb: bool,
}

impl Panicky {
    fn boxed(panic_in: u8) -> Box<Self> {
        Box::new(Self { panic_in, polls: 0, bomb: false })
    }

    fn bomb(panic_in: u8) -> Box<Self> {
        Box::new(Self { panic_in, polls: 0, bomb: true })
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
            boom("poll", self.bomb);
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
            boom("on_wake", self.bomb);
        }
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        if self.panic_in == IN_CHECKPOINT && self.polls >= 2 {
            boom("checkpoint", self.bomb);
        }
        vec![self.panic_in, self.polls, u8::from(self.bomb)]
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        let (panic_in, polls, bomb) = (bytes[0], bytes[1], bytes[2] != 0);
        if panic_in == IN_RESTORE {
            boom("restore", bomb);
        }
        Self { panic_in, polls, bomb }
    }

    fn footprint_estimate(&self) -> usize {
        if self.panic_in == IN_FOOTPRINT && self.polls >= 1 {
            boom("footprint_estimate", self.bomb);
        }
        std::mem::size_of_val(self)
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

/// Spawn a [`Panicky`] task, poll it once, checkpoint it, then wake it so it panics; its first
/// checkpoint.
fn panic_after_a_checkpoint(
    sched: &mut WakerProtocol<MemoryCheckpointStore>,
    task: Box<Panicky>,
    policy: TaskPolicy,
) -> (TaskId, CheckpointHandle) {
    let h = sched.spawn_box(PANICKY, task, policy).unwrap();
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
    let h = sched.spawn_box(SCRIPTED, Scripted::boxed(2048, &[PENDING, PENDING, READY]), policy).unwrap();
    sched.run_once().unwrap(); // over budget: checkpointed in place of its first poll
    assert!(matches!(row(&sched, h.id()).resident, Resident::Dormant));

    // Spawned, it was due that poll, so the next round restores and polls it; no wake is needed.
    sched.run_once().unwrap();
    let entry = row(&sched, h.id());
    assert_eq!((entry.stats.restore_count, entry.stats.poll_count), (1, 1), "restored, then polled");
    assert!(is_live(&sched, h.id()));

    // Still over budget: each wake checkpoints it in place of a poll, and the round after that
    // restores and polls it. One wake, one poll.
    for polls in 2..=3 {
        h.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap();
        sched.run_once().unwrap();
        match sched.tasks.get(h.id()) {
            Some(entry) => assert_eq!(entry.stats.poll_count, polls, "one wake, one poll"),
            None => assert_eq!(polls, 3, "Ready on its third poll"),
        }
    }
    assert!(sched.tasks.get(h.id()).is_none(), "it ran to the end");
    let reasons: Vec<_> = sched.reclaim_log.iter().map(|r| r.reason.clone()).collect();
    assert_eq!(reasons, vec![CheckpointReason::YieldForSla; 3]);
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
            let (id, ckpt) = panic_after_a_checkpoint(&mut sched, Panicky::boxed(panic_in), policy);

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
        let (id, _) = panic_after_a_checkpoint(&mut sched, Panicky::boxed(panic_in), TaskPolicy::default());
        let message = format!("task panicked in {call}: boom in {call}");
        assert_eq!(sched.finished, [(id, TaskState::Failed(message))]);
    }
}

// ---------------------------------------------------------------------------------------------
// 0.3.2, second review: every panic from the task's own code is caught, even one whose payload
// panics when dropped; a strategy checkpoint puts a poll off but never drops it; a stuck
// checkpoint never keeps newer ones from being pruned; the docs say what a store panic leaves.

#[test]
fn a_panic_whose_payload_panics_when_dropped_still_ends_the_task_as_failed() {
    for (panic_in, call) in [(IN_POLL, "poll"), (IN_ON_WAKE, "on_wake"), (IN_CHECKPOINT, "checkpoint")] {
        let mut sched = scheduler();
        let before = drops();
        let (id, _) = catch_unwind(AssertUnwindSafe(|| {
            panic_after_a_checkpoint(&mut sched, Panicky::bomb(panic_in), TaskPolicy::default())
        }))
        .unwrap_or_else(|_| panic!("{call}: the payload's drop panicked out of run_once"));

        assert!(sched.tasks.get(id).is_none(), "{call}: the row is removed, never polled again");
        let message = format!("task panicked in {call}: a panic payload that is not text");
        assert_eq!(sched.finished, [(id, TaskState::Failed(message))], "{call}");
        assert_eq!(drops() - before, 2, "{call}: the checkpointed and restored copies, each freed once");
    }
}

#[test]
fn a_panic_in_restore_ends_the_task_as_failed_and_the_round_goes_on() {
    for (task, text) in [(Panicky::boxed(IN_RESTORE), "boom in restore"), (Panicky::bomb(IN_RESTORE), "a panic payload that is not text")] {
        let mut sched = scheduler();
        let before = drops();
        let h = sched.spawn_box(PANICKY, task, TaskPolicy::default()).unwrap();
        let background = TaskPolicy { qos: QoSClass::Background, ..TaskPolicy::default() };
        let after = sched.spawn_box(COUNTER, Counter::boxed("after", 100), background).unwrap();
        sched.run_once().unwrap();
        h.checkpoint(CheckpointReason::Manual).unwrap();
        sched.run_once().unwrap();
        let ckpt = sched.reclaim_log[0].handle.clone().unwrap();

        h.wake(WakeReason::Manual).unwrap();
        after.wake(WakeReason::Manual).unwrap();
        let round = catch_unwind(AssertUnwindSafe(|| sched.run_once()));
        assert!(matches!(round, Ok(Ok(()))), "{text}: the restore's panic does not reach the caller");
        assert!(sched.tasks.get(h.id()).is_none(), "{text}: its row is removed");
        let message = format!("task panicked in restore: {text}");
        assert_eq!(sched.finished, [(h.id(), TaskState::Failed(message))]);
        assert_eq!(row(&sched, after.id()).stats.poll_count, 2, "{text}: the task after it is polled in that round");
        assert!(sched.store.load(&ckpt).is_ok(), "{text}: retain_on_failure keeps the checkpoint that would not restore");
        assert_eq!(drops() - before, 1, "{text}: the checkpointed copy; no restored copy was built");
    }
}

#[test]
fn a_panic_in_footprint_estimate_before_a_poll_ends_the_task_and_the_others_keep_running() {
    let mut sched = scheduler();
    let before = drops();
    let h = sched.spawn_box(PANICKY, Panicky::boxed(IN_FOOTPRINT), TaskPolicy::default()).unwrap();
    let background = TaskPolicy { qos: QoSClass::Background, ..TaskPolicy::default() };
    let after = sched.spawn_box(COUNTER, Counter::boxed("after", 100), background).unwrap();
    sched.run_once().unwrap(); // both polled; from now on the first one's footprint_estimate panics

    let rounds: Vec<bool> = (0..5)
        .map(|_| {
            h.wake(WakeReason::Manual).unwrap();
            after.wake(WakeReason::Manual).unwrap();
            matches!(catch_unwind(AssertUnwindSafe(|| sched.run_once())), Ok(Ok(())))
        })
        .collect();
    assert_eq!(rounds, [true; 5], "no round panics or fails");
    assert_eq!(row(&sched, after.id()).stats.poll_count, 6, "the task after it is polled every round");
    assert!(sched.tasks.get(h.id()).is_none());
    let message = "task panicked in footprint_estimate: boom in footprint_estimate".to_string();
    assert_eq!(sched.finished, [(h.id(), TaskState::Failed(message))]);
    assert_eq!(drops() - before, 1, "the panicking task is freed once");
}

#[test]
fn a_panic_in_footprint_estimate_during_a_checkpoint_ends_the_task_as_failed() {
    let mut sched = scheduler();
    let before = drops();
    let h = sched.spawn_box(PANICKY, Panicky::boxed(IN_FOOTPRINT), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();

    h.checkpoint(CheckpointReason::Manual).unwrap();
    let round = catch_unwind(AssertUnwindSafe(|| sched.run_once()));
    assert!(matches!(round, Ok(Ok(()))), "the panic does not reach the caller");
    assert!(sched.tasks.get(h.id()).is_none());
    let message = "task panicked in footprint_estimate: boom in footprint_estimate".to_string();
    assert_eq!(sched.finished, [(h.id(), TaskState::Failed(message))]);
    assert!(sched.reclaim_log.is_empty(), "nothing was saved");
    assert_eq!(drops() - before, 1);
}

/// Batch, with a budget of 0 bytes: any task is over it, so the default strategy checkpoints it
/// whenever it is asked.
fn over_budget() -> TaskPolicy {
    TaskPolicy { qos: QoSClass::Batch, memory_budget_hint: Some(0), ..TaskPolicy::default() }
}

#[test]
fn every_message_to_an_over_budget_task_gets_a_poll() {
    let mut sched = scheduler();
    let h = sched.spawn_box(COUNTER, Counter::boxed("w", 100), over_budget()).unwrap();
    sched.run_once().unwrap();
    sched.run_once().unwrap();

    let mut polls = Vec::new();
    let mut orders = Vec::new();
    for _ in 0..4 {
        clear_order();
        h.send("note", "job", note).unwrap();
        for _ in 0..3 {
            sched.run_once().unwrap();
        }
        polls.push(row(&sched, h.id()).stats.poll_count);
        orders.push(order());
    }
    assert_eq!(polls, [2, 3, 4, 5], "the spawn and each message got one poll");
    assert_eq!(orders, [["job", "restored"]; 4], "each message is delivered, then a poll acts on it");
}

#[test]
fn an_over_budget_task_that_yields_runs_to_the_end_without_a_wake() {
    let mut sched = scheduler();
    let h = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[YIELD, YIELD, YIELD, READY]), over_budget()).unwrap();
    for _ in 0..20 {
        sched.run_once().unwrap();
    }
    assert!(sched.tasks.get(h.id()).is_none(), "every Yield got its next poll");
    assert_eq!(sched.finished, [(h.id(), TaskState::Completed)]);
}

#[test]
fn a_task_woken_only_by_qos_or_memory_pressure_stays_dormant_after_the_strategy_checkpoints_it() {
    let plan = CapitulatePlan { reason: CheckpointReason::QoS, retain_stub: true };
    let mut sched = scheduler_with(MemoryCheckpointStore::default(), Box::new(OnQoS(plan)));
    let by_qos = sched.spawn_box(SCRIPTED, Scripted::boxed(8192, &[PENDING]), TaskPolicy::default()).unwrap();
    let mut default = scheduler();
    let by_pressure = default.spawn_box(SCRIPTED, Scripted::boxed(8192, &[PENDING]), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    default.run_once().unwrap();

    by_qos.wake(WakeReason::QoS).unwrap();
    by_pressure.wake(WakeReason::MemoryPressure).unwrap();
    for _ in 0..3 {
        sched.run_once().unwrap();
        default.run_once().unwrap();
    }
    for entry in [row(&sched, by_qos.id()), row(&default, by_pressure.id())] {
        assert!(matches!(entry.resident, Resident::Dormant), "the checkpoint answered the wake");
        assert_eq!(entry.state, TaskState::Dormant);
        assert_eq!((entry.stats.restore_count, entry.stats.poll_count), (0, 1), "not restored for it");
    }
}

#[test]
fn a_stuck_checkpoint_never_keeps_newer_ones_from_being_pruned() {
    let mut sched = flaky();
    let h = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[CHECKPOINT]), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap(); // first checkpoint
    sched.store.stick(sched.reclaim_log[0].handle.clone().unwrap());

    for _ in 0..3 {
        h.wake(WakeReason::Manual).unwrap();
        assert_eq!(sched.run_once().unwrap_err(), "remove failed", "the stuck one fails again");
    }
    let ckpts: Vec<_> = sched.reclaim_log.iter().map(|r| r.handle.clone().unwrap()).collect();
    assert_eq!(row(&sched, h.id()).checkpoint_history, [ckpts[0].clone(), ckpts[3].clone()], "the stuck one and the latest");
    let held: Vec<bool> = ckpts.iter().map(|c| sched.store.holds(c)).collect();
    assert_eq!(held, [true, false, false, true], "every checkpoint between them is removed");
}

#[test]
fn a_panic_from_the_stores_load_or_remove_reaches_the_caller_and_leaves_the_task_as_the_docs_say() {
    // load: the task stays dormant.
    let mut sched = flaky();
    let h = sched.spawn_box(COUNTER, Counter::boxed("l", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    h.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    sched.store.panic_on_load(true);
    h.wake(WakeReason::Manual).unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| sched.run_once())).is_err(), "load's panic reaches the caller");
    assert!(matches!(row(&sched, h.id()).resident, Resident::Dormant), "the task stays dormant");
    sched.store.panic_on_load(false);
    sched.run_once().unwrap();
    assert!(is_live(&sched, h.id()), "and is restored once load works");

    // remove, while a checkpoint prunes: the task is already dormant, and the checkpoint not yet
    // removed stays in its history.
    let before = drops();
    sched.store.panic_on_remove(true);
    h.checkpoint(CheckpointReason::Manual).unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| sched.run_once())).is_err(), "remove's panic reaches the caller");
    let entry = row(&sched, h.id());
    assert!(matches!(entry.resident, Resident::Dormant), "the task is already dormant");
    assert_eq!(drops() - before, 1, "its live copy is freed once");
    let ckpts: Vec<_> = sched.reclaim_log.iter().map(|r| r.handle.clone().unwrap()).collect();
    assert_eq!(entry.checkpoint_history, ckpts, "the older checkpoint is still in its history");

    // remove, at the task's end: the task is already removed and freed; the checkpoints not yet
    // removed stay in the store.
    sched.store.panic_on_remove(false);
    h.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap(); // restored and polled
    let before = drops();
    sched.store.panic_on_remove(true);
    h.cancel().unwrap();
    assert!(catch_unwind(AssertUnwindSafe(|| sched.run_once())).is_err(), "remove's panic reaches the caller");
    assert!(sched.tasks.get(h.id()).is_none(), "the task is already removed");
    assert_eq!(drops() - before, 1, "and freed once");
    assert_eq!(sched.finished, [(h.id(), TaskState::Cancelled)]);
    assert!(ckpts.iter().all(|c| sched.store.holds(c)), "its checkpoints are left in the store");
}

#[test]
fn the_strategy_is_never_shown_a_task_just_restored() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let strategy = Recording { seen: seen.clone(), inner: DefaultCapitulateStrategy::default() };
    let mut sched = scheduler_with(MemoryCheckpointStore::default(), Box::new(strategy));
    let h = sched.spawn_box(COUNTER, Counter::boxed("r", 100), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap(); // asked: spawned
    for _ in 0..3 {
        h.checkpoint(CheckpointReason::Manual).unwrap();
        sched.run_once().unwrap();
        h.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap(); // restored and polled: not asked
        h.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap(); // asked
    }
    let entry = row(&sched, h.id());
    assert_eq!((entry.stats.restore_count, entry.stats.poll_count), (3, 7));
    let pending: Vec<_> = seen.lock().unwrap().iter().map(|s| s.pending_wakes).collect();
    assert_eq!(pending, [1, 1, 1, 1], "asked for 4 of the 7 polls, never with a restore's Resume counted");
}

// ---------------------------------------------------------------------------------------------
// 0.3.2, third review: a panic in a message's `deliver`, or in a task's `cleanup` or `Drop`, is
// caught and is an error for the round; the store's `remove` is called only once the task is freed.

/// Where a [`Fragile`] task panics as it is freed: in `cleanup`, in its own drop, or both.
const IN_CLEANUP: u8 = 1;
const IN_DROP: u8 = 2;

/// Gives the poll `poll` (a [`Scripted`] code: READY, PENDING or CHECKPOINT) every time, and
/// panics as it is freed where `panic_in` says. Writes "cleanup" and "drop" to ORDER.
struct Fragile {
    poll: u8,
    panic_in: u8,
}

impl Fragile {
    fn boxed(poll: u8, panic_in: u8) -> Box<Self> {
        Box::new(Self { poll, panic_in })
    }
}

/// A message `deliver` that sets where a [`Fragile`] task panics as it is freed.
fn arm(task: &mut Fragile, panic_in: u8) -> Result<(), String> {
    task.panic_in = panic_in;
    Ok(())
}

impl Drop for Fragile {
    fn drop(&mut self) {
        DROPS.with(|d| d.set(d.get() + 1));
        ORDER.with(|o| o.borrow_mut().push("drop"));
        if self.panic_in & IN_DROP != 0 {
            panic!("boom in drop");
        }
    }
}

// SAFETY: the functions only touch `self`; `restore` panics on short bytes.
unsafe impl ProtocolTask for Fragile {
    unsafe fn poll(&mut self) -> TaskPoll {
        match self.poll {
            READY => TaskPoll::Ready,
            CHECKPOINT => TaskPoll::Checkpoint(CheckpointReason::Manual),
            _ => TaskPoll::Pending,
        }
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        vec![self.poll, self.panic_in]
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        Self { poll: bytes[0], panic_in: bytes[1] }
    }

    unsafe fn cleanup(&mut self) {
        ORDER.with(|o| o.borrow_mut().push("cleanup"));
        if self.panic_in & IN_CLEANUP != 0 {
            panic!("boom in cleanup");
        }
    }
}

derive_protocol_task_vtable!(Fragile, FRAGILE_VTABLE);
const FRAGILE: VTableKey = VTableKey { segment: "test", kind: "fragile", version: 1 };

/// A message whose drop panics. Writes "message drop" to ORDER.
struct Grenade;

impl Drop for Grenade {
    fn drop(&mut self) {
        ORDER.with(|o| o.borrow_mut().push("message drop"));
        panic!("boom in a message's drop");
    }
}

#[test]
fn a_panic_in_a_messages_deliver_drops_only_that_message_and_the_round_goes_on() {
    let mut sched = scheduler();
    let before = drops();
    let h = sched.spawn_box(COUNTER, Counter::boxed("d", 100), TaskPolicy::default()).unwrap();
    let background = TaskPolicy { qos: QoSClass::Background, ..TaskPolicy::default() };
    let after = sched.spawn_box(COUNTER, Counter::boxed("after", 100), background).unwrap();
    sched.run_once().unwrap();
    clear_order();

    // An error noted earlier in the round, which the panic must not lose.
    let wrong = typed_envelope::<Other, u64>("wrong type", 1, |_, _| Ok(()));
    sched.sender().send(SchedulerMsg::Deliver { id: h.id(), envelope: wrong }).unwrap();
    h.send("first", "first", note).unwrap();
    h.send("panics", "panics", |_, _| panic!("boom in deliver")).unwrap();
    h.send("last", "last", note).unwrap();
    after.wake(WakeReason::Manual).unwrap();
    let round = catch_unwind(AssertUnwindSafe(|| sched.run_once()));

    let Ok(Err(err)) = round else { panic!("the deliver's panic reached the caller, or nothing was returned") };
    let errors: Vec<&str> = err.split("; ").collect();
    assert_eq!(errors.len(), 2, "{err}");
    assert!(errors[0].starts_with("envelope \"wrong type\""), "the earlier error is kept: {err}");
    assert_eq!(errors[1], format!("message \"panics\" for task {:?} panicked: boom in deliver", h.id()));
    assert_eq!(order(), ["first", "last", "d", "after"], "the other messages arrive, the task and the one after it are polled");
    assert!(is_live(&sched, h.id()), "the task stays live in its row");
    assert_eq!(drops() - before, 0, "nothing is freed");
}

#[test]
fn a_panic_in_cleanup_or_drop_is_an_error_for_the_round_and_the_round_goes_on() {
    for panic_in in [IN_CLEANUP, IN_DROP, IN_CLEANUP | IN_DROP] {
        for poll in [READY, CHECKPOINT] {
            let case = format!("panic_in {panic_in}, poll {poll}");
            let mut sched = scheduler();
            let before = drops();
            let h = sched.spawn_box(FRAGILE, Fragile::boxed(poll, panic_in), TaskPolicy::default()).unwrap();
            let background = TaskPolicy { qos: QoSClass::Background, ..TaskPolicy::default() };
            let after = sched.spawn_box(COUNTER, Counter::boxed("after", 100), background).unwrap();
            let round = catch_unwind(AssertUnwindSafe(|| sched.run_once()));

            let text = if panic_in & IN_CLEANUP != 0 { "boom in cleanup" } else { "boom in drop" };
            let expected = format!("task {:?} panicked in cleanup or drop: {text}", h.id());
            assert!(matches!(&round, Ok(Err(err)) if *err == expected), "{case}: {round:?}");
            assert_eq!(row(&sched, after.id()).stats.poll_count, 1, "{case}: the task after it is polled in that round");
            assert_eq!(drops() - before, 1, "{case}: the task is freed once");
            if poll == READY {
                assert!(sched.tasks.get(h.id()).is_none(), "{case}: its row is removed");
                assert_eq!(sched.finished, [(h.id(), TaskState::Completed)], "{case}: it ended as it said");
            } else {
                let entry = row(&sched, h.id());
                assert!(matches!(entry.resident, Resident::Dormant), "{case}: it is dormant");
                assert_eq!(entry.checkpoint_history.len(), 1, "{case}: with its checkpoint");
            }
        }
    }
}

#[test]
fn the_store_is_asked_to_remove_checkpoints_only_once_the_task_is_freed() {
    // A task's end: the row goes, the task is freed, then its checkpoint is removed.
    let mut sched = flaky();
    let h = sched.spawn_box(FRAGILE, Fragile::boxed(PENDING, 0), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    h.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    h.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap(); // restored and polled
    clear_order();
    h.cancel().unwrap();
    sched.run_once().unwrap();
    assert_eq!(order(), ["cleanup", "drop", "store.remove"], "the task's end");

    // A checkpoint: the row goes dormant, the live copy is freed, then the older one is pruned.
    let h = sched.spawn_box(FRAGILE, Fragile::boxed(CHECKPOINT, 0), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap(); // first checkpoint
    h.wake(WakeReason::Manual).unwrap();
    clear_order();
    sched.run_once().unwrap(); // restored, polled, checkpointed again; the first is pruned
    assert_eq!(order(), ["cleanup", "drop", "store.remove"], "a checkpoint");
}

#[test]
fn a_panic_from_the_stores_remove_at_a_tasks_end_comes_after_the_task_and_its_messages_are_freed() {
    for live in [true, false] {
        let mut sched = flaky();
        let before = drops();
        let h = sched.spawn_box(FRAGILE, Fragile::boxed(PENDING, 0), TaskPolicy::default()).unwrap();
        sched.run_once().unwrap();
        h.checkpoint(CheckpointReason::Manual).unwrap();
        sched.run_once().unwrap(); // dormant, one checkpoint
        if live {
            h.send("arm", IN_CLEANUP, arm).unwrap();
            sched.run_once().unwrap(); // restored, armed and polled
        }

        // The task's cleanup panics (when live) and a queued message's drop panics; with a
        // store whose `remove` fails, each is an error of the round.
        sched.store.fail_remove(true);
        h.send("grenade", Grenade, |_: &mut Fragile, _| Ok(())).unwrap();
        h.cancel().unwrap();
        clear_order();
        let err = sched.run_once().unwrap_err();
        let mut expected = Vec::new();
        if live {
            expected.push(format!("task {:?} panicked in cleanup or drop: boom in cleanup", h.id()));
        }
        expected.push(format!("message \"grenade\" for task {:?} panicked as it was dropped: boom in a message's drop", h.id()));
        expected.push("remove failed".to_string());
        assert_eq!(err, expected.join("; "), "live {live}");
        assert_eq!(sched.finished, [(h.id(), TaskState::Cancelled)], "live {live}");
        let freed: &[&str] = if live { &["cleanup", "drop", "message drop"] } else { &["message drop"] };
        assert_eq!(order()[..freed.len()], *freed, "live {live}: freed before the store is asked");
        assert_eq!(order().last(), Some(&"store.remove"), "live {live}");
        assert_eq!(drops() - before, if live { 2 } else { 1 }, "live {live}: every copy freed once");
    }

    // The same with a store whose `remove` panics: its panic reaches the caller, after the frees,
    // and the process does not abort.
    for live in [true, false] {
        let mut sched = flaky();
        let before = drops();
        let h = sched.spawn_box(FRAGILE, Fragile::boxed(PENDING, 0), TaskPolicy::default()).unwrap();
        sched.run_once().unwrap();
        h.checkpoint(CheckpointReason::Manual).unwrap();
        sched.run_once().unwrap();
        if live {
            h.send("arm", IN_CLEANUP | IN_DROP, arm).unwrap();
            sched.run_once().unwrap();
        }

        sched.store.panic_on_remove(true);
        h.send("grenade", Grenade, |_: &mut Fragile, _| Ok(())).unwrap();
        h.cancel().unwrap();
        clear_order();
        assert!(catch_unwind(AssertUnwindSafe(|| sched.run_once())).is_err(), "live {live}: remove's panic reaches the caller");
        let freed: &[&str] = if live { &["cleanup", "drop", "message drop", "store.remove"] } else { &["message drop", "store.remove"] };
        assert_eq!(order(), freed, "live {live}: everything is freed before the store is asked");
        assert!(sched.tasks.get(h.id()).is_none(), "live {live}: the row is removed");
        assert_eq!(sched.finished, [(h.id(), TaskState::Cancelled)], "live {live}");
        assert_eq!(drops() - before, if live { 2 } else { 1 }, "live {live}: every copy freed once");
    }
}

#[test]
fn a_panic_from_the_stores_remove_while_a_checkpoint_prunes_comes_after_the_live_copy_is_freed() {
    let mut sched = flaky();
    let before = drops();
    let h = sched.spawn_box(FRAGILE, Fragile::boxed(PENDING, 0), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    h.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap(); // first checkpoint
    h.send("arm", IN_CLEANUP | IN_DROP, arm).unwrap();
    sched.run_once().unwrap(); // restored, armed and polled

    sched.store.panic_on_remove(true);
    h.checkpoint(CheckpointReason::Manual).unwrap();
    clear_order();
    assert!(catch_unwind(AssertUnwindSafe(|| sched.run_once())).is_err(), "remove's panic reaches the caller");
    assert_eq!(order(), ["cleanup", "drop", "store.remove"], "the live copy is freed before the store is asked");
    let ckpts: Vec<_> = sched.reclaim_log.iter().map(|r| r.handle.clone().unwrap()).collect();
    let entry = row(&sched, h.id());
    assert!(matches!(entry.resident, Resident::Dormant), "the task is dormant");
    assert_eq!(entry.checkpoint_history, ckpts, "the checkpoint not yet removed stays in its history");
    assert_eq!(drops() - before, 2, "both copies freed once");

    // A later prune tries it again.
    sched.store.panic_on_remove(false);
    h.send("arm", 0, arm).unwrap();
    sched.run_once().unwrap(); // restored, disarmed and polled
    h.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    let latest = sched.reclaim_log.last().unwrap().handle.clone().unwrap();
    assert_eq!(row(&sched, h.id()).checkpoint_history, [latest]);
    assert!(ckpts.iter().all(|c| !sched.store.holds(c)), "the older checkpoints are removed");
}

/// Proves the `memory_budget_hint` doc: every message, every wake but `QoS` and
/// `MemoryPressure`, and every `Yield` gets a poll; a `QoS` or `MemoryPressure` wake reaches
/// `on_wake` and gets none.
#[test]
fn an_over_budget_task_gets_no_poll_for_a_qos_or_memory_pressure_wake_and_one_for_any_other() {
    for reason in [WakeReason::QoS, WakeReason::MemoryPressure, WakeReason::Manual] {
        let mut sched = scheduler();
        let h = sched.spawn_box(COUNTER, Counter::boxed("b", 100), over_budget()).unwrap();
        sched.run_once().unwrap(); // checkpointed in place of its first poll
        sched.run_once().unwrap(); // restored and polled for its spawn
        h.wake(reason.clone()).unwrap();
        for _ in 0..20 {
            sched.run_once().unwrap();
        }
        let entry = row(&sched, h.id());
        let gets_a_poll = reason == WakeReason::Manual;
        assert_eq!(entry.stats.poll_count, if gets_a_poll { 2 } else { 1 }, "{reason:?}");
        assert_eq!(entry.state, if gets_a_poll { TaskState::Waiting } else { TaskState::Dormant }, "{reason:?}");
        assert_eq!(entry.stats.last_wake, Some(if gets_a_poll { WakeReason::Resume } else { reason.clone() }), "{reason:?}: on_wake heard it");
    }
}

// ---------------------------------------------------------------------------------------------
// 0.3.2, fourth review: a panic in a message's `kind` or `target_type` is caught with the message
// only borrowed, and a message for a task that has already ended is dropped under the same rule.

/// Where a [`Faulty`] message panics.
const IN_KIND: u8 = 1;
const IN_TARGET: u8 = 2;
const IN_MESSAGE_DROP: u8 = 4;

/// A message written by hand for a [`Fragile`] task, whose `kind`, `target_type` or drop panics
/// where `panic_in` says. Writes "faulty delivered" and "faulty drop" to ORDER.
struct Faulty {
    panic_in: u8,
}

impl Faulty {
    fn boxed(panic_in: u8) -> Box<Self> {
        Box::new(Self { panic_in })
    }
}

impl Drop for Faulty {
    fn drop(&mut self) {
        ORDER.with(|o| o.borrow_mut().push("faulty drop"));
        if self.panic_in & IN_MESSAGE_DROP != 0 {
            panic!("boom in a message's drop");
        }
    }
}

// SAFETY: `target_type` is `Fragile` when it returns, and `dispatch` never touches the task.
unsafe impl Envelope for Faulty {
    fn kind(&self) -> &'static str {
        if self.panic_in & IN_KIND != 0 {
            panic!("boom in kind");
        }
        "faulty"
    }

    fn target_type(&self) -> TypeId {
        if self.panic_in & IN_TARGET != 0 {
            panic!("boom in target_type");
        }
        TypeId::of::<Fragile>()
    }

    unsafe fn dispatch(self: Box<Self>, _target: *mut ()) -> Result<(), String> {
        ORDER.with(|o| o.borrow_mut().push("faulty delivered"));
        Ok(())
    }
}

#[test]
fn a_panic_in_a_messages_kind_or_target_type_loses_only_that_message_and_never_aborts() {
    let mut sched = scheduler();
    let before = drops();
    let h = sched.spawn_box(FRAGILE, Fragile::boxed(PENDING, 0), TaskPolicy::default()).unwrap();
    let background = TaskPolicy { qos: QoSClass::Background, ..TaskPolicy::default() };
    let after = sched.spawn_box(COUNTER, Counter::boxed("after", 100), background).unwrap();
    sched.run_once().unwrap();
    clear_order();

    // Each of these panics; with IN_MESSAGE_DROP the message's drop panics too, which while the
    // first panic unwound would abort the process.
    for panic_in in [IN_KIND, IN_KIND | IN_MESSAGE_DROP, IN_TARGET, IN_TARGET | IN_MESSAGE_DROP] {
        sched.sender().send(SchedulerMsg::Deliver { id: h.id(), envelope: Faulty::boxed(panic_in) }).unwrap();
    }
    sched.sender().send(SchedulerMsg::Deliver { id: h.id(), envelope: Faulty::boxed(0) }).unwrap();
    after.wake(WakeReason::Manual).unwrap();
    let round = catch_unwind(AssertUnwindSafe(|| sched.run_once()));

    let id = h.id();
    let in_kind = format!("message for task {id:?} panicked in kind: boom in kind");
    let in_target = format!("message \"faulty\" for task {id:?} panicked in target_type: boom in target_type");
    let expected = [
        in_kind.clone(),
        in_kind,
        format!("message (its kind panicked) for task {id:?} panicked as it was dropped: boom in a message's drop"),
        in_target.clone(),
        in_target,
        format!("message \"faulty\" for task {id:?} panicked as it was dropped: boom in a message's drop"),
    ]
    .join("; ");
    assert!(matches!(&round, Ok(Err(err)) if *err == expected), "{round:?}");
    let dropped = ["faulty drop"; 4];
    assert_eq!(order()[..4], dropped, "each message whose kind or target_type panicked is dropped, not delivered");
    assert_eq!(order()[4..], ["faulty delivered", "faulty drop", "after"], "the next message is delivered, and the round goes on");
    assert!(is_live(&sched, h.id()), "the task stays live in its row");
    assert_eq!(row(&sched, h.id()).stats.poll_count, 2, "and is polled");
    assert_eq!(drops() - before, 0, "nothing is freed");
}

#[test]
fn a_panic_in_kind_at_a_tasks_end_still_frees_everything_and_removes_its_checkpoints() {
    // Without and with a message behind it whose drop panics; and with a message whose kind and
    // drop both panic.
    let queues: [&[u8]; 3] = [&[IN_KIND], &[IN_KIND, IN_MESSAGE_DROP], &[IN_KIND | IN_MESSAGE_DROP]];
    for queue in queues {
        let mut sched = flaky();
        let before = drops();
        let h = sched.spawn_box(FRAGILE, Fragile::boxed(PENDING, 0), TaskPolicy::default()).unwrap();
        sched.run_once().unwrap();
        h.checkpoint(CheckpointReason::Manual).unwrap();
        sched.run_once().unwrap();
        let ckpt = row(&sched, h.id()).checkpoint_history[0].clone();
        h.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap(); // restored and polled: live, with a checkpoint in its history

        for &panic_in in queue {
            sched.sender().send(SchedulerMsg::Deliver { id: h.id(), envelope: Faulty::boxed(panic_in) }).unwrap();
        }
        h.cancel().unwrap();
        clear_order();
        let round = catch_unwind(AssertUnwindSafe(|| sched.run_once()));

        let id = h.id();
        let in_kind = format!("message for task {id:?} panicked in kind: boom in kind");
        let in_drop = |name: &str| format!("message {name} for task {id:?} panicked as it was dropped: boom in a message's drop");
        let expected = match queue.len() {
            1 if queue[0] == IN_KIND => in_kind,
            1 => [in_kind, in_drop("(its kind panicked)")].join("; "),
            _ => [in_kind, in_drop("\"faulty\"")].join("; "),
        };
        assert!(matches!(&round, Ok(Err(err)) if *err == expected), "{queue:?}: {round:?}");
        assert!(sched.tasks.get(id).is_none(), "{queue:?}: the row is removed");
        assert_eq!(sched.finished, [(id, TaskState::Cancelled)], "{queue:?}");
        assert!(!sched.store.holds(&ckpt), "{queue:?}: its checkpoint is removed from the store");
        assert_eq!(order().last(), Some(&"store.remove"), "{queue:?}: after the task and its messages are freed");
        assert_eq!(drops() - before, 2, "{queue:?}: both copies freed once");
    }
}

#[test]
fn a_message_whose_drop_panics_sent_to_a_task_already_ended_is_an_error_for_the_round() {
    for (to, envelope) in [("typed", None), ("hand-written, kind panics", Some(IN_KIND | IN_MESSAGE_DROP))] {
        let mut sched = scheduler();
        let before = drops();
        let a = sched.spawn_box(FRAGILE, Fragile::boxed(PENDING, 0), TaskPolicy::default()).unwrap();
        let b = sched.spawn_box(FRAGILE, Fragile::boxed(PENDING, IN_CLEANUP), TaskPolicy::default()).unwrap();
        let other = sched.spawn_box(COUNTER, Counter::boxed("other", 100), TaskPolicy::default()).unwrap();
        sched.run_once().unwrap();
        clear_order();

        // In one round: b's cancel (its cleanup panics: an error noted), a's cancel, a message to
        // the ended a whose drop panics, then a wake for another task.
        b.cancel().unwrap();
        a.cancel().unwrap();
        match envelope {
            None => a.send("grenade", Grenade, |_: &mut Fragile, _| Ok(())).unwrap(),
            Some(panic_in) => sched.sender().send(SchedulerMsg::Deliver { id: a.id(), envelope: Faulty::boxed(panic_in) }).unwrap(),
        }
        other.wake(WakeReason::Manual).unwrap();
        let round = catch_unwind(AssertUnwindSafe(|| sched.run_once()));

        let mut expected = vec![format!("task {:?} panicked in cleanup or drop: boom in cleanup", b.id())];
        match envelope {
            None => expected.push(format!("message \"grenade\" for task {:?} panicked as it was dropped: boom in a message's drop", a.id())),
            Some(_) => {
                expected.push(format!("message for task {:?} panicked in kind: boom in kind", a.id()));
                expected.push(format!("message (its kind panicked) for task {:?} panicked as it was dropped: boom in a message's drop", a.id()));
            }
        }
        assert!(matches!(&round, Ok(Err(err)) if *err == expected.join("; ")), "{to}: {round:?}");
        assert_eq!(row(&sched, other.id()).stats.poll_count, 2, "{to}: the other task is polled in that round");
        assert_eq!(sched.finished, [(b.id(), TaskState::Cancelled), (a.id(), TaskState::Cancelled)], "{to}");
        assert_eq!(drops() - before, 2, "{to}: both ended tasks freed once");
        sched.run_once().unwrap();
    }
}

// ---------------------------------------------------------------------------------------------
// 0.3.2, fifth review: `from_box` drops a task it refuses before it panics; the drop of a refused
// message is caught; a `Yield` keeps its poll when a `QoS` or `MemoryPressure` wake follows it.

#[test]
fn from_box_drops_a_task_it_refuses_before_it_panics() {
    let refused = "protocol vtable is for scheduler::Counter, not scheduler::Fragile";
    for (panic_in, expected) in [(0, refused.to_owned()), (IN_DROP, format!("{refused}; the task panicked as it was dropped: boom in drop"))] {
        let before = drops();
        clear_order();
        // With IN_DROP the task's drop panics, which while from_box's own panic unwound would
        // abort the process.
        let caught = catch_unwind(|| ProtocolTaskPtr::from_box(Fragile::boxed(PENDING, panic_in), &COUNTER_VTABLE));
        let payload = caught.expect_err("from_box refuses a vtable for another type");
        assert_eq!(payload.downcast_ref::<String>(), Some(&expected), "panic_in {panic_in}");
        assert_eq!(order(), ["drop"], "panic_in {panic_in}: the task is dropped once");
        assert_eq!(drops() - before, 1, "panic_in {panic_in}");
    }
}

#[test]
fn a_refused_message_whose_drop_panics_is_an_error_and_the_task_goes_on() {
    let mut sched = scheduler();
    let before = drops();
    let h = sched.spawn_box(COUNTER, Counter::boxed("c", 100), TaskPolicy::default()).unwrap();
    let background = TaskPolicy { qos: QoSClass::Background, ..TaskPolicy::default() };
    let after = sched.spawn_box(COUNTER, Counter::boxed("after", 100), background).unwrap();
    sched.run_once().unwrap();
    clear_order();

    // Written for `Other` and sent to a Counter: refused, then dropped, and its drop panics.
    let wrong = typed_envelope::<Other, Grenade>("wrong", Grenade, |_, _| Ok(()));
    sched.sender().send(SchedulerMsg::Deliver { id: h.id(), envelope: wrong }).unwrap();
    after.wake(WakeReason::Manual).unwrap();
    let round = catch_unwind(AssertUnwindSafe(|| sched.run_once()));

    let expected = [
        format!("envelope \"wrong\" for task {:?} targets another task type than scheduler::Counter", h.id()),
        format!("message \"wrong\" for task {:?} panicked as it was dropped: boom in a message's drop", h.id()),
    ]
    .join("; ");
    assert!(matches!(&round, Ok(Err(err)) if *err == expected), "{round:?}");
    assert_eq!(order(), ["message drop", "c", "after"], "the message is dropped, then both tasks are polled");
    assert!(is_live(&sched, h.id()), "the task stays live in its row");
    assert_eq!(row(&sched, h.id()).stats.poll_count, 2, "and is polled");
    assert_eq!(row(&sched, after.id()).stats.poll_count, 2, "the task after it is polled in that round");
    assert_eq!(drops() - before, 0, "nothing is freed");
}

#[test]
fn an_over_budget_task_keeps_the_poll_for_its_yield_when_a_qos_or_memory_pressure_wake_follows() {
    for reason in [WakeReason::QoS, WakeReason::MemoryPressure] {
        let mut sched = scheduler();
        let h = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[YIELD, YIELD, READY]), over_budget()).unwrap();
        sched.run_once().unwrap(); // checkpointed in place of its first poll
        sched.run_once().unwrap(); // restored and polled: it yields
        h.wake(reason.clone()).unwrap();
        sched.run_once().unwrap(); // checkpointed in place of the poll for the Yield, and the wake answered
        let entry = row(&sched, h.id());
        assert!(matches!(entry.resident, Resident::Dormant), "{reason:?}: the strategy checkpointed it");
        assert_eq!(entry.state, TaskState::Runnable, "{reason:?}: runnable for the Yield");
        assert_eq!(entry.stats.last_wake, Some(reason.clone()), "{reason:?}: on_wake heard the wake");
        for _ in 0..20 {
            sched.run_once().unwrap();
        }
        assert!(sched.tasks.get(h.id()).is_none(), "{reason:?}: every Yield got its poll");
        assert_eq!(sched.finished, [(h.id(), TaskState::Completed)], "{reason:?}");
    }
}

// ---------------------------------------------------------------------------------------------
// 0.3.2, sixth review: work a task was handed keeps its poll when the save of a strategy
// checkpoint fails or panics, or the strategy panics; a poll already made, or a `Yield` without
// `eager_repoll_after_yield`, is owed nothing; a task the strategy checkpointed without
// auto-restore waits for it.

/// Panics the next time it is asked while `armed` is set (and clears it), and otherwise asks the
/// default strategy.
struct PanicOnce {
    armed: Arc<AtomicBool>,
}

impl CapitulateStrategy for PanicOnce {
    fn plan(&self, snapshot: &TaskSnapshot, last_wake: Option<&WakeReason>) -> Option<CapitulatePlan> {
        if self.armed.swap(false, Ordering::SeqCst) {
            panic!("strategy bug");
        }
        DefaultCapitulateStrategy::default().plan(snapshot, last_wake)
    }
}

/// Hand an over-budget Counter, polled once, a message ("job") or a Manual wake. In the round that
/// delivers it, `fault` makes the strategy's checkpoint fail, or panic, in place of the poll.
/// Check that the task is live, its work handed over and not yet polled for; then, with nothing
/// failing, that the next strategy checkpoint happens and the round after it polls the task.
fn handed_work_survives<S: CheckpointStore>(
    mut sched: WakerProtocol<S>,
    fault: impl Fn(&WakerProtocol<S>, bool),
    expect_panic: bool,
    case: &str,
) {
    for message in [false, true] {
        let case = format!("{case}, message {message}");
        let before = drops();
        let h = sched.spawn_box(COUNTER, Counter::boxed("b", 100), over_budget()).unwrap();
        sched.run_once().unwrap(); // checkpointed in place of its first poll
        sched.run_once().unwrap(); // restored and polled for its spawn
        clear_order();

        if message {
            h.send("note", "job", note).unwrap();
        } else {
            h.wake(WakeReason::Manual).unwrap();
        }
        fault(&sched, true);
        let round = catch_unwind(AssertUnwindSafe(|| sched.run_once()));
        fault(&sched, false);
        if expect_panic {
            assert!(round.is_err(), "{case}: the panic reaches the caller");
        } else {
            assert!(matches!(&round, Ok(Err(err)) if err == "disk full"), "{case}: {round:?}");
        }
        let entry = row(&sched, h.id());
        assert!(matches!(entry.resident, Resident::Live(_)), "{case}: nothing was saved, so it is live");
        assert!(entry.mailbox.is_empty() && entry.pending_wakes.is_empty(), "{case}: its work was handed to it");
        assert_eq!(entry.stats.poll_count, 1, "{case}: and it was not polled for it");

        let saved = sched.reclaim_log.len();
        for _ in 0..5 {
            sched.run_once().unwrap();
        }
        let entry = row(&sched, h.id());
        assert_eq!(sched.reclaim_log.len(), saved + 1, "{case}: the strategy checkpointed it again in place of the poll");
        assert_eq!(entry.stats.poll_count, 2, "{case}: and the round after that polled it");
        assert_eq!(entry.state, TaskState::Waiting, "{case}");
        let acted: &[&str] = if message { &["job", "restored"] } else { &["restored"] };
        let order: Vec<_> = order().into_iter().filter(|step| *step != "store.remove").collect();
        assert_eq!(order, acted, "{case}: the work was delivered once, then a poll acted on it");
        assert_eq!(drops() - before, 2, "{case}: the two checkpointed copies, each freed once");
        h.cancel().unwrap();
        sched.run_once().unwrap();
    }
}

#[test]
fn work_handed_to_a_task_keeps_its_poll_when_the_save_of_a_strategy_checkpoint_fails_or_panics() {
    handed_work_survives(flaky(), |sched, on| sched.store.fail(on), false, "save fails");
    handed_work_survives(flaky(), |sched, on| sched.store.panic_on_save(on), true, "save panics");
}

#[test]
fn work_handed_to_a_task_keeps_its_poll_when_the_strategy_panics() {
    let armed = Arc::new(AtomicBool::new(false));
    let sched = scheduler_with(MemoryCheckpointStore::default(), Box::new(PanicOnce { armed: armed.clone() }));
    handed_work_survives(sched, move |_, on| armed.store(on, Ordering::SeqCst), true, "strategy panics");
}

/// Fails if the poll a task is owed is not cleared when the poll runs.
#[test]
fn a_yield_already_polled_for_gives_no_poll_to_a_later_qos_or_memory_pressure_wake() {
    for reason in [WakeReason::QoS, WakeReason::MemoryPressure] {
        let mut sched = scheduler();
        let h = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[YIELD, PENDING]), over_budget()).unwrap();
        // Checkpointed, restored and polled (it yields), checkpointed, restored and polled (Pending).
        for _ in 0..6 {
            sched.run_once().unwrap();
        }
        let entry = row(&sched, h.id());
        assert_eq!((entry.stats.poll_count, &entry.state), (2, &TaskState::Waiting), "{reason:?}: the Yield got its poll");

        h.wake(reason.clone()).unwrap();
        for _ in 0..5 {
            sched.run_once().unwrap();
        }
        let entry = row(&sched, h.id());
        assert_eq!(entry.stats.poll_count, 2, "{reason:?}: no poll for the wake");
        assert!(matches!(entry.resident, Resident::Dormant), "{reason:?}: the checkpoint answered it");
        assert_eq!(entry.state, TaskState::Dormant, "{reason:?}");
        assert_eq!(entry.stats.last_wake, Some(reason.clone()), "{reason:?}: on_wake heard it");
    }
}

/// Fails if a `Yield` without `eager_repoll_after_yield` is counted as owed a poll.
#[test]
fn without_eager_repoll_a_yield_parks_and_is_owed_no_poll() {
    for reason in [WakeReason::QoS, WakeReason::MemoryPressure] {
        let mut sched = scheduler();
        let policy = TaskPolicy { eager_repoll_after_yield: false, ..over_budget() };
        let h = sched.spawn_box(SCRIPTED, Scripted::boxed(64, &[YIELD, PENDING]), policy).unwrap();
        // Checkpointed in place of its first poll, then restored and polled: it yields and parks.
        for _ in 0..5 {
            sched.run_once().unwrap();
        }
        let entry = row(&sched, h.id());
        assert_eq!((entry.stats.poll_count, &entry.state), (1, &TaskState::Waiting), "{reason:?}: parked after its Yield");
        assert!(is_live(&sched, h.id()), "{reason:?}");

        h.wake(reason.clone()).unwrap();
        for _ in 0..5 {
            sched.run_once().unwrap();
        }
        let entry = row(&sched, h.id());
        assert_eq!(entry.stats.poll_count, 1, "{reason:?}: no poll for the Yield or the wake");
        assert!(matches!(entry.resident, Resident::Dormant), "{reason:?}: the checkpoint answered the wake");
        assert_eq!(entry.state, TaskState::Dormant, "{reason:?}");
    }
}

#[test]
fn a_task_the_strategy_checkpointed_without_auto_restore_is_polled_once_auto_restore_is_back() {
    let mut sched = scheduler();
    let policy = TaskPolicy { auto_restore_on_wake: false, ..over_budget() };
    let h = sched.spawn_box(COUNTER, Counter::boxed("n", 100), policy).unwrap();
    for _ in 0..5 {
        sched.run_once().unwrap();
    }
    let entry = row(&sched, h.id());
    assert!(matches!(entry.resident, Resident::Dormant), "checkpointed in place of its first poll");
    assert_eq!(entry.state, TaskState::Runnable, "and owed that poll");
    assert_eq!((entry.stats.restore_count, entry.stats.poll_count), (0, 0), "but not restored or polled while auto-restore is off");

    sched.tasks.get_mut(h.id()).unwrap().policy.auto_restore_on_wake = true;
    sched.run_once().unwrap();
    let entry = row(&sched, h.id());
    assert_eq!((entry.stats.restore_count, entry.stats.poll_count), (1, 1), "the next round restores and polls it");
}

/// Runs a [`Fragile`] task's `cleanup`, then its drop, each under `catch_unwind`, and lets
/// neither panic go on.
unsafe fn holding_destroy(data: *mut ()) {
    // SAFETY: `data` is a live `Fragile` from `Box::<Fragile>::into_raw` (from_raw_parts' contract),
    // and this is its last call.
    let mut task = unsafe { Box::from_raw(data as *mut Fragile) };
    // SAFETY: the task is live and exclusively held.
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe { task.cleanup() }));
    let _ = catch_unwind(AssertUnwindSafe(move || drop(task)));
}

// SAFETY: every function but the destroy is `Fragile`'s own, from `ProtocolVTable::of`, and
// `holding_destroy` frees the task as the `Box<Fragile>` it is.
static HOLDING_VTABLE: ProtocolVTable = unsafe {
    let of = ProtocolVTable::of::<Fragile>();
    ProtocolVTable::from_raw_parts::<Fragile>(
        of.poll_fn(),
        of.wake_fn(),
        of.checkpoint_fn(),
        of.restore_fn(),
        holding_destroy,
        of.footprint_fn(),
    )
};

/// A task dropped while another panic unwinds aborts the process when its `cleanup` or drop
/// panics and the destroy function lets that panic go on; this one does not.
#[test]
fn a_destroy_function_that_holds_the_panics_keeps_a_drop_during_an_unwind_from_aborting() {
    let before = drops();
    clear_order();
    let caught = catch_unwind(|| {
        let _task = ProtocolTaskPtr::from_box(Fragile::boxed(PENDING, IN_CLEANUP | IN_DROP), &HOLDING_VTABLE);
        panic!("outer");
    });
    let payload = caught.expect_err("the outer panic reaches catch_unwind");
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"outer"), "and nothing else");
    assert_eq!(order(), ["cleanup", "drop"], "both panicked while the outer panic unwound");
    assert_eq!(drops() - before, 1, "the task is freed once");
}
