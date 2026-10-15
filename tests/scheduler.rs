//! The scheduler end to end. Tasks count their drops in a thread-local, so a task freed twice
//! or never fails here, and Miri reports any leak or bad read.

use std::cell::{Cell, RefCell};
use std::task::Context;

use waker_protocol::{
    derive_protocol_task_vtable, scheduler_waker, typed_envelope, CapitulatePlan, CheckpointReason,
    CheckpointStore, MemoryCheckpointStore, ProtocolTask, ProtocolTaskPtr, QoSClass,
    Resident, RetentionPolicy, SchedulerConfig, SchedulerMsg, TaskId, TaskPoll, TaskPolicy, TaskSlab,
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
    let sched = WakerProtocol::new(MemoryCheckpointStore::default());
    sched.register_vtable(COUNTER, &COUNTER_VTABLE);
    sched.register_vtable(OTHER, &OTHER_VTABLE);
    sched
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
    let err = deliver_raw(&mut sched, SchedulerMsg::Deliver { id: handle.id(), envelope: wrong }).unwrap_err();
    assert!(err.contains("targets another task type than scheduler::Counter"), "{err}");
    assert!(sched.tasks.get(handle.id()).is_some());
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
