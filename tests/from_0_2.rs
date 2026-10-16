//! What 0.3.0 removed from 0.2.0 and 0.3.2 puts back, and the differences from 0.2.0 that the
//! README's 0.3.2 notes say this file checks.
//! Every item put back is used by a test here, so the file does not compile against 0.3.1.
//! The Debug test checks the output the README describes (it uses only items public since 0.3.0),
//! the waker test that `TASK_WAKE_SIGNAL_RAW_VTABLE` is a copy the crate's wakers do not carry,
//! the empty-pointer test that a `ProtocolTaskPtr` with a null `data` owns nothing, and the
//! derive test that `derive_protocol_task_vtable!` calls the trait's functions. Tasks
//! count their drops in a thread-local, so a task freed twice or never fails here, and Miri
//! reports any leak or bad read.

use std::cell::Cell;
use std::sync::Arc;
use std::task::{RawWaker, Waker};

use waker_protocol::{
    derive_protocol_task_vtable, scheduler_waker, CheckpointReason, CheckpointStore, MemoryCheckpointStore,
    ProtocolTask, ProtocolTaskPtr, ProtocolVTable, Resident, TaskId, TaskPoll, TaskPolicy, TaskWakeSignal,
    WakeReason, WakerProtocol, TASK_WAKE_SIGNAL_RAW_VTABLE,
};
use waker_tables::{UnsafeWakeable, VTableKey};

thread_local! {
    static DROPS: Cell<usize> = const { Cell::new(0) };
    static HAND_CALLS: Cell<usize> = const { Cell::new(0) };
}

fn drops() -> usize {
    DROPS.with(Cell::get)
}

fn hand_calls() -> usize {
    HAND_CALLS.with(Cell::get)
}

/// Counts its polls.
struct Job {
    polls: u32,
}

impl Job {
    fn boxed() -> Box<Self> {
        Box::new(Self { polls: 0 })
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        DROPS.with(|d| d.set(d.get() + 1));
    }
}

// SAFETY: the functions only touch `self`; `restore` panics on short bytes.
unsafe impl ProtocolTask for Job {
    unsafe fn poll(&mut self) -> TaskPoll {
        self.polls += 1;
        TaskPoll::Pending
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        self.polls.to_le_bytes().to_vec()
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        Self { polls: u32::from_le_bytes(bytes[..4].try_into().unwrap()) }
    }
}

derive_protocol_task_vtable!(Job, JOB_VTABLE);
const JOB: VTableKey = VTableKey { segment: "test", kind: "job", version: 1 };

fn scheduler() -> WakerProtocol<MemoryCheckpointStore> {
    let sched = WakerProtocol::new(MemoryCheckpointStore::default());
    sched.register_vtable(JOB, &JOB_VTABLE);
    sched.register_vtable(HAND, &HAND_VTABLE);
    sched
}

// Hand-written functions for `Job`, as 0.2.0 code wrote a `ProtocolVTable` literal. Each counts
// its calls in HAND_CALLS.
// SAFETY (all six): `from_raw_parts`' contract: `data` is a live `Job` from `Box::<Job>::into_raw`,
// held exclusively (shared, for `hand_footprint`), and `hand_destroy` is its last call.

unsafe fn hand_poll(data: *mut ()) -> TaskPoll {
    HAND_CALLS.with(|c| c.set(c.get() + 1));
    // SAFETY: see above; the call itself is ProtocolTask's contract.
    unsafe { (*(data as *mut Job)).poll() }
}

unsafe fn hand_wake(data: *mut (), reason: WakeReason) {
    HAND_CALLS.with(|c| c.set(c.get() + 1));
    // SAFETY: see above.
    unsafe { (*(data as *mut Job)).on_wake(reason) }
}

unsafe fn hand_checkpoint(data: *mut ()) -> Vec<u8> {
    HAND_CALLS.with(|c| c.set(c.get() + 1));
    // SAFETY: see above.
    unsafe { (*(data as *mut Job)).checkpoint() }
}

unsafe fn hand_restore(bytes: &[u8]) -> *mut () {
    HAND_CALLS.with(|c| c.set(c.get() + 1));
    // SAFETY: Job's restore accepts any bytes (it panics on short ones).
    Box::into_raw(Box::new(unsafe { Job::restore(bytes) })) as *mut ()
}

unsafe fn hand_destroy(data: *mut ()) {
    HAND_CALLS.with(|c| c.set(c.get() + 1));
    // SAFETY: see above; this is the one place the box is freed.
    let mut job = unsafe { Box::from_raw(data as *mut Job) };
    // SAFETY: the task is live and exclusively held.
    unsafe { job.cleanup() };
}

unsafe fn hand_footprint(data: *const ()) -> usize {
    HAND_CALLS.with(|c| c.set(c.get() + 1));
    // SAFETY: see above.
    unsafe { (*(data as *const Job)).footprint_estimate() }
}

// SAFETY: each function reads `data` as the `Box<Job>` pointer the contract describes, and
// `hand_restore` returns one.
static HAND_VTABLE: ProtocolVTable = unsafe {
    ProtocolVTable::from_raw_parts::<Job>(hand_poll, hand_wake, hand_checkpoint, hand_restore, hand_destroy, hand_footprint)
};
const HAND: VTableKey = VTableKey { segment: "test", kind: "hand", version: 1 };

#[test]
fn the_raw_waker_vtable_static_is_back_and_a_waker_built_on_it_wakes_the_task() {
    let mut sched = scheduler();
    let h = sched.spawn_box(JOB, Job::boxed(), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();

    let signal = Arc::new(TaskWakeSignal { id: h.id(), tx: sched.sender(), reason: WakeReason::Timer });
    let data = Arc::into_raw(signal.clone()) as *const ();
    // SAFETY: `data` owns one strong ref of an `Arc<TaskWakeSignal>`, as the vtable's functions
    // read it, and the waker gives it back when dropped.
    let waker = unsafe { Waker::from_raw(RawWaker::new(data, &TASK_WAKE_SIGNAL_RAW_VTABLE)) };
    waker.wake_by_ref();
    let second = waker.clone();
    second.wake();
    drop(waker);
    assert_eq!(Arc::strong_count(&signal), 1, "every ref the wakers held is given back");
    assert!(
        std::ptr::eq(&TASK_WAKE_SIGNAL_RAW_VTABLE, &waker_protocol::wake_ref::TASK_WAKE_SIGNAL_RAW_VTABLE),
        "one static, at the crate root and in wake_ref, as in 0.2.0"
    );

    sched.run_once().unwrap();
    let entry = sched.tasks.get(h.id()).unwrap();
    assert_eq!(entry.stats.last_wake, Some(WakeReason::Timer), "the wakes arrived");
    assert_eq!(entry.stats.poll_count, 2, "and the task was polled for them");
}

#[test]
fn raw_data_is_public_again_and_unsafe() {
    let before = drops();
    let mut task = ProtocolTaskPtr::from_box(Job::boxed(), &JOB_VTABLE);
    // SAFETY: the call itself has no requirement; the pointer is used below as its contract says.
    let data = unsafe { task.raw_data() };
    // SAFETY: `data` is the live `Job` that `task` owns, used as a `Job`, while nothing else uses it.
    unsafe { (*(data as *mut Job)).polls = 41 };
    // SAFETY: `task` owns a live `Job`, and `&mut` gives exclusive access.
    assert_eq!(unsafe { task.checkpoint() }, 41u32.to_le_bytes(), "the write reached the task");
    drop(task);
    assert_eq!(drops() - before, 1, "freed once, by `task`");
}

#[test]
fn each_function_of_a_vtable_can_be_read_and_called() {
    let before = drops();
    let vtable = &JOB_VTABLE;
    let data = Box::into_raw(Job::boxed()) as *mut ();
    // SAFETY: `data` is a live `Job` from `Box::<Job>::into_raw` and `vtable` is `Job`'s own;
    // each call has exclusive access, and each task is destroyed once, last.
    unsafe {
        (vtable.wake_fn())(data, WakeReason::Manual);
        assert_eq!((vtable.poll_fn())(data), TaskPoll::Pending);
        assert_eq!((vtable.checkpoint_fn())(data), 1u32.to_le_bytes());
        assert_eq!((vtable.footprint_fn())(data as *const ()), std::mem::size_of::<Job>());
        let copy = (vtable.restore_fn())(&7u32.to_le_bytes());
        assert_eq!((vtable.checkpoint_fn())(copy), 7u32.to_le_bytes(), "restored from the bytes");
        (vtable.destroy_fn())(copy);
        (vtable.destroy_fn())(data);
    }
    assert_eq!(drops() - before, 2, "both tasks freed once");
    assert_eq!(vtable.type_name(), std::any::type_name::<Job>());
}

#[test]
fn a_vtable_from_raw_parts_runs_its_type_in_the_scheduler() {
    assert!(HAND_VTABLE.is_for::<Job>(), "recorded as running Job");
    assert!(!HAND_VTABLE.is_for::<u32>());
    assert_eq!(HAND_VTABLE.type_name(), std::any::type_name::<Job>());

    let mut sched = scheduler();
    let before = (drops(), hand_calls());
    let h = sched.spawn_box(HAND, Job::boxed(), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap(); // footprint, on_wake(Spawn), poll
    h.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap(); // footprint, checkpoint, destroy
    h.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap(); // restore, on_wake(Manual), on_wake(Resume), poll
    let entry = sched.tasks.get(h.id()).unwrap();
    assert!(matches!(entry.resident, Resident::Live(_)));
    assert_eq!((entry.stats.restore_count, entry.stats.poll_count), (1, 2));
    h.cancel().unwrap();
    sched.run_once().unwrap(); // destroy
    assert!(sched.tasks.get(h.id()).is_none());
    assert_eq!(drops() - before.0, 2, "the spawned and the restored copy, each freed once");
    assert_eq!(hand_calls() - before.1, 3 + 3 + 4 + 1, "every call went through the hand-written functions");
}

#[test]
fn into_raw_parts_gives_the_task_up_and_from_raw_parts_takes_it_back() {
    let before = drops();
    let task = ProtocolTaskPtr::from_box(Job::boxed(), &JOB_VTABLE);
    let (data, vtable) = task.into_raw_parts();
    assert_eq!(drops() - before, 0, "giving it up does not free it");
    assert!(std::ptr::eq(vtable, &JOB_VTABLE));

    // SAFETY: the pair came from `into_raw_parts` and is given back once.
    let mut again = unsafe { ProtocolTaskPtr::from_raw_parts(data, vtable) };
    // SAFETY: `again` owns a live `Job`, and `&mut` gives exclusive access.
    assert_eq!(unsafe { again.poll() }, TaskPoll::Pending);
    drop(again);
    assert_eq!(drops() - before, 1, "freed once, by its new owner");

    let data = Box::into_raw(Job::boxed()) as *mut ();
    // SAFETY: `data` is a `Box<Job>` pointer, `JOB_VTABLE` is `Job`'s own, and the result owns it.
    let owned = unsafe { ProtocolTaskPtr::from_raw_parts(data, &JOB_VTABLE) };
    drop(owned);
    assert_eq!(drops() - before, 2, "a task from Box::into_raw is freed once too");
}

#[test]
fn debug_output_shows_the_full_type_path() {
    assert_eq!(format!("{JOB_VTABLE:?}"), "ProtocolVTable { type_name: \"from_0_2::Job\", .. }");
    let hand = format!("{HAND_VTABLE:?}");
    assert_eq!(hand, "ProtocolVTable { type_name: \"from_0_2::Job\", .. }", "from_raw_parts records the type's name");
    let task = ProtocolTaskPtr::from_box(Job::boxed(), &JOB_VTABLE);
    let shown = format!("{task:?}");
    assert!(shown.starts_with("ProtocolTaskPtr { data: "), "{shown}");
    assert!(shown.ends_with(", type_name: \"from_0_2::Job\" }"), "{shown}");
}

#[test]
fn the_raw_waker_vtable_static_is_a_copy_the_crates_wakers_do_not_carry() {
    let sched = scheduler();
    let id = TaskId::new(0, 0);
    let carried = <TaskWakeSignal as UnsafeWakeable>::raw_waker_vtable();
    assert!(!std::ptr::eq(carried, &TASK_WAKE_SIGNAL_RAW_VTABLE), "the static is a copy at its own address");

    // A waker from `scheduler_waker` and its clone carry the derive's static, and wake each other.
    let made = scheduler_waker(id, sched.sender(), WakeReason::Timer);
    let clone = made.clone();
    assert!(std::ptr::eq(made.vtable(), carried) && std::ptr::eq(clone.vtable(), carried));
    assert!(!std::ptr::eq(made.vtable(), &TASK_WAKE_SIGNAL_RAW_VTABLE), "not the static, unlike 0.2.0");
    assert!(clone.will_wake(&made));

    // A waker built on the static: its clone carries the derive's static, so the two do not
    // `will_wake` each other (in 0.2.0 they did).
    let signal = Arc::new(TaskWakeSignal { id, tx: sched.sender(), reason: WakeReason::Timer });
    let data = Arc::into_raw(signal.clone()) as *const ();
    // SAFETY: `data` owns one strong ref of an `Arc<TaskWakeSignal>`, as the vtable's functions
    // read it, and the waker gives it back when dropped.
    let built = unsafe { Waker::from_raw(RawWaker::new(data, &TASK_WAKE_SIGNAL_RAW_VTABLE)) };
    let built_clone = built.clone();
    assert!(std::ptr::eq(built.vtable(), &TASK_WAKE_SIGNAL_RAW_VTABLE));
    assert!(std::ptr::eq(built_clone.vtable(), carried), "its clone carries the derive's static");
    assert!(!built_clone.will_wake(&built));
    drop((built, built_clone));
    assert_eq!(Arc::strong_count(&signal), 1, "every ref the wakers held is given back");
}

/// Reads a `Job`'s poll count through `&WakerProtocol`, as 0.2.0 code read `p.data`.
fn polls_seen<S: CheckpointStore>(sched: &WakerProtocol<S>, id: TaskId) -> Option<u32> {
    match &sched.tasks.get(id)?.resident {
        Resident::Live(p) if p.vtable().is_for::<Job>() => {
            // SAFETY: the task is a `Job` (checked above) and live: a pointer in a task row is
            // never empty (`from_raw_parts`' contract). It is only read here, while nothing else
            // uses it.
            Some(unsafe { (*(p.data() as *const Job)).polls })
        }
        _ => None,
    }
}

#[test]
fn data_reads_the_task_pointer_through_a_shared_reference() {
    let mut sched = scheduler();
    let before = drops();
    let h = sched.spawn_box(JOB, Job::boxed(), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    h.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap();
    assert_eq!(polls_seen(&sched, h.id()), Some(2));

    let mut task = ProtocolTaskPtr::from_box(Job::boxed(), &JOB_VTABLE);
    // SAFETY: the call itself has no requirement.
    let raw = unsafe { task.raw_data() };
    assert_eq!(task.data(), raw, "the same pointer `raw_data` gives");
    drop(task);
    drop(sched);
    assert_eq!(drops() - before, 2, "each task freed once, by its owner");
}

thread_local! {
    static RAN: std::cell::RefCell<Vec<&'static str>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Has inherent `poll`, `on_wake`, `checkpoint`, `cleanup` and `footprint_estimate` methods named
/// like the trait's, with the trait's receivers, as a 0.2.0 task could; its 0.2.0 vtable called
/// them in place of the trait's.
struct Shadowed {
    big: Vec<u8>,
}

impl Shadowed {
    fn poll(&mut self) -> TaskPoll {
        RAN.with(|r| r.borrow_mut().push("inherent poll"));
        TaskPoll::Ready
    }

    fn on_wake(&mut self, _: WakeReason) {
        RAN.with(|r| r.borrow_mut().push("inherent on_wake"));
    }

    fn checkpoint(&mut self) -> Vec<u8> {
        RAN.with(|r| r.borrow_mut().push("inherent checkpoint"));
        vec![1]
    }

    fn cleanup(&mut self) {
        RAN.with(|r| r.borrow_mut().push("inherent cleanup"));
    }

    fn footprint_estimate(&self) -> usize {
        self.big.len()
    }
}

// SAFETY: the functions only touch `self`.
unsafe impl ProtocolTask for Shadowed {
    unsafe fn poll(&mut self) -> TaskPoll {
        RAN.with(|r| r.borrow_mut().push("trait poll"));
        TaskPoll::Pending
    }

    unsafe fn on_wake(&mut self, _: WakeReason) {
        RAN.with(|r| r.borrow_mut().push("trait on_wake"));
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        RAN.with(|r| r.borrow_mut().push("trait checkpoint"));
        Vec::new()
    }

    unsafe fn restore(_: &[u8]) -> Self {
        Self { big: Vec::new() }
    }

    unsafe fn cleanup(&mut self) {
        RAN.with(|r| r.borrow_mut().push("trait cleanup"));
    }

    fn footprint_estimate(&self) -> usize {
        7
    }
}

derive_protocol_task_vtable!(Shadowed, SHADOWED_VTABLE);

#[test]
fn derive_protocol_task_vtable_calls_the_trait_functions_not_inherent_methods_of_the_same_name() {
    let ran = || RAN.with(|r| r.borrow_mut().drain(..).collect::<Vec<_>>());
    let mut direct = Shadowed { big: vec![0; 100] };
    assert_eq!(direct.poll(), TaskPoll::Ready);
    direct.on_wake(WakeReason::Manual);
    assert_eq!(direct.checkpoint(), [1]);
    direct.cleanup();
    assert_eq!(direct.footprint_estimate(), 100, "method syntax on the type reaches the inherent methods");
    assert_eq!(ran(), ["inherent poll", "inherent on_wake", "inherent checkpoint", "inherent cleanup"]);

    let mut task = ProtocolTaskPtr::from_box(Box::new(direct), &SHADOWED_VTABLE);
    // SAFETY: `task` owns a live `Shadowed`, and `&mut` gives exclusive access.
    unsafe {
        assert_eq!(task.poll(), TaskPoll::Pending);
        task.on_wake(WakeReason::Manual);
        assert_eq!(task.checkpoint(), Vec::<u8>::new());
    }
    assert_eq!(task.footprint_estimate(), 7, "the vtable calls the trait's footprint_estimate");
    drop(task);
    assert_eq!(
        ran(),
        ["trait poll", "trait on_wake", "trait checkpoint", "trait cleanup"],
        "and the trait's poll, on_wake, checkpoint and cleanup"
    );
}

/// Holds a task the way 0.2.0 code could: in a field, emptied by moving the task out.
struct Holder {
    slot: ProtocolTaskPtr,
}

/// An empty pointer for `vtable`: a null `data`, owning nothing.
fn empty(vtable: &'static ProtocolVTable) -> ProtocolTaskPtr {
    // SAFETY: a null `data` makes an empty pointer; the callers only read it, move it and drop
    // it, and never poll, wake, checkpoint, restore into it or put it in a task row.
    unsafe { ProtocolTaskPtr::from_raw_parts(std::ptr::null_mut(), vtable) }
}

/// 0.2.0's `ProtocolTaskPtr { data: null_mut(), vtable }`, whose drop skipped the null `data`.
#[test]
fn an_empty_task_pointer_owns_nothing_and_dropping_it_does_nothing() {
    let before = (drops(), hand_calls());
    for vtable in [&JOB_VTABLE, &HAND_VTABLE] {
        let none = empty(vtable);
        assert!(none.data().is_null());
        assert_eq!(none.footprint_estimate(), 0, "the safe footprint of an empty pointer is 0");
        drop(none);
    }
    assert_eq!((drops(), hand_calls()), before, "no function of the vtable ran, and nothing was freed");

    // As 0.2.0 code did: move the task out of a slot and leave an empty pointer in its place.
    let mut holder = Holder { slot: ProtocolTaskPtr::from_box(Job::boxed(), &HAND_VTABLE) };
    let mut taken = std::mem::replace(&mut holder.slot, empty(&HAND_VTABLE));
    // SAFETY: `taken` owns a live `Job`, and `&mut` gives exclusive access.
    assert_eq!(unsafe { taken.poll() }, TaskPoll::Pending);
    drop(taken);
    assert_eq!(drops() - before.0, 1, "the task moved out is freed once");
    assert!(holder.slot.data().is_null());
    drop(holder);
    assert_eq!(drops() - before.0, 1, "the empty slot frees nothing");
    assert_eq!(hand_calls() - before.1, 2, "only the poll and the destroy of the task moved out ran");
}
