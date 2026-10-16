// Reproductions against ORIGINAL waker_protocol 0.2.0.
// Each test targets one claimed flaw. Run with `cargo test`.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use waker_protocol::{
    derive_protocol_task_vtable, CapitulatePlan, CapitulateStrategy, CheckpointHandle,
    CheckpointReason, CheckpointStore, DefaultCapitulateStrategy, MemoryCheckpointStore,
    ProtocolTask, QoSClass, Resident, RetentionPolicy, TaskId, TaskPoll, TaskPolicy, TaskSnapshot,
    TaskState, WakeReason, WakerProtocol,
};
use waker_tables::VTableKey;

// ---- A simple instrumented task -------------------------------------------

struct Probe {
    // shared observable state
    cleanup_flag: Arc<AtomicBool>,
    poll_result: TaskPoll,
    // record each poll
    polls: Arc<AtomicUsize>,
    // optional: make deliver fail
    footprint: usize,
}

unsafe impl ProtocolTask for Probe {
    unsafe fn poll(&mut self) -> TaskPoll {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.poll_result.clone()
    }
    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        Vec::new()
    }
    unsafe fn restore(_bytes: &[u8]) -> Self {
        // Restored tasks are inert; they are only used to show scheduling behavior.
        Probe {
            cleanup_flag: Arc::new(AtomicBool::new(false)),
            poll_result: TaskPoll::Pending,
            polls: Arc::new(AtomicUsize::new(0)),
            footprint: 0,
        }
    }
    unsafe fn cleanup(&mut self) {
        self.cleanup_flag.store(true, Ordering::SeqCst);
    }
    fn footprint_estimate(&self) -> usize {
        self.footprint
    }
}

derive_protocol_task_vtable!(Probe, PROBE_VTABLE);

fn key() -> VTableKey {
    VTableKey { segment: "probe", kind: "probe", version: 1 }
}

fn fresh() -> WakerProtocol<MemoryCheckpointStore> {
    let sched = WakerProtocol::new(MemoryCheckpointStore::default());
    sched.register_vtable(key(), &PROBE_VTABLE);
    sched
}

fn mk(poll_result: TaskPoll, footprint: usize) -> (Probe, Arc<AtomicBool>, Arc<AtomicUsize>) {
    let cleanup = Arc::new(AtomicBool::new(false));
    let polls = Arc::new(AtomicUsize::new(0));
    (
        Probe {
            cleanup_flag: cleanup.clone(),
            poll_result,
            polls: polls.clone(),
            footprint,
        },
        cleanup,
        polls,
    )
}

// ---- Claim 4: cancelled task is never removed/freed until scheduler drop ---

#[test]
fn claim4_cancelled_task_lingers() {
    let mut sched = fresh();
    let (task, cleanup, _polls) = mk(TaskPoll::Pending, 0);
    let h = sched
        .spawn_box(key(), Box::new(task), TaskPolicy::default())
        .unwrap();
    let id = h.id();

    // Drain the initial Spawn wake; task returns Pending -> Waiting.
    sched.run_once().unwrap();
    assert!(sched.tasks.get(id).is_some(), "task present after first poll");

    // Cancel via message.
    h.cancel().unwrap();
    for _ in 0..5 {
        sched.run_once().unwrap();
    }

    let present = sched.tasks.get(id).is_some();
    let state = sched.tasks.get(id).map(|e| e.state.clone());
    let freed = cleanup.load(Ordering::SeqCst);
    println!(
        "claim4: present_after_cancel={present} state={state:?} freed_before_drop={freed}"
    );
    assert!(present, "cancelled task still in slab");
    assert_eq!(state, Some(TaskState::Cancelled));
    assert!(!freed, "task NOT freed while scheduler alive");

    // Dropping the scheduler frees it.
    drop(sched);
    assert!(cleanup.load(Ordering::SeqCst), "freed on scheduler drop");
}

// ---- Claim 5: a deliver fn returning Err destroys task, leaves dead row ----

#[test]
fn claim5_deliver_err_destroys_task_dead_row() {
    let mut sched = fresh();
    let (task, cleanup, _polls) = mk(TaskPoll::Pending, 0);
    let h = sched
        .spawn_box(key(), Box::new(task), TaskPolicy::default())
        .unwrap();
    let id = h.id();
    sched.run_once().unwrap(); // first poll, Pending

    // Deliver a message whose deliver fn returns Err.
    h.send("boom", 1u64, |_t: &mut Probe, _m: u64| Err("refused".to_string()))
        .unwrap();
    let res = sched.run_once();

    let present = sched.tasks.get(id).is_some();
    let resident_empty = matches!(
        sched.tasks.get(id).map(|e| &e.resident),
        Some(Resident::Empty)
    );
    let freed = cleanup.load(Ordering::SeqCst);
    println!(
        "claim5: run_once_err={:?} present={present} resident_empty={resident_empty} freed={freed}",
        res.is_err()
    );
    assert!(res.is_err(), "deliver Err propagates out of run_once");
    assert!(present, "row remains");
    assert!(resident_empty, "resident left Empty (dead row)");
    assert!(freed, "task object destroyed");
}

// ---- Claim 6: a failed checkpoint save destroys the task -------------------

struct FailingStore {
    inner: MemoryCheckpointStore,
    fail: Arc<AtomicBool>,
}
impl CheckpointStore for FailingStore {
    type Handle = CheckpointHandle;
    fn save(&self, task_id: TaskId, payload: Vec<u8>) -> Result<Self::Handle, String> {
        if self.fail.load(Ordering::SeqCst) {
            Err("save failed".to_string())
        } else {
            self.inner.save(task_id, payload)
        }
    }
    fn load(&self, handle: &Self::Handle) -> Result<Vec<u8>, String> {
        self.inner.load(handle)
    }
    fn remove(&self, handle: &Self::Handle) -> Result<(), String> {
        self.inner.remove(handle)
    }
}

#[test]
fn claim6_failed_checkpoint_destroys_task() {
    let fail = Arc::new(AtomicBool::new(false));
    let store = FailingStore { inner: MemoryCheckpointStore::default(), fail: fail.clone() };
    let mut sched = WakerProtocol::new(store);
    sched.register_vtable(key(), &PROBE_VTABLE);

    let (task, cleanup, _polls) = mk(TaskPoll::Pending, 0);
    let h = sched
        .spawn_box(key(), Box::new(task), TaskPolicy::default())
        .unwrap();
    let id = h.id();
    sched.run_once().unwrap();

    // Now make saves fail, then force a checkpoint.
    fail.store(true, Ordering::SeqCst);
    h.checkpoint(CheckpointReason::Manual).unwrap();
    let res = sched.run_once();

    let present = sched.tasks.get(id).is_some();
    let resident_empty = matches!(
        sched.tasks.get(id).map(|e| &e.resident),
        Some(Resident::Empty)
    );
    let freed = cleanup.load(Ordering::SeqCst);
    println!(
        "claim6: run_once_err={:?} present={present} resident_empty={resident_empty} freed={freed}",
        res.is_err()
    );
    assert!(res.is_err());
    assert!(present && resident_empty, "dead row after failed save");
    assert!(freed, "task destroyed by failed checkpoint");
}

// ---- Claim 7: one error mid-round skips finalizing earlier-finished tasks --

#[test]
fn claim7_error_skips_finalize_of_earlier_finished() {
    // Task A completes (Ready) and should be finalized/removed this round.
    // Task B errors (deliver Err). If the error aborts the round before the
    // finalize loop, A stays in the slab.
    let mut sched = fresh();

    // retain_on_completion = false so a finalized Completed task is removed.
    let mut pol = TaskPolicy::default();
    pol.retention = RetentionPolicy {
        keep_latest: 0,
        retain_on_completion: false,
        retain_on_failure: false,
        retain_on_cancel: false,
    };

    let (ta, _ca, _pa) = mk(TaskPoll::Ready, 0);
    let ha = sched.spawn_box(key(), Box::new(ta), pol.clone()).unwrap();
    let ida = ha.id();

    let (tb, _cb, _pb) = mk(TaskPoll::Pending, 0);
    let hb = sched.spawn_box(key(), Box::new(tb), pol.clone()).unwrap();
    let _idb = hb.id();

    // Make B error on deliver so its poll_one returns Err.
    hb.send("boom", 1u64, |_t: &mut Probe, _m: u64| Err("refused".to_string()))
        .unwrap();

    // One round. Ordering is BiasedQoSThenAge; both Standard, A spawned first
    // (older last_polled_at None, lower runnable_seq) so A is ordered first.
    let res = sched.run_once();

    let a_present = sched.tasks.get(ida).is_some();
    println!(
        "claim7: run_once_err={:?} completed_A_still_present={a_present}",
        res.is_err()
    );
    assert!(res.is_err(), "B's error propagates");
    assert!(
        a_present,
        "A finished (Ready) but was NOT finalized/removed because the error aborted the round"
    );
    assert_eq!(sched.tasks.get(ida).map(|e| e.state.clone()), Some(TaskState::Completed));
}

// ---- Claim 8: shutdown does not stop; tasks polled after it ----------------

// A task whose polls land in a process-wide counter, so a restored instance
// still counts (restore() otherwise builds a fresh object).
static GLOBAL_POLLS: AtomicUsize = AtomicUsize::new(0);

struct GlobalProbe;
unsafe impl ProtocolTask for GlobalProbe {
    unsafe fn poll(&mut self) -> TaskPoll {
        GLOBAL_POLLS.fetch_add(1, Ordering::SeqCst);
        TaskPoll::Pending
    }
    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        Vec::new()
    }
    unsafe fn restore(_bytes: &[u8]) -> Self {
        GlobalProbe
    }
    fn footprint_estimate(&self) -> usize {
        0
    }
}
derive_protocol_task_vtable!(GlobalProbe, GLOBAL_PROBE_VTABLE);

#[test]
fn claim8_shutdown_does_not_stop_run_once() {
    GLOBAL_POLLS.store(0, Ordering::SeqCst);
    let gkey = VTableKey { segment: "probe", kind: "global", version: 1 };
    let mut sched = WakerProtocol::new(MemoryCheckpointStore::default());
    sched.register_vtable(gkey, &GLOBAL_PROBE_VTABLE);
    let h = sched
        .spawn_box(gkey, Box::new(GlobalProbe), TaskPolicy::default())
        .unwrap();
    sched.run_once().unwrap();
    let before = GLOBAL_POLLS.load(Ordering::SeqCst);

    // Shutdown.
    sched.handle().shutdown().unwrap();
    sched.run_once().unwrap(); // processes Shutdown (checkpoints live tasks)

    // Now wake and run again. run_once never checks `shutdown`.
    h.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap();
    let after = GLOBAL_POLLS.load(Ordering::SeqCst);

    println!(
        "claim8: shutdown_flag={} polls_before={before} polls_after={after}",
        sched.shutdown
    );
    assert!(sched.shutdown, "shutdown flag is set");
    assert!(
        after > before,
        "task was restored and polled AFTER shutdown ({before} -> {after})"
    );
}

// ---- Claim 9: default idle rule -> task idle past threshold never polls ----

#[test]
fn claim9_default_idle_never_polls_again() {
    // DefaultCapitulateStrategy with a short idle threshold (same logic, faster).
    let strat = DefaultCapitulateStrategy {
        min_footprint_bytes: 10,
        idle_before_checkpoint: Duration::from_millis(20),
        protect_qos_at_or_above: QoSClass::Interactive,
    };
    let mut sched = WakerProtocol::with_strategy(MemoryCheckpointStore::default(), Box::new(strat));
    sched.register_vtable(key(), &PROBE_VTABLE);

    // Standard QoS (not protected), footprint over min.
    let mut pol = TaskPolicy::default();
    pol.qos = QoSClass::Standard;
    let (task, _c, polls) = mk(TaskPoll::Pending, 4096);
    let h = sched.spawn_box(key(), Box::new(task), pol).unwrap();

    sched.run_once().unwrap(); // first poll sets last_polled_at
    let first = polls.load(Ordering::SeqCst);
    assert!(first >= 1, "polled at least once initially");

    std::thread::sleep(Duration::from_millis(40)); // now idle past threshold

    for _ in 0..5 {
        h.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap();
    }
    let after = polls.load(Ordering::SeqCst);
    println!("claim9: polls_first={first} polls_after_idle_wakes={after}");
    assert_eq!(after, first, "idle task is checkpointed instead of polled, forever");
}

// ---- Claim 10: strategy plan retain_stub=false treated as true -------------

struct NoRetainStrategy;
impl CapitulateStrategy for NoRetainStrategy {
    fn plan(&self, _s: &TaskSnapshot, _w: Option<&WakeReason>) -> Option<CapitulatePlan> {
        Some(CapitulatePlan { reason: CheckpointReason::Manual, retain_stub: false })
    }
}

#[test]
fn claim10_strategy_retain_stub_false_ignored() {
    let mut sched =
        WakerProtocol::with_strategy(MemoryCheckpointStore::default(), Box::new(NoRetainStrategy));
    sched.register_vtable(key(), &PROBE_VTABLE);
    let (task, cleanup, polls) = mk(TaskPoll::Pending, 0);
    let h = sched.spawn_box(key(), Box::new(task), TaskPolicy::default()).unwrap();
    let id = h.id();

    sched.run_once().unwrap();

    let present = sched.tasks.get(id).is_some();
    let state = sched.tasks.get(id).map(|e| e.state.clone());
    let dormant = matches!(sched.tasks.get(id).map(|e| &e.resident), Some(Resident::Dormant));
    println!(
        "claim10: present={present} state={state:?} resident_dormant={dormant} polled={} freed={}",
        polls.load(Ordering::SeqCst),
        cleanup.load(Ordering::SeqCst)
    );
    assert!(present, "task retained");
    assert!(dormant, "retain_stub=false was treated as true: task kept as Dormant stub");
    assert_eq!(state, Some(TaskState::Dormant));
}

// ---- Claim 11: a wake just before a Checkpoint message is lost -------------

#[test]
fn claim11_wake_before_checkpoint_lost() {
    let mut sched = fresh();
    let (task, _c, polls) = mk(TaskPoll::Pending, 0);
    let h = sched.spawn_box(key(), Box::new(task), TaskPolicy::default()).unwrap();
    let id = h.id();
    sched.run_once().unwrap();
    let before = polls.load(Ordering::SeqCst);

    // Queue a wake, then a checkpoint, then drain.
    h.wake(WakeReason::Manual).unwrap();
    h.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();

    // Task is now dormant with no pending wakes -> the Manual wake is lost.
    let dormant = matches!(sched.tasks.get(id).map(|e| &e.resident), Some(Resident::Dormant));
    let pending = sched.tasks.get(id).map(|e| e.pending_wakes.len());
    let runnable_after = {
        // run more rounds: nothing should restore/poll it, the wake is gone.
        for _ in 0..3 {
            sched.run_once().unwrap();
        }
        polls.load(Ordering::SeqCst)
    };
    println!(
        "claim11: dormant={dormant} pending_wakes={pending:?} polls_before={before} polls_after={runnable_after}"
    );
    assert!(dormant, "checkpoint won the race, task dormant");
    assert_eq!(pending, Some(0), "pending wakes cleared by checkpoint");
    assert_eq!(runnable_after, before, "the wake never caused a poll (lost)");
}

// ---- Claim 13: snapshot mailbox_len/pending_wakes always 0 at strategy time -

#[derive(Clone)]
struct RecordingStrategy {
    seen_mailbox: Arc<Mutex<Vec<usize>>>,
    seen_wakes: Arc<Mutex<Vec<usize>>>,
}
impl CapitulateStrategy for RecordingStrategy {
    fn plan(&self, s: &TaskSnapshot, _w: Option<&WakeReason>) -> Option<CapitulatePlan> {
        self.seen_mailbox.lock().unwrap().push(s.mailbox_len);
        self.seen_wakes.lock().unwrap().push(s.pending_wakes);
        None
    }
}

#[test]
fn claim13_snapshot_counts_always_zero() {
    let rec = RecordingStrategy {
        seen_mailbox: Arc::new(Mutex::new(Vec::new())),
        seen_wakes: Arc::new(Mutex::new(Vec::new())),
    };
    let mut sched =
        WakerProtocol::with_strategy(MemoryCheckpointStore::default(), Box::new(rec.clone()));
    sched.register_vtable(key(), &PROBE_VTABLE);
    let (task, _c, _p) = mk(TaskPoll::Pending, 0);
    let h = sched.spawn_box(key(), Box::new(task), TaskPolicy::default()).unwrap();

    // Deliver several messages and wakes before the round.
    for i in 0..3u64 {
        h.send("m", i, |_t: &mut Probe, _m: u64| Ok(())).unwrap();
        h.wake(WakeReason::Manual).unwrap();
    }
    sched.run_once().unwrap();

    let mb = rec.seen_mailbox.lock().unwrap().clone();
    let wk = rec.seen_wakes.lock().unwrap().clone();
    println!("claim13: strategy saw mailbox_len={mb:?} pending_wakes={wk:?}");
    assert!(!mb.is_empty(), "strategy was consulted");
    assert!(mb.iter().all(|&x| x == 0), "mailbox_len always 0 at strategy time");
    assert!(wk.iter().all(|&x| x == 0), "pending_wakes always 0 at strategy time");
}

// ---- Claim 14: default budget rule checkpoints over-budget Batch each poll --

#[test]
fn claim14_default_budget_never_runs() {
    let strat = DefaultCapitulateStrategy::default();
    let mut sched = WakerProtocol::with_strategy(MemoryCheckpointStore::default(), Box::new(strat));
    sched.register_vtable(key(), &PROBE_VTABLE);

    let mut pol = TaskPolicy::default();
    pol.qos = QoSClass::Batch;
    pol.memory_budget_hint = Some(10); // footprint 4096 > 10
    let (task, _c, polls) = mk(TaskPoll::Pending, 4096);
    let h = sched.spawn_box(key(), Box::new(task), pol).unwrap();

    for _ in 0..4 {
        h.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap();
    }
    let after = polls.load(Ordering::SeqCst);
    println!("claim14: polls_total={after}");
    assert_eq!(after, 0, "over-budget Batch task checkpointed before every poll, never runs");
}

// ---- Claim 15: auto_restore_on_wake=false leaves woken dormant task Runnable -

#[test]
fn claim15_no_auto_restore_stuck_runnable() {
    let mut sched = fresh();
    let mut pol = TaskPolicy::default();
    pol.auto_restore_on_wake = false;
    let (task, _c, polls) = mk(TaskPoll::Pending, 0);
    let h = sched.spawn_box(key(), Box::new(task), pol).unwrap();
    let id = h.id();
    sched.run_once().unwrap();

    // Make it dormant.
    h.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    assert!(matches!(sched.tasks.get(id).map(|e| &e.resident), Some(Resident::Dormant)));
    let before = polls.load(Ordering::SeqCst);

    // Wake it; without auto-restore it should never be restored or polled.
    for _ in 0..5 {
        h.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap();
    }
    let state = sched.tasks.get(id).map(|e| e.state.clone());
    let dormant = matches!(sched.tasks.get(id).map(|e| &e.resident), Some(Resident::Dormant));
    let after = polls.load(Ordering::SeqCst);
    println!("claim15: state={state:?} resident_dormant={dormant} polls_before={before} after={after}");
    assert_eq!(state, Some(TaskState::Runnable), "stuck Runnable");
    assert!(dormant, "still a dormant stub, never restored");
    assert_eq!(after, before, "never polled");
}

// ---- Claim 16: a panic during poll leaves the row Resident::Empty ----------

struct Panicker {
    cleanup_flag: Arc<AtomicBool>,
}
unsafe impl ProtocolTask for Panicker {
    unsafe fn poll(&mut self) -> TaskPoll {
        panic!("poll panic");
    }
    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        Vec::new()
    }
    unsafe fn restore(_bytes: &[u8]) -> Self {
        Panicker { cleanup_flag: Arc::new(AtomicBool::new(false)) }
    }
    unsafe fn cleanup(&mut self) {
        self.cleanup_flag.store(true, Ordering::SeqCst);
    }
}
derive_protocol_task_vtable!(Panicker, PANICKER_VTABLE);

#[test]
fn claim16_panic_leaves_resident_empty() {
    let pkey = VTableKey { segment: "probe", kind: "panic", version: 1 };
    let mut sched = WakerProtocol::new(MemoryCheckpointStore::default());
    sched.register_vtable(pkey, &PANICKER_VTABLE);
    let cleanup = Arc::new(AtomicBool::new(false));
    let h = sched
        .spawn_box(pkey, Box::new(Panicker { cleanup_flag: cleanup.clone() }), TaskPolicy::default())
        .unwrap();
    let id = h.id();

    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sched.run_once()));
    assert!(res.is_err(), "poll panicked");

    let present = sched.tasks.get(id).is_some();
    let resident_empty = matches!(sched.tasks.get(id).map(|e| &e.resident), Some(Resident::Empty));
    let freed = cleanup.load(Ordering::SeqCst);
    println!("claim16: present={present} resident_empty={resident_empty} task_freed={freed}");
    assert!(present, "row remains");
    assert!(resident_empty, "resident left Empty after panic (dead row)");
}
