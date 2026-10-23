//! The three things the kit's time kit found missing in 0.3 and 0.4.0 adds: a task declares its
//! own interval (`TaskPolicy::interval`) and its next moment (`ProtocolTask::wake_at`), and the
//! scheduler keeps that time with no thread; `TaskPolicy::dormancy_ttl` parks a task with no work
//! and a wake resumes it; and a checkpoint is one the task gives (`Checkpoint`,
//! `ProtocolVTable::checkpointed`) or none at all (`ProtocolVTable::live`), never one the
//! scheduler makes up.

use std::cell::Cell;
use std::time::{Duration, Instant};

use waker_protocol::{
    CapitulatePlan, Checkpoint, CheckpointReason, CheckpointStore, MemoryCheckpointStore, ProtocolTask, ProtocolVTable,
    QoSClass, Resident, TaskPoll, TaskPolicy, TaskState, WakeReason, WakerProtocol,
};
use waker_tables::VTableKey;

thread_local! {
    /// The count the last polled `Counter` reached.
    static COUNT: Cell<u64> = const { Cell::new(0) };
    /// How many `Timer` wakes tasks were told of.
    static TIMERS: Cell<u32> = const { Cell::new(0) };
    /// How many times a `Live` task was polled.
    static LIVE_POLLS: Cell<u32> = const { Cell::new(0) };
}

/// Counts its polls; gives its count as its state. With `next` set, asks to be woken then.
struct Counter {
    count: u64,
    next: Option<Instant>,
}

impl Checkpoint for Counter {
    fn save(&self) -> Vec<u8> {
        self.count.to_le_bytes().to_vec()
    }

    fn restore(bytes: &[u8]) -> Result<Self, String> {
        let bytes: [u8; 8] = bytes.try_into().map_err(|_| format!("{} bytes, not 8", bytes.len()))?;
        Ok(Counter { count: u64::from_le_bytes(bytes), next: None })
    }
}

// SAFETY: every function touches only `self`.
unsafe impl ProtocolTask for Counter {
    unsafe fn poll(&mut self) -> TaskPoll {
        self.count += 1;
        COUNT.with(|c| c.set(self.count));
        TaskPoll::Pending
    }

    unsafe fn on_wake(&mut self, reason: WakeReason) {
        if reason == WakeReason::Timer {
            TIMERS.with(|t| t.set(t.get() + 1));
            self.next = None;
        }
    }

    fn wake_at(&self) -> Option<Instant> {
        self.next
    }
}

/// A task in the 0.3 form: its own `checkpoint` and `restore`, under `ProtocolVTable::of`.
struct OldForm(u64);

// SAFETY: every function touches only `self`; `restore` reads any bytes.
unsafe impl ProtocolTask for OldForm {
    unsafe fn poll(&mut self) -> TaskPoll {
        self.0 += 1;
        COUNT.with(|c| c.set(self.0));
        TaskPoll::Pending
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        self.0.to_le_bytes().to_vec()
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        OldForm(bytes.try_into().map(u64::from_le_bytes).unwrap_or(0))
    }
}

/// Live state (a closure), no checkpoint: under `ProtocolVTable::live` it writes only `poll`.
struct Live {
    body: Box<dyn FnMut() -> TaskPoll + Send + Sync>,
}

// SAFETY: `poll` only calls the task's own closure.
unsafe impl ProtocolTask for Live {
    unsafe fn poll(&mut self) -> TaskPoll {
        LIVE_POLLS.with(|n| n.set(n.get() + 1));
        (self.body)()
    }

    fn footprint_estimate(&self) -> usize {
        1 << 20
    }
}

/// Writes neither `checkpoint` nor `restore`, yet is built with `ProtocolVTable::of`.
struct Unwritten;

// SAFETY: `poll` touches nothing.
unsafe impl ProtocolTask for Unwritten {
    unsafe fn poll(&mut self) -> TaskPoll {
        TaskPoll::Pending
    }
}

static COUNTER: ProtocolVTable = ProtocolVTable::checkpointed::<Counter>();
static OLD_FORM: ProtocolVTable = ProtocolVTable::of::<OldForm>();
static LIVE: ProtocolVTable = ProtocolVTable::live::<Live>();
static UNWRITTEN: ProtocolVTable = ProtocolVTable::of::<Unwritten>();

const COUNTER_KEY: VTableKey = VTableKey { segment: "time_kit", kind: "counter", version: 1 };
const OLD_FORM_KEY: VTableKey = VTableKey { segment: "time_kit", kind: "old_form", version: 1 };
const LIVE_KEY: VTableKey = VTableKey { segment: "time_kit", kind: "live", version: 1 };
const UNWRITTEN_KEY: VTableKey = VTableKey { segment: "time_kit", kind: "unwritten", version: 1 };

fn scheduler() -> WakerProtocol<MemoryCheckpointStore> {
    let sched = WakerProtocol::new(MemoryCheckpointStore::default());
    sched.register_vtable(COUNTER_KEY, &COUNTER);
    sched.register_vtable(OLD_FORM_KEY, &OLD_FORM);
    sched.register_vtable(LIVE_KEY, &LIVE);
    sched.register_vtable(UNWRITTEN_KEY, &UNWRITTEN);
    sched
}

fn counter() -> Box<Counter> {
    Box::new(Counter { count: 0, next: None })
}

fn timers() -> u32 {
    TIMERS.with(Cell::get)
}

fn count() -> u64 {
    COUNT.with(Cell::get)
}

// Finding 1: a task declares its own interval and its next moment.

/// The policy's interval sets the first tick one interval after spawn; a tick wakes the task with
/// `Timer` once and moves to the next interval. No policy interval, no tick.
#[test]
fn an_interval_in_the_policy_wakes_the_task_with_timer_and_no_thread() {
    let mut sched = scheduler();
    let hour = Duration::from_secs(3600);
    let task = sched.spawn_box(COUNTER_KEY, counter(), TaskPolicy { interval: Some(hour), ..TaskPolicy::default() }).unwrap();
    let plain = sched.spawn_box(COUNTER_KEY, counter(), TaskPolicy::default()).unwrap();
    let row = sched.tasks.get(task.id()).unwrap();
    assert_eq!(row.next_tick, Some(row.stats.created_at + hour));
    assert_eq!(sched.tasks.get(plain.id()).unwrap().next_tick, None);
    sched.run_once().unwrap();
    assert_eq!(timers(), 0, "an hour has not passed");

    // The hour has come.
    let tick = Instant::now();
    sched.tasks.get_mut(task.id()).unwrap().next_tick = Some(tick);
    sched.run_once().unwrap();
    assert_eq!(timers(), 1);
    let row = sched.tasks.get(task.id()).unwrap();
    assert_eq!(row.stats.poll_count, 2);
    assert_eq!(row.next_tick, Some(tick + hour), "the next tick is one interval on");
    sched.run_once().unwrap();
    assert_eq!(timers(), 1);
}

/// Ticks are at a fixed rate: a round that comes several ticks late wakes the task once, and the
/// next tick is the next multiple of the interval after that round.
#[test]
fn a_late_round_wakes_once_and_keeps_the_rate() {
    let mut sched = scheduler();
    let every = Duration::from_millis(1);
    let task = sched.spawn_box(COUNTER_KEY, counter(), TaskPolicy { interval: Some(every), ..TaskPolicy::default() }).unwrap();
    let first = sched.tasks.get(task.id()).unwrap().next_tick.unwrap();
    std::thread::sleep(Duration::from_millis(5));
    sched.run_once().unwrap();
    assert_eq!(timers(), 1, "five ticks late, one wake");
    let next = sched.tasks.get(task.id()).unwrap().next_tick.unwrap();
    let ticks = (next - first).as_nanos() / every.as_nanos();
    assert_eq!((next - first).as_nanos() % every.as_nanos(), 0, "on the rate");
    assert!(ticks >= 5, "{ticks} ticks on");
}

/// `pump` waits no longer than the next tick: the scheduler keeps the time itself.
#[test]
fn pump_waits_no_longer_than_the_next_tick() {
    let mut sched = scheduler();
    let task = sched
        .spawn_box(COUNTER_KEY, counter(), TaskPolicy { interval: Some(Duration::from_millis(10)), ..TaskPolicy::default() })
        .unwrap();
    sched.run_once().unwrap();
    let started = Instant::now();
    while timers() == 0 {
        assert!(sched.pump(Duration::from_secs(3600)).unwrap());
        assert!(started.elapsed() < Duration::from_secs(60), "pump waited past the tick");
    }
    assert_eq!(sched.tasks.get(task.id()).unwrap().stats.last_wake, Some(WakeReason::Timer));
}

/// After each poll the scheduler asks the task for its next moment and wakes it with `Timer`
/// then, once; the moment is cleared when it comes.
#[test]
fn wake_at_wakes_the_task_at_the_moment_its_poll_gave() {
    let mut sched = scheduler();
    let later = Instant::now() + Duration::from_secs(3600);
    let task = sched.spawn_box(COUNTER_KEY, Box::new(Counter { count: 0, next: Some(later) }), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    assert_eq!(sched.tasks.get(task.id()).unwrap().wake_at, Some(later), "the poll's answer is in the row");
    sched.run_once().unwrap();
    assert_eq!(timers(), 0);

    // The moment has come.
    sched.tasks.get_mut(task.id()).unwrap().wake_at = Some(Instant::now());
    sched.run_once().unwrap();
    assert_eq!(timers(), 1);
    let row = sched.tasks.get(task.id()).unwrap();
    assert_eq!(row.stats.poll_count, 2);
    assert_eq!(row.wake_at, None, "the task gave no next moment");
    sched.run_once().unwrap();
    assert_eq!(timers(), 1);
}

/// A lifecycle's stages wake at their own moments with the task parked between them: the moment
/// stays in the row while the task is dormant, and its tick restores it from the state it gave.
#[test]
fn a_moment_wakes_a_parked_task_and_it_resumes_from_its_state() {
    let mut sched = scheduler();
    let later = Instant::now() + Duration::from_secs(3600);
    let policy = TaskPolicy { dormancy_ttl: Some(Duration::ZERO), ..TaskPolicy::default() };
    let task = sched.spawn_box(COUNTER_KEY, Box::new(Counter { count: 41, next: Some(later) }), policy).unwrap();
    sched.run_once().unwrap(); // polled: 42
    sched.run_once().unwrap(); // no work: parked
    let row = sched.tasks.get(task.id()).unwrap();
    assert!(matches!(row.resident, Resident::Dormant));
    assert_eq!(row.wake_at, Some(later));

    sched.tasks.get_mut(task.id()).unwrap().wake_at = Some(Instant::now());
    sched.run_once().unwrap();
    assert_eq!(count(), 43, "restored from 42 and polled");
    let row = sched.tasks.get(task.id()).unwrap();
    assert_eq!(row.stats.restore_count, 1);
    assert_eq!(row.stats.last_wake, Some(WakeReason::Resume));
}

// Finding 2: dormancy_ttl is honoured.

/// Past its dormancy with no work, a task is checkpointed (`Idle`) and dormant; a wake restores it
/// from its checkpoint and polls it. Both forms of checkpoint: the 0.3 one and `Checkpoint`.
#[test]
fn dormancy_ttl_parks_a_task_with_no_work_and_a_wake_resumes_it() {
    for key in [COUNTER_KEY, OLD_FORM_KEY] {
        let mut sched = scheduler();
        let policy = TaskPolicy { dormancy_ttl: Some(Duration::ZERO), ..TaskPolicy::default() };
        let task = if key == COUNTER_KEY {
            sched.spawn_box(key, Box::new(Counter { count: 6, next: None }), policy).unwrap().id()
        } else {
            sched.spawn_box(key, Box::new(OldForm(6)), policy).unwrap().id()
        };
        sched.run_once().unwrap();
        assert_eq!(count(), 7);
        assert!(matches!(sched.tasks.get(task).unwrap().resident, Resident::Live(_)), "it had work: its spawn");

        sched.run_once().unwrap();
        let row = sched.tasks.get(task).unwrap();
        assert!(matches!(row.resident, Resident::Dormant), "{key:?}: parked");
        assert_eq!(row.state, TaskState::Dormant);
        assert_eq!(row.stats.checkpoint_count, 1);
        assert_eq!(sched.reclaim_log.last().map(|r| r.reason.clone()), Some(CheckpointReason::Idle));
        let poll_count = row.stats.poll_count;

        sched.handle().wake(task, WakeReason::Manual).unwrap();
        sched.run_once().unwrap();
        assert_eq!(count(), 8, "{key:?}: resumed from 7");
        let row = sched.tasks.get(task).unwrap();
        assert_eq!(row.stats.restore_count, 1);
        assert_eq!(row.stats.poll_count, poll_count + 1);
    }
}

/// A task with time left, with work waiting, owed a poll, or with no `dormancy_ttl` is not parked.
#[test]
fn dormancy_ttl_spares_a_task_with_time_left_or_work() {
    let mut sched = scheduler();
    let hour = sched.spawn_box(COUNTER_KEY, counter(), TaskPolicy { dormancy_ttl: Some(Duration::from_secs(3600)), ..TaskPolicy::default() }).unwrap();
    let none = sched.spawn_box(COUNTER_KEY, counter(), TaskPolicy::default()).unwrap();
    let woken = sched.spawn_box(COUNTER_KEY, counter(), TaskPolicy { dormancy_ttl: Some(Duration::ZERO), ..TaskPolicy::default() }).unwrap();
    sched.run_once().unwrap();
    for _ in 0..3 {
        woken.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap();
    }
    for id in [hour.id(), none.id(), woken.id()] {
        assert!(matches!(sched.tasks.get(id).unwrap().resident, Resident::Live(_)), "{id:?} stays live");
    }
    assert!(sched.reclaim_log.is_empty());
    assert_eq!(sched.tasks.get(woken.id()).unwrap().stats.poll_count, 4, "polled for every wake");
}

/// `pump` waits no longer than the moment a task's dormancy parks it.
#[test]
fn pump_parks_a_task_when_its_dormancy_passes() {
    let mut sched = scheduler();
    let policy = TaskPolicy { dormancy_ttl: Some(Duration::from_millis(10)), ..TaskPolicy::default() };
    let task = sched.spawn_box(COUNTER_KEY, counter(), policy).unwrap();
    sched.run_once().unwrap();
    let started = Instant::now();
    while !matches!(sched.tasks.get(task.id()).unwrap().resident, Resident::Dormant) {
        assert!(sched.pump(Duration::from_secs(3600)).unwrap());
        assert!(started.elapsed() < Duration::from_secs(60), "pump waited past the dormancy");
    }
}

// Finding 3: a checkpoint is one the task gives, or none.

/// The scheduler saves exactly the bytes `Checkpoint::save` gives, and a wake resumes the task from
/// them through `Checkpoint::restore`.
#[test]
fn a_task_that_gives_its_state_resumes_from_it() {
    let mut sched = scheduler();
    assert!(COUNTER.gives_checkpoint() && OLD_FORM.gives_checkpoint() && !LIVE.gives_checkpoint());
    let task = sched.spawn_box(COUNTER_KEY, counter(), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    for _ in 0..2 {
        task.wake(WakeReason::Manual).unwrap();
        sched.run_once().unwrap();
    }
    assert_eq!(count(), 3);
    task.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    let row = sched.tasks.get(task.id()).unwrap();
    assert!(matches!(row.resident, Resident::Dormant));
    let saved = sched.store.load(row.checkpoint_history.back().unwrap()).unwrap();
    assert_eq!(saved, 3u64.to_le_bytes(), "the bytes the task gave");

    task.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap();
    assert_eq!(count(), 4, "resumed at 3, then polled");
    assert_eq!(sched.tasks.get(task.id()).unwrap().stats.restore_count, 1);
}

/// Bytes `Checkpoint::restore` refuses end the task as Failed, the message naming the type and
/// carrying its reason.
#[test]
fn bytes_the_task_refuses_end_it_failed_saying_why() {
    let mut sched = scheduler();
    let task = sched.spawn_box(COUNTER_KEY, counter(), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    task.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    let bad = sched.store.save(task.id(), vec![1, 2, 3]).unwrap();
    sched.tasks.get_mut(task.id()).unwrap().checkpoint_history.push_back(bad);
    task.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap();
    let (id, state) = sched.finished.last().unwrap().clone();
    assert_eq!(id, task.id());
    let TaskState::Failed(why) = state else { panic!("{state:?}") };
    assert!(why.contains("Counter refused its checkpoint: 3 bytes, not 8"), "{why}");
}

/// A task under `ProtocolVTable::live` is never checkpointed: not by the strategy (over its budget
/// at Batch), not by its dormancy, not by Shutdown; a Checkpoint or a Capitulate that keeps a
/// stub, from a message or its own poll, leaves it live and is returned as an error.
#[test]
fn a_live_task_is_never_checkpointed() {
    let mut sched = scheduler();
    let policy = TaskPolicy { qos: QoSClass::Batch, memory_budget_hint: Some(0), dormancy_ttl: Some(Duration::ZERO), ..TaskPolicy::default() };
    let mut asks = 0;
    let task = sched
        .spawn_box(
            LIVE_KEY,
            Box::new(Live {
                body: Box::new(move || {
                    asks += 1;
                    if asks == 3 { TaskPoll::Checkpoint(CheckpointReason::Manual) } else { TaskPoll::Pending }
                }),
            }),
            policy,
        )
        .unwrap();
    sched.run_once().unwrap(); // poll 1
    sched.run_once().unwrap(); // its dormancy has passed: still live
    task.wake(WakeReason::Manual).unwrap();
    sched.run_once().unwrap(); // poll 2: over budget at Batch, the strategy is not asked
    assert_eq!(LIVE_POLLS.with(Cell::get), 2);

    task.checkpoint(CheckpointReason::Manual).unwrap();
    let err = sched.run_once().unwrap_err();
    assert!(err.contains("Live) gives no checkpoint: Manual not taken, it stays live"), "{err}");
    task.capitulate(CapitulatePlan { reason: CheckpointReason::QoS, retain_stub: true }).unwrap();
    let err = sched.run_once().unwrap_err();
    assert!(err.contains("gives no checkpoint: QoS not taken"), "{err}");
    task.wake(WakeReason::Manual).unwrap();
    let err = sched.run_once().unwrap_err(); // poll 3 asks for a checkpoint
    assert!(err.contains("gives no checkpoint: Manual not taken"), "{err}");
    let row = sched.tasks.get(task.id()).unwrap();
    assert!(matches!(row.resident, Resident::Live(_)));
    assert_eq!(row.state, TaskState::Waiting, "not polled again until a wake");
    sched.run_once().unwrap();
    assert_eq!(LIVE_POLLS.with(Cell::get), 3);

    sched.handle().shutdown().unwrap();
    sched.run_once().unwrap();
    let row = sched.tasks.get(task.id()).unwrap();
    assert!(matches!(row.resident, Resident::Live(_)), "Shutdown leaves it live");
    assert_eq!(row.stats.checkpoint_count, 0);
    assert!(sched.reclaim_log.is_empty());
}

/// A capitulation with no stub needs no checkpoint: a live task ends as Completed.
#[test]
fn a_live_task_capitulates_without_a_stub() {
    let mut sched = scheduler();
    let task = sched.spawn_box(LIVE_KEY, Box::new(Live { body: Box::new(|| TaskPoll::Pending) }), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    task.capitulate(CapitulatePlan { reason: CheckpointReason::QoS, retain_stub: false }).unwrap();
    sched.run_once().unwrap();
    assert_eq!(sched.finished, [(task.id(), TaskState::Completed)]);
}

/// A task built with `ProtocolVTable::of` that wrote no `checkpoint` ends as Failed when asked for
/// one: the scheduler never saves bytes the task did not give.
#[test]
fn a_checkpoint_the_task_did_not_write_fails_rather_than_saving_nothing() {
    let mut sched = scheduler();
    let task = sched.spawn_box(UNWRITTEN_KEY, Box::new(Unwritten), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    task.checkpoint(CheckpointReason::Manual).unwrap();
    sched.run_once().unwrap();
    let TaskState::Failed(why) = sched.finished[0].1.clone() else { panic!("{:?}", sched.finished) };
    assert!(why.contains("Unwritten gives no checkpoint"), "{why}");
    assert!(sched.reclaim_log.is_empty());
}

/// Panics in `wake_at`, asked after every poll.
struct BadMoment;

// SAFETY: `poll` touches nothing.
unsafe impl ProtocolTask for BadMoment {
    unsafe fn poll(&mut self) -> TaskPoll {
        TaskPoll::Pending
    }

    fn wake_at(&self) -> Option<Instant> {
        panic!("no moment")
    }
}

static BAD_MOMENT: ProtocolVTable = ProtocolVTable::live::<BadMoment>();

/// A panic in `wake_at` ends the task as Failed, its message naming the panic, as a panic in
/// `poll` does; the round goes on.
#[test]
fn a_panic_in_wake_at_ends_the_task_as_failed() {
    let mut sched = scheduler();
    let key = VTableKey { segment: "time_kit", kind: "bad_moment", version: 1 };
    sched.register_vtable(key, &BAD_MOMENT);
    let task = sched.spawn_box(key, Box::new(BadMoment), TaskPolicy::default()).unwrap();
    let other = sched.spawn_box(COUNTER_KEY, counter(), TaskPolicy::default()).unwrap();
    sched.run_once().unwrap();
    assert_eq!(sched.finished, [(task.id(), TaskState::Failed("task panicked in wake_at: no moment".into()))]);
    assert_eq!(sched.tasks.get(other.id()).unwrap().stats.poll_count, 1);
}
