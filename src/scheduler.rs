use crate::capitulate::{CapitulateStrategy, DefaultCapitulateStrategy, ReclaimReport, TaskSnapshot};
use crate::checkpoint::CheckpointStore;
use crate::envelope::{typed_envelope, EnvelopePacket};
use crate::policy::{SchedulerConfig, WakeOrdering};
use crate::slab::TaskSlab;
use crate::task::{ProtocolTask, ProtocolTaskPtr, ProtocolVTable};
use crate::types::{CheckpointReason, SchedulerMsg, TaskId, TaskPoll, TaskState, TaskStats, WakeReason};
use crate::{CapitulatePlan, TaskPolicy};
use crate::unwind::catch;
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::marker::PhantomData;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use waker_tables::VTableKey;

/// Task vtables by key. Registering replaces; tasks already spawned keep theirs.
pub struct ProtocolVTableRegistry {
    inner: RwLock<BTreeMap<VTableKey, &'static ProtocolVTable>>,
}

impl Default for ProtocolVTableRegistry {
    fn default() -> Self {
        Self {
            inner: RwLock::new(BTreeMap::new()),
        }
    }
}

impl ProtocolVTableRegistry {
    /// Register `vtable` under `key`.
    pub fn register(&self, key: VTableKey, vtable: &'static ProtocolVTable) {
        let mut guard = self.inner.write().expect("protocol vtable registry poisoned");
        guard.insert(key, vtable);
    }

    /// The vtable under `key`.
    pub fn get(&self, key: &VTableKey) -> Option<&'static ProtocolVTable> {
        let guard = self.inner.read().expect("protocol vtable registry poisoned");
        guard.get(key).copied()
    }
}

/// Where a task's memory is.
pub enum Resident {
    /// In memory.
    Live(ProtocolTaskPtr),
    /// Checkpointed out; restored when woken (by policy).
    Dormant,
    /// Out of its row while the scheduler works on it. No row is left this way when `run_once` or
    /// `pump` returns. A panic in the task's own `poll`, `on_wake`, `checkpoint`, `restore` or
    /// `footprint_estimate` ends the task as Failed; a panic in a message's `deliver` loses only
    /// that message; a panic from the store's `save` or from the capitulation strategy reaches the
    /// caller with the task put back in its row, live. The store's `load` and `remove` are never
    /// called while the task is out of its row (see [`run_once`](WakerProtocol::run_once)).
    Empty,
}

/// One task's row in the scheduler.
pub struct TaskEntry<H> {
    /// The key it was spawned under.
    pub key: VTableKey,
    /// Its type's vtable, used to restore it.
    pub vtable: &'static ProtocolVTable,
    /// Where its memory is.
    pub resident: Resident,

    /// Its state.
    pub state: TaskState,
    /// Its policy.
    pub policy: TaskPolicy,
    /// Its counters.
    pub stats: TaskStats,

    /// Messages waiting for its next poll.
    pub mailbox: VecDeque<EnvelopePacket>,
    /// Wakes waiting for its next poll.
    pub pending_wakes: VecDeque<WakeReason>,
    /// Its checkpoints, oldest first. One the store failed to remove stays here, and a later
    /// prune tries it again.
    pub checkpoint_history: VecDeque<H>,

    /// When it last became runnable; breaks ties in the poll order.
    pub runnable_seq: u64,
}

impl<H> TaskEntry<H> {
    fn new(
        key: VTableKey,
        vtable: &'static ProtocolVTable,
        resident: Resident,
        policy: TaskPolicy,
        runnable_seq: u64,
    ) -> Self {
        let mut pending_wakes = VecDeque::new();
        pending_wakes.push_back(WakeReason::Spawn);

        Self {
            key,
            vtable,
            resident,
            state: TaskState::Runnable,
            policy,
            stats: TaskStats::new(),
            mailbox: VecDeque::new(),
            pending_wakes,
            checkpoint_history: VecDeque::new(),
            runnable_seq,
        }
    }

    /// Dormant, and kept so by its policy: `auto_restore_on_wake` is off.
    fn stays_dormant(&self) -> bool {
        matches!(self.resident, Resident::Dormant) && !self.policy.auto_restore_on_wake
    }

    /// Keep `reason` for the task's next poll and make it runnable; a task that stays dormant only
    /// keeps the wake.
    fn wake(&mut self, reason: WakeReason, seq: u64) {
        self.pending_wakes.push_back(reason);
        if !self.stays_dormant() {
            self.state = TaskState::Runnable;
            self.runnable_seq = seq;
        }
    }

    /// Whether it has work a poll should act on: a message, a wake other than `QoS` or
    /// `MemoryPressure` (those ask it to make room, and a checkpoint answers them), or, with
    /// nothing waiting, a runnable state (it yielded).
    fn has_work(&self) -> bool {
        let asks_for_room = |reason: &WakeReason| matches!(reason, WakeReason::QoS | WakeReason::MemoryPressure);
        !self.mailbox.is_empty()
            || self.pending_wakes.iter().any(|reason| !asks_for_room(reason))
            || (self.pending_wakes.is_empty() && matches!(self.state, TaskState::Runnable))
    }

    /// Whether the next round polls it (restoring it first if it is dormant).
    fn is_runnable(&self) -> bool {
        !self.stays_dormant()
            && (matches!(self.state, TaskState::Runnable)
                || !self.mailbox.is_empty()
                || !self.pending_wakes.is_empty())
    }

    fn latest_checkpoint(&self) -> Option<&H> {
        self.checkpoint_history.back()
    }

    /// Take the task out of its row if it is live, leaving `Resident::Empty` until it is settled.
    fn take_live(&mut self) -> Option<ProtocolTaskPtr> {
        match std::mem::replace(&mut self.resident, Resident::Empty) {
            Resident::Live(live) => Some(live),
            other => {
                self.resident = other;
                None
            }
        }
    }
}

/// A live task out of its row while the scheduler calls the task, the store's `save` or the
/// strategy. If the guard is dropped still holding the task, it puts the task back in its row,
/// live: a panic from `save` or the strategy reaches the caller with the scheduler consistent, and
/// a save that fails leaves the task as it was.
struct OutOfRow<'a> {
    row: &'a mut Resident,
    task: Option<ProtocolTaskPtr>,
}

impl<'a> OutOfRow<'a> {
    /// Hold `task`, taken out of `row`.
    fn hold(row: &'a mut Resident, task: ProtocolTaskPtr) -> Self {
        Self { row, task: Some(task) }
    }

    fn task(&mut self) -> &mut ProtocolTaskPtr {
        self.task.as_mut().expect("the guard holds its task until `release`")
    }

    /// The task, for the scheduler to settle; the guard no longer puts it back.
    fn release(mut self) -> ProtocolTaskPtr {
        self.task.take().expect("the guard holds its task until `release`")
    }
}

impl Drop for OutOfRow<'_> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            *self.row = Resident::Live(task);
        }
    }
}

/// Run `call`, the task's own function `name`. A panic in it is caught and becomes the message
/// the task fails with. No panic leaves this function, not even one from dropping the payload.
fn caught<R>(name: &str, call: impl FnOnce() -> R) -> Result<R, String> {
    catch(call).map_err(|text| format!("task panicked in {name}: {text}"))
}

/// Free `live`, task `id`'s live copy: its `cleanup`, then its drop. A panic in either is caught
/// and returned as the error; the copy is freed either way. Every live copy `run_once` and `pump`
/// free goes through here, once its row is settled and before the store is asked to remove
/// anything.
fn free(id: TaskId, live: ProtocolTaskPtr) -> Result<(), String> {
    catch(move || drop(live)).map_err(|text| format!("task {id:?} panicked in cleanup or drop: {text}"))
}

/// Drop `envelope`, a message queued for task `id`, which is ending. A panic in the message's
/// drop is caught and returned as the error.
fn drop_message(id: TaskId, envelope: EnvelopePacket) -> Result<(), String> {
    let kind = envelope.kind();
    catch(move || drop(envelope)).map_err(|text| format!("message {kind:?} for task {id:?} panicked as it was dropped: {text}"))
}

/// Every error met while a round runs. A round never stops at an error: it finishes, then
/// returns them all, joined with `"; "`.
#[derive(Default)]
struct Errors(Vec<String>);

impl Errors {
    fn note(&mut self, result: Result<(), String>) {
        if let Err(err) = result {
            self.0.push(err);
        }
    }

    fn finish(self) -> Result<(), String> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(self.0.join("; "))
        }
    }
}

/// Where a task goes once the scheduler is done with it for now. Every path that takes a task
/// out of its row ends in `WakerProtocol::settle` with one of these.
enum Settle {
    /// Live in its row, waiting for a wake.
    Wait,
    /// Live in its row and runnable, behind the tasks already waiting to run.
    RunAgain,
    /// Checkpointed and dormant, restored when woken.
    Checkpoint(CheckpointReason),
    /// Checkpointed in place of a poll the task had work for: dormant but runnable, so the next
    /// round restores and polls it.
    CheckpointAndRunAgain(CheckpointReason),
    /// Ended in this state and removed; its checkpoints follow the retention policy.
    Finish(TaskState),
}

impl Settle {
    /// A capitulation: keep a stub and go dormant, or end as Completed.
    fn capitulate(plan: CapitulatePlan) -> Self {
        if plan.retain_stub {
            Self::Checkpoint(plan.reason)
        } else {
            Self::Finish(TaskState::Completed)
        }
    }

    /// A strategy's plan, carried out in place of a poll. A task it checkpoints that had work
    /// waiting stays runnable, so the plan puts the poll off to the next round but never drops it.
    fn in_place_of_poll(plan: CapitulatePlan, had_work: bool) -> Self {
        match Self::capitulate(plan) {
            Self::Checkpoint(reason) if had_work => Self::CheckpointAndRunAgain(reason),
            settle => settle,
        }
    }

    /// What a poll result means for the task.
    fn after_poll(poll: TaskPoll, policy: &TaskPolicy) -> Self {
        match poll {
            TaskPoll::Ready => Self::Finish(TaskState::Completed),
            TaskPoll::Pending | TaskPoll::Park => Self::Wait,
            TaskPoll::Yield if policy.eager_repoll_after_yield => Self::RunAgain,
            TaskPoll::Yield => Self::Wait,
            TaskPoll::Checkpoint(reason) => Self::Checkpoint(reason),
            TaskPoll::Capitulate(plan) => Self::capitulate(plan),
            TaskPoll::Cancelled => Self::Finish(TaskState::Cancelled),
            TaskPoll::Failed(err) => Self::Finish(TaskState::Failed(err)),
        }
    }
}

#[derive(Clone)]
/// Sends messages to a scheduler from anywhere.
pub struct SchedulerHandle {
    tx: Sender<SchedulerMsg>,
}

impl SchedulerHandle {
    /// Wake task `id`.
    pub fn wake(&self, id: TaskId, reason: WakeReason) -> Result<(), String> {
        self.tx
            .send(SchedulerMsg::Wake { id, reason })
            .map_err(|e| e.to_string())
    }

    /// Checkpoint task `id`.
    pub fn checkpoint(&self, id: TaskId, reason: CheckpointReason) -> Result<(), String> {
        self.tx
            .send(SchedulerMsg::Checkpoint { id, reason })
            .map_err(|e| e.to_string())
    }

    /// Make task `id` capitulate.
    pub fn capitulate(&self, id: TaskId, plan: CapitulatePlan) -> Result<(), String> {
        self.tx
            .send(SchedulerMsg::Capitulate { id, plan })
            .map_err(|e| e.to_string())
    }

    /// Cancel task `id`.
    pub fn cancel(&self, id: TaskId) -> Result<(), String> {
        self.tx
            .send(SchedulerMsg::Cancel { id })
            .map_err(|e| e.to_string())
    }

    /// Checkpoint every live task and stop; see [`SchedulerMsg::Shutdown`].
    pub fn shutdown(&self) -> Result<(), String> {
        self.tx.send(SchedulerMsg::Shutdown).map_err(|e| e.to_string())
    }

    /// A thread that wakes task `id` with `WakeReason::Timer` every `every`, until the scheduler is gone.
    pub fn spawn_interval_waker(
        &self,
        id: TaskId,
        every: Duration,
    ) -> std::thread::JoinHandle<()> {
        let tx = self.tx.clone();
        std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(every);
            while ticker.recv().is_ok() {
                if tx
                    .send(SchedulerMsg::Wake {
                        id,
                        reason: WakeReason::Timer,
                    })
                    .is_err()
                {
                    break;
                }
            }
        })
    }
}

/// A handle to one spawned task of type `T`.
pub struct TaskHandle<T> {
    id: TaskId,
    tx: Sender<SchedulerMsg>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for TaskHandle<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            tx: self.tx.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: ProtocolTask> TaskHandle<T> {
    /// The task's id.
    pub fn id(&self) -> TaskId {
        self.id
    }

    /// Wake the task.
    pub fn wake(&self, reason: WakeReason) -> Result<(), String> {
        self.tx
            .send(SchedulerMsg::Wake {
                id: self.id,
                reason,
            })
            .map_err(|e| e.to_string())
    }

    /// Checkpoint the task.
    pub fn checkpoint(&self, reason: CheckpointReason) -> Result<(), String> {
        self.tx
            .send(SchedulerMsg::Checkpoint {
                id: self.id,
                reason,
            })
            .map_err(|e| e.to_string())
    }

    /// Make the task capitulate.
    pub fn capitulate(&self, plan: CapitulatePlan) -> Result<(), String> {
        self.tx
            .send(SchedulerMsg::Capitulate {
                id: self.id,
                plan,
            })
            .map_err(|e| e.to_string())
    }

    /// Cancel the task.
    pub fn cancel(&self) -> Result<(), String> {
        self.tx
            .send(SchedulerMsg::Cancel { id: self.id })
            .map_err(|e| e.to_string())
    }

    /// Send the task a message; `deliver` runs with the task before its next poll, after the
    /// messages sent before it. If `deliver` returns `Err` or panics, only this message is lost:
    /// the task goes on, and `run_once` or `pump` returns the error (for a panic, one naming the
    /// message, the task and the panic) once its round is done.
    pub fn send<M: Send + 'static>(
        &self,
        kind: &'static str,
        msg: M,
        deliver: fn(&mut T, M) -> Result<(), String>,
    ) -> Result<(), String> {
        self.tx
            .send(SchedulerMsg::Deliver {
                id: self.id,
                envelope: typed_envelope::<T, M>(kind, msg, deliver),
            })
            .map_err(|e| e.to_string())
    }
}

/// The scheduler.
pub struct WakerProtocol<S: CheckpointStore> {
    /// Task vtables by key.
    pub registry: ProtocolVTableRegistry,
    /// The tasks.
    pub tasks: TaskSlab<TaskEntry<S::Handle>>,
    /// Every reclaim so far.
    pub reclaim_log: Vec<ReclaimReport<S::Handle>>,
    /// Every task the scheduler has removed, oldest first, with the state it ended in:
    /// `Completed`, `Cancelled`, or `Failed` with its message. A task that panicked in its own
    /// `poll`, `on_wake`, `checkpoint`, `restore` or `footprint_estimate` is here as `Failed`, its
    /// message naming the panic. Like `reclaim_log` it only grows; clear it once read.
    pub finished: Vec<(TaskId, TaskState)>,
    /// Settings.
    pub config: SchedulerConfig,
    /// Where checkpoints go.
    pub store: S,
    /// Set by a Shutdown message; while it is set, `run_once` and `pump` do nothing.
    pub shutdown: bool,

    strategy: Box<dyn CapitulateStrategy>,
    /// Tasks restored from a checkpoint and not polled since; the strategy is not asked about them.
    restored: HashSet<TaskId>,
    tx: Sender<SchedulerMsg>,
    rx: Receiver<SchedulerMsg>,
    seq: u64,
    rr_cursor: usize,
}

impl<S: CheckpointStore> WakerProtocol<S> {
    /// A scheduler with the default capitulation strategy.
    pub fn new(store: S) -> Self {
        Self::with_strategy(store, Box::new(DefaultCapitulateStrategy::default()))
    }

    /// A scheduler with your own capitulation strategy.
    pub fn with_strategy(store: S, strategy: Box<dyn CapitulateStrategy>) -> Self {
        let (tx, rx) = unbounded();

        Self {
            registry: ProtocolVTableRegistry::default(),
            tasks: TaskSlab::default(),
            reclaim_log: Vec::new(),
            finished: Vec::new(),
            config: SchedulerConfig::default(),
            store,
            shutdown: false,
            strategy,
            restored: HashSet::new(),
            tx,
            rx,
            seq: 1,
            rr_cursor: 0,
        }
    }

    /// The scheduler's own channel, for [`scheduler_waker`](crate::scheduler_waker) or a hand-built
    /// [`SchedulerMsg`]. A message for a task of another type is refused when its turn comes: only
    /// that message is dropped, the task goes on, and `run_once` or `pump` returns the error once
    /// its round is done. A message whose `dispatch` panics is dropped the same way.
    pub fn sender(&self) -> Sender<SchedulerMsg> {
        self.tx.clone()
    }

    /// A handle for sending messages to this scheduler.
    pub fn handle(&self) -> SchedulerHandle {
        SchedulerHandle {
            tx: self.tx.clone(),
        }
    }

    /// Register `vtable` under `key`.
    pub fn register_vtable(&self, key: VTableKey, vtable: &'static ProtocolVTable) {
        self.registry.register(key, vtable);
    }

    /// Spawn `task` with the vtable registered under `key`.
    ///
    /// Refuses a key that is not registered, or whose vtable runs another type.
    pub fn spawn_box<T: ProtocolTask>(
        &mut self,
        key: VTableKey,
        task: Box<T>,
        policy: TaskPolicy,
    ) -> Result<TaskHandle<T>, String> {
        let vtable = self
            .registry
            .get(&key)
            .ok_or_else(|| format!("no protocol vtable registered for {:?}", key))?;
        if !vtable.is_for::<T>() {
            return Err(format!(
                "protocol vtable for {:?} is for {}, not {}",
                key,
                vtable.type_name(),
                std::any::type_name::<T>()
            ));
        }

        let entry = TaskEntry::new(
            key,
            vtable,
            Resident::Live(ProtocolTaskPtr::from_box(task, vtable)),
            policy,
            self.bump_seq(),
        );

        let id = self.tasks.insert(entry);

        Ok(TaskHandle {
            id,
            tx: self.tx.clone(),
            _marker: PhantomData,
        })
    }

    /// Handle every queued message, then poll every runnable task once.
    ///
    /// An error does not stop the round. A message refused for being written for another task
    /// type, or one whose `deliver` returns `Err`, is dropped and its task goes on; a checkpoint
    /// that cannot be saved leaves its task live in its row as it was; a task whose checkpoint
    /// cannot be loaded stays dormant. Once the round is done, every error it met is returned,
    /// joined with `"; "`.
    ///
    /// A panic in a task's own `poll`, `on_wake`, `checkpoint`, `restore` or `footprint_estimate`
    /// is caught: the task ends as `TaskState::Failed`, with a message naming the panic, and is
    /// removed like any failed task (see [`finished`](Self::finished)); the round goes on. A panic
    /// in a message's `deliver` is caught and treated as a `deliver` that returned `Err`: only that
    /// message is lost, the task stays live and is polled, and the error is returned after the
    /// round. A panic in a task's `cleanup` or `Drop`, or in the drop of a message queued for a
    /// task that is ending, is caught as the task is freed: the task is freed anyway, its row is
    /// left removed (the task's end) or dormant (a checkpoint), the error is returned after the
    /// round, and the round goes on.
    ///
    /// A panic from the store's `save` or from the capitulation strategy reaches the caller, with
    /// the task put back in its row, live.
    ///
    /// A panic from the store's `load` also reaches the caller, and the task stays dormant. The
    /// store's `remove` is called only once the task is dormant (a checkpoint) or removed and
    /// freed, its queued messages dropped (the task's end), so a panic from it reaches the caller
    /// after all of that; the checkpoints it had not yet removed stay in the store, and in a
    /// dormant task's history, where a later prune tries them again.
    ///
    /// After a Shutdown message has been handled this does nothing: later messages stay in the
    /// channel, and no task is restored or polled.
    pub fn run_once(&mut self) -> Result<(), String> {
        let mut errors = Errors::default();
        self.round(&mut errors);
        errors.finish()
    }

    /// Wait up to `timeout` for a message, then run a round as [`run_once`](Self::run_once) does.
    ///
    /// Returns `Ok(true)` while the scheduler runs and `Ok(false)` once a Shutdown message has
    /// been handled; from then on it returns `Ok(false)` at once and does nothing. Dropping every
    /// handle does not stop it: the scheduler holds a sender of its own, so only a Shutdown
    /// message makes it return false. An `Err` carries every error of the round, after the round.
    ///
    /// ```
    /// # use std::time::Duration;
    /// # use waker_protocol::{MemoryCheckpointStore, WakerProtocol};
    /// let mut sched = WakerProtocol::new(MemoryCheckpointStore::default());
    /// drop(sched.handle());
    /// assert!(sched.pump(Duration::from_millis(1))?, "every handle is dropped; it still runs");
    /// sched.handle().shutdown()?;
    /// assert!(!sched.pump(Duration::from_millis(1))?);
    /// assert!(!sched.pump(Duration::from_millis(1))?);
    /// # Ok::<(), String>(())
    /// ```
    pub fn pump(&mut self, timeout: Duration) -> Result<bool, String> {
        if self.shutdown {
            return Ok(false);
        }

        let mut errors = Errors::default();
        // The scheduler holds a sender itself, so this ends with a message or the timeout.
        if let Ok(msg) = self.rx.recv_timeout(timeout) {
            self.process_msg(msg, &mut errors);
        }
        self.round(&mut errors);
        errors.finish().map(|()| !self.shutdown)
    }

    fn bump_seq(&mut self) -> u64 {
        let out = self.seq;
        self.seq = self.seq.wrapping_add(1);
        out
    }

    /// Every queued message, then every runnable task once; after Shutdown, nothing.
    fn round(&mut self, errors: &mut Errors) {
        self.drain_inbox(errors);
        if !self.shutdown {
            self.poll_ready(errors);
        }
    }

    /// Handle queued messages up to and including a Shutdown; what comes after it stays queued.
    fn drain_inbox(&mut self, errors: &mut Errors) {
        while !self.shutdown {
            match self.rx.try_recv() {
                Ok(msg) => self.process_msg(msg, errors),
                Err(_) => break,
            }
        }
    }

    fn process_msg(&mut self, msg: SchedulerMsg, errors: &mut Errors) {
        match msg {
            SchedulerMsg::Wake { id, reason } => {
                let seq = self.bump_seq();
                if let Some(entry) = self.tasks.get_mut(id) {
                    entry.wake(reason, seq);
                }
            }
            SchedulerMsg::Deliver { id, envelope } => {
                let seq = self.bump_seq();
                if let Some(entry) = self.tasks.get_mut(id) {
                    entry.mailbox.push_back(envelope);
                    entry.wake(WakeReason::Inbox, seq);
                }
            }
            SchedulerMsg::Checkpoint { id, reason } => {
                errors.note(self.force(id, Settle::Checkpoint(reason)));
            }
            SchedulerMsg::Capitulate { id, plan } => {
                errors.note(self.force(id, Settle::capitulate(plan)));
            }
            SchedulerMsg::Cancel { id } => {
                errors.note(self.force(id, Settle::Finish(TaskState::Cancelled)));
            }
            SchedulerMsg::Shutdown => {
                self.shutdown = true;
                for id in self.tasks.live_ids() {
                    errors.note(self.force(id, Settle::Checkpoint(CheckpointReason::Shutdown)));
                }
            }
        }
    }

    fn order_ready_ids(&mut self) -> Vec<TaskId> {
        let mut ids: Vec<_> = self
            .tasks
            .live_ids()
            .into_iter()
            .filter(|id| self.tasks.get(*id).map(|e| e.is_runnable()).unwrap_or(false))
            .collect();

        match self.config.wake_ordering {
            WakeOrdering::UnbiasedRoundRobin => {
                if !ids.is_empty() {
                    let rotate = self.rr_cursor % ids.len();
                    ids.rotate_left(rotate);
                    self.rr_cursor = self.rr_cursor.wrapping_add(1);
                }
            }
            WakeOrdering::BiasedQoS => {
                ids.sort_by(|a, b| {
                    let ea = self.tasks.get(*a).unwrap();
                    let eb = self.tasks.get(*b).unwrap();

                    eb.policy
                        .qos
                        .priority()
                        .cmp(&ea.policy.qos.priority())
                        .then_with(|| ea.runnable_seq.cmp(&eb.runnable_seq))
                });
            }
            WakeOrdering::BiasedQoSThenAge => {
                ids.sort_by(|a, b| {
                    let ea = self.tasks.get(*a).unwrap();
                    let eb = self.tasks.get(*b).unwrap();

                    eb.policy
                        .qos
                        .priority()
                        .cmp(&ea.policy.qos.priority())
                        .then_with(|| {
                            ea.stats
                                .last_polled_at
                                .cmp(&eb.stats.last_polled_at)
                        })
                        .then_with(|| ea.runnable_seq.cmp(&eb.runnable_seq))
                });
            }
        }

        ids
    }

    /// Poll every runnable task once. A task that finishes is removed at once, so an error later
    /// in the round never leaves it behind.
    fn poll_ready(&mut self, errors: &mut Errors) {
        for id in self.order_ready_ids() {
            // A task that cannot be restored stays dormant, and poll_one leaves it alone.
            errors.note(self.restore_if_needed(id));
            self.poll_one(id, errors);
        }
    }

    fn restore_if_needed(&mut self, id: TaskId) -> Result<(), String> {
        let (should_restore, vtable, latest_handle) = match self.tasks.get(id) {
            // `is_runnable` is false for a task its policy keeps dormant.
            Some(entry) => (
                matches!(entry.resident, Resident::Dormant) && entry.is_runnable(),
                entry.vtable,
                entry.latest_checkpoint().cloned(),
            ),
            None => return Ok(()),
        };

        if !should_restore {
            return Ok(());
        }

        let handle = latest_handle.ok_or_else(|| format!("dormant task {:?} missing checkpoint", id))?;
        let bytes = self.store.load(&handle)?;
        let restored = match caught("restore", || ProtocolTaskPtr::restore_from_bytes(vtable, &bytes)) {
            Ok(restored) => restored,
            // These bytes will never restore: the task ends as Failed, and retain_on_failure
            // decides whether they stay in the store.
            Err(panic) => return self.finish(id, TaskState::Failed(panic), None),
        };

        let Some(entry) = self.tasks.get_mut(id) else { return free(id, restored) };
        entry.resident = Resident::Live(restored);
        entry.state = TaskState::Runnable;
        entry.stats.restore_count += 1;
        entry.pending_wakes.push_back(WakeReason::Resume);
        self.restored.insert(id);

        Ok(())
    }

    /// What the capitulation strategy sees of task `id`, read with the task in its row; `None`
    /// unless the task is live, and `Err` naming the panic if its `footprint_estimate` panics.
    fn snapshot(id: TaskId, entry: &TaskEntry<S::Handle>) -> Option<Result<TaskSnapshot, String>> {
        let Resident::Live(live) = &entry.resident else { return None };
        let now = Instant::now();

        Some(caught("footprint_estimate", || live.footprint_estimate()).map(|footprint_bytes| TaskSnapshot {
            id,
            qos: entry.policy.qos,
            state: entry.state.clone(),
            footprint_bytes,
            mailbox_len: entry.mailbox.len(),
            pending_wakes: entry.pending_wakes.len(),
            poll_count: entry.stats.poll_count,
            idle_for: entry.stats.last_polled_at.map(|t| now.saturating_duration_since(t)),
            memory_budget_hint: entry.policy.memory_budget_hint,
        }))
    }

    /// Deliver task `id`'s messages and wakes, ask the strategy, then poll it. Every error is
    /// noted in `errors`, and the task always ends settled: live, dormant or removed. A panic in
    /// the task's `footprint_estimate`, `on_wake` or `poll` ends it as Failed; a panic in a
    /// message's `deliver` loses only that message; a panic from the strategy reaches the caller
    /// with the task back in its row, live.
    fn poll_one(&mut self, id: TaskId, errors: &mut Errors) {
        // A task restored for this round is polled before the strategy is asked about it again.
        let restored = self.restored.contains(&id);
        let Some(entry) = self.tasks.get_mut(id) else { return };
        // The strategy sees the task as it was before this round's messages and wakes.
        let snapshot = if restored { None } else { Self::snapshot(id, entry) };
        let last_wake = entry.pending_wakes.back().or(entry.stats.last_wake.as_ref()).cloned();
        let had_work = entry.has_work();
        let Some(live) = entry.take_live() else { return };
        let mut out = OutOfRow::hold(&mut entry.resident, live);

        let settle = 'settle: {
            let snapshot = match snapshot.transpose() {
                Ok(snapshot) => snapshot,
                Err(panic) => break 'settle Settle::Finish(TaskState::Failed(panic)),
            };
            while let Some(envelope) = entry.mailbox.pop_front() {
                errors.note(Self::deliver(id, out.task(), envelope));
            }
            while let Some(reason) = entry.pending_wakes.pop_front() {
                entry.stats.last_wake = Some(reason.clone());
                let task = out.task();
                // SAFETY: the task is out of its row, so this call has exclusive access to a live task (ProtocolTask's contract).
                if let Err(panic) = caught("on_wake", || unsafe { task.on_wake(reason) }) {
                    break 'settle Settle::Finish(TaskState::Failed(panic));
                }
            }
            if let Some(plan) = snapshot.and_then(|snapshot| self.strategy.plan(&snapshot, last_wake.as_ref())) {
                break 'settle Settle::in_place_of_poll(plan, had_work);
            }

            self.restored.remove(&id);
            entry.stats.poll_count += 1;
            entry.stats.last_polled_at = Some(Instant::now());
            let task = out.task();
            // SAFETY: the task is out of its row, so this call has exclusive access to a live task (ProtocolTask's contract).
            match caught("poll", || unsafe { task.poll() }) {
                Ok(poll) => Settle::after_poll(poll, &entry.policy),
                Err(panic) => Settle::Finish(TaskState::Failed(panic)),
            }
        };
        let live = out.release();
        errors.note(self.settle(id, Some(live), settle));
    }

    /// Hand `envelope` to `live`, task `id`, or refuse it if it was written for another task
    /// type. Either way the envelope is used up; the task is untouched by a refusal. A panic in
    /// the message's `deliver` (or in its drop, once refused) is caught and returned as the
    /// error, as an `Err` from `deliver` is: only this message is lost, and the task goes on.
    fn deliver(id: TaskId, live: &mut ProtocolTaskPtr, envelope: EnvelopePacket) -> Result<(), String> {
        let kind = envelope.kind();
        catch(move || {
            if envelope.target_type() != live.task_type_id() {
                return Err(format!(
                    "envelope {:?} for task {:?} targets another task type than {}",
                    kind,
                    id,
                    live.vtable().type_name()
                ));
            }
            // SAFETY: the envelope targets exactly the type `live` holds (checked above), and
            // `live` is out of its row, so this access is exclusive; `raw_data` itself has no
            // requirement beyond that use.
            unsafe { envelope.dispatch(live.raw_data()) }
        })
        .unwrap_or_else(|text| Err(format!("message {kind:?} for task {id:?} panicked: {text}")))
    }

    /// Carry out `settle` on task `id` from outside a poll: a message, or Shutdown.
    fn force(&mut self, id: TaskId, settle: Settle) -> Result<(), String> {
        let live = self.tasks.get_mut(id).and_then(TaskEntry::take_live);
        self.settle(id, live, settle)
    }

    /// Put task `id` where `settle` says; the one place a task is put back, checkpointed or
    /// removed. `live` is the task when it was taken out of its row, `None` when it is dormant.
    fn settle(&mut self, id: TaskId, live: Option<ProtocolTaskPtr>, settle: Settle) -> Result<(), String> {
        let Some(live) = live else {
            // Dormant: its checkpoint is already in the store, so only Finish changes anything.
            return match settle {
                Settle::Finish(state) => self.finish(id, state, None),
                Settle::Wait | Settle::RunAgain | Settle::Checkpoint(_) | Settle::CheckpointAndRunAgain(_) => Ok(()),
            };
        };

        match settle {
            Settle::Wait => self.put_back(id, live, TaskState::Waiting, None),
            Settle::RunAgain => {
                let seq = self.bump_seq();
                self.put_back(id, live, TaskState::Runnable, Some(seq))
            }
            Settle::Checkpoint(reason) => self.checkpoint_live(id, live, reason, false),
            Settle::CheckpointAndRunAgain(reason) => self.checkpoint_live(id, live, reason, true),
            Settle::Finish(state) => self.finish(id, state, Some(live)),
        }
    }

    /// Put `live` back in task `id`'s row as `state`; with `seq`, it waits to run behind the tasks
    /// already waiting. With no row left to put it in, it is freed.
    fn put_back(&mut self, id: TaskId, live: ProtocolTaskPtr, state: TaskState, seq: Option<u64>) -> Result<(), String> {
        let Some(entry) = self.tasks.get_mut(id) else { return free(id, live) };
        entry.resident = Resident::Live(live);
        entry.state = state;
        if let Some(seq) = seq {
            entry.runnable_seq = seq;
        }
        Ok(())
    }

    /// Checkpoint `live`, task `id`, and leave the task dormant; with `run_again` its state stays
    /// `Runnable`, so the next round restores and polls it. The task is freed only once its bytes
    /// are saved: if the save fails, or panics, it goes back into its row, live and as it was, and
    /// the error (or the panic) goes to the caller. If the task's own `footprint_estimate` or
    /// `checkpoint` panics, the task ends as Failed. Once saved, the row is made dormant, then the
    /// live copy is freed (a panic in its `cleanup` or drop is returned as an error; the task stays
    /// dormant), and only then are older checkpoints pruned: a panic from the store's `remove`
    /// comes once the task is dormant and its live copy freed, and leaves the checkpoints not yet
    /// removed in its history.
    fn checkpoint_live(
        &mut self,
        id: TaskId,
        live: ProtocolTaskPtr,
        reason: CheckpointReason,
        run_again: bool,
    ) -> Result<(), String> {
        // Kept runnable, it waits behind the tasks already waiting to run, as after a Yield.
        let run_again = run_again.then(|| self.bump_seq());
        let Some(entry) = self.tasks.get_mut(id) else { return Ok(()) };
        let mut out = OutOfRow::hold(&mut entry.resident, live);
        let task = out.task();
        let checkpointed = caught("footprint_estimate", || task.footprint_estimate()).and_then(|before_bytes| {
            // SAFETY: the task is out of its row, so this call has exclusive access to a live task (ProtocolTask's contract).
            caught("checkpoint", || unsafe { task.checkpoint() }).map(|payload| (before_bytes, payload))
        });
        let (before_bytes, payload) = match checkpointed {
            Ok(checkpointed) => checkpointed,
            Err(panic) => {
                let live = out.release();
                return self.settle(id, Some(live), Settle::Finish(TaskState::Failed(panic)));
            }
        };
        // If the save fails, `out` puts the task back in its row as it returns the error; so it
        // does if the store's code panics in `save` or in cloning the handle.
        let handle = self.store.save(id, payload)?;
        let logged = handle.clone();
        let live = out.release();

        entry.checkpoint_history.push_back(handle);
        entry.stats.checkpoint_count += 1;
        entry.resident = Resident::Dormant;
        match run_again {
            Some(seq) => {
                entry.state = TaskState::Runnable;
                entry.runnable_seq = seq;
            }
            None => entry.state = TaskState::Dormant,
        }
        let mut errors = Errors::default();
        errors.note(free(id, live));

        let after_bytes = std::mem::size_of::<TaskId>();
        self.reclaim_log.push(ReclaimReport {
            id,
            reason,
            before_bytes,
            after_bytes,
            reclaimed_bytes: before_bytes.saturating_sub(after_bytes),
            handle: Some(logged),
        });
        // A dormant task keeps at least the checkpoint it will be restored from.
        let keep = entry.policy.retention.keep_latest.max(1);
        errors.note(Self::prune(&self.store, &mut entry.checkpoint_history, keep));
        errors.finish()
    }

    /// End task `id` as `state`, record it in `finished` and remove its row; the one way a task
    /// leaves the scheduler. `live` is its live copy when it was taken out of its row.
    ///
    /// The row is removed first. Then the task's live copy (`live`, or one still in the row) is
    /// freed and its queued messages are dropped, each on its own, a panic in any of them returned
    /// as an error. Only then are its checkpoints removed from the store, so a panic from the
    /// store's `remove` never unwinds through the task or its messages. Its checkpoints go, unless
    /// the retention policy keeps them for `state`; then the latest `keep_latest` stay in the store.
    fn finish(&mut self, id: TaskId, state: TaskState, live: Option<ProtocolTaskPtr>) -> Result<(), String> {
        let mut errors = Errors::default();
        let Some(entry) = self.tasks.remove(id) else {
            errors.note(live.map_or(Ok(()), |live| free(id, live)));
            return errors.finish();
        };
        self.restored.remove(&id);
        let TaskEntry { resident, mailbox, mut checkpoint_history, policy, .. } = entry;
        let retention = &policy.retention;
        let retain = match &state {
            TaskState::Completed => retention.retain_on_completion,
            TaskState::Failed(_) => retention.retain_on_failure,
            TaskState::Cancelled => retention.retain_on_cancel,
            TaskState::Runnable | TaskState::Waiting | TaskState::Dormant => false,
        };
        let keep = if retain { retention.keep_latest } else { 0 };
        self.finished.push((id, state));

        let in_row = match resident {
            Resident::Live(in_row) => Some(in_row),
            Resident::Dormant | Resident::Empty => None,
        };
        for live in live.into_iter().chain(in_row) {
            errors.note(free(id, live));
        }
        for envelope in mailbox {
            errors.note(drop_message(id, envelope));
        }
        errors.note(Self::prune(&self.store, &mut checkpoint_history, keep));
        errors.finish()
    }

    /// Remove all but the latest `keep` checkpoints of `history` from the store, oldest first. A
    /// handle the store fails to remove stays in `history`, so a later prune tries it again;
    /// every one is tried, and their errors are returned.
    fn prune(store: &S, history: &mut VecDeque<S::Handle>, keep: usize) -> Result<(), String> {
        let mut errors = Errors::default();
        let mut at = 0;
        for _ in keep..history.len() {
            let removed = store.remove(&history[at]);
            if removed.is_ok() {
                history.remove(at);
            } else {
                at += 1;
            }
            errors.note(removed);
        }
        errors.finish()
    }
}
