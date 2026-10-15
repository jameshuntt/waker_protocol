use crate::capitulate::{CapitulateStrategy, DefaultCapitulateStrategy, ReclaimReport, TaskSnapshot};
use crate::checkpoint::CheckpointStore;
use crate::envelope::{typed_envelope, EnvelopePacket};
use crate::policy::{SchedulerConfig, WakeOrdering};
use crate::slab::TaskSlab;
use crate::task::{ProtocolTask, ProtocolTaskPtr, ProtocolVTable};
use crate::types::{CheckpointReason, SchedulerMsg, TaskId, TaskPoll, TaskState, TaskStats, WakeReason};
use crate::{CapitulatePlan, TaskPolicy};
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
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
    /// `pump` returns. A panic in the task's own `poll`, `on_wake` or `checkpoint` ends the task as
    /// Failed; a panic from the checkpoint store or the capitulation strategy reaches the caller
    /// with the task put back in its row, live.
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

/// A live task out of its row while the scheduler calls the task, the store or the strategy. If
/// the guard is dropped still holding the task, it puts the task back in its row, live: a panic
/// from the store or the strategy reaches the caller with the scheduler consistent, and a save
/// that fails leaves the task as it was.
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
/// the task fails with.
fn caught<R>(name: &str, call: impl FnOnce() -> R) -> Result<R, String> {
    catch_unwind(AssertUnwindSafe(call)).map_err(|panic| {
        let text = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("a panic payload that is not text");
        format!("task panicked in {name}: {text}")
    })
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
    /// messages sent before it. If `deliver` returns `Err`, only this message is lost: the task
    /// goes on, and `run_once` or `pump` returns the error once its round is done.
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
    /// `poll`, `on_wake` or `checkpoint` is here as `Failed`, its message naming the panic. Like
    /// `reclaim_log` it only grows; clear it once read.
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
    /// its round is done.
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
    /// that cannot be saved leaves its task live in its row as it was; a task that cannot be
    /// restored stays dormant. Once the round is done, every error it met is returned, joined
    /// with `"; "`.
    ///
    /// A panic in a task's own `poll`, `on_wake` or `checkpoint` is caught: the task ends as
    /// `TaskState::Failed`, with a message naming the panic, and is removed like any failed task
    /// (see [`finished`](Self::finished)). A panic from the checkpoint store or the capitulation
    /// strategy reaches the caller, with the task put back in its row, live.
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
        let restored = ProtocolTaskPtr::restore_from_bytes(vtable, &bytes);

        if let Some(entry) = self.tasks.get_mut(id) {
            entry.resident = Resident::Live(restored);
            entry.state = TaskState::Runnable;
            entry.stats.restore_count += 1;
            entry.pending_wakes.push_back(WakeReason::Resume);
            self.restored.insert(id);
        }

        Ok(())
    }

    /// What the capitulation strategy sees of task `id`, read with the task in its row; `None`
    /// unless the task is live.
    fn snapshot(id: TaskId, entry: &TaskEntry<S::Handle>) -> Option<TaskSnapshot> {
        let Resident::Live(live) = &entry.resident else { return None };
        let now = Instant::now();

        Some(TaskSnapshot {
            id,
            qos: entry.policy.qos,
            state: entry.state.clone(),
            footprint_bytes: live.footprint_estimate(),
            mailbox_len: entry.mailbox.len(),
            pending_wakes: entry.pending_wakes.len(),
            poll_count: entry.stats.poll_count,
            idle_for: entry.stats.last_polled_at.map(|t| now.saturating_duration_since(t)),
            memory_budget_hint: entry.policy.memory_budget_hint,
        })
    }

    /// Deliver task `id`'s messages and wakes, ask the strategy, then poll it. Every error is
    /// noted in `errors`, and the task always ends settled: live, dormant or removed. A panic in
    /// the task's `on_wake` or `poll` ends it as Failed; a panic from the strategy reaches the
    /// caller with the task back in its row, live.
    fn poll_one(&mut self, id: TaskId, errors: &mut Errors) {
        // A task restored for this round is polled before the strategy is asked about it again.
        let restored = self.restored.contains(&id);
        let Some(entry) = self.tasks.get_mut(id) else { return };
        // The strategy sees the task as it was before this round's messages and wakes.
        let snapshot = if restored { None } else { Self::snapshot(id, entry) };
        let last_wake = entry.pending_wakes.back().or(entry.stats.last_wake.as_ref()).cloned();
        let Some(live) = entry.take_live() else { return };
        let mut out = OutOfRow::hold(&mut entry.resident, live);

        while let Some(envelope) = entry.mailbox.pop_front() {
            errors.note(Self::deliver(id, out.task(), envelope));
        }
        let mut woken = Ok(());
        while let Some(reason) = entry.pending_wakes.pop_front() {
            entry.stats.last_wake = Some(reason.clone());
            let task = out.task();
            // SAFETY: the task is out of its row, so this call has exclusive access to a live task (ProtocolTask's contract).
            woken = caught("on_wake", || unsafe { task.on_wake(reason) });
            if woken.is_err() {
                break;
            }
        }

        let plan = match (&woken, snapshot) {
            (Ok(()), Some(snapshot)) => self.strategy.plan(&snapshot, last_wake.as_ref()),
            _ => None,
        };
        let settle = match (woken, plan) {
            (Err(panic), _) => Settle::Finish(TaskState::Failed(panic)),
            (Ok(()), Some(plan)) => Settle::capitulate(plan),
            (Ok(()), None) => {
                self.restored.remove(&id);
                entry.stats.poll_count += 1;
                entry.stats.last_polled_at = Some(Instant::now());
                let task = out.task();
                // SAFETY: the task is out of its row, so this call has exclusive access to a live task (ProtocolTask's contract).
                match caught("poll", || unsafe { task.poll() }) {
                    Ok(poll) => Settle::after_poll(poll, &entry.policy),
                    Err(panic) => Settle::Finish(TaskState::Failed(panic)),
                }
            }
        };
        let live = out.release();
        errors.note(self.settle(id, Some(live), settle));
    }

    /// Hand `envelope` to `live`, task `id`, or refuse it if it was written for another task
    /// type. Either way the envelope is used up; the task is untouched by a refusal.
    fn deliver(id: TaskId, live: &mut ProtocolTaskPtr, envelope: EnvelopePacket) -> Result<(), String> {
        if envelope.target_type() != live.task_type_id() {
            return Err(format!(
                "envelope {:?} for task {:?} targets another task type than {}",
                envelope.kind(),
                id,
                live.vtable().type_name()
            ));
        }
        // SAFETY: the envelope targets exactly the type `live` holds (checked above), and `live`
        // is out of its row, so this access is exclusive.
        unsafe { envelope.dispatch(live.raw_data()) }
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
                Settle::Finish(state) => self.finish(id, state),
                Settle::Wait | Settle::RunAgain | Settle::Checkpoint(_) => Ok(()),
            };
        };

        match settle {
            Settle::Wait => self.put_back(id, live, TaskState::Waiting),
            Settle::RunAgain => {
                let seq = self.bump_seq();
                self.put_back(id, live, TaskState::Runnable);
                if let Some(entry) = self.tasks.get_mut(id) {
                    entry.runnable_seq = seq;
                }
            }
            Settle::Checkpoint(reason) => return self.checkpoint_live(id, live, reason),
            Settle::Finish(state) => {
                let finished = self.finish(id, state);
                drop(live); // runs the task's cleanup and frees it, once its row is gone
                return finished;
            }
        }
        Ok(())
    }

    fn put_back(&mut self, id: TaskId, live: ProtocolTaskPtr, state: TaskState) {
        if let Some(entry) = self.tasks.get_mut(id) {
            entry.resident = Resident::Live(live);
            entry.state = state;
        }
    }

    /// Checkpoint `live`, task `id`, and leave the task dormant. The task is freed only once its
    /// bytes are saved: if the save fails, or the store panics, it goes back into its row, live and
    /// as it was, and the error (or the panic) goes to the caller. If the task's own `checkpoint`
    /// panics, the task ends as Failed.
    fn checkpoint_live(&mut self, id: TaskId, live: ProtocolTaskPtr, reason: CheckpointReason) -> Result<(), String> {
        let Some(entry) = self.tasks.get_mut(id) else { return Ok(()) };
        let mut out = OutOfRow::hold(&mut entry.resident, live);
        let before_bytes = out.task().footprint_estimate();
        let task = out.task();
        // SAFETY: the task is out of its row, so this call has exclusive access to a live task (ProtocolTask's contract).
        let payload = match caught("checkpoint", || unsafe { task.checkpoint() }) {
            Ok(payload) => payload,
            Err(panic) => {
                let live = out.release();
                return self.settle(id, Some(live), Settle::Finish(TaskState::Failed(panic)));
            }
        };
        // If the save fails, `out` puts the task back in its row as it returns the error.
        let handle = self.store.save(id, payload)?;
        let live = out.release();

        entry.checkpoint_history.push_back(handle.clone());
        entry.stats.checkpoint_count += 1;
        entry.state = TaskState::Dormant;
        entry.resident = Resident::Dormant;

        let after_bytes = std::mem::size_of::<TaskId>();
        self.reclaim_log.push(ReclaimReport {
            id,
            reason,
            before_bytes,
            after_bytes,
            reclaimed_bytes: before_bytes.saturating_sub(after_bytes),
            handle: Some(handle),
        });
        // A dormant task keeps at least the checkpoint it will be restored from.
        let keep = entry.policy.retention.keep_latest.max(1);
        let pruned = Self::prune(&self.store, &mut entry.checkpoint_history, keep);
        drop(live); // runs the task's cleanup and frees it, once its row is settled
        pruned
    }

    /// End task `id` as `state`, record it in `finished` and remove its row; the one way a task
    /// leaves the scheduler. A live copy still in the row is freed with it. Its checkpoints go,
    /// unless the retention policy keeps them for `state`; then the latest `keep_latest` stay in
    /// the store.
    fn finish(&mut self, id: TaskId, state: TaskState) -> Result<(), String> {
        let Some(mut entry) = self.tasks.remove(id) else { return Ok(()) };
        self.restored.remove(&id);
        let retention = &entry.policy.retention;
        let retain = match &state {
            TaskState::Completed => retention.retain_on_completion,
            TaskState::Failed(_) => retention.retain_on_failure,
            TaskState::Cancelled => retention.retain_on_cancel,
            TaskState::Runnable | TaskState::Waiting | TaskState::Dormant => false,
        };
        let keep = if retain { retention.keep_latest } else { 0 };
        self.finished.push((id, state));
        let pruned = Self::prune(&self.store, &mut entry.checkpoint_history, keep);
        drop(entry);
        pruned
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
