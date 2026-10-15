use crate::capitulate::{CapitulateStrategy, DefaultCapitulateStrategy, ReclaimReport, TaskSnapshot};
use crate::checkpoint::CheckpointStore;
use crate::envelope::{typed_envelope, EnvelopePacket};
use crate::policy::{SchedulerConfig, WakeOrdering};
use crate::slab::TaskSlab;
use crate::task::{ProtocolTask, ProtocolTaskPtr, ProtocolVTable};
use crate::types::{CheckpointReason, SchedulerMsg, TaskId, TaskPoll, TaskState, TaskStats, WakeReason};
use crate::{CapitulatePlan, TaskPolicy};
use crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender};
use std::collections::{BTreeMap, VecDeque};
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
    /// Gone, or out for a poll.
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
    /// Its checkpoints, oldest first.
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

    fn is_runnable(&self) -> bool {
        matches!(self.state, TaskState::Runnable)
            || !self.mailbox.is_empty()
            || !self.pending_wakes.is_empty()
    }

    fn latest_checkpoint(&self) -> Option<&H> {
        self.checkpoint_history.back()
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

    /// Checkpoint every task and stop.
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

    /// Send the task a message; `deliver` runs with the task before its next poll.
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
    /// Settings.
    pub config: SchedulerConfig,
    /// Where checkpoints go.
    pub store: S,
    /// Set by a Shutdown message.
    pub shutdown: bool,

    strategy: Box<dyn CapitulateStrategy>,
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
            config: SchedulerConfig::default(),
            store,
            shutdown: false,
            strategy,
            tx,
            rx,
            seq: 1,
            rr_cursor: 0,
        }
    }

    /// The scheduler's own channel, for [`scheduler_waker`](crate::scheduler_waker) or a hand-built
    /// [`SchedulerMsg`]. A message for a task of another type is refused when it would be delivered.
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
    pub fn run_once(&mut self) -> Result<(), String> {
        self.drain_inbox()?;
        self.poll_ready()?;
        Ok(())
    }

    /// Wait up to `timeout` for a message, then run once. False once shut down or every handle is gone.
    pub fn pump(&mut self, timeout: Duration) -> Result<bool, String> {
        if self.shutdown {
            return Ok(false);
        }

        match self.rx.recv_timeout(timeout) {
            Ok(msg) => self.process_msg(msg)?,
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return Ok(false),
        }

        self.drain_inbox()?;
        self.poll_ready()?;
        Ok(!self.shutdown)
    }

    fn bump_seq(&mut self) -> u64 {
        let out = self.seq;
        self.seq = self.seq.wrapping_add(1);
        out
    }

    fn drain_inbox(&mut self) -> Result<(), String> {
        while let Ok(msg) = self.rx.try_recv() {
            self.process_msg(msg)?;
        }
        Ok(())
    }

    fn process_msg(&mut self, msg: SchedulerMsg) -> Result<(), String> {
        match msg {
            SchedulerMsg::Wake { id, reason } => {
                let seq = self.bump_seq();
                if let Some(entry) = self.tasks.get_mut(id) {
                    entry.pending_wakes.push_back(reason);
                    entry.state = TaskState::Runnable;
                    entry.runnable_seq = seq;
                }
            }
            SchedulerMsg::Deliver { id, envelope } => {
                let seq = self.bump_seq();
                if let Some(entry) = self.tasks.get_mut(id) {
                    entry.mailbox.push_back(envelope);
                    entry.pending_wakes.push_back(WakeReason::Inbox);
                    entry.state = TaskState::Runnable;
                    entry.runnable_seq = seq;
                }
            }
            SchedulerMsg::Checkpoint { id, reason } => {
                self.force_checkpoint(id, reason)?;
            }
            SchedulerMsg::Capitulate { id, plan } => {
                self.force_capitulate(id, plan)?;
            }
            SchedulerMsg::Cancel { id } => {
                if let Some(entry) = self.tasks.get_mut(id) {
                    entry.state = TaskState::Cancelled;
                }
                // Removing the row frees the task; its checkpoints follow the retention policy.
                self.finalize_removal(id)?;
            }
            SchedulerMsg::Shutdown => {
                self.shutdown = true;
                let ids = self.tasks.live_ids();
                for id in ids {
                    let _ = self.force_checkpoint(id, CheckpointReason::Shutdown);
                }
            }
        }

        Ok(())
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

    fn poll_ready(&mut self) -> Result<(), String> {
        let ids = self.order_ready_ids();
        let mut to_remove = Vec::new();

        for id in ids {
            self.restore_if_needed(id)?;

            if self.poll_one(id)? {
                to_remove.push(id);
            }
        }

        for id in to_remove {
            self.finalize_removal(id)?;
        }

        Ok(())
    }

    fn restore_if_needed(&mut self, id: TaskId) -> Result<(), String> {
        let (should_restore, vtable, latest_handle) = match self.tasks.get(id) {
            Some(entry) => {
                let dormant = matches!(entry.resident, Resident::Dormant);
                (
                    dormant && entry.policy.auto_restore_on_wake && entry.is_runnable(),
                    entry.vtable,
                    entry.latest_checkpoint().cloned(),
                )
            }
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
        }

        Ok(())
    }

    fn snapshot_for(
        &self,
        id: TaskId,
        entry: &TaskEntry<S::Handle>,
        live: &ProtocolTaskPtr,
    ) -> TaskSnapshot {
        let now = Instant::now();

        TaskSnapshot {
            id,
            qos: entry.policy.qos,
            state: entry.state.clone(),
            footprint_bytes: live.footprint_estimate(),
            mailbox_len: entry.mailbox.len(),
            pending_wakes: entry.pending_wakes.len(),
            poll_count: entry.stats.poll_count,
            idle_for: entry.stats.last_polled_at.map(|t| now.saturating_duration_since(t)),
            memory_budget_hint: entry.policy.memory_budget_hint,
        }
    }

    fn force_checkpoint(&mut self, id: TaskId, reason: CheckpointReason) -> Result<(), String> {
        let resident = match self.tasks.get_mut(id) {
            Some(entry) => std::mem::replace(&mut entry.resident, Resident::Empty),
            None => return Ok(()),
        };

        match resident {
            Resident::Live(mut live) => {
                let before_bytes = live.footprint_estimate();
                // SAFETY: `live` is out of the slab, so this call has exclusive access to a live task (ProtocolTask's contract).
                let payload = unsafe { live.checkpoint() };
                let handle = self.store.save(id, payload)?;

                let pruned = if let Some(entry) = self.tasks.get_mut(id) {
                    entry.checkpoint_history.push_back(handle.clone());
                    entry.stats.checkpoint_count += 1;
                    entry.pending_wakes.clear();
                    entry.state = TaskState::Dormant;
                    entry.resident = Resident::Dormant;

                    self.reclaim_log.push(ReclaimReport {
                        id,
                        reason,
                        before_bytes,
                        after_bytes: std::mem::size_of::<TaskId>(),
                        reclaimed_bytes: before_bytes.saturating_sub(std::mem::size_of::<TaskId>()),
                        handle: Some(handle.clone()),
                    });

                    Self::collect_pruned_history(entry)
                } else {
                    Vec::new()
                };

                for old in pruned {
                    self.store.remove(&old)?;
                }

                drop(live);
            }
            other => {
                if let Some(entry) = self.tasks.get_mut(id) {
                    entry.resident = other;
                }
            }
        }

        Ok(())
    }

    fn force_capitulate(&mut self, id: TaskId, plan: CapitulatePlan) -> Result<(), String> {
        if plan.retain_stub {
            self.force_checkpoint(id, plan.reason)
        } else {
            self.finalize_removal(id)
        }
    }

    // fn prune_history(&mut self, entry: &mut TaskEntry<S::Handle>) -> Result<(), String> {
    //     let keep = entry.policy.retention.keep_latest.max(
    //         if matches!(entry.resident, Resident::Dormant) { 1 } else { 0 },
    //     );

    //     while entry.checkpoint_history.len() > keep {
    //         if let Some(old) = entry.checkpoint_history.pop_front() {
    //             self.store.remove(&old)?;
    //         }
    //     }

    //     Ok(())
    // }

    fn collect_pruned_history(entry: &mut TaskEntry<S::Handle>) -> Vec<S::Handle> {
        let keep = entry.policy.retention.keep_latest.max(
            if matches!(entry.resident, Resident::Dormant) { 1 } else { 0 },
        );

        let mut removed = Vec::new();

        while entry.checkpoint_history.len() > keep {
            if let Some(old) = entry.checkpoint_history.pop_front() {
                removed.push(old);
            }
        }

        removed
    }

    fn poll_one(&mut self, id: TaskId) -> Result<bool, String> {
        let mut live = {
            let entry = match self.tasks.get_mut(id) {
                Some(entry) => entry,
                None => return Ok(false),
            };

            match std::mem::replace(&mut entry.resident, Resident::Empty) {
                Resident::Live(live) => live,
                other => {
                    entry.resident = other;
                    return Ok(false);
                }
            }
        };

        {
            let entry = self.tasks.get_mut(id).unwrap();

            while let Some(env) = entry.mailbox.pop_front() {
                if env.target_type() != live.task_type_id() {
                    return Err(format!(
                        "envelope {:?} for task {:?} targets another task type than {}",
                        env.kind(),
                        id,
                        live.vtable().type_name()
                    ));
                }
                // SAFETY: the envelope targets exactly the type `live` holds (checked above),
                // and `live` is out of the slab, so this access is exclusive.
                unsafe { env.dispatch(live.raw_data())? };
            }

            while let Some(reason) = entry.pending_wakes.pop_front() {
                entry.stats.last_wake = Some(reason.clone());
                // SAFETY: `live` is out of the slab, so this call has exclusive access to a live task (ProtocolTask's contract).
                unsafe { live.on_wake(reason) };
            }
        }

        {
            let entry = self.tasks.get(id).unwrap();
            let snapshot = self.snapshot_for(id, entry, &live);
            if let Some(plan) = self.strategy.plan(&snapshot, entry.stats.last_wake.as_ref()) {
                let before_bytes = snapshot.footprint_bytes;
                // SAFETY: `live` is out of the slab, so this call has exclusive access to a live task (ProtocolTask's contract).
                let payload = unsafe { live.checkpoint() };
                let handle = self.store.save(id, payload)?;

                let pruned = if let Some(entry) = self.tasks.get_mut(id) {
                    entry.checkpoint_history.push_back(handle.clone());
                    entry.stats.checkpoint_count += 1;
                    entry.state = TaskState::Dormant;
                    entry.resident = Resident::Dormant;
                    entry.pending_wakes.clear();

                    self.reclaim_log.push(ReclaimReport {
                        id,
                        reason: plan.reason,
                        before_bytes,
                        after_bytes: std::mem::size_of::<TaskId>(),
                        reclaimed_bytes: before_bytes.saturating_sub(std::mem::size_of::<TaskId>()),
                        handle: Some(handle.clone()),
                    });

                    Self::collect_pruned_history(entry)
                } else {
                    Vec::new()
                };

                for old in pruned {
                    self.store.remove(&old)?;
                }

                drop(live);
                return Ok(false);
            }
        }

        {
            let entry = self.tasks.get_mut(id).unwrap();
            entry.stats.poll_count += 1;
            entry.stats.last_polled_at = Some(Instant::now());
        }

        // SAFETY: `live` is out of the slab, so this call has exclusive access to a live task (ProtocolTask's contract).
        let poll = unsafe { live.poll() };

        match poll {
            TaskPoll::Ready => {
                if let Some(entry) = self.tasks.get_mut(id) {
                    entry.state = TaskState::Completed;
                    entry.resident = Resident::Empty;
                }
                drop(live);
                Ok(true)
            }
            TaskPoll::Pending | TaskPoll::Park => {
                if let Some(entry) = self.tasks.get_mut(id) {
                    entry.state = TaskState::Waiting;
                    entry.resident = Resident::Live(live);
                }
                Ok(false)
            }
            TaskPoll::Yield => {
                let seq = self.bump_seq();
                if let Some(entry) = self.tasks.get_mut(id) {
                    entry.state = if entry.policy.eager_repoll_after_yield {
                        TaskState::Runnable
                    } else {
                        TaskState::Waiting
                    };
                    entry.runnable_seq = seq;
                    entry.resident = Resident::Live(live);
                }
                Ok(false)
            }
            TaskPoll::Checkpoint(reason) => {
                let before_bytes = live.footprint_estimate();
                // SAFETY: `live` is out of the slab, so this call has exclusive access to a live task (ProtocolTask's contract).
                let payload = unsafe { live.checkpoint() };
                let handle = self.store.save(id, payload)?;

                let pruned = if let Some(entry) = self.tasks.get_mut(id) {
                    entry.checkpoint_history.push_back(handle.clone());
                    entry.stats.checkpoint_count += 1;
                    entry.state = TaskState::Dormant;
                    entry.resident = Resident::Dormant;
                    entry.pending_wakes.clear();

                    self.reclaim_log.push(ReclaimReport {
                        id,
                        reason,
                        before_bytes,
                        after_bytes: std::mem::size_of::<TaskId>(),
                        reclaimed_bytes: before_bytes.saturating_sub(std::mem::size_of::<TaskId>()),
                        handle: Some(handle.clone()),
                    });

                    Self::collect_pruned_history(entry)
                } else {
                    Vec::new()
                };

                for old in pruned {
                    self.store.remove(&old)?;
                }

                drop(live);
                Ok(false)
            }
            TaskPoll::Capitulate(plan) => {
                if plan.retain_stub {
                    let before_bytes = live.footprint_estimate();
                    // SAFETY: `live` is out of the slab, so this call has exclusive access to a live task (ProtocolTask's contract).
                    let payload = unsafe { live.checkpoint() };
                    let handle = self.store.save(id, payload)?;

                    let pruned = if let Some(entry) = self.tasks.get_mut(id) {
                        entry.checkpoint_history.push_back(handle.clone());
                        entry.stats.checkpoint_count += 1;
                        entry.state = TaskState::Dormant;
                        entry.resident = Resident::Dormant;
                        entry.pending_wakes.clear();

                        self.reclaim_log.push(ReclaimReport {
                            id,
                            reason: plan.reason,
                            before_bytes,
                            after_bytes: std::mem::size_of::<TaskId>(),
                            reclaimed_bytes: before_bytes.saturating_sub(std::mem::size_of::<TaskId>()),
                            handle: Some(handle.clone()),
                        });

                        Self::collect_pruned_history(entry)
                    } else {
                        Vec::new()
                    };

                    for old in pruned {
                        self.store.remove(&old)?;
                    }

                    drop(live);
                    Ok(false)
                } else {
                    if let Some(entry) = self.tasks.get_mut(id) {
                        entry.state = TaskState::Completed;
                        entry.resident = Resident::Empty;
                    }
                    drop(live);
                    Ok(true)
                }
            }
            TaskPoll::Cancelled => {
                if let Some(entry) = self.tasks.get_mut(id) {
                    entry.state = TaskState::Cancelled;
                    entry.resident = Resident::Empty;
                }
                drop(live);
                Ok(true)
            }
            TaskPoll::Failed(err) => {
                if let Some(entry) = self.tasks.get_mut(id) {
                    entry.state = TaskState::Failed(err);
                    entry.resident = Resident::Empty;
                }
                drop(live);
                Ok(true)
            }
        }
    }

    fn finalize_removal(&mut self, id: TaskId) -> Result<(), String> {
        let mut entry = match self.tasks.remove(id) {
            Some(entry) => entry,
            None => return Ok(()),
        };

        let retain = match &entry.state {
            TaskState::Completed => entry.policy.retention.retain_on_completion,
            TaskState::Failed(_) => entry.policy.retention.retain_on_failure,
            TaskState::Cancelled => entry.policy.retention.retain_on_cancel,
            TaskState::Dormant => true,
            _ => false,
        };

        if !retain {
            while let Some(handle) = entry.checkpoint_history.pop_front() {
                self.store.remove(&handle)?;
            }
        } else {
            let keep = entry.policy.retention.keep_latest;
            while entry.checkpoint_history.len() > keep {
                if let Some(handle) = entry.checkpoint_history.pop_front() {
                    self.store.remove(&handle)?;
                }
            }
        }

        Ok(())
    }
}