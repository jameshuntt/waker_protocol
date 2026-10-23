use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// A task's priority class; higher classes are polled first and are protected from capitulation.
pub enum QoSClass {
    /// Priority 5.
    Realtime,
    /// Priority 4.
    Interactive,
    /// Priority 3 (the default).
    Standard,
    /// Priority 2.
    Batch,
    /// Priority 1.
    Background,
}

impl QoSClass {
    /// 5 for Realtime down to 1 for Background.
    pub const fn priority(self) -> u8 {
        match self {
            Self::Realtime => 5,
            Self::Interactive => 4,
            Self::Standard => 3,
            Self::Batch => 2,
            Self::Background => 1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// The order runnable tasks are polled in.
pub enum WakeOrdering {
    /// Round robin, ignoring priority.
    UnbiasedRoundRobin,
    /// Highest priority first, then least recently woken: within a class, the task with the
    /// lowest [`TaskEntry::runnable_seq`](crate::TaskEntry::runnable_seq) first. A wake to a task
    /// already runnable sets it again, so that task waits behind tasks woken before that wake,
    /// even ones that became runnable after it.
    BiasedQoS,
    /// Highest priority first, then least recently polled (the default).
    BiasedQoSThenAge,
}

#[derive(Debug, Clone)]
/// What happens to a task's checkpoints.
pub struct RetentionPolicy {
    /// How many checkpoints to keep (a dormant task always keeps one). An older checkpoint the
    /// store fails to remove stays in the task's `checkpoint_history`, beyond this count, and a
    /// later prune tries it again.
    pub keep_latest: usize,
    /// Keep checkpoints after the task completes.
    pub retain_on_completion: bool,
    /// Keep checkpoints after the task fails (the default).
    pub retain_on_failure: bool,
    /// Keep checkpoints after the task is cancelled.
    pub retain_on_cancel: bool,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            keep_latest: 1,
            retain_on_completion: false,
            retain_on_failure: true,
            retain_on_cancel: false,
        }
    }
}

#[derive(Debug, Clone)]
/// Per-task policy, given at spawn.
pub struct TaskPolicy {
    /// Priority class.
    pub qos: QoSClass,
    /// Restore a dormant task when it is woken (the default). When false, a dormant task stays
    /// Dormant and is not polled: wakes and messages sent to it are kept in its row
    /// (`pending_wakes`, `mailbox`). Set it back to true in the task's row, and the next round
    /// restores the task and delivers them. A task checkpointed while it is owed a poll (by the
    /// capitulation strategy in place of a poll it had work for, or by a Checkpoint or Capitulate
    /// message after a `Yield` or work it was not yet polled for) is kept the same way, with its
    /// state `Runnable`, and polled once this is back to true.
    pub auto_restore_on_wake: bool,
    /// After `Yield`, keep the task runnable so the next `run_once` or `pump` polls it again (the
    /// default); a task is polled at most once per round. When false, `Yield` parks the task
    /// like `Pending`, until a wake.
    pub eager_repoll_after_yield: bool,
    /// Checkpoint retention.
    pub retention: RetentionPolicy,
    /// Memory budget the default strategy enforces at Batch or lower: a task over it is
    /// checkpointed in place of a poll, and, if it had work waiting, the next round restores and
    /// polls it; the rule is not applied to that first poll after a restore. So every message,
    /// every wake other than `QoS` or `MemoryPressure`, and every `Yield` is followed by a poll at
    /// most one round later than for a task within its budget, and work that arrives while the
    /// task waits for that poll shares it. A `Yield` keeps its poll even when a `QoS` or
    /// `MemoryPressure` wake comes after it, and counts only with `eager_repoll_after_yield`. A
    /// `QoS` or `MemoryPressure` wake with no other work waiting reaches its `on_wake` and is
    /// answered by the checkpoint, with no poll: the task stays dormant until another wake or
    /// message.
    pub memory_budget_hint: Option<usize>,
    /// How long the task may wait with no work before it is parked: once it has gone this long
    /// without a poll (counted from its spawn until its first), with no message, no wake and no
    /// poll owed, the scheduler checkpoints it ([`CheckpointReason::Idle`](crate::CheckpointReason::Idle))
    /// and it is dormant until a wake or message restores it (with `auto_restore_on_wake`; with it
    /// off, as for any dormant task). Its interval and its `wake_at` moment still wake it. A task
    /// whose vtable gives no checkpoint ([`ProtocolVTable::live`](crate::ProtocolVTable::live)) is
    /// never parked. `None` (the default) parks nothing.
    pub dormancy_ttl: Option<Duration>,
    /// Wake the task with [`WakeReason::Timer`](crate::WakeReason::Timer) every `interval`, from its
    /// spawn, dormant or not; the scheduler keeps the time itself, with no thread. The ticks are at
    /// a fixed rate: one round that comes late wakes the task once, and the next tick is the next
    /// multiple of `interval` after it. `pump` waits no longer than the next tick. `None` (the
    /// default) sets no interval; a zero interval is taken as none.
    pub interval: Option<Duration>,
}

impl Default for TaskPolicy {
    fn default() -> Self {
        Self {
            qos: QoSClass::Standard,
            auto_restore_on_wake: true,
            eager_repoll_after_yield: true,
            retention: RetentionPolicy::default(),
            memory_budget_hint: None,
            dormancy_ttl: None,
            interval: None,
        }
    }
}

#[derive(Debug, Clone)]
/// Scheduler-wide settings.
pub struct SchedulerConfig {
    /// Poll order.
    pub wake_ordering: WakeOrdering,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            wake_ordering: WakeOrdering::BiasedQoSThenAge,
        }
    }
}