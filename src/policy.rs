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
    /// Highest priority first, then longest runnable.
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
    /// restores the task and delivers them.
    pub auto_restore_on_wake: bool,
    /// After `Yield`, keep the task runnable so the next `run_once` or `pump` polls it again (the
    /// default); a task is polled at most once per round. When false, `Yield` parks the task
    /// like `Pending`, until a wake.
    pub eager_repoll_after_yield: bool,
    /// Checkpoint retention.
    pub retention: RetentionPolicy,
    /// Memory budget the default strategy enforces at Batch or lower: a task over it is
    /// checkpointed before a poll, and after each restore it is polled once before the rule can
    /// checkpoint it again.
    pub memory_budget_hint: Option<usize>,
    /// Reserved: the scheduler does not read it yet.
    pub dormancy_ttl: Option<Duration>,
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