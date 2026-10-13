use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QoSClass {
    Realtime,
    Interactive,
    Standard,
    Batch,
    Background,
}

impl QoSClass {
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
pub enum WakeOrdering {
    UnbiasedRoundRobin,
    BiasedQoS,
    BiasedQoSThenAge,
}

#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub keep_latest: usize,
    pub retain_on_completion: bool,
    pub retain_on_failure: bool,
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
pub struct TaskPolicy {
    pub qos: QoSClass,
    pub auto_restore_on_wake: bool,
    pub eager_repoll_after_yield: bool,
    pub retention: RetentionPolicy,
    pub memory_budget_hint: Option<usize>,
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
pub struct SchedulerConfig {
    pub wake_ordering: WakeOrdering,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            wake_ordering: WakeOrdering::BiasedQoSThenAge,
        }
    }
}