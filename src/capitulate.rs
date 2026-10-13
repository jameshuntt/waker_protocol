use crate::{CheckpointReason, QoSClass, TaskId, TaskState, WakeReason};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapitulatePlan {
    pub reason: CheckpointReason,
    pub retain_stub: bool,
}

#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    pub id: TaskId,
    pub qos: QoSClass,
    pub state: TaskState,
    pub footprint_bytes: usize,
    pub mailbox_len: usize,
    pub pending_wakes: usize,
    pub poll_count: u64,
    pub idle_for: Option<Duration>,
    pub memory_budget_hint: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct ReclaimReport<H> {
    pub id: TaskId,
    pub reason: CheckpointReason,
    pub before_bytes: usize,
    pub after_bytes: usize,
    pub reclaimed_bytes: usize,
    pub handle: Option<H>,
}

pub trait CapitulateStrategy: Send + Sync + 'static {
    fn plan(
        &self,
        snapshot: &TaskSnapshot,
        last_wake: Option<&WakeReason>,
    ) -> Option<CapitulatePlan>;
}

pub struct DefaultCapitulateStrategy {
    pub min_footprint_bytes: usize,
    pub idle_before_checkpoint: Duration,
    pub protect_qos_at_or_above: QoSClass,
}

impl Default for DefaultCapitulateStrategy {
    fn default() -> Self {
        Self {
            min_footprint_bytes: 4096,
            idle_before_checkpoint: Duration::from_secs(15),
            protect_qos_at_or_above: QoSClass::Interactive,
        }
    }
}

impl CapitulateStrategy for DefaultCapitulateStrategy {
    fn plan(
        &self,
        snapshot: &TaskSnapshot,
        last_wake: Option<&WakeReason>,
    ) -> Option<CapitulatePlan> {
        if snapshot.qos.priority() >= self.protect_qos_at_or_above.priority() {
            return None;
        }

        if matches!(last_wake, Some(WakeReason::MemoryPressure))
            && snapshot.footprint_bytes >= self.min_footprint_bytes
        {
            return Some(CapitulatePlan {
                reason: CheckpointReason::MemoryPressure,
                retain_stub: true,
            });
        }

        if let Some(idle_for) = snapshot.idle_for {
            if idle_for >= self.idle_before_checkpoint
                && snapshot.mailbox_len == 0
                && snapshot.pending_wakes == 0
                && snapshot.footprint_bytes >= self.min_footprint_bytes
            {
                return Some(CapitulatePlan {
                    reason: CheckpointReason::Idle,
                    retain_stub: true,
                });
            }
        }

        if let Some(budget) = snapshot.memory_budget_hint {
            if snapshot.footprint_bytes > budget && snapshot.qos.priority() <= QoSClass::Batch.priority() {
                return Some(CapitulatePlan {
                    reason: CheckpointReason::YieldForSla,
                    retain_stub: true,
                });
            }
        }

        None
    }
}