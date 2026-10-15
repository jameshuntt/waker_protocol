use crate::{CheckpointReason, QoSClass, TaskId, TaskState, WakeReason};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
/// How a task surrenders.
pub struct CapitulatePlan {
    /// Why.
    pub reason: CheckpointReason,
    /// Keep a checkpoint and go dormant (true) or finish and be removed (false).
    pub retain_stub: bool,
}

#[derive(Debug, Clone)]
/// What a capitulation strategy sees of a task.
pub struct TaskSnapshot {
    /// The task.
    pub id: TaskId,
    /// Its quality-of-service class.
    pub qos: QoSClass,
    /// Its state.
    pub state: TaskState,
    /// Its own size estimate.
    pub footprint_bytes: usize,
    /// Messages waiting for it.
    pub mailbox_len: usize,
    /// Wakes waiting for it.
    pub pending_wakes: usize,
    /// How many times it was polled.
    pub poll_count: u64,
    /// How long since its last poll.
    pub idle_for: Option<Duration>,
    /// Its memory budget, from its policy.
    pub memory_budget_hint: Option<usize>,
}

#[derive(Debug, Clone)]
/// One reclaim, written to the scheduler's reclaim log.
pub struct ReclaimReport<H> {
    /// The task.
    pub id: TaskId,
    /// Why it was reclaimed.
    pub reason: CheckpointReason,
    /// Its size estimate before.
    pub before_bytes: usize,
    /// What is left of it (its id).
    pub after_bytes: usize,
    /// `before_bytes - after_bytes`.
    pub reclaimed_bytes: usize,
    /// Where its checkpoint is.
    pub handle: Option<H>,
}

/// Decides, before each poll, whether a task should capitulate.
pub trait CapitulateStrategy: Send + Sync + 'static {
    /// A plan to capitulate, or `None` to poll as usual.
    fn plan(
        &self,
        snapshot: &TaskSnapshot,
        last_wake: Option<&WakeReason>,
    ) -> Option<CapitulatePlan>;
}

/// Capitulate below `protect_qos_at_or_above` when memory is short, when idle too long, or when over budget at Batch or lower.
pub struct DefaultCapitulateStrategy {
    /// Tasks smaller than this are left alone (memory pressure and idle rules).
    pub min_footprint_bytes: usize,
    /// How long idle before an idle checkpoint.
    pub idle_before_checkpoint: Duration,
    /// This class and above are never capitulated.
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