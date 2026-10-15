use crate::{CheckpointReason, QoSClass, TaskId, TaskState, WakeReason};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
/// How a task surrenders.
pub struct CapitulatePlan {
    /// Why.
    pub reason: CheckpointReason,
    /// Keep a checkpoint and go dormant (true), or end as Completed and be removed (false); the
    /// task's checkpoints then follow `retain_on_completion`. The same from a poll, a strategy
    /// or a Capitulate message.
    pub retain_stub: bool,
}

#[derive(Debug, Clone)]
/// What a capitulation strategy sees of a task: the task as it was before this round's messages
/// and wakes were delivered.
pub struct TaskSnapshot {
    /// The task.
    pub id: TaskId,
    /// Its quality-of-service class.
    pub qos: QoSClass,
    /// Its state.
    pub state: TaskState,
    /// Its own size estimate.
    pub footprint_bytes: usize,
    /// Messages waiting for it, counted before this round delivers them.
    pub mailbox_len: usize,
    /// Wakes waiting for it, counted before this round delivers them. A restore's `Resume` is
    /// never counted: a just-restored task is not shown to the strategy.
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

/// Decides, before a poll, whether a task should capitulate (not before the first poll after a
/// restore).
///
/// It sees the task as it was before this round's messages and wakes were delivered, and
/// `last_wake` is the newest wake the task has been sent, this round's included. If it returns a
/// plan, the messages and wakes are delivered first, so the checkpoint holds them, and the task is
/// not polled in this round.
///
/// A task the plan checkpoints that had work waiting (a message, a wake other than `QoS` or
/// `MemoryPressure`, or a `Yield` it has not been polled for, whatever wakes came after it; a
/// `Yield` counts only with `eager_repoll_after_yield`) stays runnable while dormant: the next
/// round restores and polls it (with `auto_restore_on_wake` on; with it off, once it is set back
/// to true), so the plan puts the poll off but never drops it. Work the task was handed keeps its
/// poll even when the save of the plan's checkpoint fails or the strategy panics: a later plan
/// that checkpoints the task, or a Checkpoint or Capitulate message, keeps it runnable too. A
/// task woken only by `QoS` or `MemoryPressure` is answered by the checkpoint and stays dormant
/// until its next wake.
///
/// It is not asked about a task restored from a checkpoint until that task has been polled once,
/// so a plan that holds for the task's whole life (such as the default budget rule) still lets it
/// run once after every restore.
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