use crate::capitulate::CapitulatePlan;
use crate::envelope::EnvelopePacket;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// A task's slot in the slab plus the slot's generation; a stale id never reaches a newer task,
/// because a slot is retired rather than given a generation it has had before.
pub struct TaskId {
    /// The slab slot.
    pub slot: u32,
    /// How many times the slot has been reused.
    pub generation: u32,
}

impl TaskId {
    /// An id for `slot` at `generation`.
    pub const fn new(slot: u32, generation: u32) -> Self {
        Self { slot, generation }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Why a task was woken.
pub enum WakeReason {
    /// Just spawned.
    Spawn,
    /// Woken by hand.
    Manual,
    /// A message arrived.
    Inbox,
    /// A timer fired.
    Timer,
    /// A condition it waits on changed.
    Condition,
    /// Restored from a checkpoint.
    Resume,
    /// Quality-of-service scheduling.
    QoS,
    /// Memory is short.
    MemoryPressure,
    /// Being cancelled.
    Cancel,
    /// The scheduler is shutting down.
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Why a task was checkpointed.
pub enum CheckpointReason {
    /// Asked for by hand.
    Manual,
    /// Idle too long.
    Idle,
    /// Memory is short.
    MemoryPressure,
    /// Making room for higher-priority work.
    QoS,
    /// Not making progress.
    Stall,
    /// The scheduler is shutting down.
    Shutdown,
    /// Over its memory budget at low priority.
    YieldForSla,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// What a task's poll returns.
pub enum TaskPoll {
    /// Finished.
    Ready,
    /// Waiting to be woken.
    Pending,
    /// Run again: in the next round when the task's policy has `eager_repoll_after_yield` (the
    /// default), otherwise not until a wake, like `Pending`.
    Yield,
    /// Waiting, like Pending.
    Park,
    /// Save me and go dormant.
    Checkpoint(CheckpointReason),
    /// Surrender: checkpoint and go dormant if the plan keeps a stub, otherwise end as Completed.
    Capitulate(CapitulatePlan),
    /// Stopped by cancellation.
    Cancelled,
    /// Stopped with an error.
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Where a task is in its life.
pub enum TaskState {
    /// Ready to be polled; a dormant task is restored first.
    Runnable,
    /// Polled; waiting to be woken.
    Waiting,
    /// Checkpointed out of memory; restored when woken (by policy).
    Dormant,
    /// Finished.
    Completed,
    /// Cancelled.
    Cancelled,
    /// Failed with an error, or with a message naming a panic in the task's own
    /// `poll`, `on_wake`, `checkpoint`, `restore` or `footprint_estimate`.
    Failed(String),
}

#[derive(Debug, Clone)]
/// Counters the scheduler keeps per task.
pub struct TaskStats {
    /// When the task was spawned.
    pub created_at: Instant,
    /// When it was last polled.
    pub last_polled_at: Option<Instant>,
    /// How many times it was polled.
    pub poll_count: u64,
    /// How many times it was checkpointed.
    pub checkpoint_count: u64,
    /// How many times it was restored.
    pub restore_count: u64,
    /// The last wake reason it saw.
    pub last_wake: Option<WakeReason>,
}

impl Default for TaskStats {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskStats {
    /// Zeroed counters, created now.
    pub fn new() -> Self {
        Self {
            created_at: Instant::now(),
            last_polled_at: None,
            poll_count: 0,
            checkpoint_count: 0,
            restore_count: 0,
            last_wake: None,
        }
    }
}

/// A message to the scheduler, sent through its channel.
pub enum SchedulerMsg {
    /// Wake a task.
    Wake {
        /// The task.
        id: TaskId,
        /// Why.
        reason: WakeReason,
    },
    /// Queue a message for a task.
    Deliver {
        /// The task.
        id: TaskId,
        /// The message.
        envelope: EnvelopePacket,
    },
    /// Checkpoint a task now. A task owed a poll (for a `Yield`, or for work it was handed and not
    /// yet polled for) stays runnable while dormant: the next round restores it and polls it (with
    /// `auto_restore_on_wake` on; with it off, once it is set back to true).
    Checkpoint {
        /// The task.
        id: TaskId,
        /// Why.
        reason: CheckpointReason,
    },
    /// Make a task capitulate now. With a stub, a task owed a poll stays runnable while dormant,
    /// as after a Checkpoint message.
    Capitulate {
        /// The task.
        id: TaskId,
        /// The plan.
        plan: CapitulatePlan,
    },
    /// Cancel a task.
    Cancel {
        /// The task.
        id: TaskId,
    },
    /// Checkpoint every live task and stop. Once it is handled, `run_once` and `pump` restore and
    /// poll nothing, and every later message stays in the channel. A task whose checkpoint cannot
    /// be saved stays live in its row, and the error is returned.
    Shutdown,
}