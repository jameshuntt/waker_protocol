use crate::capitulate::CapitulatePlan;
use crate::envelope::EnvelopePacket;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId {
    pub slot: u32,
    pub generation: u32,
}

impl TaskId {
    pub const fn new(slot: u32, generation: u32) -> Self {
        Self { slot, generation }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeReason {
    Spawn,
    Manual,
    Inbox,
    Timer,
    Condition,
    Resume,
    QoS,
    MemoryPressure,
    Cancel,
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointReason {
    Manual,
    Idle,
    MemoryPressure,
    QoS,
    Stall,
    Shutdown,
    YieldForSla,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskPoll {
    Ready,
    Pending,
    Yield,
    Park,
    Checkpoint(CheckpointReason),
    Capitulate(CapitulatePlan),
    Cancelled,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskState {
    Runnable,
    Waiting,
    Dormant,
    Completed,
    Cancelled,
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct TaskStats {
    pub created_at: Instant,
    pub last_polled_at: Option<Instant>,
    pub poll_count: u64,
    pub checkpoint_count: u64,
    pub restore_count: u64,
    pub last_wake: Option<WakeReason>,
}

impl TaskStats {
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

pub enum SchedulerMsg {
    Wake {
        id: TaskId,
        reason: WakeReason,
    },
    Deliver {
        id: TaskId,
        envelope: EnvelopePacket,
    },
    Checkpoint {
        id: TaskId,
        reason: CheckpointReason,
    },
    Capitulate {
        id: TaskId,
        plan: CapitulatePlan,
    },
    Cancel {
        id: TaskId,
    },
    Shutdown,
}