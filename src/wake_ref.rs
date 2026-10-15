use crate::{SchedulerMsg, TaskId, WakeReason};
use crossbeam_channel::Sender;
use std::sync::Arc;
use std::task::Waker;
use waker_tables::{
    derive_arc_unsafe_wakeable, waker_from_arc,
};

/// What a scheduler waker carries.
pub struct TaskWakeSignal {
    /// The task to wake.
    pub id: TaskId,
    /// The scheduler's channel.
    pub tx: Sender<SchedulerMsg>,
    /// The wake reason it sends.
    pub reason: WakeReason,
}

impl TaskWakeSignal {
    /// Send the wake.
    pub fn emit(self: Arc<Self>) {
        let _ = self.tx.send(SchedulerMsg::Wake {
            id: self.id,
            reason: self.reason.clone(),
        });
    }
}

derive_arc_unsafe_wakeable!(TaskWakeSignal, emit);

/// A `std::task::Waker` that wakes task `id` with `reason`.
pub fn scheduler_waker(
    id: TaskId,
    tx: Sender<SchedulerMsg>,
    reason: WakeReason,
) -> Waker {
    let signal = Arc::new(TaskWakeSignal { id, tx, reason });
    waker_from_arc(signal)
}