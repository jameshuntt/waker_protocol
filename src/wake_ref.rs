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

/// The raw waker vtable of [`TaskWakeSignal`], declared as in 0.2.0 with
/// `declare_raw_waker_vtable!(TaskWakeSignal, TASK_WAKE_SIGNAL_RAW_VTABLE)`. Its functions read a
/// `RawWaker`'s data as the pointer `Arc::<TaskWakeSignal>::into_raw` returns, and the waker owns
/// that strong ref. [`scheduler_waker`] builds its waker from the same four functions.
pub use raw_vtable::TASK_WAKE_SIGNAL_RAW_VTABLE;

mod raw_vtable {
    // A macro invocation cannot carry a doc comment; the `pub use` above documents the static.
    #![allow(missing_docs)]
    use super::TaskWakeSignal;
    use waker_tables::declare_raw_waker_vtable;

    declare_raw_waker_vtable!(TaskWakeSignal, TASK_WAKE_SIGNAL_RAW_VTABLE);
}

/// A `std::task::Waker` that wakes task `id` with `reason`.
pub fn scheduler_waker(
    id: TaskId,
    tx: Sender<SchedulerMsg>,
    reason: WakeReason,
) -> Waker {
    let signal = Arc::new(TaskWakeSignal { id, tx, reason });
    waker_from_arc(signal)
}