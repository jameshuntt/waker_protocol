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
/// that strong ref.
///
/// Unlike 0.2.0, it is a copy at its own address, not the vtable the crate's wakers carry. A waker
/// from [`scheduler_waker`], and every clone of a `TaskWakeSignal` waker (a clone of one built on
/// this static included), carries `<TaskWakeSignal as UnsafeWakeable>::raw_waker_vtable()`, the
/// static `derive_arc_unsafe_wakeable!` declares. So a waker's vtable compared with this static's
/// address is not equal, and a waker built on this static does not `will_wake` its own clone. To
/// recognise a `TaskWakeSignal` waker by its vtable, compare it with `raw_waker_vtable()`. In
/// 0.2.0 `scheduler_waker` built its waker on this static and every clone kept it; waker_tables
/// 0.3's `waker_from_arc` takes no vtable, and its derive gives no way to name the static it
/// declares.
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