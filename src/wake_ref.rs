use crate::{SchedulerMsg, TaskId, WakeReason};
use crossbeam_channel::Sender;
use std::sync::Arc;
use std::task::Waker;
use waker_tables::{
    declare_raw_waker_vtable, derive_arc_unsafe_wakeable, waker_from_arc,
};

pub struct TaskWakeSignal {
    pub id: TaskId,
    pub tx: Sender<SchedulerMsg>,
    pub reason: WakeReason,
}

impl TaskWakeSignal {
    pub fn emit(self: Arc<Self>) {
        let _ = self.tx.send(SchedulerMsg::Wake {
            id: self.id,
            reason: self.reason.clone(),
        });
    }
}

derive_arc_unsafe_wakeable!(TaskWakeSignal, emit);
declare_raw_waker_vtable!(TaskWakeSignal, TASK_WAKE_SIGNAL_RAW_VTABLE);

pub fn scheduler_waker(
    id: TaskId,
    tx: Sender<SchedulerMsg>,
    reason: WakeReason,
) -> Waker {
    let signal = Arc::new(TaskWakeSignal { id, tx, reason });
    waker_from_arc(signal, &TASK_WAKE_SIGNAL_RAW_VTABLE)
}