use std::time::Instant;
use waker_tables::{RegisteredTask, VTableKey};
use crate::{WakeTrigger, TaskId, WakeResult};

pub struct TaskEntry<T> {
    pub id: TaskId,
    pub key: VTableKey,
    pub task: RegisteredTask,
    pub trigger: WakeTrigger<T>,
    pub cancelled: bool,
    pub created_at: Instant,
    pub last_polled: usize,
}

impl<T> TaskEntry<T> {
    pub fn new(id: TaskId, key: VTableKey, task: RegisteredTask, trigger: WakeTrigger<T>) -> Self {
        Self {
            id,
            key,
            task,
            trigger,
            cancelled: false,
            created_at: Instant::now(),
            last_polled: 0,
        }
    }

    pub fn cancel(&mut self) {
        self.cancelled = true;
    }

    pub fn should_poll(&mut self) -> bool {
        !self.cancelled && self.trigger.should_wake()
    }

    pub unsafe fn poll(&mut self) -> WakeResult {
        if self.cancelled {
            WakeResult::Cancelled
        } else {
            self.last_polled += 1;
            unsafe {
                match self.task.poll() {
                    std::task::Poll::Ready(_) => WakeResult::PollReady,
                    std::task::Poll::Pending => WakeResult::PollPending,
                }
            }
        }
    }

    pub unsafe fn wake(&self) {
        if !self.cancelled {
            unsafe {
                self.task.wake();
            }
        }
    }

    pub unsafe fn drop_in_place(&mut self) {
        unsafe {
            self.task.drop_in_place();
        }
    }
}
