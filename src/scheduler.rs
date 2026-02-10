use std::collections::HashMap;
use waker_tables::{RegisteredTask, VTableKey};
use crate::{TaskEntry, WakeTrigger, TaskId, WakeResult};

pub struct WakerScheduler<T> {
    pub tasks: HashMap<TaskId, TaskEntry<T>>,
    pub next_id: u64,
}

impl<T> WakerScheduler<T> {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
            next_id: 1,
        }
    }

    pub fn spawn(
        &mut self,
        key: VTableKey,
        task: RegisteredTask,
        trigger: WakeTrigger<T>,
    ) -> TaskId {
        let id = TaskId(self.next_id);
        self.next_id += 1;

        let entry = TaskEntry::new(id, key, task, trigger);
        self.tasks.insert(id, entry);
        id
    }

    pub fn cancel(&mut self, id: TaskId) {
        if let Some(entry) = self.tasks.get_mut(&id) {
            entry.cancel();
        }
    }

    pub fn wake(&mut self, id: TaskId) {
        if let Some(entry) = self.tasks.get(&id) {
            unsafe { entry.wake() };
        }
    }

    pub fn tick(&mut self) {
        let mut to_remove = vec![];

        for (id, entry) in self.tasks.iter_mut() {
            if entry.cancelled {
                unsafe { entry.drop_in_place() };
                to_remove.push(*id);
                continue;
            }

            if entry.should_poll() {
                let result = unsafe { entry.poll() };
                match result {
                    WakeResult::PollReady | WakeResult::Dropped => {
                        to_remove.push(*id);
                    }
                    WakeResult::PollPending | WakeResult::Cancelled => {}
                }
            }
        }

        for id in to_remove {
            self.tasks.remove(&id);
        }
    }
}
