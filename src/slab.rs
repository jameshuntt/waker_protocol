use crate::TaskId;

struct Slot<T> {
    generation: u32,
    value: Option<T>,
}

/// Slots of `T`, each reused with a new generation so an old id cannot reach a new value.
pub struct TaskSlab<T> {
    slots: Vec<Slot<T>>,
    free: Vec<u32>,
}

impl<T> Default for TaskSlab<T> {
    fn default() -> Self {
        Self {
            slots: Vec::new(),
            free: Vec::new(),
        }
    }
}

impl<T> TaskSlab<T> {
    /// Store `value`; its id.
    pub fn insert(&mut self, value: T) -> TaskId {
        if let Some(slot_idx) = self.free.pop() {
            let slot = &mut self.slots[slot_idx as usize];
            slot.value = Some(value);
            TaskId::new(slot_idx, slot.generation)
        } else {
            let slot_idx = self.slots.len() as u32;
            self.slots.push(Slot {
                generation: 0,
                value: Some(value),
            });
            TaskId::new(slot_idx, 0)
        }
    }

    /// The value under `id`, if `id` is current.
    pub fn get(&self, id: TaskId) -> Option<&T> {
        let slot = self.slots.get(id.slot as usize)?;
        if slot.generation == id.generation {
            slot.value.as_ref()
        } else {
            None
        }
    }

    /// The value under `id`, if `id` is current.
    pub fn get_mut(&mut self, id: TaskId) -> Option<&mut T> {
        let slot = self.slots.get_mut(id.slot as usize)?;
        if slot.generation == id.generation {
            slot.value.as_mut()
        } else {
            None
        }
    }

    /// Take the value under `id` out and retire the id.
    pub fn remove(&mut self, id: TaskId) -> Option<T> {
        let slot = self.slots.get_mut(id.slot as usize)?;
        if slot.generation != id.generation {
            return None;
        }

        let value = slot.value.take()?;
        slot.generation = slot.generation.wrapping_add(1);
        self.free.push(id.slot);
        Some(value)
    }

    /// Every current id.
    pub fn live_ids(&self) -> Vec<TaskId> {
        let mut out = Vec::new();

        for (idx, slot) in self.slots.iter().enumerate() {
            if slot.value.is_some() {
                out.push(TaskId::new(idx as u32, slot.generation));
            }
        }

        out
    }
}