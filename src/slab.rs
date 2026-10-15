use crate::TaskId;

struct Slot<T> {
    generation: u32,
    value: Option<T>,
}

/// Slots of `T`. A freed slot is reused with the next generation, and a slot whose generation
/// has reached `u32::MAX` is retired instead of wrapping, so an old id never reaches a new value.
/// A retired slot stays empty for good: one slot's memory per 2^32 reuses.
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
        // At the last generation the slot is retired: never reused, so no old id matches it again.
        if let Some(next) = slot.generation.checked_add(1) {
            slot.generation = next;
            self.free.push(id.slot);
        }
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
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slot_is_retired_instead_of_wrapping_its_generation() {
        let mut slab = TaskSlab::default();
        let old = slab.insert("old");
        assert_eq!(slab.remove(old), Some("old"));

        // Fast-forward slot 0 to its last generation, as 2^32 - 1 more reuses would.
        slab.slots[0].generation = u32::MAX;
        let last = slab.insert("last");
        assert_eq!(last, TaskId::new(0, u32::MAX));
        assert_eq!(slab.remove(last), Some("last"));

        let new = slab.insert("new");
        assert_eq!(slab.get(old), None, "old {old:?} must not reach new {new:?}");
        assert_eq!(slab.get(last), None);
        assert_ne!(new.slot, old.slot, "slot 0 is retired");
        assert_eq!(slab.live_ids(), vec![new]);
    }
}
