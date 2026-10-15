use crate::types::TaskId;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

#[derive(Clone, PartialEq, Eq, Hash)]
/// A key into a [`MemoryCheckpointStore`].
pub struct CheckpointHandle(pub String);

impl fmt::Debug for CheckpointHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CheckpointHandle").field(&self.0).finish()
    }
}

/// Where checkpoint bytes are kept.
pub trait CheckpointStore: Send + Sync + 'static {
    /// What `save` hands back to find the bytes again.
    type Handle: Clone + fmt::Debug + Send + Sync + 'static;

    /// Keep `payload` for `task_id`.
    fn save(&self, task_id: TaskId, payload: Vec<u8>) -> Result<Self::Handle, String>;
    /// The bytes `handle` points at.
    fn load(&self, handle: &Self::Handle) -> Result<Vec<u8>, String>;
    /// Forget the bytes `handle` points at.
    fn remove(&self, handle: &Self::Handle) -> Result<(), String>;
}

/// Checkpoints held in memory, keyed by task and a running number.
pub struct MemoryCheckpointStore {
    next: AtomicU64,
    inner: RwLock<HashMap<String, Vec<u8>>>,
    // capacity: usize, // Add a limit in bytes
    // current_usage: AtomicUsize, // Track how much is in there
}

impl Default for MemoryCheckpointStore {
    fn default() -> Self {
        Self {
            next: AtomicU64::new(1),
            inner: RwLock::new(HashMap::new()),
        }
    }
}
// impl MemoryCheckpointStore {
//     pub fn with_capacity(capacity: usize) -> Self {
//         Self {
//             next: AtomicU64::new(1),
//             inner: RwLock::new(HashMap::new()),
//             capacity,
//             current_usage: AtomicUsize::new(0),
//         }
//     }
// }
impl CheckpointStore for MemoryCheckpointStore {
    type Handle = CheckpointHandle;

    fn save(&self, task_id: TaskId, payload: Vec<u8>) -> Result<Self::Handle, String> {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        let key = format!("task-{}-{}-ckpt-{}", task_id.slot, task_id.generation, n);

        let mut guard = self.inner.write().map_err(|_| "checkpoint store poisoned".to_string())?;
        guard.insert(key.clone(), payload);

        Ok(CheckpointHandle(key))
    }

    fn load(&self, handle: &Self::Handle) -> Result<Vec<u8>, String> {
        let guard = self.inner.read().map_err(|_| "checkpoint store poisoned".to_string())?;
        guard
            .get(&handle.0)
            .cloned()
            .ok_or_else(|| format!("checkpoint not found: {}", handle.0))
    }

    fn remove(&self, handle: &Self::Handle) -> Result<(), String> {
        let mut guard = self.inner.write().map_err(|_| "checkpoint store poisoned".to_string())?;
        guard.remove(&handle.0);
        Ok(())
    }
}