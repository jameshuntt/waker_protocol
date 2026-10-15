use waker_protocol::{
    derive_protocol_task_vtable, CapitulatePlan, CheckpointReason, MemoryCheckpointStore,
    ProtocolTask, TaskPolicy, TaskPoll, WakeReason, WakerProtocol,
};
use waker_tables::VTableKey;

#[derive(Debug)]
struct ChunkReducer {
    data: Vec<u8>,
    cursor: usize,
    soft_limit: usize,
    pressure: bool,
}

impl ChunkReducer {
    fn push_chunk(&mut self, chunk: Vec<u8>) {
        self.data.extend_from_slice(&chunk);
    }
}

// SAFETY: the functions only touch `self`, and `restore` panics rather than misreading short bytes.
unsafe impl ProtocolTask for ChunkReducer {
    unsafe fn poll(&mut self) -> TaskPoll {
        if self.pressure || self.data.capacity() > self.soft_limit {
            self.pressure = false;
            return TaskPoll::Capitulate(CapitulatePlan {
                reason: CheckpointReason::MemoryPressure,
                retain_stub: true,
            });
        }

        if self.cursor >= self.data.len() {
            return TaskPoll::Ready;
        }

        let end = (self.cursor + 128).min(self.data.len());
        let _slice = &self.data[self.cursor..end];
        self.cursor = end;

        if self.cursor < self.data.len() {
            TaskPoll::Yield
        } else {
            TaskPoll::Ready
        }
    }

    unsafe fn on_wake(&mut self, reason: WakeReason) {
        if matches!(reason, WakeReason::MemoryPressure | WakeReason::QoS) {
            self.pressure = true;
        }
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.cursor as u64).to_le_bytes());
        out.extend_from_slice(&(self.soft_limit as u64).to_le_bytes());
        out.extend_from_slice(&(self.data.len() as u64).to_le_bytes());
        out.extend_from_slice(&self.data);
        out
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        let cursor = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
        let soft_limit = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let len = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
        let data = bytes[24..24 + len].to_vec();

        Self {
            data,
            cursor,
            soft_limit,
            pressure: false,
        }
    }

    fn footprint_estimate(&self) -> usize {
        std::mem::size_of::<Self>() + self.data.capacity()
    }
}

derive_protocol_task_vtable!(ChunkReducer, CHUNK_REDUCER_VTABLE);

fn deliver_chunk(task: &mut ChunkReducer, chunk: Vec<u8>) -> Result<(), String> {
    task.push_chunk(chunk);
    Ok(())
}

fn main() -> Result<(), String> {
    let mut sched = WakerProtocol::new(MemoryCheckpointStore::default());

    let key = VTableKey {
        segment: "blob",
        kind: "chunk_reducer",
        version: 1,
    };

    sched.register_vtable(key, &CHUNK_REDUCER_VTABLE);

    let handle = sched.spawn_box(
        key,
        Box::new(ChunkReducer {
            data: vec![1; 2048],
            cursor: 0,
            soft_limit: 1024,
            pressure: false,
        }),
        TaskPolicy::default(),
    )?;

    handle.send("chunk", vec![9; 2048], deliver_chunk)?;
    handle.wake(WakeReason::Manual)?;
    sched.run_once()?; // should capitulate into checkpoint-backed dormant state

    handle.wake(WakeReason::Resume)?;
    sched.run_once()?; // restores and continues

    for event in &sched.reclaim_log {
        println!("task {:?} reclaimed {} bytes for {:?}", event.id, event.reclaimed_bytes, event.reason);
    }
    let first = sched.reclaim_log.first().expect("the scenario reclaims the task");
    assert_eq!(first.id, handle.id());
    assert_eq!(first.reason, waker_protocol::CheckpointReason::MemoryPressure);
    assert_eq!(sched.tasks.get(handle.id()).unwrap().stats.restore_count, 1);
    Ok(())
}