use waker_protocol::{
    derive_protocol_task_vtable, CapitulatePlan, CheckpointReason, MemoryCheckpointStore,
    ProtocolTask, TaskPolicy, TaskPoll, WakeReason, WakerProtocol,
};
use waker_tables::VTableKey;

#[derive(Debug)]
struct BlobTask {
    buffer: Vec<u8>,
    cursor: usize,
    pressure: bool,
}

// SAFETY: the functions only touch `self`, and `restore` panics rather than misreading short bytes.
unsafe impl ProtocolTask for BlobTask {
    unsafe fn poll(&mut self) -> TaskPoll {
        if self.pressure || self.buffer.len() > 4096 {
            self.pressure = false;
            return TaskPoll::Capitulate(CapitulatePlan {
                reason: CheckpointReason::MemoryPressure,
                retain_stub: true,
            });
        }

        if self.cursor >= self.buffer.len() {
            return TaskPoll::Ready;
        }

        let end = (self.cursor + 256).min(self.buffer.len());
        let _slice = &self.buffer[self.cursor..end];
        self.cursor = end;

        if self.cursor < self.buffer.len() {
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
        let mut out = Vec::with_capacity(16 + self.buffer.len());
        out.extend_from_slice(&(self.cursor as u64).to_le_bytes());
        out.extend_from_slice(&(self.buffer.len() as u64).to_le_bytes());
        out.extend_from_slice(&self.buffer);
        out
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        let cursor = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
        let len = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let buffer = bytes[16..16 + len].to_vec();

        Self {
            buffer,
            cursor,
            pressure: false,
        }
    }

    fn footprint_estimate(&self) -> usize {
        std::mem::size_of::<Self>() + self.buffer.capacity()
    }
}

derive_protocol_task_vtable!(BlobTask, BLOB_TASK_VTABLE);

fn main() -> Result<(), String> {
    let mut sched = WakerProtocol::new(MemoryCheckpointStore::default());

    let key = VTableKey {
        segment: "blob",
        kind: "ingest",
        version: 1,
    };

    sched.register_vtable(key, &BLOB_TASK_VTABLE);

    let task = sched.spawn_box(
        key,
        Box::new(BlobTask {
            buffer: vec![7; 10_000],
            cursor: 0,
            pressure: false,
        }),
        TaskPolicy::default(),
    )?;

    task.wake(WakeReason::Manual)?;
    sched.run_once()?; // likely checkpoints into dormant stub

    task.wake(WakeReason::Resume)?;
    sched.run_once()?; // restore + continue

    for event in &sched.reclaim_log {
        println!("task {:?} reclaimed {} bytes for {:?}", event.id, event.reclaimed_bytes, event.reason);
    }
    let first = sched.reclaim_log.first().expect("the scenario reclaims the task");
    assert_eq!(first.id, task.id());
    assert_eq!(first.reason, waker_protocol::CheckpointReason::MemoryPressure);
    assert_eq!(sched.tasks.get(task.id()).unwrap().stats.restore_count, 1);
    Ok(())
}