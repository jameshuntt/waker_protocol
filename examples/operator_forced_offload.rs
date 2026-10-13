use waker_protocol::{
    derive_protocol_task_vtable, CapitulatePlan, CheckpointReason, MemoryCheckpointStore,
    ProtocolTask, TaskPolicy, TaskPoll, WakeReason, WakerProtocol,
};
use waker_tables::VTableKey;

#[derive(Debug)]
struct AnalyticsTask {
    working_set: Vec<u8>,
    cursor: usize,
}

unsafe impl ProtocolTask for AnalyticsTask {
    unsafe fn poll(&mut self) -> TaskPoll {
        if self.cursor >= self.working_set.len() {
            return TaskPoll::Ready;
        }

        let step = 256.min(self.working_set.len() - self.cursor);
        self.cursor += step;
        TaskPoll::Yield
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.cursor as u64).to_le_bytes());
        out.extend_from_slice(&(self.working_set.len() as u64).to_le_bytes());
        out.extend_from_slice(&self.working_set);
        out
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        let cursor = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
        let len = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
        let working_set = bytes[16..16 + len].to_vec();

        Self { working_set, cursor }
    }

    fn footprint_estimate(&self) -> usize {
        std::mem::size_of::<Self>() + self.working_set.capacity()
    }
}

derive_protocol_task_vtable!(AnalyticsTask, ANALYTICS_TASK_VTABLE);

fn main() -> Result<(), String> {
    let mut sched = WakerProtocol::new(MemoryCheckpointStore::default());

    let key = VTableKey {
        segment: "analytics",
        kind: "heavy_scan",
        version: 1,
    };

    sched.register_vtable(key, &ANALYTICS_TASK_VTABLE);

    let handle = sched.spawn_box(
        key,
        Box::new(AnalyticsTask {
            working_set: vec![3; 16 * 1024],
            cursor: 0,
        }),
        TaskPolicy::default(),
    )?;

    handle.wake(WakeReason::Manual)?;
    sched.run_once()?;

    handle.capitulate(CapitulatePlan {
        reason: CheckpointReason::Manual,
        retain_stub: true,
    })?;
    sched.run_once()?; // forced operator-driven offload

    handle.wake(WakeReason::Resume)?;
    sched.run_once()?; // restore and continue

    println!("reclaim events = {}", sched.reclaim_log.len());
    Ok(())
}