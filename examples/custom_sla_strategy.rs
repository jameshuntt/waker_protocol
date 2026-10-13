use std::time::Duration;
use waker_protocol::{
    derive_protocol_task_vtable, CapitulatePlan, CapitulateStrategy, CheckpointReason,
    MemoryCheckpointStore, ProtocolTask, QoSClass, TaskPolicy, TaskPoll, TaskSnapshot,
    WakeReason, WakerProtocol,
};
use waker_tables::VTableKey;

#[derive(Debug)]
struct BatchCompactor {
    pages: Vec<Vec<u8>>,
    current: usize,
}

unsafe impl ProtocolTask for BatchCompactor {
    unsafe fn poll(&mut self) -> TaskPoll {
        if self.current >= self.pages.len() {
            return TaskPoll::Ready;
        }

        self.current += 1;
        TaskPoll::Yield
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.current as u64).to_le_bytes());
        out.extend_from_slice(&(self.pages.len() as u64).to_le_bytes());

        for p in &self.pages {
            out.extend_from_slice(&(p.len() as u64).to_le_bytes());
            out.extend_from_slice(p);
        }

        out
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        let current = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
        let count = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;

        let mut cursor = 16;
        let mut pages = Vec::with_capacity(count);

        for _ in 0..count {
            let len = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap()) as usize;
            cursor += 8;
            pages.push(bytes[cursor..cursor + len].to_vec());
            cursor += len;
        }

        Self { pages, current }
    }

    fn footprint_estimate(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.pages.iter().map(|p| p.capacity()).sum::<usize>()
    }
}

derive_protocol_task_vtable!(BatchCompactor, BATCH_COMPACTOR_VTABLE);

struct SlaStrategy;

impl CapitulateStrategy for SlaStrategy {
    fn plan(
        &self,
        snapshot: &TaskSnapshot,
        last_wake: Option<&WakeReason>,
    ) -> Option<CapitulatePlan> {
        if snapshot.qos == QoSClass::Background
            && snapshot.footprint_bytes > 8 * 1024
            && matches!(last_wake, Some(WakeReason::QoS | WakeReason::MemoryPressure))
        {
            return Some(CapitulatePlan {
                reason: CheckpointReason::YieldForSla,
                retain_stub: true,
            });
        }

        if snapshot.qos == QoSClass::Batch
            && snapshot.idle_for.unwrap_or(Duration::ZERO) > Duration::from_secs(2)
            && snapshot.footprint_bytes > 4 * 1024
        {
            return Some(CapitulatePlan {
                reason: CheckpointReason::Idle,
                retain_stub: true,
            });
        }

        None
    }
}

fn main() -> Result<(), String> {
    let mut sched = WakerProtocol::with_strategy(
        MemoryCheckpointStore::default(),
        Box::new(SlaStrategy),
    );

    let key = VTableKey {
        segment: "storage",
        kind: "batch_compactor",
        version: 1,
    };

    sched.register_vtable(key, &BATCH_COMPACTOR_VTABLE);

    let mut policy = TaskPolicy::default();
    policy.qos = QoSClass::Background;
    policy.memory_budget_hint = Some(4096);

    let handle = sched.spawn_box(
        key,
        Box::new(BatchCompactor {
            pages: vec![vec![8; 4096], vec![8; 4096], vec![8; 4096]],
            current: 0,
        }),
        policy,
    )?;

    handle.wake(WakeReason::QoS)?;
    sched.run_once()?; // custom strategy should checkpoint it for SLA reasons

    handle.wake(WakeReason::Resume)?;
    sched.run_once()?; // restore + continue

    println!("reclaim events = {}", sched.reclaim_log.len());
    Ok(())
}