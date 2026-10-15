use waker_protocol::{
    derive_protocol_task_vtable, MemoryCheckpointStore, ProtocolTask, QoSClass, TaskPolicy,
    TaskPoll, WakeReason, WakerProtocol,
};
use waker_tables::VTableKey;

#[derive(Debug)]
struct UiTask {
    frames: usize,
}

// SAFETY: the functions only touch `self`, and `restore` panics rather than misreading short bytes.
unsafe impl ProtocolTask for UiTask {
    unsafe fn poll(&mut self) -> TaskPoll {
        self.frames += 1;
        TaskPoll::Yield
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        self.frames.to_le_bytes().to_vec()
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        let mut arr = [0u8; std::mem::size_of::<usize>()];
        arr.copy_from_slice(&bytes[..std::mem::size_of::<usize>()]);
        Self {
            frames: usize::from_le_bytes(arr),
        }
    }
}

derive_protocol_task_vtable!(UiTask, UI_TASK_VTABLE);

#[derive(Debug)]
struct IndexerTask {
    buffer: Vec<u8>,
}

// SAFETY: the functions only touch `self`, and `restore` panics rather than misreading short bytes.
unsafe impl ProtocolTask for IndexerTask {
    unsafe fn poll(&mut self) -> TaskPoll {
        if self.buffer.is_empty() {
            TaskPoll::Ready
        } else {
            let cut = self.buffer.len().min(512);
            self.buffer.drain(..cut);
            TaskPoll::Yield
        }
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        self.buffer.clone()
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        Self {
            buffer: bytes.to_vec(),
        }
    }

    fn footprint_estimate(&self) -> usize {
        std::mem::size_of::<Self>() + self.buffer.capacity()
    }
}

derive_protocol_task_vtable!(IndexerTask, INDEXER_TASK_VTABLE);

fn main() -> Result<(), String> {
    let mut sched = WakerProtocol::new(MemoryCheckpointStore::default());

    let ui_key = VTableKey {
        segment: "ui",
        kind: "renderer",
        version: 1,
    };
    let batch_key = VTableKey {
        segment: "search",
        kind: "indexer",
        version: 1,
    };

    sched.register_vtable(ui_key, &UI_TASK_VTABLE);
    sched.register_vtable(batch_key, &INDEXER_TASK_VTABLE);

    let ui_policy = TaskPolicy { qos: QoSClass::Interactive, ..TaskPolicy::default() };

    let batch_policy = TaskPolicy {
        qos: QoSClass::Background,
        memory_budget_hint: Some(1024),
        ..TaskPolicy::default()
    };

    let ui = sched.spawn_box(ui_key, Box::new(UiTask { frames: 0 }), ui_policy)?;
    let batch = sched.spawn_box(
        batch_key,
        Box::new(IndexerTask {
            buffer: vec![7; 32 * 1024],
        }),
        batch_policy,
    )?;

    ui.wake(WakeReason::MemoryPressure)?;
    batch.wake(WakeReason::MemoryPressure)?;
    sched.run_once()?;


    for event in &sched.reclaim_log {
        println!("task {:?} reclaimed {} bytes for {:?}", event.id, event.reclaimed_bytes, event.reason);
    }
    let first = sched.reclaim_log.first().expect("the scenario reclaims the task");
    assert_eq!(first.id, batch.id());
    assert_eq!(first.reason, waker_protocol::CheckpointReason::MemoryPressure);
    assert_eq!(sched.tasks.get(batch.id()).unwrap().stats.restore_count, 0);
    assert!(sched.reclaim_log.iter().all(|e| e.id != ui.id()), "the interactive task is never reclaimed");
    Ok(())
}