use waker_protocol::{
    derive_protocol_task_vtable, CapitulatePlan, CheckpointReason, MemoryCheckpointStore,
    ProtocolTask, QoSClass, TaskPolicy, TaskPoll, WakerProtocol,
};
use waker_tables::VTableKey;

#[derive(Debug, Clone)]
struct Job {
    cost: u32,
}

#[derive(Debug)]
struct MailboxBacklogTask {
    queue: Vec<Job>,
    watermark: usize,
}

impl MailboxBacklogTask {
    fn push_job(&mut self, job: Job) {
        self.queue.push(job);
    }
}

// SAFETY: the functions only touch `self`, and `restore` panics rather than misreading short bytes.
unsafe impl ProtocolTask for MailboxBacklogTask {
    unsafe fn poll(&mut self) -> TaskPoll {
        if self.queue.len() > self.watermark {
            return TaskPoll::Capitulate(CapitulatePlan {
                reason: CheckpointReason::Stall,
                retain_stub: true,
            });
        }

        if self.queue.is_empty() {
            return TaskPoll::Park;
        }

        let n = self.queue.len().min(4);
        self.queue.drain(..n);
        TaskPoll::Yield
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.watermark as u64).to_le_bytes());
        out.extend_from_slice(&(self.queue.len() as u64).to_le_bytes());
        for j in &self.queue {
            out.extend_from_slice(&j.cost.to_le_bytes());
        }
        out
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        let watermark = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as usize;
        let count = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;

        let mut cursor = 16;
        let mut queue = Vec::with_capacity(count);
        for _ in 0..count {
            let cost = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap());
            cursor += 4;
            queue.push(Job { cost });
        }

        Self { queue, watermark }
    }

    fn footprint_estimate(&self) -> usize {
        std::mem::size_of::<Self>() + self.queue.capacity() * std::mem::size_of::<Job>()
    }
}

derive_protocol_task_vtable!(MailboxBacklogTask, MAILBOX_BACKLOG_TASK_VTABLE);

fn deliver_job(task: &mut MailboxBacklogTask, job: Job) -> Result<(), String> {
    task.push_job(job);
    Ok(())
}

fn main() -> Result<(), String> {
    let mut sched = WakerProtocol::new(MemoryCheckpointStore::default());

    let key = VTableKey {
        segment: "jobs",
        kind: "backlog_worker",
        version: 1,
    };

    sched.register_vtable(key, &MAILBOX_BACKLOG_TASK_VTABLE);

    let policy = TaskPolicy { qos: QoSClass::Batch, ..TaskPolicy::default() };

    let handle = sched.spawn_box(
        key,
        Box::new(MailboxBacklogTask {
            queue: vec![],
            watermark: 8,
        }),
        policy,
    )?;

    for _ in 0..12 {
        handle.send("job", Job { cost: 10 }, deliver_job)?;
    }

    sched.run_once()?; // should checkpoint because backlog > watermark
    handle.wake(waker_protocol::WakeReason::Resume)?;
    sched.run_once()?; // restore + continue reducing queue

    for event in &sched.reclaim_log {
        println!("task {:?} reclaimed {} bytes for {:?}", event.id, event.reclaimed_bytes, event.reason);
    }
    let first = sched.reclaim_log.first().expect("the scenario reclaims the task");
    assert_eq!(first.id, handle.id());
    assert_eq!(first.reason, waker_protocol::CheckpointReason::Stall);
    assert_eq!(sched.tasks.get(handle.id()).unwrap().stats.restore_count, 1);
    Ok(())
}