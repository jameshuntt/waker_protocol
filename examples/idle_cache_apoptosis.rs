use waker_protocol::{
    derive_protocol_task_vtable, CheckpointReason, MemoryCheckpointStore, ProtocolTask,
    QoSClass, TaskPolicy, TaskPoll, WakeReason, WakerProtocol,
};
use waker_tables::VTableKey;

#[derive(Debug, Clone)]
struct Session {
    id: u64,
    hits: u64,
}

#[derive(Debug)]
struct IdleCacheTask {
    sessions: Vec<Session>,
    idle_ticks: u32,
    idle_before_checkpoint: u32,
}

impl IdleCacheTask {
    fn touch(&mut self, id: u64) {
        if let Some(s) = self.sessions.iter_mut().find(|s| s.id == id) {
            s.hits += 1;
        } else {
            self.sessions.push(Session { id, hits: 1 });
        }
        self.idle_ticks = 0;
    }
}

unsafe impl ProtocolTask for IdleCacheTask {
    unsafe fn poll(&mut self) -> TaskPoll {
        if self.idle_ticks >= self.idle_before_checkpoint {
            return TaskPoll::Checkpoint(CheckpointReason::Idle);
        }

        TaskPoll::Park
    }

    unsafe fn on_wake(&mut self, reason: WakeReason) {
        if matches!(reason, WakeReason::Timer) {
            self.idle_ticks += 1;
        }
    }

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.idle_ticks as u64).to_le_bytes());
        out.extend_from_slice(&(self.idle_before_checkpoint as u64).to_le_bytes());
        out.extend_from_slice(&(self.sessions.len() as u64).to_le_bytes());

        for s in &self.sessions {
            out.extend_from_slice(&s.id.to_le_bytes());
            out.extend_from_slice(&s.hits.to_le_bytes());
        }

        out
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        let idle_ticks = u64::from_le_bytes(bytes[0..8].try_into().unwrap()) as u32;
        let idle_before_checkpoint = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as u32;
        let count = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;

        let mut cursor = 24;
        let mut sessions = Vec::with_capacity(count);

        for _ in 0..count {
            let id = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            let hits = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            sessions.push(Session { id, hits });
        }

        Self {
            sessions,
            idle_ticks,
            idle_before_checkpoint,
        }
    }

    fn footprint_estimate(&self) -> usize {
        std::mem::size_of::<Self>() + self.sessions.capacity() * std::mem::size_of::<Session>()
    }
}

derive_protocol_task_vtable!(IdleCacheTask, IDLE_CACHE_TASK_VTABLE);

fn deliver_touch(task: &mut IdleCacheTask, id: u64) -> Result<(), String> {
    task.touch(id);
    Ok(())
}

fn main() -> Result<(), String> {
    let mut sched = WakerProtocol::new(MemoryCheckpointStore::default());

    let key = VTableKey {
        segment: "cache",
        kind: "session_idle",
        version: 1,
    };

    sched.register_vtable(key, &IDLE_CACHE_TASK_VTABLE);

    let mut policy = TaskPolicy::default();
    policy.qos = QoSClass::Background;

    let handle = sched.spawn_box(
        key,
        Box::new(IdleCacheTask {
            sessions: vec![],
            idle_ticks: 0,
            idle_before_checkpoint: 3,
        }),
        policy,
    )?;

    handle.send("touch", 10_u64, deliver_touch)?;
    sched.run_once()?;

    handle.wake(WakeReason::Timer)?;
    sched.run_once()?;
    handle.wake(WakeReason::Timer)?;
    sched.run_once()?;
    handle.wake(WakeReason::Timer)?;
    sched.run_once()?; // checkpoints due to idle

    handle.send("touch", 10_u64, deliver_touch)?;
    handle.wake(WakeReason::Resume)?;
    sched.run_once()?; // restore + process touch

    println!("reclaim events = {}", sched.reclaim_log.len());
    Ok(())
}