use waker_protocol::{
    derive_protocol_task_vtable, MemoryCheckpointStore, ProtocolTask, QoSClass, TaskPolicy,
    TaskPoll, WakeReason, WakerProtocol,
};
use waker_tables::VTableKey;
use std::time::{Instant};
use std::collections::HashMap;

// --- 1. The Stateful Chaos Task ---
struct ChaosTask {
    id: u32,
    state: Vec<u64>,
    #[allow(dead_code)]
    wake_counts: HashMap<WakeReason, usize>,
    is_cancelled: bool,
}

// unsafe impl ProtocolTask for ChaosTask {
//     unsafe fn poll(&mut self) -> TaskPoll {
//         // Simulate a task that "works" by allocating and then potentially finishing
//         if self.is_cancelled {
//             return TaskPoll::Ready;
//         }

//         // Heavy state mutation to stress checkpointing
//         self.state.push(self.id as u64);
        
//         // Randomly "finish" some tasks to see how the scheduler handles completion during churn
//         if self.state.len() > 100 {
//             TaskPoll::Ready
//         } else {
//             TaskPoll::Yield
//         }
//     }
// 1. Make the tasks much "heavier" so they actually exceed budgets
// SAFETY: the functions only touch `self`, and `restore` panics rather than misreading short bytes.
unsafe impl ProtocolTask for ChaosTask {
    unsafe fn poll(&mut self) -> TaskPoll {
        if self.is_cancelled { return TaskPoll::Ready; }

        // Push 1KB of data EVERY poll cycle. 
        // With 1000 tasks and 500 steps, that's potentially 500MB.
        let extra = vec![self.id as u8; 1024]; 
        for val in extra {
            self.state.push(val as u64);
        }
        
        if self.state.len() > 5000 { TaskPoll::Ready } else { TaskPoll::Yield }
    }
    // ... rest of implementation ...

    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        // Serialize the full state and the wake history
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.id.to_le_bytes());
        for val in &self.state {
            buf.extend_from_slice(&val.to_le_bytes());
        }
        buf
    }

    unsafe fn restore(bytes: &[u8]) -> Self {
        let id = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let mut state = Vec::new();
        for chunk in bytes[4..].chunks_exact(8) {
            state.push(u64::from_le_bytes(chunk.try_into().unwrap()));
        }
        Self {
            id,
            state,
            wake_counts: HashMap::new(),
            is_cancelled: false,
        }
    }
}

derive_protocol_task_vtable!(ChaosTask, CHAOS_VTABLE);

fn main() -> Result<(), String> {
    let mut sched = WakerProtocol::new(MemoryCheckpointStore::default());
    let key = VTableKey { segment: "test", kind: "chaos", version: 1 };
    sched.register_vtable(key, &CHAOS_VTABLE);

    let task_count = 1000;
    let mut handles = Vec::new();

    println!("🔥 Initializing Chaos Test with {} tasks...", task_count);

    // for i in 0..task_count {
    //     let mut policy = TaskPolicy::default();
    //     // Give 1 in 5 tasks high priority
    //     policy.qos = if i % 5 == 0 { QoSClass::Interactive } else { QoSClass::Background };

    
    for i in 0..1000 {
        // FORCE RECLAMATION: Tell the scheduler this task should only use 1KB.
        // When it hits 5KB in the poll, the scheduler MUST react.
        // Background tasks are evicted first.
        let policy = TaskPolicy {
            memory_budget_hint: Some(1024),
            qos: QoSClass::Background,
            ..TaskPolicy::default()
        };
        let handle = sched.spawn_box(
            key, 
            Box::new(ChaosTask { 
                id: i as u32, 
                state: Vec::with_capacity(10), 
                wake_counts: HashMap::new(),
                is_cancelled: false 
            }), 
            policy
        )?;
        handles.push(handle);
    }

    let start = Instant::now();
    let mut step = 0;

    // // --- 2. The Interference Loop ---
    // while sched.has_pending_tasks() && step < 500 {
    //     step += 1;

    //     // Randomly wake tasks with different reasons to test the library's internal state handling
    //     for (i, handle) in handles.iter().enumerate() {
    //         match step {
    //             s if s % 50 == 0 => { let _ = handle.wake(WakeReason::MemoryPressure); }
    //             s if s % 30 == 0 => { let _ = handle.wake(WakeReason::Timer); }
    //             s if s % 100 == 0 => { let _ = handle.wake(WakeReason::QoS); }
    //             _ => { let _ = handle.wake(WakeReason::Resume); }
    //         }
            
    //         // Test "Cancel" logic mid-flight for 5% of tasks
    //         if i % 20 == 0 && step == 50 {
    //             let _ = handle.wake(WakeReason::Cancel);
    //         }
    //     }

    //     // Execute one scheduling epoch
    //     sched.run_once()?;

    //     if step % 50 == 0 {
    //         println!("Step {}: Active tasks still in flight...", step);
    //     }
    // }

    // // --- 3. The "Grand Finale" (Shutdown Test) ---
    // println!("🛑 Triggering Global Shutdown Reason...");
    // sched.broadcast_wake(WakeReason::Shutdown)?;
    // --- 2. The Interference Loop ---
    // Instead of .has_pending_tasks(), we check if there are any tasks 
    // still registered in the scheduler's internal tracking.
    while step < 500 {
        step += 1;

        // Manually broadcast wakes since .broadcast_wake() doesn't exist
        for handle in &handles {
            match step {
                s if s % 50 == 0 => { let _ = handle.wake(WakeReason::MemoryPressure); }
                s if s % 30 == 0 => { let _ = handle.wake(WakeReason::Timer); }
                s if s % 100 == 0 => { let _ = handle.wake(WakeReason::QoS); }
                _ => { let _ = handle.wake(WakeReason::Resume); }
            }
        }

        sched.run_once()?;

        // Break early if everything is finished (all tasks returned Ready)
        // This assumes your scheduler has a way to check active counts, 
        // otherwise, let the 'step' limit handle the exit.
        if step % 50 == 0 {
            println!("Step {}: Pumping the scheduler queue...", step);
        }
    }

    // --- 3. The "Grand Finale" (Manual Shutdown) ---
    println!("🛑 Triggering Global Shutdown Reason...");
    
    // Manual broadcast for shutdown
    for handle in &handles {
        let _ = handle.wake(WakeReason::Shutdown);
    }
    
    sched.run_once()?;
    sched.run_once()?; 

    println!("--- Chaos Results ---");
    println!("Elapsed: {:?}", start.elapsed());
    println!("Reclaim Events: {}", sched.reclaim_log.len());
    
    // Verify that the library didn't crash during massive QoS/Memory churn
    if !sched.reclaim_log.is_empty() {
        let sample = &sched.reclaim_log[0];
        println!("Sample Reclaim: Task {:?} for reason {:?}", sample.id, sample.reason);
    }

    Ok(())
}