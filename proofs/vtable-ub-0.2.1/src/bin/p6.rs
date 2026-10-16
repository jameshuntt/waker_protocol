use std::sync::atomic::{AtomicUsize, Ordering};
use waker_protocol::*;
static DROPS: AtomicUsize = AtomicUsize::new(0);
struct Job(u64);
impl Drop for Job { fn drop(&mut self) { DROPS.fetch_add(1, Ordering::SeqCst); } }
// SAFETY: the bodies only touch `self`.
unsafe impl ProtocolTask for Job {
    unsafe fn poll(&mut self) -> TaskPoll { self.0 += 1; TaskPoll::Pending }
    unsafe fn checkpoint(&mut self) -> Vec<u8> { self.0.to_le_bytes().to_vec() }
    unsafe fn restore(b: &[u8]) -> Self { Job(u64::from_le_bytes(b[..8].try_into().unwrap())) }
}
derive_protocol_task_vtable!(Job, JOB_VTABLE);
fn main() {
    let mut sched = WakerProtocol::new(MemoryCheckpointStore::default());
    sched.register_vtable(ub::KEY, &JOB_VTABLE);
    let h = sched.spawn_box(ub::KEY, Box::new(Job(0)), TaskPolicy::default()).unwrap();
    let id = h.id();
    sched.run_once().unwrap();
    h.cancel().unwrap();
    for _ in 0..3 { sched.run_once().unwrap(); }
    println!("row present = {}, state = {:?}, Job drops = {}", sched.tasks.get(id).is_some(), sched.tasks.get(id).map(|e| e.state.clone()), DROPS.load(Ordering::SeqCst));
    drop(sched);
    println!("after dropping the scheduler: Job drops = {}", DROPS.load(Ordering::SeqCst));
}
