use ub::*; use waker_protocol::*;
fn main() {
    let mut sched = WakerProtocol::new(MemoryCheckpointStore::default());
    let key = KEY;
    sched.register_vtable(key, &BIG_VTABLE);
    println!("spawn_box -> {:?}", sched.spawn_box(key, Box::new(Small(1)), TaskPolicy::default()).map(|h| h.id()));
    println!("run_once -> {:?}", sched.run_once());
}
