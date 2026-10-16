use ub::*; use waker_protocol::*;
fn main() {
    let mut sched = WakerProtocol::new(MemoryCheckpointStore::default());
    sched.register_vtable(KEY, &SMALL_VTABLE);
    let id = sched.spawn_box(KEY, Box::new(Small(1)), TaskPolicy::default()).unwrap().id();
    sched.run_once().unwrap();
    sched.tasks.get_mut(id).unwrap().mailbox.push_back(typed_envelope::<Big, u64>("poke", 7, |t, n| { t.0[7] = n; Ok(()) }));
    println!("run_once -> {:?}", sched.run_once());
}
