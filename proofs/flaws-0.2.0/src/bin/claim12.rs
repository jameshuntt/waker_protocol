// Claim 12: slot generation wraps so an OLD TaskId reaches a NEW task.
// TaskSlab generation is u32, bumped with wrapping_add and never guarded.
// Reproduce by cycling one slot 2^32 times so its generation returns to 0,
// then showing a TaskId captured at gen 0 now resolves to a brand-new task.
use waker_protocol::TaskSlab;

fn main() {
    let mut slab: TaskSlab<u64> = TaskSlab::default();

    // First occupant of slot 0, generation 0.
    let old_id = slab.insert(111);
    println!("old_id = {:?}", old_id);
    // Remove it (generation -> 1), slot goes on the free list.
    slab.remove(old_id);

    // Confirm the stale id is correctly rejected right now.
    println!("stale id resolves before wrap: {:?}", slab.get(old_id));

    // Cycle the single free slot 2^32 - 1 more times. Each insert reuses slot 0
    // (it is the only free slot); each remove bumps its generation by one.
    // After a further (2^32 - 1) removes, generation has wrapped back to 0.
    let cycles: u64 = (1u64 << 32) - 1;
    let mut last = old_id;
    for _ in 0..cycles {
        let id = slab.insert(222);
        last = id;
        slab.remove(id);
    }

    // Now insert a NEW task. Its generation should be 0 again (wrapped),
    // which equals old_id.generation.
    let new_id = slab.insert(999);
    println!("new_id = {:?}", new_id);

    let collide = slab.get(old_id).copied();
    println!("last cycled id before final insert = {:?}", last);
    println!("slab.get(old_id) after wrap = {:?}", collide);
    assert_eq!(old_id.slot, new_id.slot);
    assert_eq!(
        old_id.generation, new_id.generation,
        "generation wrapped back to the old value"
    );
    assert_eq!(collide, Some(999), "the OLD TaskId now resolves to the NEW task");
    println!("CLAIM12 REPRODUCED: stale TaskId {:?} now reaches new task value {:?}", old_id, collide);
}
