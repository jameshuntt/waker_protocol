// Memory-safety reproductions against ORIGINAL waker_protocol 0.2.0.
// Run under Miri: `cargo +nightly miri test --test mem`.
//
// Each test is EXPECTED to trigger a Miri-detected UB on 0.2.0. Run them one
// at a time; Miri aborts the process at the first UB.

use waker_protocol::{
    derive_protocol_task_vtable, typed_envelope, ProtocolTask, ProtocolTaskPtr, TaskPoll,
    TaskPolicy, WakerProtocol, MemoryCheckpointStore,
};
use waker_tables::VTableKey;

// Type A: owns a heap box; its functions dereference that pointer.
struct TypeA {
    boxed: Box<u64>,
}
unsafe impl ProtocolTask for TypeA {
    unsafe fn poll(&mut self) -> TaskPoll {
        // Reads through the owned pointer at offset 0.
        let _ = *self.boxed;
        TaskPoll::Pending
    }
    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        Vec::new()
    }
    unsafe fn restore(_: &[u8]) -> Self {
        TypeA { boxed: Box::new(0) }
    }
    fn footprint_estimate(&self) -> usize {
        // Also dereferences the owned pointer.
        (*self.boxed) as usize
    }
}
derive_protocol_task_vtable!(TypeA, TYPE_A_VTABLE);

// Type B: a single byte. Smaller than A; holds no pointer.
struct TypeB {
    _x: u8,
}
unsafe impl ProtocolTask for TypeB {
    unsafe fn poll(&mut self) -> TaskPoll {
        TaskPoll::Pending
    }
    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        Vec::new()
    }
    unsafe fn restore(_: &[u8]) -> Self {
        TypeB { _x: 0 }
    }
}
derive_protocol_task_vtable!(TypeB, TYPE_B_VTABLE);

// ---- Claim 1: spawn_box accepts a key whose vtable is for another type -----
#[test]
fn claim1_spawn_box_type_confusion() {
    let mut sched = WakerProtocol::new(MemoryCheckpointStore::default());
    let k = VTableKey { segment: "mem", kind: "a", version: 1 };
    // Register A's vtable under key k.
    sched.register_vtable(k, &TYPE_A_VTABLE);

    // Spawn a B under the SAME key. spawn_box is safe and takes the vtable from
    // the registry (A's), not from T=B.
    let _h = sched
        .spawn_box(k, Box::new(TypeB { _x: 7 }), TaskPolicy::default())
        .unwrap();

    // Driving the scheduler calls A's functions on the B allocation -> UB.
    let _ = sched.run_once();
}

// ---- Claim 2: Envelope::dispatch (safe method) with a mismatched type ------
#[test]
fn claim2_dispatch_type_confusion() {
    // A's allocation (owns one u64 pointer). We hand dispatch a pointer to A
    // but an envelope typed for B2, whose deliver writes far past A's size.
    struct Small {
        _x: u8,
    }
    let mut small = Small { _x: 0 };
    let ptr = &mut small as *mut Small as *mut ();

    struct Big {
        a: u64,
        b: u64,
        c: u64,
    }
    // dispatch() is a SAFE method; typed_envelope() is SAFE. No `unsafe` here.
    let env = typed_envelope::<Big, u64>("x", 0xAAAA_BBBB, |t: &mut Big, m: u64| {
        t.a = m;
        t.b = m;
        t.c = m; // writes 24 bytes into a 1-byte allocation -> UB
        Ok(())
    });
    let _ = env.dispatch(ptr);
}

// ---- Claim 3: ProtocolTaskPtr pub fields let safe code mismatch ptr+vtable -
#[test]
fn claim3_protocol_task_ptr_pub_fields_mismatch() {
    // Build the mismatched pair entirely in SAFE code: a B allocation paired
    // with A's vtable, using the public fields directly.
    let b = Box::new(TypeB { _x: 9 });
    let ptr = ProtocolTaskPtr {
        data: Box::into_raw(b) as *mut (),
        vtable: &TYPE_A_VTABLE,
    };

    // Using it runs A's functions on the B allocation -> UB (reads a pointer
    // out of a 1-byte allocation, then dereferences it).
    let mut ptr = ptr;
    let _ = unsafe { ptr.poll() };
}
