# vtable-ub-0.3.0

The same probes as `../vtable-ub-0.2.1`, run against waker_protocol `=0.3.0` and waker_tables `=0.3.0` from crates.io. Run each command in this folder. `p3` and `p4` do not compile, which is the point.

| command | expected output |
|---|---|
| `MIRIFLAGS=-Zmiri-strict-provenance cargo +nightly miri run --bin p1` | `spawn_box -> Err("protocol vtable for VTableKey { segment: \"ub\", kind: \"big\", version: 1 } is for ub::Big, not ub::Small")`, then `run_once -> Ok(())` |
| `MIRIFLAGS=-Zmiri-strict-provenance cargo +nightly miri run --bin p2` | a panic, `protocol vtable is for ub::Big, not ub::Small`, exit code 101 |
| `MIRIFLAGS=-Zmiri-strict-provenance cargo +nightly miri run --bin p3` | ``error[E0451]: fields `data` and `vtable` of struct `waker_protocol::ProtocolTaskPtr` are private`` |
| `MIRIFLAGS=-Zmiri-strict-provenance cargo +nightly miri run --bin p4` | ``error[E0133]: call to unsafe function `waker_protocol::Envelope::dispatch` is unsafe and requires unsafe block`` |
| `MIRIFLAGS=-Zmiri-strict-provenance cargo +nightly miri run --bin p5` | `run_once -> Err("envelope \"poke\" for task TaskId { slot: 0, generation: 0 } targets another task type than ub::Small")` |
| `MIRIFLAGS=-Zmiri-strict-provenance cargo +nightly miri run --bin p6` | `row present = false, state = None, Job drops = 1`, then `after dropping the scheduler: Job drops = 1` |
