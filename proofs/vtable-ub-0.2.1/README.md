# vtable-ub-0.2.1

The 0.3.0 Security probes, run against waker_protocol `=0.2.1` and waker_tables `=0.1.4` from crates.io. `Small` is an 8-byte task and `Big` a 64-byte one (`src/lib.rs`). The only `unsafe` is the `unsafe impl ProtocolTask` the trait requires. Run each command in this folder.

| command | expected output |
|---|---|
| `MIRIFLAGS=-Zmiri-strict-provenance cargo +nightly miri run --bin p1` | `spawn_box -> Ok(TaskId { slot: 0, generation: 0 })`, then `error: Undefined Behavior: constructing invalid value of type &mut ub::Big: encountered a dangling reference (going beyond the bounds of its allocation)` at 0.2.1's `src/task.rs:97` (the macro's `wake_fn`) |
| `MIRIFLAGS=-Zmiri-strict-provenance cargo +nightly miri run --bin p2` | `error: Undefined Behavior: constructing invalid value of type std::boxed::Box<ub::Big>: encountered a dangling box (going beyond the bounds of its allocation)`, from 0.2.1's `src/task.rs:110` (the macro's `destroy_fn`) |
| `MIRIFLAGS=-Zmiri-strict-provenance cargo +nightly miri run --bin p3` | the same as `p2` |
| `MIRIFLAGS=-Zmiri-strict-provenance cargo +nightly miri run --bin p4` | `error: Undefined Behavior: constructing invalid value of type &mut ub::Small: encountered a null reference` at 0.2.1's `src/envelope.rs:31:29` |
| `MIRIFLAGS=-Zmiri-strict-provenance cargo +nightly miri run --bin p5` | `error: Undefined Behavior: constructing invalid value of type &mut ub::Big: encountered a dangling reference (going beyond the bounds of its allocation)` at 0.2.1's `src/envelope.rs:31:29` |
| `MIRIFLAGS=-Zmiri-strict-provenance cargo +nightly miri run --bin p6` | `row present = true, state = Some(Cancelled), Job drops = 0`, then `after dropping the scheduler: Job drops = 1` |
