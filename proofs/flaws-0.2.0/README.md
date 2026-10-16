# flaws-0.2.0

Probes for flaws in waker_protocol 0.2.0 that later versions fixed. The crate is pinned to waker_protocol `=0.2.0` and waker_tables `=0.1.4` from crates.io. The CHANGELOG's "In 0.2.0" lines come from here. Run each command in this folder.

Each test in `tests/logic.rs` asserts the flaw, so on 0.2.0 it passes and prints the line shown. Each test in `tests/mem.rs` makes Miri stop with undefined behavior; run them one at a time.

| command | expected output |
|---|---|
| `cargo test --test logic claim4_ -- --nocapture` | `claim4: present_after_cancel=true state=Some(Cancelled) freed_before_drop=false` |
| `cargo test --test logic claim5_ -- --nocapture` | `claim5: run_once_err=true present=true resident_empty=true freed=true` |
| `cargo test --test logic claim6_ -- --nocapture` | `claim6: run_once_err=true present=true resident_empty=true freed=true` |
| `cargo test --test logic claim7_ -- --nocapture` | `claim7: run_once_err=true completed_A_still_present=true` |
| `cargo test --test logic claim8_ -- --nocapture` | `claim8: shutdown_flag=true polls_before=1 polls_after=2` |
| `cargo test --test logic claim9_ -- --nocapture` | `claim9: polls_first=1 polls_after_idle_wakes=1` |
| `cargo test --test logic claim10_ -- --nocapture` | `claim10: present=true state=Some(Dormant) resident_dormant=true polled=0 freed=true` |
| `cargo test --test logic claim11_ -- --nocapture` | `claim11: dormant=true pending_wakes=Some(0) polls_before=1 polls_after=1` |
| `cargo test --test logic claim13_ -- --nocapture` | `claim13: strategy saw mailbox_len=[0] pending_wakes=[0]` |
| `cargo test --test logic claim14_ -- --nocapture` | `claim14: polls_total=0` |
| `cargo test --test logic claim15_ -- --nocapture` | `claim15: state=Some(Runnable) resident_dormant=true polls_before=1 after=1` |
| `cargo test --test logic claim16_ -- --nocapture` | `claim16: present=true resident_empty=true task_freed=true` |
| `cargo run --release --bin claim12` | `slab.get(old_id) after wrap = Some(999)` |
| `MIRIFLAGS=-Zmiri-strict-provenance cargo +nightly miri test --test mem claim1_` | `error: Undefined Behavior: constructing invalid value of type &mut TypeA: encountered a dangling reference (going beyond the bounds of its allocation)`, in 0.2.0's `src/task.rs:97` |
| `MIRIFLAGS=-Zmiri-strict-provenance cargo +nightly miri test --test mem claim2_` | `error: Undefined Behavior: constructing invalid value of type &mut claim2_dispatch_type_confusion::Big: encountered a dangling reference (going beyond the bounds of its allocation)`, at 0.2.0's `src/envelope.rs:31:29` |
| `MIRIFLAGS=-Zmiri-strict-provenance cargo +nightly miri test --test mem claim3_` | `error: Undefined Behavior: constructing invalid value of type &mut TypeA: encountered a dangling reference (going beyond the bounds of its allocation)`, in 0.2.0's `src/task.rs:94` |
