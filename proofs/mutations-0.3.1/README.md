# mutations-0.3.1

`src/main.rs` breaks waker_protocol 0.3.1 twenty ways, one at a time, runs `cargo test --no-fail-fast --test scheduler` in the clone after each, and puts the file back. For the two breaks that name an example, it also runs that example as 0.3.1 wrote it and as 0.3.0 wrote it (taken from git rev `1ca72a5`), both against the same broken scheduler. It stops unless the clone is at git rev `61bd5cdb15187681a581f455c2aa621ceb5de460`.

Command, run in this folder, with `<clone>` a clone of https://github.com/jameshuntt/waker_protocol checked out at that rev and `<target>` a fresh folder for the clone's builds: `cargo run -- <clone> <target>`

Expected output: one line per break ending in `CAUGHT`, the `M11 QoS protection removed` line ending `examples exit: [("qos_pressure_reclaim", 101), ("qos_pressure_reclaim (0.3.0 file)", 0)]`, the `MC1 a Ready task is never removed` line ending `examples exit: [("chaos_test", 101), ("chaos_test (0.3.0 file)", 0)]`, then `tree restored: true` and `ALL CAUGHT`.
