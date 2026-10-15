# waker_protocol

A custom waker protocol built on [`waker_tables`](https://crates.io/crates/waker_tables).

Tasks live in a generation-checked slab and are driven by a scheduler that
understands quality of service, retention, checkpoints and capitulation.
Capitulation is the idea the crate is built around: a task surrenders with a
stated reason, after a checkpoint, so the work can go somewhere else. The
scheduler keeps a reclaim log of every such event.

## Pieces

| module | what it holds |
|---|---|
| `scheduler` | `WakerProtocol`, `SchedulerHandle`, `TaskHandle`, the resident task table |
| `policy` | `QoSClass`, `WakeOrdering`, `RetentionPolicy`, `TaskPolicy`, `SchedulerConfig` |
| `capitulate` | `CapitulateStrategy`, `CapitulatePlan`, `TaskSnapshot`, `ReclaimReport` |
| `checkpoint` | `CheckpointStore`, `CheckpointHandle`, an in-memory store |
| `envelope` | `Envelope`, `TypedEnvelope` for typed task messages |
| `types` | `TaskId`, `WakeReason`, `CheckpointReason`, `TaskPoll`, `TaskState`, `TaskStats`, `SchedulerMsg` |
| `slab`, `task`, `wake_ref` | the slab, the protocol vtable and task pointer, the wake signal |

## Examples

Each example is a complete scheduler run: it prints its reclaim log and asserts the outcome its name
promises.

```bash
cargo run --example qos_pressure_reclaim      # memory pressure reclaims the lowest QoS task
cargo run --example mailbox_backlog_capitulate # a task capitulates when its mailbox backs up
cargo run --example manual_capitulate          # a task decides to capitulate itself
cargo run --example operator_forced_offload    # the operator forces a task off the node
cargo run --example idle_cache_apoptosis       # an idle cache is reclaimed
cargo run --example custom_sla_strategy        # your own CapitulateStrategy
cargo run --example checkpoints                # checkpoint before capitulating
cargo run --example chaos_test                 # a stateful chaos task
```

## Safety

Tasks run through hand-built vtables, so the crate makes sure a task is only ever run, restored
or sent a message as its own type:

- `ProtocolVTable` records the type it was built for, and its fields are private: build it with
  `ProtocolVTable::of::<T>()` or `derive_protocol_task_vtable!`.
- `spawn_box` refuses a key whose vtable runs another type.
- A message carries the type it was written for; the scheduler refuses one sent to a task of
  another type instead of delivering it.
- `ProtocolTaskPtr` owns its task and frees it once, when dropped.

`ProtocolTask` is an `unsafe trait`: its functions are called with exclusive access to a live task
and nothing more, and `restore` must accept any bytes the checkpoint store returns.

## 0.3.0

- On `waker_tables` 0.3. Minimum Rust 1.85.
- `ProtocolVTable` records its task type and its fields are private; `spawn_box` and
  `ProtocolTaskPtr::from_box` refuse a vtable for another type. In 0.2 a task could be run as
  another type.
- `Envelope` is an `unsafe trait` with `target_type`; `dispatch` is `unsafe`. In 0.2 `dispatch` was
  a safe method on a raw pointer, and a message for another task type was delivered anyway.
- `ProtocolTaskPtr`'s fields are private; `restore_from_bytes` and `footprint_estimate` are safe.
- Cancelling a task removes it and frees it; in 0.2 it stayed in memory until the scheduler was
  dropped.
- `WakerProtocol::sender()` gives the scheduler's channel, for `scheduler_waker`.

## Version note

0.2.0 is a rewrite of 0.1.0. The old `apoptosis` naming is now `capitulate`.

## Testing

```bash
cargo test
cargo +nightly miri test
MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-many-seeds=0..4" cargo +nightly miri test
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.
