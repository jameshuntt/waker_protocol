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

Each example is a complete scheduler run and prints its reclaim log.

```bash
cargo run --example qos_pressure_reclaim      # memory pressure reclaims the lowest QoS task
cargo run --example mailbox_backlog_capitulate # a task capitulates when its mailbox backs up
cargo run --example manual_capitulate          # a task decides to capitulate itself
cargo run --example operator_forced_offload    # the operator forces a task off the node
cargo run --example idle_cache_apoptosis       # an idle cache is reclaimed
cargo run --example custom_sla_strategy        # your own CapitulateStrategy
cargo run --example checkpoints                # checkpoint before capitulating
cargo run --example heav_task                  # a heavy task under policy
cargo run --example chaos_test                 # a stateful chaos task
```

## Version note

0.2.0 is a rewrite of 0.1.0. The old `apoptosis` naming is now `capitulate`.

## License

MIT or Apache-2.0, at your option.
