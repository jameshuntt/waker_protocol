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

Each example below is a complete scheduler run that asserts its outcome. `examples/heav_task.rs` is
not one of them: its code is commented out, so it runs nothing and asserts nothing.

```bash
cargo run --example qos_pressure_reclaim      # memory pressure reclaims the lowest QoS task
cargo run --example mailbox_backlog_capitulate # a task capitulates when its mailbox backs up
cargo run --example manual_capitulate          # a task decides to capitulate itself
cargo run --example operator_forced_offload    # the operator forces a task off the node
cargo run --example idle_cache_apoptosis       # an idle cache is reclaimed
cargo run --example custom_sla_strategy        # your own CapitulateStrategy
cargo run --example checkpoints                # checkpoint before capitulating
cargo run --example chaos_test                 # 1000 tasks under wake churn: each finishes and is freed once
```

## Safety

Tasks run through hand-built vtables, so the crate makes sure a task is only ever run, restored
or sent a message as its own type:

- `ProtocolVTable` records the type it was built for, and its fields are private: build it with
  `ProtocolVTable::of::<T>()` or `derive_protocol_task_vtable!`.
- `spawn_box` refuses a key whose vtable runs another type.
- A message carries the type it was written for; the scheduler refuses one sent to a task of
  another type instead of delivering it. Only that message is dropped: the task goes on, and the
  error is returned once the round is done.
- `ProtocolTaskPtr` owns its task and frees it once, when dropped.

`ProtocolTask` is an `unsafe trait`: its functions are called with exclusive access to a live task
and nothing more, and `restore` must accept any bytes the checkpoint store returns.

## 0.3.1

Fixes from a review of 0.3.0. No public signature changed; the behaviour changes are these.

- **An error no longer destroys a task.** A message refused for being written for another task
  type, or one whose `deliver` returns `Err`, is dropped on its own. The task stays live in its
  row, its other messages are delivered, and it is polled as usual. In 0.3.0 the task was freed
  with no checkpoint, and its row stayed behind for good, piling up every later wake and message.
- **A round finishes before it reports.** `run_once` and `pump` no longer stop at the first error.
  They finish the round, then return every error they met, joined with `"; "`. A task that
  finished earlier in the round is always removed, and its checkpoints go as the retention policy
  says. In 0.3.0 one error left such tasks behind as dead `Completed` rows, checkpoints and all.
- **A failed checkpoint save keeps the task.** The task is freed only once its bytes are saved.
  If the store's `save` fails, the task goes back into its row live, as it was, and the error is
  returned. That holds for every checkpoint: `TaskPoll::Checkpoint`, a capitulation with a stub, a
  strategy plan, a Checkpoint or Capitulate message, and Shutdown.
- **Shutdown stops.** Shutdown checkpoints every live task and returns every failed save; 0.3.0
  threw those errors away and reported a clean stop. After Shutdown, `run_once` and `pump` restore
  and poll nothing. Messages sent later stay in the channel; messages and wakes queued before it
  stay in the task's row. In 0.3.0 a task with a queued message or a later wake was restored and
  polled right after its shutdown checkpoint. To retry after a failed save, set `shutdown` back to
  false and send Shutdown again.
- **The capitulation strategy sees waiting work.** `TaskSnapshot` is taken before the round
  delivers the task's messages and wakes, so `mailbox_len` and `pending_wakes` count what was
  waiting; in 0.3.0 they were always 0. The default idle rule therefore spares a task that has
  work waiting. In 0.3.0 a task idle past the threshold was checkpointed again on every wake and
  never polled again. `footprint_bytes` is read before the messages are delivered, too.
- **A plan with `retain_stub: false` ends the task.** A strategy plan without a stub now removes
  the task as Completed, as `TaskPoll::Capitulate` already did; in 0.3.0 a strategy plan always
  kept a stub. The task's poll, the strategy and a Capitulate message now share one code path. So
  a Capitulate message without a stub also ends the task as Completed, and `retain_on_completion`
  decides its checkpoints. In 0.3.0 that message kept a dormant task's checkpoints and deleted a
  waiting task's, whatever the policy said.
- **A wake that comes before a Checkpoint message is kept.** The task is restored and polled for
  it, as it already was for a queued message. 0.3.0 dropped the wake.
- **`Yield` docs.** With `eager_repoll_after_yield` (the default) a task that yields is polled
  again in the next round, never twice in one round. Without it the task waits for a wake, like
  `Pending`. The behaviour is unchanged; the docs said "same round" and "run again soon".
- **`pump` docs.** `pump` returns false only once a Shutdown message has been handled. The
  scheduler holds a sender of its own, so dropping every handle does not stop it; the old doc
  said it did.
- **Task ids never come back.** A slab slot whose generation reaches `u32::MAX` is retired instead
  of wrapping to 0, so an old `TaskId` never reaches a new task. In 0.3.0, after one slot was
  reused 2^32 times, an old id reached whatever task held the slot.
- **Tests.** New tests cover each fix above, and also QoS protection, the default poll order,
  round robin, message order, every way a task ends under each retention flag, `keep_latest` 0,
  `auto_restore_on_wake`, the `Resume` wake, and each rule of the default strategy. Each of them
  was checked to fail against a deliberately broken scheduler.
- **Examples.** `qos_pressure_reclaim`'s interactive task is now big enough to be reclaimed, so
  its "never reclaimed" check really tests the QoS protection. `chaos_test` now asserts what it
  really does: every task finishes and is freed once, and nothing is reclaimed, because its task
  does not report its heap. `heav_task` is commented out and asserts nothing.

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
