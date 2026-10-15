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
  `ProtocolVTable::of::<T>()` or `derive_protocol_task_vtable!`, or from hand-written functions
  with the `unsafe` `ProtocolVTable::from_raw_parts::<T>`.
- `spawn_box` refuses a key whose vtable runs another type.
- A message carries the type it was written for; the scheduler refuses one sent to a task of
  another type instead of delivering it. Only that message is dropped: the task goes on, and the
  error is returned once the round is done.
- `ProtocolTaskPtr` owns its task and frees it once, when dropped. `into_raw_parts` gives the task
  up instead, and only the `unsafe` `from_raw_parts` pairs a task pointer with a vtable by hand.

`ProtocolTask` is an `unsafe trait`: its functions are called with exclusive access to a live task
and nothing more, and `restore` must accept any bytes the checkpoint store returns.

## 0.3.2

Fixes from reviews of 0.3.1 and of this release's drafts, and what 0.3.0 removed from 0.2.0, put
back. No public signature changed. New public items: the field
`WakerProtocol::finished`, and the 0.2.0 items listed under **Put back from 0.2.0**.

- **A restored task runs before it can be checkpointed again.** The capitulation strategy is not
  asked about a task restored from a checkpoint until that task has been polled once, whatever
  the strategy. In 0.3.1 the default budget rule checkpointed an over-budget Batch or Background
  task before every poll, including right after each restore, so the task never ran again.
- **A strategy checkpoint puts a poll off; it never drops it.** When the strategy checkpoints a
  task instead of polling it, and the task had work waiting (a message, a wake other than `QoS`
  or `MemoryPressure`, or a `Yield` and no wake since), the task stays runnable while dormant:
  the next round restores it and polls it. So an over-budget Batch task gets one poll for every
  message, every wake other than `QoS` or `MemoryPressure`, and every `Yield`, a round later
  than a task within its budget. A task woken only by `QoS` or `MemoryPressure` gets no poll for
  that wake: its `on_wake` hears it, the checkpoint answers it, and the task stays dormant until
  its next wake or message, so a memory-pressure reclaim is not undone the round after. In 0.3.1
  such a task was left dormant with its work delivered but not acted on, until another wake; a
  task that yielded and was checkpointed this way never ran again.
- **A dormant task without auto-restore stays dormant.** With `auto_restore_on_wake` off, a wake
  or a message sent to a dormant task is kept in its row (`pending_wakes`, `mailbox`), and the
  task stays `Dormant` and out of the ready list. Set `auto_restore_on_wake` back to true in its
  row, and the next round restores it and delivers them. In 0.3.1 a wake set the task's state to
  `Runnable`, so it sat in the ready list every round and nothing happened.
- **A checkpoint the store cannot remove is kept and tried again.** When old checkpoints are
  pruned and the store's `remove` fails for one, that handle stays in the task's
  `checkpoint_history`, the error is returned, and the next prune tries it again. Newer handles
  past `keep_latest` are still removed, so a handle that stays stuck never holds them back. In
  0.3.1 the handle was already gone from the history, so its bytes stayed in the store for good.
  When a task ends, its history goes with its row: a removal that fails then is returned as an
  error but not tried again (the handle is still in `reclaim_log`).
- **`finished` says how every task ended.** `WakerProtocol::finished` lists every task the
  scheduler removed, oldest first, with the state it ended in: `Completed`, `Cancelled`, or
  `Failed` with its message. In 0.3.1 a `Failed` message was lost when the row was removed. Like
  `reclaim_log` it only grows; clear it once read.
- **A panic in `poll`, `on_wake`, `checkpoint`, `restore` or `footprint_estimate` ends only
  that task.** A panic in any of these five functions of a task is caught: the task ends as
  `Failed`, with a message naming the panic (`task panicked in poll: ...`), its checkpoints
  follow `retain_on_failure`, its row is removed, and the round goes on to the other tasks. That
  holds even when the panic's payload panics again as it is dropped. A `restore` that panics
  ends the task for good, because its checkpoint will never restore; in 0.3.1 every later round
  panicked at the same restore, and no task after it in the round was polled.
- **A panic in a message's `deliver` loses only that message.** It is caught and treated like a
  `deliver` that returned `Err`: that message is dropped, the task stays live and is polled, the
  round goes on, and the error (`message "kind" for task ... panicked: ...`) is returned with the
  round's other errors. In 0.3.1 it reached the caller, freed the task and left its row as
  `Resident::Empty` for good.
- **A panic in `cleanup` or in a task's `Drop` is an error, not a stop.** When the scheduler frees
  a task (at the task's end, or once its checkpoint is saved) and the task's `cleanup` or `Drop`
  panics, the panic is caught: the task is freed anyway, its row is left removed or dormant as it
  would have been, the round goes on, and the error (`task ... panicked in cleanup or drop: ...`)
  is returned after the round. So is a panic in the drop of a message still queued for a task
  that ends. And when `cleanup` panics, the task's own drop now runs after that panic is caught,
  not while it unwinds, so a panic in the drop as well no longer aborts the process; that holds
  wherever a `ProtocolTaskPtr` is dropped. In 0.3.1 a panic in `cleanup` or `Drop` reached the
  caller and left the task's row as `Resident::Empty` for good (after a checkpoint, with the
  saved checkpoint left out of the task's history); a panic in a queued message's drop reached
  the caller with the row removed and the task's checkpoints left in the store; and a panic in
  both `cleanup` and `Drop` aborted the process.
- **What a panic from the checkpoint store leaves.** A panic from the store's `save` or from the
  capitulation strategy still reaches the caller, but the task is put back in its row, live,
  first, so the scheduler still works if the caller catches the panic. A panic from the store's
  `load` also reaches the caller, and the task stays dormant. The store's `remove` is called only
  once the task is dormant and its live copy freed (a checkpoint), or once its row is removed,
  the task freed and its queued messages dropped (the task's end), so a panic from `remove`
  comes after all of that and never unwinds through the task or a message. The checkpoints it
  had not yet removed stay in the store, and in a dormant task's history, where a later prune
  tries them again. In 0.3.1 a panic in the task's own `poll`, `on_wake`, `checkpoint` or
  `footprint_estimate`, in the strategy, or in the store's `save` freed the task and left its row
  as `Resident::Empty` for good. A panic from `load` did then what it does now. A panic from
  `remove` came, as now, once the task was dormant or freed; but 0.3.1 had already taken the
  handles not yet removed out of a dormant task's history, so their bytes stayed in the store for
  good.
- **Strategy docs.** `CapitulateStrategy` is asked before a poll, but not before the first poll
  after a restore, and `TaskSnapshot::pending_wakes` never counts a restore's `Resume`; the docs
  said "before each poll" and "a restore adds `Resume`".
- **Put back from 0.2.0.** 0.3.0 removed these, and its notes did not say so. Each is back. The
  public fields of `ProtocolVTable` and `ProtocolTaskPtr` let safe code cause undefined behavior
  in 0.2.0, so building either from parts comes back as an `unsafe fn` whose `# Safety` section
  says what the caller must promise; safe code still cannot.
  - `TASK_WAKE_SIGNAL_RAW_VTABLE`, the raw waker vtable of `TaskWakeSignal`, declared as in 0.2.0
    with `declare_raw_waker_vtable!(TaskWakeSignal, TASK_WAKE_SIGNAL_RAW_VTABLE)` and reached at
    the crate root. 0.3.0 dropped it, with no safety reason, when `waker_from_arc` stopped taking
    a vtable.
  - `ProtocolTaskPtr::raw_data(&mut self) -> *mut ()`, public again and `unsafe`, as in 0.2.0.
  - `ProtocolVTable`'s functions and type name. In 0.2.0 its fields were public, so safe code
    could build a vtable whose functions read a task as another type. The fields stay private.
    `poll_fn()`, `wake_fn()`, `checkpoint_fn()`, `restore_fn()`, `destroy_fn()` and
    `footprint_fn()` read each function (calling one is `unsafe`), and `type_name()` reads the
    name. `ProtocolVTable::from_raw_parts::<T>(..)`, an `unsafe const fn`, builds a vtable from
    hand-written functions, recorded as running `T`; it can build a `static`.
  - `ProtocolTaskPtr`'s pointer and vtable. In 0.2.0 its fields were public, so safe code could
    pair a task with another type's vtable, or a dangling pointer with any vtable, and the drop
    freed it. The fields stay private. `ProtocolTaskPtr::from_raw_parts(data, vtable)`, an
    `unsafe fn`, owns a task given as the two parts; `into_raw_parts(self)` gives both back
    without freeing the task.
- **Debug output names the type by its full path.** `{:?}` of a `ProtocolVTable` shows
  `type_name` as `std::any::type_name` gives it (`my_crate::jobs::Job`), where 0.2.0 showed the
  type as written in `derive_protocol_task_vtable!` (`Job`). This changed in 0.3.0 and stays: it
  is the same value the vtable's type check uses. `ProtocolTaskPtr`'s `{:?}`, new in 0.3.0, shows
  the same full path.
- **Tests.** A test for each fix above, each run against the code before the fix first, where it
  fails: 0.3.1's code, or the draft's for a fix to the draft. A fix to the docs alone has its test
  in `tests/doc_claims.rs`, which checks that the old sentence is gone and the new one is there.
  `tests/from_0_2.rs` uses every item put back from 0.2.0; it does not compile against 0.3.1. The
  test for a handle that stays stuck passed before too, because that code was right; it fails on
  a prune that keeps retrying the stuck handle, which the earlier test did not catch.

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
