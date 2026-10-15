//! The docs say what the code does. Each test below checks a sentence a review of 0.3.0, 0.3.1 or
//! the 0.3.2 draft found false, or a sentence 0.3.2 adds: the old wording, if any, is gone and the
//! new wording is there. The test named in each comment, in tests/scheduler.rs, tests/from_0_2.rs,
//! src/slab.rs or a doctest, proves the new wording against the code.

const SCHEDULER: &str = include_str!("../src/scheduler.rs");
const POLICY: &str = include_str!("../src/policy.rs");
const TYPES: &str = include_str!("../src/types.rs");
const SLAB: &str = include_str!("../src/slab.rs");
const CAPITULATE: &str = include_str!("../src/capitulate.rs");
const TASK: &str = include_str!("../src/task.rs");
const WAKE_REF: &str = include_str!("../src/wake_ref.rs");
const README: &str = include_str!("../readme.md");
const FROM_0_2: &str = include_str!("from_0_2.rs");

const DOC_CLAIMS: &str = include_str!("doc_claims.rs");

fn says(doc: &str, old: &str, new: &str) {
    assert!(!doc.contains(old), "still says {old:?}");
    assert!(doc.contains(new), "does not say {new:?}");
}

/// For a sentence that is new, with no old wording it replaces.
fn tells(doc: &str, new: &str) {
    assert!(doc.contains(new), "does not say {new:?}");
}

/// Proved by `pump_keeps_returning_true_with_every_handle_dropped_until_shutdown` and the doctest
/// on `pump`.
#[test]
fn pump_does_not_promise_to_stop_when_every_handle_is_gone() {
    says(SCHEDULER, "every handle is gone", "the scheduler holds a sender of its own");
}

/// Proved by `yield_runs_again_next_round_when_eager_and_waits_for_a_wake_when_not`.
#[test]
fn yield_docs_say_next_round_or_after_a_wake() {
    says(POLICY, "Poll again in the same round", "so the next `run_once` or `pump` polls it again");
    says(POLICY, "Poll again in the same round", "like `Pending`, until a wake");
    says(
        TYPES,
        "at once if the policy says eager repoll",
        "Run again: in the next round when the task's policy has `eager_repoll_after_yield`",
    );
}

/// Proved by `a_message_for_another_type_is_refused_not_delivered` and
/// `a_failed_delivery_drops_only_that_message`.
#[test]
fn a_refused_message_is_described_as_dropped_with_the_task_going_on() {
    says(SCHEDULER, "is refused when it would be delivered", "that message is dropped, the task goes on");
    says(README, "instead of delivering it.\n", "Only that message is dropped: the task goes on");
}

/// Proved by `shutdown_returns_every_failed_save_and_keeps_those_tasks_live` and
/// `after_shutdown_nothing_is_restored_or_polled_and_later_work_stays_queued`.
#[test]
fn shutdown_docs_say_what_stops_and_what_a_failed_save_leaves() {
    says(
        TYPES,
        "/// Checkpoint every task and stop.",
        "Checkpoint every live task and stop. Once it is handled, `run_once` and `pump` restore and",
    );
    says(TYPES, "/// Checkpoint every task and stop.", "be saved stays live in its row, and the error is returned.");
}

/// Proved by `the_strategy_sees_waiting_messages_and_wakes_before_they_are_delivered`.
#[test]
fn snapshot_counts_are_described_as_taken_before_delivery() {
    says(CAPITULATE, "/// Messages waiting for it.\n", "/// Messages waiting for it, counted before this round delivers them.");
    says(CAPITULATE, "/// Wakes waiting for it.\n", "/// Wakes waiting for it, counted before this round delivers them");
}

/// Proved by `slab::tests::a_slot_is_retired_instead_of_wrapping_its_generation` and the ignored
/// full run `slab_ids_stay_stale_after_every_generation_of_a_slot_is_used`.
#[test]
fn slab_docs_say_a_slot_is_retired_rather_than_wrapped() {
    says(
        SLAB,
        "each reused with a new generation so an old id cannot reach a new value",
        "is retired instead of wrapping, so an old id never reaches a new value.",
    );
    says(TYPES, "a stale id never reaches a newer task.\n", "because a slot is retired rather than given a generation it has had before.");
}

/// Proved by running the examples, as CI does: each listed one asserts its outcome.
#[test]
fn readme_does_not_claim_heav_task_asserts_anything() {
    says(
        README,
        "Each example is a complete scheduler run: it prints its reclaim log and asserts",
        "its code is commented out, so it runs nothing and asserts nothing.",
    );
    says(README, "# a stateful chaos task", "# 1000 tasks under wake churn: each finishes and is freed once");
}

/// Proved by `a_panic_whose_payload_panics_when_dropped_still_ends_the_task_as_failed`,
/// `a_panic_in_restore_ends_the_task_as_failed_and_the_round_goes_on` and the two
/// `a_panic_in_footprint_estimate_...` tests.
#[test]
fn every_task_function_the_scheduler_catches_is_named() {
    let old = "`poll`, `on_wake` or `checkpoint`";
    says(TASK, old, "If `poll`, `on_wake`, `checkpoint`, `restore` or `footprint_estimate` panics, the");
    says(TASK, old, "even one whose own drop panics");
    says(SCHEDULER, old, "`poll`, `on_wake`, `checkpoint`, `restore` or `footprint_estimate`");
    says(TYPES, "`on_wake`\n    /// or `checkpoint`.", "`poll`, `on_wake`, `checkpoint`, `restore` or `footprint_estimate`.");
}

/// Proved by `a_panic_from_the_stores_load_or_remove_reaches_the_caller_and_leaves_the_task_as_the_docs_say`
/// and `a_panic_from_the_store_reaches_the_caller_with_the_task_live_in_its_row`.
#[test]
fn only_a_panic_in_the_stores_save_is_said_to_put_the_task_back_live() {
    says(SCHEDULER, "a panic from the checkpoint store or the capitulation strategy", "from the store's `save` or from the capitulation strategy reaches the caller");
    says(SCHEDULER, "or the store panics, it goes back", "A panic from the store's `load` also reaches the caller, and the task stays dormant.");
    says(SCHEDULER, "from the store or the strategy reaches the caller", "freed, its queued messages dropped (the task's end)");
    says(README, "In 0.3.1 each of these panics freed the task", "or in the store's `save` freed the task");
    says(README, "A panic from the checkpoint store or the capitulation strategy still", "`load` also reaches the caller, and the task stays dormant.");
}

/// Proved by `the_strategy_is_never_shown_a_task_just_restored`, `every_message_to_an_over_budget_task_gets_a_poll`
/// and `an_over_budget_task_that_yields_runs_to_the_end_without_a_wake`.
#[test]
fn strategy_docs_say_when_it_is_asked_and_that_a_plan_never_drops_a_poll() {
    says(CAPITULATE, "Decides, before each poll,", "Decides, before a poll, whether a task should capitulate (not before the first poll");
    says(CAPITULATE, "(a restore adds `Resume`)", "a just-restored task is not shown to the strategy");
    says(CAPITULATE, "and the task is\n/// not polled.", "so the plan puts the poll off but never drops it");
}

/// The header of this file named only the releases, but two of its tests check wording only the
/// 0.3.2 draft had. The strings are split so the test does not find itself.
#[test]
fn this_files_header_names_the_draft_as_well() {
    says(
        DOC_CLAIMS,
        concat!("a review of 0.3.0 ", "or 0.3.1 found false"),
        concat!("a review of 0.3.0, 0.3.1 or\n//! the 0.3.2 draft ", "found false"),
    );
}

/// Proved by `an_over_budget_task_gets_no_poll_for_a_qos_or_memory_pressure_wake_and_one_for_any_other`
/// and `a_task_woken_only_by_qos_or_memory_pressure_stays_dormant_after_the_strategy_checkpoints_it`.
#[test]
fn the_budget_docs_say_a_qos_or_memory_pressure_wake_gets_no_poll() {
    says(POLICY, "checkpointed in place of a poll, and the next round restores and polls it;", "if it had work waiting, the next round restores and");
    says(POLICY, "one poll for every wake,", "poll for every message, every wake other than `QoS` or `MemoryPressure`, and every `Yield`,");
    says(POLICY, "one poll for every wake,", "`on_wake` and is\n    /// answered by the checkpoint, with no poll");
    says(README, "wake, every message and every `Yield`", "every message, every wake other than `QoS` or\n  `MemoryPressure`, and every `Yield`, a round later");
}

/// Proved by `a_panic_in_a_messages_deliver_drops_only_that_message_and_the_round_goes_on` and
/// `a_panic_in_cleanup_or_drop_is_an_error_for_the_round_and_the_round_goes_on`.
#[test]
fn the_docs_say_what_a_panic_in_deliver_cleanup_or_drop_does() {
    let old_heading = "**A panic in the task's own code ends only that task.**";
    says(README, old_heading, "**A panic in a message's `deliver` loses only that message.**");
    says(README, old_heading, "**A panic in `cleanup` or in a task's `Drop` is an error, not a stop.**");
    says(README, old_heading, "**A panic in `poll`, `on_wake`, `checkpoint`, `restore` or `footprint_estimate` ends only\n  that task.**");
    says(SCHEDULER, "If `deliver` returns `Err`, only this message is lost", "If `deliver` returns `Err` or panics, only this message is lost");
    tells(SCHEDULER, "is caught and treated as a `deliver` that\n    /// returned `Err`");
    tells(SCHEDULER, "A panic in a task's `cleanup` or `Drop` is caught as the task\n    /// is freed");
    says(TASK, "/// Runs just before the task is freed.\n", "Runs just before the task is freed. If it panics, the task is still dropped and freed");
    tells(TASK, "If `cleanup` or the task's own `Drop` panics while `run_once` or `pump` frees the task");
}

/// Proved by `the_store_is_asked_to_remove_checkpoints_only_once_the_task_is_freed`,
/// `a_panic_from_the_stores_remove_at_a_tasks_end_comes_after_the_task_and_its_messages_are_freed`
/// and `a_panic_from_the_stores_remove_while_a_checkpoint_prunes_comes_after_the_live_copy_is_freed`.
#[test]
fn the_docs_say_the_store_removes_checkpoints_only_once_the_task_is_freed() {
    says(
        SCHEDULER,
        "panic from its `remove` reaches the caller after the task is already dormant",
        "store's `remove` is called only once the task is dormant (a checkpoint) or removed and",
    );
    says(README, "a panic from `load` or `remove` did then what", "A panic from `load` did then what it does now.");
    says(README, "a panic from `load` or `remove` did then what", "handles not yet removed out of a dormant task's history");
    says(
        README,
        "comes after the task is already dormant (a checkpoint) or already removed and freed (the task's",
        "The store's `remove` is called only\n  once the task is dormant and its live copy freed (a checkpoint)",
    );
}

/// Proved by every test in tests/from_0_2.rs; the Debug output by
/// `debug_output_shows_the_full_type_path`.
#[test]
fn the_readme_lists_what_came_back_from_0_2_0_and_the_debug_output() {
    tells(README, "**Put back from 0.2.0.** 0.3.0 removed these.");
    tells(README, "`TASK_WAKE_SIGNAL_RAW_VTABLE`, the raw waker vtable of `TaskWakeSignal`");
    tells(README, "`ProtocolTaskPtr::raw_data(&mut self) -> *mut ()`, public again and `unsafe`, as in 0.2.0.");
    tells(README, "`ProtocolVTable::from_raw_parts::<T>(..)`, an `unsafe const fn`");
    tells(README, "`ProtocolTaskPtr::from_raw_parts(data, vtable)`, an `unsafe fn`");
    tells(README, "**Debug output names the type by its full path.**");
    says(
        README,
        "No public signature changed; `WakerProtocol` has one new public\nfield, `finished`.",
        "New public items: the field\n`WakerProtocol::finished`, and the items listed under **Put back from 0.2.0**, except\n`type_name()` and `vtable()`, which are public since 0.3.0.",
    );
}

/// Proved by `a_panic_in_a_messages_kind_or_target_type_loses_only_that_message_and_never_aborts`,
/// `a_panic_in_kind_at_a_tasks_end_still_frees_everything_and_removes_its_checkpoints`,
/// `a_message_whose_drop_panics_sent_to_a_task_already_ended_is_an_error_for_the_round` and
/// `a_refused_message_whose_drop_panics_is_an_error_and_the_task_goes_on`; what 0.3.1 did with a
/// refused message whose drop panics, by that last test's case run against 0.3.1's code.
#[test]
fn the_docs_say_a_panic_in_kind_or_target_type_or_in_a_message_for_an_ended_task_is_caught() {
    says(
        SCHEDULER,
        "or in the drop of a message queued for a\n    /// task that is ending, is caught",
        "So is a panic in\n    /// the drop of a message that is not delivered: one queued for a task that ends, one sent to a\n    /// task that has already ended, or one refused.",
    );
    tells(SCHEDULER, "in a message's `deliver`, or in the `kind` or `target_type` of an");
    says(
        SCHEDULER,
        "A message whose `dispatch` panics is dropped the same way.",
        "A message whose `kind`, `target_type` or `dispatch` panics is lost the\n    /// same way, with the panic as the error. A message sent to a task that has already ended is\n    /// dropped",
    );
    tells(README, "The same holds for a panic in the `kind` or `target_type` of an\n  `Envelope` written by hand");
    says(
        README,
        "So is a panic in the drop of a message still queued for a task\n  that ends.",
        "So is a panic in the drop of a message that is not delivered:\n  one still queued for a task that ends, one sent to a task that has already ended, or one\n  refused.",
    );
    tells(README, "a panic in the drop of a refused message reached the caller and left the task's row as\n  `Resident::Empty` for good;");
}

/// Proved by `the_raw_waker_vtable_static_is_a_copy_the_crates_wakers_do_not_carry`.
#[test]
fn the_docs_say_the_raw_waker_vtable_static_is_a_copy_the_crates_wakers_do_not_carry() {
    says(
        WAKE_REF,
        "[`scheduler_waker`] builds its waker from the same four functions.",
        "Unlike 0.2.0, it is a copy at its own address, not the vtable the crate's wakers carry.",
    );
    tells(WAKE_REF, "a waker built on this static does not `will_wake` its own clone");
    tells(README, "It is back as a copy at its own address, not as the vtable the crate's wakers\n    carry, which it was in 0.2.0.");
    tells(README, "a waker built on it does not `will_wake` its own clone; in 0.2.0\n    both were true.");
}

/// Proved by `a_panic_in_cleanup_or_drop_is_an_error_for_the_round_and_the_round_goes_on` (a
/// vtable from `derive_protocol_task_vtable!`); a probe with the 0.2.0-style destroy function of
/// tests/from_0_2.rs, whose `cleanup` and `Drop` both panic, aborts.
#[test]
fn the_double_panic_claim_names_the_vtables_it_holds_for() {
    says(
        TASK,
        "is returned as an error once the round is done. If both panic, the task is still freed",
        "With a vtable from [`ProtocolVTable::of`] or\n/// [`derive_protocol_task_vtable!`](crate::derive_protocol_task_vtable), if both panic, the task\n/// is still freed",
    );
    tells(TASK, "otherwise a panic in both aborts the process.");
    tells(TASK, "A `destroy_fn` that calls the task's `cleanup` must hold a panic from `cleanup` until the\n    /// task is dropped");
    says(
        README,
        "so a panic in the drop as well no longer aborts the process; that holds\n  wherever a `ProtocolTaskPtr` is dropped.",
        "that holds wherever a\n  `ProtocolTaskPtr` with such a vtable is dropped outside an unwind.",
    );
    tells(README, "still aborts when both panic.");
}

/// The 0.3.0 notes named the private fields, and `type_name()` was public in 0.3.0 (its
/// published task.rs has `pub fn type_name(&self)`).
#[test]
fn the_put_back_notes_say_what_0_3_0_named_and_that_type_name_is_older() {
    says(
        README,
        "0.3.0 removed these, and its notes did not say so.",
        "Its notes said the fields of `ProtocolVTable`\n  and `ProtocolTaskPtr` became private, but not that reading them or building from parts went\n  with the fields, and they did not mention `TASK_WAKE_SIGNAL_RAW_VTABLE` or `raw_data` at all.",
    );
    says(
        README,
        "and `type_name()` reads the\n    name.",
        "`type_name()`, which reads the type's name, has been\n    public since 0.3.0.",
    );
}

/// tests/from_0_2.rs's Debug test uses only items public since 0.3.0, so it compiles against
/// 0.3.1 on its own.
#[test]
fn the_from_0_2_header_says_what_its_tests_check() {
    says(
        FROM_0_2,
        "Each test uses one item put back; none of\n//! them compiles against 0.3.1.",
        "Every item put back is used by a test here, so the file does not compile against 0.3.1.",
    );
    tells(FROM_0_2, "The Debug test checks the output the README describes");
}

/// Proved by `data_reads_the_task_pointer_through_a_shared_reference` and the doctest on
/// `ProtocolTaskPtr::data`.
#[test]
fn the_docs_say_the_task_pointer_can_be_read_through_a_shared_reference() {
    tells(README, "`ProtocolTaskPtr::data(&self) -> *mut ()` reads the task\n    pointer through a shared reference, as `p.data` did");
    says(TASK, "Safe code\n/// can neither pair them nor reach the pointer:", "Safe code\n/// can read the pointer with [`data`](Self::data), as it could read 0.2.0's public field, but\n/// using the pointer takes `unsafe`.");
}

/// Proved by `derive_protocol_task_vtable_calls_the_trait_functions_not_inherent_methods_of_the_same_name`.
#[test]
fn the_readme_says_the_derive_calls_the_trait_functions() {
    tells(README, "**`derive_protocol_task_vtable!` calls the `ProtocolTask` functions.**");
    tells(README, "Such a method no longer\n  runs from the scheduler, with no error");
}

// The fifth review of the 0.3.2 draft.

/// Proved by `an_over_budget_task_keeps_the_poll_for_its_yield_when_a_qos_or_memory_pressure_wake_follows`,
/// `an_over_budget_task_that_yields_runs_to_the_end_without_a_wake` and
/// `a_yield_already_polled_for_gives_no_poll_to_a_later_qos_or_memory_pressure_wake`; for a task
/// without `eager_repoll_after_yield`, by `without_eager_repoll_a_yield_parks_and_is_owed_no_poll`.
#[test]
fn the_strategy_docs_say_a_yield_keeps_its_poll_whatever_wakes_come_after_it() {
    let old = "or a `Yield` and no wake since";
    says(CAPITULATE, old, "or a `Yield` it has not been polled for, whatever wakes came after it; a\n/// `Yield` counts only with `eager_repoll_after_yield`");
    says(README, old, "or a `Yield` it has not been polled for, whatever wakes came after it),");
    tells(README, "A `Yield`\n  counts only with `eager_repoll_after_yield` (the default)");
    says(
        SCHEDULER,
        "with\n    /// nothing waiting, a runnable state (it yielded)",
        "or a poll it\n    /// is `owed` (work it was handed, or a `Yield`, it has not been polled for since), whatever\n    /// wakes came after it.",
    );
    tells(POLICY, "A `Yield` keeps its poll even when a `QoS` or\n    /// `MemoryPressure` wake comes after it, and counts only with `eager_repoll_after_yield`.");
}

/// A probe that drops a `ProtocolTaskPtr` of `derive_protocol_task_vtable!` whose `cleanup` and
/// `Drop` both panic: dropped on its own, the panic is caught; dropped while another panic
/// unwinds, the process aborts (an abort cannot be caught in a test). The sixth review found that
/// this does not hold "whatever its vtable":
/// `a_destroy_function_that_holds_the_panics_keeps_a_drop_during_an_unwind_from_aborting` drops
/// such a task, with a destroy function that holds both panics, while another panic unwinds.
#[test]
fn the_double_panic_claim_says_a_drop_during_an_unwind_still_aborts() {
    says(
        README,
        "that holds wherever a\n  `ProtocolTaskPtr` with such a vtable is dropped. ",
        "a `ProtocolTaskPtr` dropped while another panic unwinds (by a scheduler dropped during a\n  panic, say) aborts the process if its `cleanup` or its drop panics",
    );
    says(
        README,
        "drop panics, whatever its vtable",
        "With a vtable from `ProtocolVTable::of` or\n  `derive_protocol_task_vtable!`, or any destroy function that lets a `cleanup` or drop panic go\n  on, a `ProtocolTaskPtr` dropped while another panic unwinds",
    );
    says(
        TASK,
        "`Drop` panics, whatever its\n/// vtable",
        "With a vtable from [`ProtocolVTable::of`] or\n/// `derive_protocol_task_vtable!`, or any destroy function that lets a `cleanup` or drop panic go\n/// on, a task dropped while another panic unwinds (a scheduler dropped during a panic, say)\n/// aborts the process if its `cleanup` or its `Drop` panics",
    );
    says(
        TASK,
        "Dropped\n/// while another panic unwinds, it aborts the process",
        "With such a\n/// vtable, or any destroy function that lets a `cleanup` or drop panic go on, a `ProtocolTaskPtr`\n/// dropped while another panic unwinds aborts the process if its `cleanup` or the task's drop\n/// panics",
    );
}

/// Proved by `debug_output_shows_the_full_type_path` (a vtable from `from_raw_parts` included)
/// and `spawn_refuses_a_key_for_another_type`; the check is `ProtocolVTable::is_for`, which
/// compares `TypeId`.
#[test]
fn the_debug_note_says_the_check_compares_type_id_and_a_hand_written_vtable_had_its_own_name() {
    says(
        README,
        "it\n  is the same value the vtable's type check uses.",
        "The type check itself compares\n  `TypeId`; the name is what its error messages print.",
    );
    tells(README, "A vtable written by hand in 0.2.0\n  carried whatever name its `type_name` field was given; `ProtocolVTable::from_raw_parts::<T>`\n  records `std::any::type_name::<T>()` and takes no name.");
}

/// 0.2.0's expansion is `(&mut *(ptr as *mut $t)).poll()`, `(&*(ptr as *const $t)).footprint_estimate()`
/// and `Box::from_raw(ptr as *mut $t).cleanup()`; a probe of those calls on types with inherent
/// methods of each receiver ran the inherent `poll` and `footprint_estimate` only with the trait's
/// receiver, and the inherent `cleanup` with `self`, `&self` and `&mut self`.
#[test]
fn the_derive_note_says_which_inherent_methods_ran_in_0_2_0() {
    says(
        README,
        "so an inherent method of the same name, when the type had one, ran in\n  place of the trait's.",
        "for `poll`, `on_wake`, `checkpoint` and `footprint_estimate`, one with\n  the same receiver as the trait's (`&mut self`, or `&self` for `footprint_estimate`); for\n  `cleanup`, which it called on the task's `Box`, one with any receiver.",
    );
}

/// Proved by `from_box_drops_a_task_it_refuses_before_it_panics`.
#[test]
fn the_docs_say_from_box_drops_a_task_it_refuses_before_it_panics() {
    says(
        TASK,
        "If `vtable` was built for a type other than `T`. The task is dropped first.\n",
        "The task is dropped first, before the\n    /// panic, never while it unwinds: a panic in the task's drop is caught and named in the\n    /// panic's message.",
    );
    tells(README, "**`ProtocolTaskPtr::from_box` drops a task it refuses before it panics.**");
    tells(README, "In 0.3.0 and 0.3.1 it panicked first and dropped the task while that panic\n  unwound, so a task whose drop panicked aborted the process.");
}

// The sixth review of the 0.3.2 draft.

/// Proved by `work_handed_to_a_task_keeps_its_poll_when_the_save_of_a_strategy_checkpoint_fails_or_panics`
/// and `work_handed_to_a_task_keeps_its_poll_when_the_strategy_panics`.
#[test]
fn the_docs_say_work_a_task_was_handed_keeps_its_poll_when_a_save_fails_or_the_strategy_panics() {
    tells(README, "Work the task was handed keeps its poll even when the\n  save of the strategy's checkpoint fails or the strategy panics");
    tells(CAPITULATE, "Work the task was handed keeps its\n/// poll even when the save of the plan's checkpoint fails or the strategy panics");
    says(SCHEDULER, "/// Tasks that yielded and have not been polled since.", "/// Tasks owed a poll: each was handed work");
}

/// Proved by `a_task_the_strategy_checkpointed_without_auto_restore_is_polled_once_auto_restore_is_back`.
#[test]
fn the_docs_say_a_task_the_strategy_checkpointed_waits_for_auto_restore() {
    says(
        README,
        "the next round restores it and polls it. So an",
        "the next round restores it and polls it (with\n  `auto_restore_on_wake` on; with it off, once it is set back to true).",
    );
    says(
        CAPITULATE,
        "round restores and polls it, so the plan",
        "round restores and polls it (with `auto_restore_on_wake` on; with it off, once it is set back\n/// to true)",
    );
}

/// Proved by `an_empty_task_pointer_owns_nothing_and_dropping_it_does_nothing`, which Miri runs too.
#[test]
fn the_docs_say_an_empty_task_pointer_is_back() {
    tells(README, "  - The empty `ProtocolTaskPtr`. In 0.2.0 a `ProtocolTaskPtr` whose `data` was null owned");
    tells(README, "`poll`, `on_wake`, `checkpoint`\n    and `raw_data` require a non-null `data`.");
    tells(TASK, "Or `data` is null, for an empty pointer that owns nothing, as 0.2.0's could be");
    tells(TASK, "ProtocolTask's contract for `poll`, and a non-null `data`");
    tells(TASK, "Using the pointer does: it requires a non-null `data`");
    says(TASK, "/// The task's own size estimate.\n", "/// The task's own size estimate; 0 for an empty pointer (a null `data`)");
}

/// Proved by the `compile_fail` doctest on `ProtocolVTable::type_name`.
#[test]
fn the_put_back_notes_say_type_name_reads_the_name_at_run_time_only() {
    tells(README, "`type_name()` reads the name at run time only, because `std::any::type_name` is not `const`\n    on stable");
    tells(README, "a name needed in a\n    `static` now has to be written out by hand.");
    tells(TASK, "It is read at run time only: `std::any::type_name` is not `const` on stable");
}
