//! The docs say what the code does. Each test below checks a sentence a review of 0.3.0, 0.3.1 or
//! the 0.3.2 draft found false, or a sentence 0.3.2 adds: the old wording, if any, is gone and the
//! new wording is there. The test named in each comment, in tests/scheduler.rs, tests/from_0_2.rs
//! or src/slab.rs, proves the new wording against the code.

const SCHEDULER: &str = include_str!("../src/scheduler.rs");
const POLICY: &str = include_str!("../src/policy.rs");
const TYPES: &str = include_str!("../src/types.rs");
const SLAB: &str = include_str!("../src/slab.rs");
const CAPITULATE: &str = include_str!("../src/capitulate.rs");
const TASK: &str = include_str!("../src/task.rs");
const README: &str = include_str!("../readme.md");

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
    says(POLICY, "one poll for every wake,", "`on_wake` and is answered by the checkpoint, with no poll");
    says(README, "wake, every message and every `Yield`", "message, every wake other than `QoS` or `MemoryPressure`, and every `Yield`, a round later");
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
    tells(SCHEDULER, "is caught and treated as a `deliver` that returned `Err`");
    tells(SCHEDULER, "A panic in a task's `cleanup` or `Drop`, or in the drop of a message queued for a");
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
    tells(README, "**Put back from 0.2.0.** 0.3.0 removed these, and its notes did not say so.");
    tells(README, "`TASK_WAKE_SIGNAL_RAW_VTABLE`, the raw waker vtable of `TaskWakeSignal`");
    tells(README, "`ProtocolTaskPtr::raw_data(&mut self) -> *mut ()`, public again and `unsafe`, as in 0.2.0.");
    tells(README, "`ProtocolVTable::from_raw_parts::<T>(..)`, an `unsafe const fn`");
    tells(README, "`ProtocolTaskPtr::from_raw_parts(data, vtable)`, an\n    `unsafe fn`");
    tells(README, "**Debug output names the type by its full path.**");
    says(
        README,
        "No public signature changed; `WakerProtocol` has one new public\nfield, `finished`.",
        "New public items: the field\n`WakerProtocol::finished`, and the 0.2.0 items listed under **Put back from 0.2.0**.",
    );
}
