//! Breaks waker_protocol 0.3.1 (git rev 61bd5cd) one way at a time, runs its scheduler tests after
//! each break (and, for two breaks, an example as 0.3.1 and as 0.3.0 wrote it), and puts the file
//! back. It refuses to run on a clone at any other rev.
//!
//! Usage: `cargo run -- <clone of waker_protocol at 61bd5cd> <fresh CARGO_TARGET_DIR for the clone>`

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::{env, fs};

const REV: &str = "61bd5cdb15187681a581f455c2aa621ceb5de460";
const OLD_REV: &str = "1ca72a5803549914aa822c42cbb91705e267875f";
const S: &str = "src/scheduler.rs";
const C: &str = "src/capitulate.rs";

struct Break {
    name: &'static str,
    file: &'static str,
    old: &'static str,
    new: &'static str,
    must_fail: &'static [&'static str],
    examples: &'static [&'static str],
}

const BREAKS: &[Break] = &[
    Break {
        name: r#"M11 QoS protection removed"#,
        file: C,
        old: r#"if snapshot.qos.priority() >= self.protect_qos_at_or_above.priority() {"#,
        new: r#"if false {"#,
        must_fail: &[r#"memory_pressure_reclaims_below_interactive_but_never_interactive_or_above"#],
        examples: &[r#"qos_pressure_reclaim"#],
    },
    Break {
        name: r#"M15 default order ignores QoS"#,
        file: S,
        old: r#"eb.policy
                        .qos
                        .priority()
                        .cmp(&ea.policy.qos.priority())
                        .then_with(|| {"#,
        new: r#"ea.policy
                        .qos
                        .priority()
                        .cmp(&eb.policy.qos.priority())
                        .then_with(|| {"#,
        must_fail: &[r#"the_default_order_polls_higher_classes_first"#],
        examples: &[],
    },
    Break {
        name: r#"M14 default order ignores age"#,
        file: S,
        old: r#"ea.stats
                                .last_polled_at
                                .cmp(&eb.stats.last_polled_at)"#,
        new: r#"eb.stats
                                .last_polled_at
                                .cmp(&ea.stats.last_polled_at)"#,
        must_fail: &[r#"the_default_order_polls_the_one_polled_longest_ago_first_within_a_class"#],
        examples: &[],
    },
    Break {
        name: r#"M16 round robin never rotates"#,
        file: S,
        old: r#"ids.rotate_left(rotate);"#,
        new: r#"let _ = rotate;"#,
        must_fail: &[r#"round_robin_starts_one_later_each_round"#],
        examples: &[],
    },
    Break {
        name: r#"M06 mailbox LIFO"#,
        file: S,
        old: r#"entry.mailbox.pop_front()"#,
        new: r#"entry.mailbox.pop_back()"#,
        must_fail: &[r#"messages_arrive_in_the_order_they_were_sent"#],
        examples: &[],
    },
    Break {
        name: r#"M21 Failed task not removed"#,
        file: S,
        old: r#"TaskPoll::Failed(err) => Self::Finish(TaskState::Failed(err)),"#,
        new: r#"TaskPoll::Failed(_) => Self::Wait,"#,
        must_fail: &[r#"every_terminal_poll_removes_and_frees_the_task_and_retention_decides_its_checkpoint"#],
        examples: &[],
    },
    Break {
        name: r#"M22 Cancelled poll not removed"#,
        file: S,
        old: r#"TaskPoll::Cancelled => Self::Finish(TaskState::Cancelled),"#,
        new: r#"TaskPoll::Cancelled => Self::Wait,"#,
        must_fail: &[r#"every_terminal_poll_removes_and_frees_the_task_and_retention_decides_its_checkpoint"#],
        examples: &[],
    },
    Break {
        name: r#"M23 Capitulate without a stub not removed"#,
        file: S,
        old: r#"        } else {
            Self::Finish(TaskState::Completed)
        }"#,
        new: r#"        } else {
            Self::Wait
        }"#,
        must_fail: &[r#"every_terminal_poll_removes_and_frees_the_task_and_retention_decides_its_checkpoint"#, r#"every_capitulation_without_a_stub_ends_the_task_as_completed"#],
        examples: &[],
    },
    Break {
        name: r#"M08 retain_on_failure ignored"#,
        file: S,
        old: r#"TaskState::Failed(_) => retention.retain_on_failure,"#,
        new: r#"TaskState::Failed(_) => true,"#,
        must_fail: &[r#"every_terminal_poll_removes_and_frees_the_task_and_retention_decides_its_checkpoint"#],
        examples: &[],
    },
    Break {
        name: r#"M09 retain_on_cancel ignored"#,
        file: S,
        old: r#"TaskState::Cancelled => retention.retain_on_cancel,"#,
        new: r#"TaskState::Cancelled => false,"#,
        must_fail: &[r#"every_terminal_poll_removes_and_frees_the_task_and_retention_decides_its_checkpoint"#],
        examples: &[],
    },
    Break {
        name: r#"M28 retain_on_completion ignored"#,
        file: S,
        old: r#"TaskState::Completed => retention.retain_on_completion,"#,
        new: r#"TaskState::Completed => false,"#,
        must_fail: &[r#"every_terminal_poll_removes_and_frees_the_task_and_retention_decides_its_checkpoint"#, r#"every_capitulation_without_a_stub_ends_the_task_as_completed"#],
        examples: &[],
    },
    Break {
        name: r#"M20 dormant keeps no checkpoint"#,
        file: S,
        old: r#"keep_latest.max(1)"#,
        new: r#"keep_latest.max(0)"#,
        must_fail: &[r#"a_dormant_task_keeps_its_checkpoint_even_with_keep_latest_zero"#],
        examples: &[],
    },
    Break {
        name: r#"M10 checkpoints never pruned"#,
        file: S,
        old: r#"let pruned = entry.prune_history(entry.policy.retention.keep_latest.max(1));"#,
        new: r#"let pruned: Vec<S::Handle> = Vec::new();"#,
        must_fail: &[r#"checkpoints_a_task_asks_for_are_pruned_to_keep_latest"#, r#"retention_keep_latest_prunes_older_checkpoints"#],
        examples: &[],
    },
    Break {
        name: r#"M17 eager repoll inverted"#,
        file: S,
        old: r#"TaskPoll::Yield if policy.eager_repoll_after_yield => Self::RunAgain,"#,
        new: r#"TaskPoll::Yield if !policy.eager_repoll_after_yield => Self::RunAgain,"#,
        must_fail: &[r#"yield_runs_again_next_round_when_eager_and_waits_for_a_wake_when_not"#],
        examples: &[],
    },
    Break {
        name: r#"M19 auto_restore_on_wake ignored"#,
        file: S,
        old: r#"dormant && entry.policy.auto_restore_on_wake && entry.is_runnable()"#,
        new: r#"dormant && entry.is_runnable()"#,
        must_fail: &[r#"a_dormant_task_stays_dormant_when_auto_restore_is_off"#],
        examples: &[],
    },
    Break {
        name: r#"M18 restore sends no Resume"#,
        file: S,
        old: r#"entry.pending_wakes.push_back(WakeReason::Resume);"#,
        new: r#""#,
        must_fail: &[r#"a_restored_task_is_told_it_resumed"#],
        examples: &[],
    },
    Break {
        name: r#"M12 idle rule removed"#,
        file: C,
        old: r#"if idle_for >= self.idle_before_checkpoint"#,
        new: r#"if false && idle_for >= self.idle_before_checkpoint"#,
        must_fail: &[r#"the_idle_rule_checkpoints_a_task_with_no_waiting_work_and_a_wake_brings_it_back"#],
        examples: &[],
    },
    Break {
        name: r#"M13 budget rule removed"#,
        file: C,
        old: r#"if snapshot.footprint_bytes > budget"#,
        new: r#"if false && snapshot.footprint_bytes > budget"#,
        must_fail: &[r#"the_budget_rule_reclaims_only_batch_and_below_over_their_budget"#],
        examples: &[],
    },
    Break {
        name: r#"M25 reclaimed_bytes wrong"#,
        file: S,
        old: r#"reclaimed_bytes: before_bytes.saturating_sub(after_bytes),"#,
        new: r#"reclaimed_bytes: 0,"#,
        must_fail: &[r#"a_reclaim_report_counts_the_bytes_freed"#],
        examples: &[],
    },
    Break {
        name: r#"MC1 a Ready task is never removed"#,
        file: S,
        old: r#"TaskPoll::Ready => Self::Finish(TaskState::Completed),"#,
        new: r#"TaskPoll::Ready => Self::Wait,"#,
        must_fail: &[],
        examples: &[r#"chaos_test"#],
    },
];

fn run(repo: &Path, target: &Path, args: &[&str]) -> Output {
    Command::new(args[0])
        .args(&args[1..])
        .current_dir(repo)
        .env("CARGO_TARGET_DIR", target)
        .output()
        .expect("the command runs")
}

fn text(out: &Output) -> String {
    format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr))
}

fn main() {
    let args: Vec<String> = env::args().collect();
    assert_eq!(args.len(), 3, "usage: cargo run -- <clone at {REV}> <fresh CARGO_TARGET_DIR>");
    let repo = PathBuf::from(&args[1]);
    let target = PathBuf::from(&args[2]);
    let head = text(&run(&repo, &target, &["git", "rev-parse", "HEAD"]));
    assert_eq!(head.trim(), REV, "{} is not at {REV}", repo.display());

    let diff_before = text(&run(&repo, &target, &["git", "diff"]));
    let mut all_caught = true;
    for b in BREAKS {
        let path = repo.join(b.file);
        let original = fs::read_to_string(&path).expect("the file reads");
        assert_eq!(original.matches(b.old).count(), 1, "{}: the text to break is there once", b.name);
        fs::write(&path, original.replacen(b.old, b.new, 1)).expect("the file writes");

        let out = text(&run(&repo, &target, &["cargo", "test", "--no-fail-fast", "--test", "scheduler"]));
        let failed: BTreeSet<&str> = out
            .lines()
            .filter_map(|l| l.strip_prefix("test ")?.strip_suffix(" ... FAILED"))
            .collect();
        let build_error = out.contains("error[");
        let missing: Vec<&str> = b.must_fail.iter().copied().filter(|t| !failed.contains(t)).collect();

        let mut exits = Vec::new();
        for ex in b.examples {
            let code = |r: Output| r.status.code().unwrap_or(-1);
            exits.push((ex.to_string(), code(run(&repo, &target, &["cargo", "run", "-q", "--example", ex]))));
            let file = repo.join("examples").join(format!("{ex}.rs"));
            let mine = fs::read_to_string(&file).expect("the example reads");
            let old = run(&repo, &target, &["git", "show", &format!("{OLD_REV}:examples/{ex}.rs")]);
            fs::write(&file, &old.stdout).expect("the example writes");
            exits.push((format!("{ex} (0.3.0 file)"), code(run(&repo, &target, &["cargo", "run", "-q", "--example", ex]))));
            fs::write(&file, mine).expect("the example is put back");
        }
        fs::write(&path, &original).expect("the file is put back");

        let caught = !build_error
            && missing.is_empty()
            && exits.iter().filter(|(n, _)| !n.ends_with("(0.3.0 file)")).all(|(_, c)| *c != 0);
        all_caught &= caught;
        let verdict = if build_error { "BUILD ERROR" } else if caught { "CAUGHT" } else { "NOT CAUGHT" };
        println!("{}: {verdict}; failing tests: {failed:?}; examples exit: {exits:?}", b.name);
    }
    let diff_after = text(&run(&repo, &target, &["git", "diff"]));
    println!("tree restored: {}", diff_before == diff_after);
    println!("{}", if all_caught { "ALL CAUGHT" } else { "SOME NOT CAUGHT" });
}
