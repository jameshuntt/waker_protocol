//! A task scheduler on [`waker_tables`]: tasks live in a generation-checked slab and are driven
//! through per-type vtables, with quality-of-service ordering, checkpoints, typed messages and
//! capitulation (a task surrenders with a stated reason, after a checkpoint, so the work can go
//! somewhere else). Every reclaim is written to the scheduler's reclaim log.
//!
//! Each task type's vtable records the type it runs: `spawn_box` refuses a key registered for
//! another type, and a message is delivered only to a task of the type it was written for.

#![deny(unsafe_op_in_unsafe_fn, missing_docs)]
/// Capitulation: a task surrendering with a stated reason, and the strategy that decides it.
pub mod capitulate;
/// Where checkpoint bytes are kept.
pub mod checkpoint;
/// Typed messages delivered to a task before its next poll.
pub mod envelope;
/// Per-task and scheduler-wide policy.
pub mod policy;
/// The scheduler, its handles and its task table.
pub mod scheduler;
/// The generation-checked task slab.
pub mod slab;
/// The task trait, its vtable and the owned task pointer.
pub mod task;
/// Ids, reasons, poll results, states and scheduler messages.
pub mod types;
/// `std::task::Waker`s that wake a scheduler task.
pub mod wake_ref;

pub use capitulate::*;
pub use checkpoint::*;
pub use envelope::*;
pub use policy::*;
pub use scheduler::*;
pub use slab::*;
pub use task::*;
pub use types::*;
pub use wake_ref::*;