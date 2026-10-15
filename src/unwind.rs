//! Catching a panic from code the scheduler does not own (a task's, a message's), without letting
//! a second panic out.

use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};

/// Run `call`. A panic in it is caught and becomes the panic's text; no panic leaves this
/// function, not even one from dropping the payload.
pub(crate) fn catch<R>(call: impl FnOnce() -> R) -> Result<R, String> {
    catch_unwind(AssertUnwindSafe(call)).map_err(|payload| {
        let text = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("a panic payload that is not text")
            .to_owned();
        drop_payload(payload);
        text
    })
}

/// Drop a caught panic's payload without letting a panic out. If the payload's own drop panics,
/// that panic is caught too and its payload dropped the same way; after three such panics in a
/// row the last payload is leaked instead, so a payload that always panics cannot loop forever.
pub(crate) fn drop_payload(mut payload: Box<dyn Any + Send>) {
    for _ in 0..3 {
        match catch_unwind(AssertUnwindSafe(move || drop(payload))) {
            Ok(()) => return,
            Err(again) => payload = again,
        }
    }
    std::mem::forget(payload);
}
