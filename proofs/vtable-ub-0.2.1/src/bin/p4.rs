use ub::*; use waker_protocol::*;
fn main() {
    let r = typed_envelope::<Small, u64>("poke", 7, |t, n| { t.0 = n; Ok(()) })
        .dispatch(std::ptr::null_mut());
    println!("p4 -> {r:?}");
}
