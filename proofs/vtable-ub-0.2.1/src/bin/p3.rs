use ub::*; use waker_protocol::*;
fn main() { drop(ProtocolTaskPtr { data: Box::into_raw(Box::new(0u8)).cast(), vtable: &BIG_VTABLE }); println!("p3 done"); }
