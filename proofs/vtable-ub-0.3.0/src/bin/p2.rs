use ub::*; use waker_protocol::*;
fn main() { drop(ProtocolTaskPtr::from_box(Box::new(Small(1)), &BIG_VTABLE)); println!("p2 done"); }
