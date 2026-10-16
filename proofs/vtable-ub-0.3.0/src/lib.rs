use waker_protocol::*;
pub use waker_tables::VTableKey;

pub struct Small(pub u64);
pub struct Big(pub [u64; 8]);

// SAFETY: the bodies only touch `self`.
unsafe impl ProtocolTask for Small {
    unsafe fn poll(&mut self) -> TaskPoll { self.0 += 1; TaskPoll::Pending }
    unsafe fn checkpoint(&mut self) -> Vec<u8> { self.0.to_le_bytes().to_vec() }
    unsafe fn restore(b: &[u8]) -> Self { Small(u64::from_le_bytes(b[..8].try_into().unwrap())) }
}
// SAFETY: the bodies only touch `self`.
unsafe impl ProtocolTask for Big {
    unsafe fn poll(&mut self) -> TaskPoll { self.0[7] += 1; TaskPoll::Pending }
    unsafe fn checkpoint(&mut self) -> Vec<u8> { self.0[7].to_le_bytes().to_vec() }
    unsafe fn restore(b: &[u8]) -> Self { Big([u64::from_le_bytes(b[..8].try_into().unwrap()); 8]) }
}
derive_protocol_task_vtable!(Small, SMALL_VTABLE);
derive_protocol_task_vtable!(Big, BIG_VTABLE);
pub const KEY: VTableKey = VTableKey { segment: "ub", kind: "big", version: 1 };
