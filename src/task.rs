use crate::{TaskPoll, WakeReason};
use std::fmt;

pub unsafe trait ProtocolTask: Send + Sync + 'static {
    unsafe fn poll(&mut self) -> TaskPoll;

    unsafe fn on_wake(&mut self, _reason: WakeReason) {}

    unsafe fn checkpoint(&mut self) -> Vec<u8>;

    unsafe fn restore(bytes: &[u8]) -> Self
    where
        Self: Sized;

    unsafe fn cleanup(&mut self) {}

    fn footprint_estimate(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

pub struct ProtocolVTable {
    pub poll_fn: unsafe fn(*mut ()) -> TaskPoll,
    pub wake_fn: unsafe fn(*mut (), WakeReason),
    pub checkpoint_fn: unsafe fn(*mut ()) -> Vec<u8>,
    pub restore_fn: unsafe fn(&[u8]) -> *mut (),
    pub destroy_fn: unsafe fn(*mut ()),
    pub footprint_fn: unsafe fn(*const ()) -> usize,
    pub type_name: &'static str,
}

impl fmt::Debug for ProtocolVTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtocolVTable")
            .field("type_name", &self.type_name)
            .finish_non_exhaustive()
    }
}

pub struct ProtocolTaskPtr {
    pub data: *mut (),
    pub vtable: &'static ProtocolVTable,
}

impl ProtocolTaskPtr {
    pub fn from_box<T: ProtocolTask>(task: Box<T>, vtable: &'static ProtocolVTable) -> Self {
        Self {
            data: Box::into_raw(task) as *mut (),
            vtable,
        }
    }

    pub unsafe fn poll(&mut self) -> TaskPoll {
        unsafe { (self.vtable.poll_fn)(self.data) }
    }

    pub unsafe fn on_wake(&mut self, reason: WakeReason) {
        unsafe { (self.vtable.wake_fn)(self.data, reason) }
    }

    pub unsafe fn checkpoint(&mut self) -> Vec<u8> {
        unsafe { (self.vtable.checkpoint_fn)(self.data) }
    }

    pub unsafe fn footprint_estimate(&self) -> usize {
        unsafe { (self.vtable.footprint_fn)(self.data as *const ()) }
    }

    pub unsafe fn raw_data(&mut self) -> *mut () {
        self.data
    }

    pub unsafe fn restore_from_bytes(vtable: &'static ProtocolVTable, bytes: &[u8]) -> Self {
        Self {
            data: unsafe { (vtable.restore_fn)(bytes) },
            vtable,
        }
    }
}

impl Drop for ProtocolTaskPtr {
    fn drop(&mut self) {
        if !self.data.is_null() {
            unsafe { (self.vtable.destroy_fn)(self.data) };
            self.data = std::ptr::null_mut();
        }
    }
}

#[macro_export]
macro_rules! derive_protocol_task_vtable {
    ($t:ty, $name:ident) => {
        pub static $name: $crate::ProtocolVTable = $crate::ProtocolVTable {
            poll_fn: |ptr| unsafe { (&mut *(ptr as *mut $t)).poll() },

            wake_fn: |ptr, reason| unsafe {
                (&mut *(ptr as *mut $t)).on_wake(reason)
            },

            checkpoint_fn: |ptr| unsafe {
                (&mut *(ptr as *mut $t)).checkpoint()
            },

            restore_fn: |bytes| unsafe {
                let restored = <$t as $crate::ProtocolTask>::restore(bytes);
                Box::into_raw(Box::new(restored)) as *mut ()
            },

            destroy_fn: |ptr| unsafe {
                let mut boxed = Box::from_raw(ptr as *mut $t);
                boxed.cleanup();
            },

            footprint_fn: |ptr| unsafe {
                (&*(ptr as *const $t)).footprint_estimate()
            },

            type_name: stringify!($t),
        };
    };
}