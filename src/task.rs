use crate::{TaskPoll, WakeReason};
use std::any::TypeId;
use std::fmt;

/// A task the scheduler drives through a [`ProtocolVTable`].
///
/// # Safety
/// The scheduler calls these functions on a live `Self` it owns, one call at a time, with
/// exclusive access, and promises nothing else. An implementor may rely on exactly that.
/// `restore` receives whatever bytes the [`CheckpointStore`](crate::CheckpointStore) returns,
/// which may not be bytes this type wrote; it must not cause undefined behavior for any input
/// (panicking is fine).
pub unsafe trait ProtocolTask: Send + Sync + 'static {
    /// Advance the task.
    ///
    /// # Safety
    /// Called by the scheduler as the trait's contract states.
    unsafe fn poll(&mut self) -> TaskPoll;

    /// Told why the task was woken, before its next poll.
    ///
    /// # Safety
    /// As `poll`.
    unsafe fn on_wake(&mut self, _reason: WakeReason) {}

    /// The task's state as bytes, for [`ProtocolTask::restore`].
    ///
    /// # Safety
    /// As `poll`.
    unsafe fn checkpoint(&mut self) -> Vec<u8>;

    /// Rebuild the task from checkpoint bytes.
    ///
    /// # Safety
    /// As the trait's contract states: `bytes` can be anything.
    unsafe fn restore(bytes: &[u8]) -> Self
    where
        Self: Sized;

    /// Runs just before the task is freed.
    ///
    /// # Safety
    /// As `poll`.
    unsafe fn cleanup(&mut self) {}

    /// Roughly how many bytes the task holds; the capitulation strategy reads it.
    fn footprint_estimate(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// The functions that run one task type, plus the type they were built for.
///
/// Build it with [`ProtocolVTable::of`] or [`derive_protocol_task_vtable!`](crate::derive_protocol_task_vtable).
/// The fields are private, so a vtable cannot claim one type while holding another type's
/// functions:
///
/// ```compile_fail,E0451
/// # use waker_protocol::{ProtocolTask, ProtocolVTable, TaskPoll};
/// # struct B;
/// # unsafe impl ProtocolTask for B {
/// #     unsafe fn poll(&mut self) -> TaskPoll { TaskPoll::Ready }
/// #     unsafe fn checkpoint(&mut self) -> Vec<u8> { Vec::new() }
/// #     unsafe fn restore(_: &[u8]) -> Self { B }
/// # }
/// fn other_type() -> std::any::TypeId { std::any::TypeId::of::<u8>() }
/// static LIAR: ProtocolVTable = ProtocolVTable { type_id: other_type, ..ProtocolVTable::of::<B>() };
/// ```
pub struct ProtocolVTable {
    poll_fn: unsafe fn(*mut ()) -> TaskPoll,
    wake_fn: unsafe fn(*mut (), WakeReason),
    checkpoint_fn: unsafe fn(*mut ()) -> Vec<u8>,
    restore_fn: unsafe fn(&[u8]) -> *mut (),
    destroy_fn: unsafe fn(*mut ()),
    footprint_fn: unsafe fn(*const ()) -> usize,
    type_id: fn() -> TypeId,
    type_name: fn() -> &'static str,
}

// SAFETY (all six): `data` is a live, exclusively held `T` from `Box::<T>::into_raw`, reached
// only through a ProtocolTaskPtr whose vtable is `ProtocolVTable::of::<T>()`.
unsafe fn task_poll<T: ProtocolTask>(data: *mut ()) -> TaskPoll {
    // SAFETY: see above; the call itself is ProtocolTask's contract.
    unsafe { (*(data as *mut T)).poll() }
}

unsafe fn task_wake<T: ProtocolTask>(data: *mut (), reason: WakeReason) {
    // SAFETY: see above.
    unsafe { (*(data as *mut T)).on_wake(reason) }
}

unsafe fn task_checkpoint<T: ProtocolTask>(data: *mut ()) -> Vec<u8> {
    // SAFETY: see above.
    unsafe { (*(data as *mut T)).checkpoint() }
}

unsafe fn task_restore<T: ProtocolTask>(bytes: &[u8]) -> *mut () {
    // SAFETY: ProtocolTask's contract: restore accepts any bytes.
    let restored = unsafe { T::restore(bytes) };
    Box::into_raw(Box::new(restored)) as *mut ()
}

unsafe fn task_destroy<T: ProtocolTask>(data: *mut ()) {
    // SAFETY: see above; this is the one place the box is freed.
    let mut boxed = unsafe { Box::from_raw(data as *mut T) };
    // SAFETY: the task is live and exclusively held.
    unsafe { boxed.cleanup() };
}

unsafe fn task_footprint<T: ProtocolTask>(data: *const ()) -> usize {
    // SAFETY: see above.
    unsafe { (*(data as *const T)).footprint_estimate() }
}

impl ProtocolVTable {
    /// The vtable of `T`.
    pub const fn of<T: ProtocolTask>() -> Self {
        Self {
            poll_fn: task_poll::<T>,
            wake_fn: task_wake::<T>,
            checkpoint_fn: task_checkpoint::<T>,
            restore_fn: task_restore::<T>,
            destroy_fn: task_destroy::<T>,
            footprint_fn: task_footprint::<T>,
            type_id: TypeId::of::<T>,
            type_name: std::any::type_name::<T>,
        }
    }

    /// Whether this vtable runs `T`.
    pub fn is_for<T: 'static>(&self) -> bool {
        (self.type_id)() == TypeId::of::<T>()
    }

    /// The name of the task type this vtable runs.
    pub fn type_name(&self) -> &'static str {
        (self.type_name)()
    }

    fn type_id(&self) -> TypeId {
        (self.type_id)()
    }
}

impl fmt::Debug for ProtocolVTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtocolVTable")
            .field("type_name", &self.type_name())
            .finish_non_exhaustive()
    }
}

/// An owned task behind its vtable; freed (after `cleanup`) when dropped.
///
/// The fields are private, so the pointer and the vtable always belong to the same type.
pub struct ProtocolTaskPtr {
    data: *mut (),
    vtable: &'static ProtocolVTable,
}

impl ProtocolTaskPtr {
    /// Own `task` behind `vtable`.
    ///
    /// # Panics
    /// If `vtable` was built for a type other than `T`. The task is dropped first.
    pub fn from_box<T: ProtocolTask>(task: Box<T>, vtable: &'static ProtocolVTable) -> Self {
        assert!(
            vtable.is_for::<T>(),
            "protocol vtable is for {}, not {}",
            vtable.type_name(),
            std::any::type_name::<T>(),
        );
        Self {
            data: Box::into_raw(task) as *mut (),
            vtable,
        }
    }

    /// Rebuild a task of `vtable`'s type from checkpoint bytes.
    pub fn restore_from_bytes(vtable: &'static ProtocolVTable, bytes: &[u8]) -> Self {
        Self {
            // SAFETY: the vtable's restore builds its own type; ProtocolTask's contract accepts any bytes.
            data: unsafe { (vtable.restore_fn)(bytes) },
            vtable,
        }
    }

    /// The task's vtable.
    pub fn vtable(&self) -> &'static ProtocolVTable {
        self.vtable
    }

    /// The `TypeId` of the task behind this pointer.
    pub(crate) fn task_type_id(&self) -> TypeId {
        self.vtable.type_id()
    }

    /// # Safety
    /// ProtocolTask's contract for `poll`.
    pub unsafe fn poll(&mut self) -> TaskPoll {
        // SAFETY: the pointer and vtable belong together (private fields), the task is live
        // until Drop, and `&mut self` is exclusive.
        unsafe { (self.vtable.poll_fn)(self.data) }
    }

    /// # Safety
    /// ProtocolTask's contract for `on_wake`.
    pub unsafe fn on_wake(&mut self, reason: WakeReason) {
        // SAFETY: as `poll`.
        unsafe { (self.vtable.wake_fn)(self.data, reason) }
    }

    /// # Safety
    /// ProtocolTask's contract for `checkpoint`.
    pub unsafe fn checkpoint(&mut self) -> Vec<u8> {
        // SAFETY: as `poll`.
        unsafe { (self.vtable.checkpoint_fn)(self.data) }
    }

    /// The task's own size estimate.
    pub fn footprint_estimate(&self) -> usize {
        // SAFETY: as `poll`; footprint_estimate is a safe trait method that only reads.
        unsafe { (self.vtable.footprint_fn)(self.data as *const ()) }
    }

    /// The raw task pointer, for an [`Envelope`](crate::Envelope) whose target type matches.
    pub(crate) fn raw_data(&mut self) -> *mut () {
        self.data
    }
}

impl Drop for ProtocolTaskPtr {
    fn drop(&mut self) {
        // SAFETY: `data` came from Box::into_raw (from_box) or the vtable's restore, of the
        // vtable's own type, and is freed only here.
        unsafe { (self.vtable.destroy_fn)(self.data) };
    }
}

impl fmt::Debug for ProtocolTaskPtr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProtocolTaskPtr")
            .field("data", &self.data)
            .field("type_name", &self.vtable.type_name())
            .finish()
    }
}

/// Declare a `'static` [`ProtocolVTable`] for a [`ProtocolTask`] type.
#[macro_export]
macro_rules! derive_protocol_task_vtable {
    ($t:ty, $name:ident) => {
        pub static $name: $crate::ProtocolVTable = $crate::ProtocolVTable::of::<$t>();
    };
}
