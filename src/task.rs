use crate::unwind::{catch, drop_payload};
use crate::{Checkpoint, TaskPoll, WakeReason};
use std::any::TypeId;
use std::fmt;
use std::mem::ManuallyDrop;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::time::Instant;

/// A task the scheduler drives through a [`ProtocolVTable`].
///
/// # Safety
/// The scheduler calls these functions on a live `Self` it owns, one call at a time, with
/// exclusive access, and promises nothing else. An implementor may rely on exactly that.
/// `restore` receives whatever bytes the [`CheckpointStore`](crate::CheckpointStore) returns,
/// which may not be bytes this type wrote; it must not cause undefined behavior for any input
/// (panicking is fine).
///
/// If `poll`, `on_wake`, `checkpoint`, `restore`, `footprint_estimate` or `wake_at` panics, the scheduler
/// catches it and the task ends as [`TaskState::Failed`](crate::TaskState::Failed), with a
/// message naming the panic. That holds for any panic payload, even one whose own drop panics.
///
/// If `cleanup` or the task's own `Drop` panics while `run_once` or `pump` frees the task (at its
/// end, or once its checkpoint is saved), the scheduler catches that too: the task is freed anyway,
/// its row is left as it would have been (removed, or dormant after a checkpoint), and the panic
/// is returned as an error once the round is done. With a vtable from [`ProtocolVTable::of`] or
/// [`derive_protocol_task_vtable!`](crate::derive_protocol_task_vtable), if both panic, the task
/// is still freed and the error names the panic from `cleanup`. A vtable from
/// [`ProtocolVTable::from_raw_parts`] does that only if its destroy function holds a `cleanup`
/// panic until the task is dropped; otherwise a panic in both aborts the process. Dropping the
/// scheduler itself drops its live tasks as any value is dropped: a panic there goes to the code
/// that drops the scheduler. With a vtable from [`ProtocolVTable::of`] or
/// `derive_protocol_task_vtable!`, or any destroy function that lets a `cleanup` or drop panic go
/// on, a task dropped while another panic unwinds (a scheduler dropped during a panic, say)
/// aborts the process if its `cleanup` or its `Drop` panics, as any value whose drop panics
/// during an unwind does.
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

    /// The task's state as bytes, for [`ProtocolTask::restore`]. Only a vtable from
    /// [`ProtocolVTable::of`] calls it. A task that gives its state through [`Checkpoint`]
    /// (vtable [`ProtocolVTable::checkpointed`]), or gives none (vtable [`ProtocolVTable::live`]),
    /// does not write it; written by neither, it panics, naming the task type, and the task ends
    /// as Failed: the scheduler never saves bytes the task did not give.
    ///
    /// # Safety
    /// As `poll`.
    unsafe fn checkpoint(&mut self) -> Vec<u8> {
        panic!("{} gives no checkpoint: build its vtable with ProtocolVTable::checkpointed or ProtocolVTable::live", std::any::type_name::<Self>())
    }

    /// Rebuild the task from checkpoint bytes. Only a vtable from [`ProtocolVTable::of`] calls it;
    /// as [`checkpoint`](ProtocolTask::checkpoint), it panics when not written.
    ///
    /// # Safety
    /// As the trait's contract states: `bytes` can be anything.
    unsafe fn restore(bytes: &[u8]) -> Self
    where
        Self: Sized,
    {
        let _ = bytes;
        panic!("{} gives no checkpoint: build its vtable with ProtocolVTable::checkpointed or ProtocolVTable::live", std::any::type_name::<Self>())
    }

    /// Runs just before the task is freed. If it panics, the task is still dropped and freed, and
    /// the panic goes on after that, with a vtable from [`ProtocolVTable::of`] or
    /// `derive_protocol_task_vtable!`; [`ProtocolVTable::from_raw_parts`] says what a hand-written
    /// destroy function must do for the same.
    ///
    /// # Safety
    /// As `poll`.
    unsafe fn cleanup(&mut self) {}

    /// Roughly how many bytes the task holds; the capitulation strategy reads it.
    fn footprint_estimate(&self) -> usize {
        std::mem::size_of_val(self)
    }

    /// The task's next moment: after every poll that leaves the task in its row, the scheduler
    /// asks, and wakes the task with [`WakeReason::Timer`] at the moment it gives, dormant or not.
    /// Each answer replaces the one before; `None` (the default) sets none. A task woken at fixed
    /// intervals declares that in its policy instead
    /// ([`TaskPolicy::interval`](crate::TaskPolicy::interval)).
    fn wake_at(&self) -> Option<Instant> {
        None
    }
}

/// The functions that run one task type, plus the type they were built for.
///
/// Build it with [`ProtocolVTable::of`] or [`derive_protocol_task_vtable!`](crate::derive_protocol_task_vtable),
/// or from hand-written functions with the `unsafe` [`ProtocolVTable::from_raw_parts`]. The fields
/// are private, so safe code cannot build a vtable that claims one type while holding another
/// type's functions:
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
///
/// and building one from hand-written functions takes `unsafe`:
///
/// ```compile_fail,E0133
/// # use waker_protocol::{ProtocolVTable, TaskPoll, WakeReason};
/// unsafe fn poll(_: *mut ()) -> TaskPoll { TaskPoll::Ready }
/// unsafe fn wake(_: *mut (), _: WakeReason) {}
/// unsafe fn checkpoint(_: *mut ()) -> Vec<u8> { Vec::new() }
/// unsafe fn restore(_: &[u8]) -> *mut () { std::ptr::null_mut() }
/// unsafe fn destroy(_: *mut ()) {}
/// unsafe fn footprint(_: *const ()) -> usize { 0 }
/// static LIAR: ProtocolVTable =
///     ProtocolVTable::from_raw_parts::<u8>(poll, wake, checkpoint, restore, destroy, footprint);
/// ```
pub struct ProtocolVTable {
    poll_fn: unsafe fn(*mut ()) -> TaskPoll,
    wake_fn: unsafe fn(*mut (), WakeReason),
    checkpoint_fn: unsafe fn(*mut ()) -> Vec<u8>,
    restore_fn: unsafe fn(&[u8]) -> *mut (),
    destroy_fn: unsafe fn(*mut ()),
    footprint_fn: unsafe fn(*const ()) -> usize,
    wake_at_fn: unsafe fn(*const ()) -> Option<Instant>,
    gives_checkpoint: bool,
    type_id: fn() -> TypeId,
    type_name: fn() -> &'static str,
}

// SAFETY (all six): `data` is a live, exclusively held `T` from `Box::<T>::into_raw`, reached
// only through a ProtocolTaskPtr whose vtable is `ProtocolVTable::of::<T>()`, `checkpointed::<T>()`
// or `live::<T>()` (built by `from_box`,
// `restore_from_bytes`, or a caller of `ProtocolTaskPtr::from_raw_parts` who promised exactly that).
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

/// Run the task's `cleanup`, then drop and free it. A panic in `cleanup` is held until the task is
/// dropped, so it never unwinds through the live task (whose own drop, panicking during that
/// unwind, would abort the process). The first panic goes on; if the drop panics too, its payload
/// is dropped.
unsafe fn task_destroy<T: ProtocolTask>(data: *mut ()) {
    // SAFETY: see above; this is the one place the box is freed.
    let mut boxed = unsafe { Box::from_raw(data as *mut T) };
    // SAFETY: the task is live and exclusively held.
    let cleaned = catch_unwind(AssertUnwindSafe(|| unsafe { boxed.cleanup() }));
    let dropped = catch_unwind(AssertUnwindSafe(move || drop(boxed)));
    match (cleaned, dropped) {
        (Ok(()), Ok(())) => {}
        (Err(payload), Ok(())) | (Ok(()), Err(payload)) => resume_unwind(payload),
        (Err(payload), Err(second)) => {
            drop_payload(second);
            resume_unwind(payload)
        }
    }
}

unsafe fn task_footprint<T: ProtocolTask>(data: *const ()) -> usize {
    // SAFETY: see above.
    unsafe { (*(data as *const T)).footprint_estimate() }
}

unsafe fn task_wake_at<T: ProtocolTask>(data: *const ()) -> Option<Instant> {
    // SAFETY: see above.
    unsafe { (*(data as *const T)).wake_at() }
}

/// The `wake_at` of a vtable built from hand-written functions: no moment.
unsafe fn no_wake_at(_: *const ()) -> Option<Instant> {
    None
}

unsafe fn given_save<T: ProtocolTask + Checkpoint>(data: *mut ()) -> Vec<u8> {
    // SAFETY: see above.
    unsafe { (*(data as *const T)).save() }
}

fn given_restore<T: ProtocolTask + Checkpoint>(bytes: &[u8]) -> *mut () {
    match <T as Checkpoint>::restore(bytes) {
        Ok(restored) => Box::into_raw(Box::new(restored)) as *mut (),
        Err(why) => panic!("{} refused its checkpoint: {why}", std::any::type_name::<T>()),
    }
}

/// The checkpoint function of a [`ProtocolVTable::live`] vtable; the scheduler never calls it.
unsafe fn live_checkpoint<T: ProtocolTask>(_: *mut ()) -> Vec<u8> {
    panic!("{} gives no checkpoint (its vtable is ProtocolVTable::live)", std::any::type_name::<T>())
}

/// The restore function of a [`ProtocolVTable::live`] vtable; the scheduler never calls it.
fn live_restore<T: ProtocolTask>(_: &[u8]) -> *mut () {
    panic!("{} gives no checkpoint (its vtable is ProtocolVTable::live)", std::any::type_name::<T>())
}

impl ProtocolVTable {
    /// The vtable of `T`, checkpointed through `T`'s own [`ProtocolTask::checkpoint`] and
    /// [`ProtocolTask::restore`], as in 0.3.
    pub const fn of<T: ProtocolTask>() -> Self {
        // SAFETY: these are `T`'s own functions, each sound for a `T` from `Box::<T>::into_raw`
        // under ProtocolTask's contract (see the comment above `task_poll`).
        let raw = unsafe {
            Self::from_raw_parts::<T>(
                task_poll::<T>,
                task_wake::<T>,
                task_checkpoint::<T>,
                task_restore::<T>,
                task_destroy::<T>,
                task_footprint::<T>,
            )
        };
        Self { wake_at_fn: task_wake_at::<T>, ..raw }
    }

    /// The vtable of `T`, checkpointed through the state `T` gives: [`Checkpoint::save`] and
    /// [`Checkpoint::restore`]. A `restore` that returns `Err` ends the task as Failed, with a
    /// message naming the type and carrying the error.
    pub const fn checkpointed<T: ProtocolTask + Checkpoint>() -> Self {
        Self { checkpoint_fn: given_save::<T>, restore_fn: given_restore::<T>, ..Self::of::<T>() }
    }

    /// The vtable of `T`, a task whose state is live (a closure, a socket, a lock) and that gives
    /// no checkpoint. The scheduler never checkpoints it: the capitulation strategy is not asked
    /// about it, its policy's `dormancy_ttl` does not park it, Shutdown leaves it live in its row,
    /// and a `Checkpoint` (or a `Capitulate` that keeps a stub), from its own poll or as a
    /// message, leaves it live and is returned as an error (see
    /// [`gives_checkpoint`](Self::gives_checkpoint)).
    pub const fn live<T: ProtocolTask>() -> Self {
        Self { checkpoint_fn: live_checkpoint::<T>, restore_fn: live_restore::<T>, gives_checkpoint: false, ..Self::of::<T>() }
    }

    /// Whether the scheduler may checkpoint a task of this vtable: false only for
    /// [`live`](Self::live).
    pub const fn gives_checkpoint(&self) -> bool {
        self.gives_checkpoint
    }

    /// A vtable from hand-written functions, recorded as running `T`. It is `const`, so it can
    /// build a `static`:
    ///
    /// ```
    /// # use waker_protocol::{ProtocolTask, ProtocolTaskPtr, ProtocolVTable, TaskPoll, WakeReason};
    /// struct Job(u32);
    /// # unsafe impl ProtocolTask for Job {
    /// #     unsafe fn poll(&mut self) -> TaskPoll { TaskPoll::Ready }
    /// #     unsafe fn checkpoint(&mut self) -> Vec<u8> { Vec::new() }
    /// #     unsafe fn restore(_: &[u8]) -> Self { Job(0) }
    /// # }
    /// unsafe fn poll(data: *mut ()) -> TaskPoll {
    ///     // SAFETY: `data` is a live, exclusively held `Job` (from_raw_parts' contract).
    ///     let job = unsafe { &mut *(data as *mut Job) };
    ///     job.0 += 1;
    ///     TaskPoll::Ready
    /// }
    /// unsafe fn wake(_: *mut (), _: WakeReason) {}
    /// unsafe fn checkpoint(_: *mut ()) -> Vec<u8> { Vec::new() }
    /// unsafe fn restore(_: &[u8]) -> *mut () { Box::into_raw(Box::new(Job(0))) as *mut () }
    /// // SAFETY: `data` is from `Box::<Job>::into_raw` and this is its last call (the contract).
    /// unsafe fn destroy(data: *mut ()) { drop(unsafe { Box::from_raw(data as *mut Job) }) }
    /// unsafe fn footprint(_: *const ()) -> usize { std::mem::size_of::<Job>() }
    ///
    /// // SAFETY: each function treats `data` as a `Box<Job>` pointer, as the contract asks.
    /// static JOB: ProtocolVTable =
    ///     unsafe { ProtocolVTable::from_raw_parts::<Job>(poll, wake, checkpoint, restore, destroy, footprint) };
    ///
    /// assert!(JOB.is_for::<Job>());
    /// let mut task = ProtocolTaskPtr::from_box(Box::new(Job(0)), &JOB);
    /// // SAFETY: `task` owns a live `Job` and `&mut` gives exclusive access.
    /// assert_eq!(unsafe { task.poll() }, TaskPoll::Ready);
    /// ```
    ///
    /// A `destroy_fn` that calls the task's `cleanup` must hold a panic from `cleanup` until the
    /// task is dropped, as the destroy function of [`of`](Self::of) does: it runs `cleanup` under
    /// `catch_unwind`, drops the task, and only then lets the panic go on. A `destroy_fn` that
    /// drops the task while a `cleanup` panic unwinds, as 0.2.0's `derive_protocol_task_vtable!`
    /// wrote it (`let mut b = Box::from_raw(..); b.cleanup();`), aborts the process when the
    /// task's own drop panics as well.
    ///
    /// # Safety
    /// The vtable records `T` as its task type, and the crate trusts that:
    /// [`WakerProtocol::spawn_box`](crate::WakerProtocol::spawn_box) and
    /// [`ProtocolTaskPtr::from_box`] accept it for a `Box<T>`, and the scheduler delivers a message
    /// written for `T` by reading the task pointer as `*mut T`. Every task pointer `data` these
    /// functions are given is a pointer from `Box::<T>::into_raw`, as `from_box` makes it, and is
    /// live until `destroy_fn` frees it. So:
    ///
    /// - `restore_fn` is sound to call with any bytes (panicking is fine), and returns a new `data`
    ///   from `Box::<T>::into_raw`, owned by its caller;
    /// - `poll_fn`, `wake_fn` and `checkpoint_fn` are sound to call with a live `data` while the
    ///   caller has exclusive access to the task, and leave it live;
    /// - `footprint_fn` is sound to call with a live `data` while the caller has shared access to
    ///   the task, and only reads;
    /// - `destroy_fn` is sound to call once with a live `data`, as the last call, and frees it as
    ///   the `Box<T>` it is.
    pub const unsafe fn from_raw_parts<T: 'static>(
        poll_fn: unsafe fn(*mut ()) -> TaskPoll,
        wake_fn: unsafe fn(*mut (), WakeReason),
        checkpoint_fn: unsafe fn(*mut ()) -> Vec<u8>,
        restore_fn: unsafe fn(&[u8]) -> *mut (),
        destroy_fn: unsafe fn(*mut ()),
        footprint_fn: unsafe fn(*const ()) -> usize,
    ) -> Self {
        Self {
            poll_fn,
            wake_fn,
            checkpoint_fn,
            restore_fn,
            destroy_fn,
            footprint_fn,
            wake_at_fn: no_wake_at,
            gives_checkpoint: true,
            type_id: TypeId::of::<T>,
            type_name: std::any::type_name::<T>,
        }
    }

    /// The function that polls a task. Calling it is `unsafe`; see
    /// [`from_raw_parts`](Self::from_raw_parts) for what it expects.
    pub const fn poll_fn(&self) -> unsafe fn(*mut ()) -> TaskPoll {
        self.poll_fn
    }

    /// The function that tells a task why it woke. Calling it is `unsafe`; see
    /// [`from_raw_parts`](Self::from_raw_parts).
    pub const fn wake_fn(&self) -> unsafe fn(*mut (), WakeReason) {
        self.wake_fn
    }

    /// The function that turns a task into checkpoint bytes. Calling it is `unsafe`; see
    /// [`from_raw_parts`](Self::from_raw_parts).
    pub const fn checkpoint_fn(&self) -> unsafe fn(*mut ()) -> Vec<u8> {
        self.checkpoint_fn
    }

    /// The function that builds a task from checkpoint bytes and returns its new, owned pointer.
    /// Calling it is `unsafe`; see [`from_raw_parts`](Self::from_raw_parts).
    pub const fn restore_fn(&self) -> unsafe fn(&[u8]) -> *mut () {
        self.restore_fn
    }

    /// The function that runs a task's `cleanup` and frees it. Calling it is `unsafe`; see
    /// [`from_raw_parts`](Self::from_raw_parts).
    pub const fn destroy_fn(&self) -> unsafe fn(*mut ()) {
        self.destroy_fn
    }

    /// The function that reads a task's size estimate. Calling it is `unsafe`; see
    /// [`from_raw_parts`](Self::from_raw_parts).
    pub const fn footprint_fn(&self) -> unsafe fn(*const ()) -> usize {
        self.footprint_fn
    }

    /// The function that reads a task's next moment ([`ProtocolTask::wake_at`]); a vtable from
    /// [`from_raw_parts`](Self::from_raw_parts) has one that gives none. Calling it is `unsafe`,
    /// as [`footprint_fn`](Self::footprint_fn).
    pub const fn wake_at_fn(&self) -> unsafe fn(*const ()) -> Option<Instant> {
        self.wake_at_fn
    }

    /// Whether this vtable runs `T`.
    pub fn is_for<T: 'static>(&self) -> bool {
        (self.type_id)() == TypeId::of::<T>()
    }

    /// The name of the task type this vtable runs: its full path, from
    /// [`std::any::type_name`], the type the vtable records.
    ///
    /// It is read at run time only: `std::any::type_name` is not `const` on stable, so a name
    /// needed in a `static` has to be written out by hand.
    ///
    /// ```compile_fail,E0015
    /// # use waker_protocol::{derive_protocol_task_vtable, ProtocolTask, TaskPoll};
    /// # struct Job;
    /// # unsafe impl ProtocolTask for Job {
    /// #     unsafe fn poll(&mut self) -> TaskPoll { TaskPoll::Ready }
    /// #     unsafe fn checkpoint(&mut self) -> Vec<u8> { Vec::new() }
    /// #     unsafe fn restore(_: &[u8]) -> Self { Job }
    /// # }
    /// derive_protocol_task_vtable!(Job, JOB_VTABLE);
    /// static NAME: &str = JOB_VTABLE.type_name();
    /// ```
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

/// An owned task behind its vtable; freed (after `cleanup`) when dropped. With a vtable from
/// [`ProtocolVTable::of`] or [`derive_protocol_task_vtable!`](crate::derive_protocol_task_vtable),
/// if `cleanup` panics, the task is still dropped and freed before the panic goes on. With such a
/// vtable, or any destroy function that lets a `cleanup` or drop panic go on, a `ProtocolTaskPtr`
/// dropped while another panic unwinds aborts the process if its `cleanup` or the task's drop
/// panics, as any value whose drop panics during an unwind does.
///
/// An empty one, whose `data` is null, owns nothing, as in 0.2.0: see
/// [`from_raw_parts`](Self::from_raw_parts).
///
/// The fields are private, so the pointer and the vtable always belong to the same type; the one
/// way to pair them by hand is the `unsafe` [`from_raw_parts`](Self::from_raw_parts). Safe code
/// can read the pointer with [`data`](Self::data), as it could read 0.2.0's public field, but
/// using the pointer takes `unsafe`. Safe code cannot pair them, and [`raw_data`](Self::raw_data)
/// is an `unsafe fn`, as in 0.2.0:
///
/// ```compile_fail,E0451
/// # use waker_protocol::{ProtocolTaskPtr, ProtocolVTable};
/// fn forge(data: *mut (), vtable: &'static ProtocolVTable) -> ProtocolTaskPtr {
///     ProtocolTaskPtr { data, vtable }
/// }
/// ```
///
/// ```compile_fail,E0133
/// # use waker_protocol::{ProtocolTaskPtr, ProtocolVTable};
/// fn forge(data: *mut (), vtable: &'static ProtocolVTable) -> ProtocolTaskPtr {
///     ProtocolTaskPtr::from_raw_parts(data, vtable)
/// }
/// ```
///
/// ```compile_fail,E0133
/// # use waker_protocol::ProtocolTaskPtr;
/// fn peek(task: &mut ProtocolTaskPtr) -> *mut () {
///     task.raw_data()
/// }
/// ```
pub struct ProtocolTaskPtr {
    data: *mut (),
    vtable: &'static ProtocolVTable,
}

impl ProtocolTaskPtr {
    /// Own `task` behind `vtable`.
    ///
    /// # Panics
    /// If `vtable` was built for a type other than `T`. The task is dropped first, before the
    /// panic, never while it unwinds: a panic in the task's drop is caught and named in the
    /// panic's message.
    pub fn from_box<T: ProtocolTask>(task: Box<T>, vtable: &'static ProtocolVTable) -> Self {
        if !vtable.is_for::<T>() {
            let refused = format!("protocol vtable is for {}, not {}", vtable.type_name(), std::any::type_name::<T>());
            match catch(move || drop(task)) {
                Ok(()) => panic!("{refused}"),
                Err(text) => panic!("{refused}; the task panicked as it was dropped: {text}"),
            }
        }
        Self {
            data: Box::into_raw(task) as *mut (),
            vtable,
        }
    }

    /// Rebuild a task of `vtable`'s type from checkpoint bytes.
    pub fn restore_from_bytes(vtable: &'static ProtocolVTable, bytes: &[u8]) -> Self {
        Self {
            // SAFETY: the vtable's restore builds its own type and accepts any bytes (ProtocolTask's
            // contract, or `ProtocolVTable::from_raw_parts`').
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
    /// ProtocolTask's contract for `poll`, and a non-null `data`: `self` is not an empty pointer
    /// (see [`from_raw_parts`](Self::from_raw_parts)).
    pub unsafe fn poll(&mut self) -> TaskPoll {
        // SAFETY: `data` is not null (the caller's promise), so it is a live task until Drop; the
        // pointer and vtable belong together (private fields; a caller of `from_raw_parts`
        // promised it), and `&mut self` is exclusive.
        unsafe { (self.vtable.poll_fn)(self.data) }
    }

    /// # Safety
    /// ProtocolTask's contract for `on_wake`, and a non-null `data`, as `poll`.
    pub unsafe fn on_wake(&mut self, reason: WakeReason) {
        // SAFETY: as `poll`.
        unsafe { (self.vtable.wake_fn)(self.data, reason) }
    }

    /// # Safety
    /// ProtocolTask's contract for `checkpoint`, and a non-null `data`, as `poll`.
    pub unsafe fn checkpoint(&mut self) -> Vec<u8> {
        // SAFETY: as `poll`.
        unsafe { (self.vtable.checkpoint_fn)(self.data) }
    }

    /// The task's own size estimate; 0 for an empty pointer (a null `data`), whose vtable is not
    /// called.
    pub fn footprint_estimate(&self) -> usize {
        if self.data.is_null() {
            return 0;
        }
        // SAFETY: `data` is not null, so it is a live task until Drop, of the vtable's type (as
        // `poll`); footprint_estimate is a safe trait method that only reads.
        unsafe { (self.vtable.footprint_fn)(self.data as *const ()) }
    }

    /// The task's next moment ([`ProtocolTask::wake_at`]); `None` for an empty pointer (a null
    /// `data`), whose vtable is not called.
    pub fn wake_at(&self) -> Option<Instant> {
        if self.data.is_null() {
            return None;
        }
        // SAFETY: as `footprint_estimate`; wake_at is a safe trait method that only reads.
        unsafe { (self.vtable.wake_at_fn)(self.data as *const ()) }
    }

    /// The raw task pointer, read through a shared reference, as 0.2.0's public `data` field could
    /// be; null for an empty pointer. The task stays owned by this `ProtocolTaskPtr`. Reading the
    /// pointer is safe; using it is `unsafe`, under the rules [`raw_data`](Self::raw_data) states.
    /// A vtable check alone does not prove a task is behind the pointer: an empty pointer's `data`
    /// is null, whatever its vtable.
    ///
    /// ```
    /// # use waker_protocol::{derive_protocol_task_vtable, ProtocolTask, ProtocolTaskPtr, TaskPoll};
    /// struct Job { polls: u32 }
    /// # unsafe impl ProtocolTask for Job {
    /// #     unsafe fn poll(&mut self) -> TaskPoll { self.polls += 1; TaskPoll::Pending }
    /// #     unsafe fn checkpoint(&mut self) -> Vec<u8> { Vec::new() }
    /// #     unsafe fn restore(_: &[u8]) -> Self { Job { polls: 0 } }
    /// # }
    /// derive_protocol_task_vtable!(Job, JOB_VTABLE);
    ///
    /// /// The polls of the `Job` behind `task`; `None` for another type or an empty pointer.
    /// fn polls(task: &ProtocolTaskPtr) -> Option<u32> {
    ///     if !task.vtable().is_for::<Job>() || task.data().is_null() {
    ///         return None;
    ///     }
    ///     // SAFETY: `data` is not null, so a live task is behind it, and it is a `Job` (both
    ///     // checked above); it is only read here, while nothing writes it.
    ///     Some(unsafe { (*(task.data() as *const Job)).polls })
    /// }
    ///
    /// let task = ProtocolTaskPtr::from_box(Box::new(Job { polls: 3 }), &JOB_VTABLE);
    /// assert_eq!(polls(&task), Some(3));
    /// // SAFETY: a null `data` makes an empty pointer; it is only read and dropped here.
    /// let empty = unsafe { ProtocolTaskPtr::from_raw_parts(std::ptr::null_mut(), &JOB_VTABLE) };
    /// assert_eq!(polls(&empty), None, "a `Job` vtable, but no task behind it");
    /// ```
    pub fn data(&self) -> *mut () {
        self.data
    }

    /// The raw task pointer, as [`Envelope::dispatch`](crate::Envelope::dispatch) takes it: the
    /// same pointer [`data`](Self::data) reads, with 0.2.0's signature. The task stays owned by
    /// this `ProtocolTaskPtr`.
    ///
    /// # Safety
    /// The call itself has no requirement. Using the pointer does: it requires a non-null `data`
    /// (an empty pointer has no task behind it), and then it points to the live task, a value of
    /// the type `self.vtable()` records ([`ProtocolVTable::type_name`]), until the task is freed
    /// (when `self` is dropped). Reads and writes through it must be as that type and must not
    /// overlap any other use of the task, such as a call through `self`; and the task must not be
    /// freed through it, since `self` owns it.
    pub unsafe fn raw_data(&mut self) -> *mut () {
        self.data
    }

    /// Own the task `data` behind `vtable`, as [`into_raw_parts`](Self::into_raw_parts) gave
    /// them out. Dropping the result runs `vtable`'s destroy function on `data`, unless `data` is
    /// null.
    ///
    /// # Safety
    /// Either `data` is a pointer from `Box::<T>::into_raw`, where `T` is the type `vtable` records
    /// (see [`ProtocolVTable::from_raw_parts`]), and the task behind it is live. The result owns it:
    /// nothing else uses or frees `data` while the result lives. A pair from `into_raw_parts`
    /// meets this, once.
    ///
    /// Or `data` is null, for an empty pointer that owns nothing, as 0.2.0's could be: dropping it
    /// runs nothing, and [`footprint_estimate`](Self::footprint_estimate) returns 0. It is never
    /// polled, woken, checkpointed, restored into, or put in a task row
    /// ([`Resident::Live`](crate::Resident::Live)): [`poll`](Self::poll),
    /// [`on_wake`](Self::on_wake), [`checkpoint`](Self::checkpoint) and the use of
    /// [`raw_data`](Self::raw_data) require a non-null `data`.
    pub unsafe fn from_raw_parts(data: *mut (), vtable: &'static ProtocolVTable) -> Self {
        Self { data, vtable }
    }

    /// The task pointer and its vtable, without freeing the task: the caller owns it now. Give
    /// both back to [`from_raw_parts`](Self::from_raw_parts) to own it again; a task never given
    /// back is leaked.
    pub fn into_raw_parts(self) -> (*mut (), &'static ProtocolVTable) {
        let this = ManuallyDrop::new(self);
        (this.data, this.vtable)
    }
}

impl Drop for ProtocolTaskPtr {
    fn drop(&mut self) {
        // An empty pointer owns nothing.
        if self.data.is_null() {
            return;
        }
        // SAFETY: `data` is not null, so it came from Box::into_raw (from_box), the vtable's
        // restore, or a caller of `from_raw_parts` who promised the same: a live task of the
        // vtable's own type, owned by `self`, freed only here (`into_raw_parts` gives it up
        // without dropping `self`).
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
