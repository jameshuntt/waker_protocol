use std::any::TypeId;

/// A message for one task, delivered by the scheduler before the task's next poll.
///
/// # Safety
/// `target_type` returns the type `dispatch` reads `target` as. The scheduler calls
/// `dispatch` only with a live, exclusively held task of exactly that type.
pub unsafe trait Envelope: Send + 'static {
    /// A label for the message.
    fn kind(&self) -> &'static str;

    /// The task type this message is for.
    fn target_type(&self) -> TypeId;

    /// Deliver the message.
    ///
    /// # Safety
    /// `target` is a live, exclusively held task of the type `target_type` names.
    unsafe fn dispatch(self: Box<Self>, target: *mut ()) -> Result<(), String>;
}

/// A boxed message, as the scheduler queues it.
pub type EnvelopePacket = Box<dyn Envelope>;

/// A message `M` for a task `T`, delivered by a plain function.
pub struct TypedEnvelope<T, M> {
    kind: &'static str,
    msg: M,
    deliver: fn(&mut T, M) -> Result<(), String>,
}

impl<T, M> TypedEnvelope<T, M> {
    /// A message labelled `kind`, handed to `deliver` with the task.
    pub fn new(
        kind: &'static str,
        msg: M,
        deliver: fn(&mut T, M) -> Result<(), String>,
    ) -> Self {
        Self { kind, msg, deliver }
    }
}

// SAFETY: `target_type` is `T`, and `dispatch` reads `target` only as `T`.
unsafe impl<T: 'static, M: Send + 'static> Envelope for TypedEnvelope<T, M> {
    fn kind(&self) -> &'static str {
        self.kind
    }

    fn target_type(&self) -> TypeId {
        TypeId::of::<T>()
    }

    unsafe fn dispatch(self: Box<Self>, target: *mut ()) -> Result<(), String> {
        let Self { msg, deliver, .. } = *self;
        // SAFETY: the caller passes a live, exclusively held `T` (trait contract).
        let task = unsafe { &mut *(target as *mut T) };
        deliver(task, msg)
    }
}

/// Box a [`TypedEnvelope`].
pub fn typed_envelope<T: 'static, M: Send + 'static>(
    kind: &'static str,
    msg: M,
    deliver: fn(&mut T, M) -> Result<(), String>,
) -> EnvelopePacket {
    Box::new(TypedEnvelope::<T, M>::new(kind, msg, deliver))
}
