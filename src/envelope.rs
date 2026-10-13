pub trait Envelope: Send + 'static {
    fn kind(&self) -> &'static str;
    fn dispatch(self: Box<Self>, target: *mut ()) -> Result<(), String>;
}

pub type EnvelopePacket = Box<dyn Envelope>;

pub struct TypedEnvelope<T, M> {
    kind: &'static str,
    msg: M,
    deliver: fn(&mut T, M) -> Result<(), String>,
}

impl<T, M> TypedEnvelope<T, M> {
    pub fn new(
        kind: &'static str,
        msg: M,
        deliver: fn(&mut T, M) -> Result<(), String>,
    ) -> Self {
        Self { kind, msg, deliver }
    }
}

impl<T: 'static, M: Send + 'static> Envelope for TypedEnvelope<T, M> {
    fn kind(&self) -> &'static str {
        self.kind
    }

    fn dispatch(self: Box<Self>, target: *mut ()) -> Result<(), String> {
        let Self { msg, deliver, .. } = *self;
        let task = unsafe { &mut *(target as *mut T) };
        deliver(task, msg)
    }
}

pub fn typed_envelope<T: 'static, M: Send + 'static>(
    kind: &'static str,
    msg: M,
    deliver: fn(&mut T, M) -> Result<(), String>,
) -> EnvelopePacket {
    Box::new(TypedEnvelope::<T, M>::new(kind, msg, deliver))
}