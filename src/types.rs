#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeResult {
    PollReady,
    PollPending,
    Cancelled,
    Dropped,
}
