use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub enum WakeTrigger<T> {
    Interval {
        interval: Duration,
        next_poll: Instant,
    },
    Receiver {
        receiver: mpsc::Receiver<T>,
    },
    IntervalAndReceiver {
        interval: Duration,
        next_poll: Instant,
        receiver: mpsc::Receiver<T>,
    },
    Condition {
        check: Box<dyn Fn() -> bool + Send + Sync>,
    },
    Manual,
}

impl<T> WakeTrigger<T> {
    pub fn should_wake(&mut self) -> bool {
        let now = Instant::now();
        match self {
            Self::Interval { interval, next_poll } => {
                if now >= *next_poll {
                    *next_poll = now + *interval;
                    true
                } else {
                    false
                }
            }
            Self::Receiver { receiver } => !receiver.is_empty(),
            Self::IntervalAndReceiver { interval, next_poll, receiver } => {
                let time_ready = if now >= *next_poll {
                    *next_poll = now + *interval;
                    true
                } else {
                    false
                };
                time_ready || !receiver.is_empty()
            }
            Self::Condition { check } => check(),
            Self::Manual => false,
        }
    }
}
