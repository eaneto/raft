//! The driver's time source.
//!
//! The core takes a [`LogicalInstant`] on every `step`; it never reads a real
//! clock. The driver turns a monotonic wall clock into a [`LogicalInstant`]
//! via a [`Clock`], so tests can substitute a controllable one.

use std::time::Instant;

use crate::core::LogicalInstant;

/// A source of monotonically non-decreasing [`LogicalInstant`]s.
pub trait Clock: Send {
    /// The current instant. Successive calls never go backwards.
    fn now(&self) -> LogicalInstant;
}

/// A [`Clock`] backed by [`std::time::Instant`], counting milliseconds from
/// the moment it was created.
#[derive(Clone, Debug)]
pub struct MonotonicClock {
    start: Instant,
}

impl MonotonicClock {
    /// Starts the clock now.
    #[must_use]
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for MonotonicClock {
    fn now(&self) -> LogicalInstant {
        // Milliseconds since `start` cannot exceed `u64` for ~584 million
        // years; saturate rather than wrap in the impossible case.
        let millis = u64::try_from(self.start.elapsed().as_millis()).unwrap_or(u64::MAX);
        LogicalInstant::from_millis(millis)
    }
}
