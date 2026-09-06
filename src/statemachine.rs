//! The application state machine that Raft's committed log is applied to.
//!
//! The core decides *what* is committed and *in what order*; it hands the
//! driver an [`Effect::ApplyToStateMachine`](crate::core::Effect::ApplyToStateMachine)
//! for each entry, one at a time in index order (§9.10). The driver forwards
//! that to a [`StateMachine`].

use bytes::Bytes;

use crate::core::LogIndex;

/// A deterministic application state machine.
///
/// [`StateMachine::apply`] is called exactly once per committed entry, in
/// strictly increasing `index` order, and must be deterministic: two servers
/// that apply the same commands in the same order must reach the same state
/// (State Machine Safety, §9.5).
pub trait StateMachine: Send {
    /// Applies the committed entry at `index`. `command` is the opaque bytes
    /// the client proposed.
    fn apply(&mut self, index: LogIndex, command: &Bytes);
}

/// A [`StateMachine`] that records every `(index, command)` it is asked to
/// apply. Useful for tests and for eyeballing what a node has committed.
#[derive(Clone, Debug, Default)]
pub struct RecordingStateMachine {
    applied: Vec<(LogIndex, Bytes)>,
}

impl RecordingStateMachine {
    /// A fresh recorder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The entries applied so far, in order.
    #[must_use]
    pub fn applied(&self) -> &[(LogIndex, Bytes)] {
        &self.applied
    }
}

impl StateMachine for RecordingStateMachine {
    fn apply(&mut self, index: LogIndex, command: &Bytes) {
        self.applied.push((index, command.clone()));
    }
}
