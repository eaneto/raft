//! The application state machine that Raft's committed log is applied to.
//!
//! The core decides *what* is committed and *in what order*; it hands the
//! driver an [`Effect::ApplyToStateMachine`](crate::core::Effect::ApplyToStateMachine)
//! for each entry, one at a time in strictly increasing index order and never
//! past `commitIndex`. The driver forwards that to a [`StateMachine`].
//!
//! For log compaction the driver also asks the state machine to serialize its
//! whole state ([`StateMachine::snapshot`]) and, on a restart or after
//! receiving a leader's snapshot, to replace its state from such bytes
//! ([`StateMachine::restore`]).

use bytes::Bytes;

use crate::core::LogIndex;

/// A deterministic application state machine.
///
/// [`StateMachine::apply`] is called exactly once per committed entry, in
/// strictly increasing `index` order, and must be deterministic: two servers
/// that apply the same commands in the same order must reach the same state.
/// This is what lets Raft guarantee that no two servers ever apply a different
/// entry at the same index (State Machine Safety).
///
/// [`StateMachine::snapshot`] and [`StateMachine::restore`] round-trip that
/// state through opaque bytes so the log before some applied index can be
/// discarded. `restore(snapshot(...))` must reproduce a state that behaves
/// identically under further `apply` calls.
pub trait StateMachine: Send {
    /// Applies the committed entry at `index`. `command` is the opaque bytes
    /// the client proposed.
    fn apply(&mut self, index: LogIndex, command: &Bytes);

    /// Serializes the entire current state. The driver persists the result as
    /// a snapshot and then has the core compact the log up to the last applied
    /// index. Raft never inspects these bytes.
    fn snapshot(&self) -> Bytes;

    /// Replaces the entire state with one previously produced by
    /// [`StateMachine::snapshot`]. Called once at startup when a snapshot was
    /// recovered, and again whenever the node installs a snapshot from the
    /// leader. The driver only ever passes bytes a `snapshot` call produced,
    /// round-tripped through checksummed storage.
    fn restore(&mut self, snapshot: &Bytes);
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

    fn snapshot(&self) -> Bytes {
        // `(LogIndex, Bytes)` both implement `serde`, so the applied log
        // serializes directly. `bincode` cannot fail on an owned `Vec` of
        // plain data.
        let encoded: Vec<(u64, Vec<u8>)> = self
            .applied
            .iter()
            .map(|(index, command)| (index.get(), command.to_vec()))
            .collect();
        Bytes::from(bincode::serialize(&encoded).unwrap_or_default())
    }

    fn restore(&mut self, snapshot: &Bytes) {
        // A malformed blob is a programming error (storage checksums the
        // bytes), so fall back to empty rather than propagate.
        let decoded: Vec<(u64, Vec<u8>)> = bincode::deserialize(snapshot).unwrap_or_default();
        self.applied = decoded
            .into_iter()
            .map(|(index, command)| (LogIndex::new(index), Bytes::from(command)))
            .collect();
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{RecordingStateMachine, StateMachine};
    use crate::core::LogIndex;

    #[test]
    fn snapshot_and_restore_round_trip_the_applied_log() {
        let mut sm = RecordingStateMachine::new();
        sm.apply(LogIndex::new(1), &Bytes::from_static(b"set x=1"));
        sm.apply(LogIndex::new(2), &Bytes::from_static(b"set y=2"));
        sm.apply(LogIndex::new(3), &Bytes::from_static(b""));

        let snapshot = sm.snapshot();

        let mut restored = RecordingStateMachine::new();
        restored.apply(LogIndex::new(9), &Bytes::from_static(b"stale"));
        restored.restore(&snapshot);

        assert_eq!(restored.applied(), sm.applied());
    }

    #[test]
    fn restore_from_garbage_yields_empty_state() {
        let mut sm = RecordingStateMachine::new();
        sm.apply(LogIndex::new(1), &Bytes::from_static(b"x"));
        sm.restore(&Bytes::from_static(b"not bincode"));
        assert!(sm.applied().is_empty());
    }
}
