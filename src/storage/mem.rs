//! [`MemStorage`]: an in-memory [`Storage`] with selectable `fsync`-failure
//! behaviour, for the deterministic simulator.
//!
//! It keeps two copies of the state: `staged`, which every `persist_*` call
//! mutates (the page cache), and `durable`, which only a *successful* sync
//! advances (the disk). [`MemStorage::restart`] models a crash: `staged` is
//! reset to `durable`, so a following [`Storage::load`] returns exactly what
//! reached the disk.

use std::path::PathBuf;

use super::{Error, PersistentState, Storage};
use crate::core::{LogEntry, LogIndex, NodeId, Term};

/// What a simulated `fsync` does once it is triggered (`AGENTS.md` §8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncFault {
    /// The sync errors and the not-yet-durable writes are lost (dropped dirty
    /// pages); every *later* sync then returns `Ok` although nothing more ever
    /// reaches the disk. This is the ext4 `data=ordered` worst case and the
    /// reason `AGENTS.md` §8 rule 1 makes a failed `fsync` fatal.
    DropWritesThenLie,
    /// The sync errors, but the pending writes actually survived (best case).
    RetainWrites,
}

/// In-memory durable storage for one simulated Raft server.
#[derive(Clone, Debug)]
pub struct MemStorage {
    durable: PersistentState,
    staged: PersistentState,
    fail_on_call: Option<u64>,
    fault: SyncFault,
    calls: u64,
    lying: bool,
}

impl Default for MemStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemStorage {
    /// Fresh storage: nothing persisted, no faults armed.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            durable: PersistentState::fresh(),
            staged: PersistentState::fresh(),
            fail_on_call: None,
            fault: SyncFault::RetainWrites,
            calls: 0,
            lying: false,
        }
    }

    /// Storage that already holds `state` on disk, as after a restart.
    #[must_use]
    pub fn with_state(state: PersistentState) -> Self {
        Self {
            durable: state.clone(),
            staged: state,
            ..Self::new()
        }
    }

    /// Arms a `fsync` fault: the `nth` (1-based) `persist_*` call's sync fails
    /// in the given way. Chainable.
    #[must_use]
    pub const fn fail_sync_on_call(mut self, nth: u64, fault: SyncFault) -> Self {
        self.fail_on_call = Some(nth);
        self.fault = fault;
        self
    }

    /// Models a crash and reboot: everything not proven durable is gone, and
    /// any armed / triggered fault is cleared.
    pub fn restart(&mut self) {
        self.staged = self.durable.clone();
        self.calls = 0;
        self.lying = false;
        self.fail_on_call = None;
    }

    /// The state that has actually reached the (simulated) disk.
    #[must_use]
    pub const fn durable(&self) -> &PersistentState {
        &self.durable
    }

    /// Whether a `DropWritesThenLie` fault has fired: syncs now return `Ok`
    /// but nothing further is persisted.
    #[must_use]
    pub const fn is_lying(&self) -> bool {
        self.lying
    }

    /// Advances `durable` to `staged`, or applies the armed fault.
    fn sync(&mut self) -> Result<(), Error> {
        self.calls += 1;
        if self.lying {
            // The page cache reports success; the disk never moves again.
            return Ok(());
        }
        if self.fail_on_call == Some(self.calls) {
            match self.fault {
                SyncFault::DropWritesThenLie => {
                    self.staged = self.durable.clone();
                    self.lying = true;
                }
                SyncFault::RetainWrites => {
                    self.durable = self.staged.clone();
                }
            }
            return Err(Error::Sync {
                path: PathBuf::from("<mem>"),
                source: std::io::Error::other("simulated fsync failure"),
            });
        }
        self.durable = self.staged.clone();
        Ok(())
    }
}

impl Storage for MemStorage {
    fn load(&mut self) -> Result<PersistentState, Error> {
        Ok(self.durable.clone())
    }

    fn persist_metadata(
        &mut self,
        current_term: Term,
        voted_for: Option<NodeId>,
    ) -> Result<(), Error> {
        self.staged.current_term = current_term;
        self.staged.voted_for = voted_for;
        self.sync()
    }

    fn persist_log(&mut self, from_index: LogIndex, entries: &[LogEntry]) -> Result<(), Error> {
        let keep = usize::try_from(from_index.get().saturating_sub(1))
            .unwrap_or(usize::MAX)
            .min(self.staged.entries.len());
        self.staged.entries.truncate(keep);
        self.staged.entries.extend_from_slice(entries);
        self.sync()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{MemStorage, SyncFault};
    use crate::core::{LogEntry, LogIndex, NodeId, Term};
    use crate::storage::{Error, PersistentState, Storage};

    fn entry(term: u64, cmd: &'static [u8]) -> LogEntry {
        LogEntry {
            term: Term::new(term),
            command: Bytes::from_static(cmd),
        }
    }

    #[track_caller]
    fn ok<T>(result: Result<T, Error>) -> T {
        match result {
            Ok(value) => value,
            Err(err) => unreachable!("expected Ok, got {err}"),
        }
    }

    #[track_caller]
    fn fatal_err(result: Result<(), Error>) {
        match result {
            Ok(()) => unreachable!("expected a fatal error, got Ok"),
            Err(err) => assert!(err.is_fatal(), "expected a fatal error, got {err}"),
        }
    }

    #[track_caller]
    fn reloaded(store: &MemStorage) -> PersistentState {
        let mut copy = store.clone();
        copy.restart();
        ok(copy.load())
    }

    #[test]
    fn round_trips_metadata_and_log() {
        let mut store = MemStorage::new();
        ok(store.persist_metadata(Term::new(3), Some(NodeId::new(2))));
        ok(store.persist_log(LogIndex::new(1), &[entry(1, b"a"), entry(3, b"b")]));

        let state = reloaded(&store);
        assert_eq!(state.current_term, Term::new(3));
        assert_eq!(state.voted_for, Some(NodeId::new(2)));
        assert_eq!(state.entries, vec![entry(1, b"a"), entry(3, b"b")]);
    }

    #[test]
    fn persist_log_truncates_then_appends() {
        let mut store = MemStorage::new();
        ok(store.persist_log(
            LogIndex::new(1),
            &[entry(1, b"a"), entry(1, b"b"), entry(1, b"c")],
        ));
        // Splice: keep index 1, replace from index 2.
        ok(store.persist_log(LogIndex::new(2), &[entry(2, b"x")]));

        assert_eq!(
            reloaded(&store).entries,
            vec![entry(1, b"a"), entry(2, b"x")],
        );
    }

    #[test]
    fn dropped_write_is_lost_and_later_syncs_lie() {
        // The 2nd persist call's sync drops its writes and then lies.
        let mut store = MemStorage::new().fail_sync_on_call(2, SyncFault::DropWritesThenLie);
        ok(store.persist_metadata(Term::new(1), None)); // call 1: ok

        fatal_err(store.persist_metadata(Term::new(2), Some(NodeId::new(1)))); // call 2

        // A later sync "succeeds" but nothing new is persisted.
        ok(store.persist_log(LogIndex::new(1), &[entry(2, b"z")]));
        assert!(store.is_lying());

        let state = reloaded(&store);
        assert_eq!(state.current_term, Term::new(1)); // the term-2 write is gone
        assert_eq!(state.voted_for, None);
        assert!(state.entries.is_empty()); // and so is the post-fault append
    }

    #[test]
    fn retained_write_survives_its_failed_sync() {
        let mut store = MemStorage::new().fail_sync_on_call(1, SyncFault::RetainWrites);
        fatal_err(store.persist_metadata(Term::new(5), None));

        assert_eq!(reloaded(&store).current_term, Term::new(5));
    }
}
