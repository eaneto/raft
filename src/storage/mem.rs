//! [`MemStorage`]: an in-memory [`Storage`] with selectable `fsync`-failure
//! behaviour, for the deterministic simulator.
//!
//! It keeps two copies of the state: `staged`, which every `persist_*` call
//! mutates (the page cache), and `durable`, which only a *successful* sync
//! advances (the disk). [`MemStorage::restart`] models a crash: `staged` is
//! reset to `durable`, so a following [`Storage::load`] returns exactly what
//! reached the disk.

use std::path::PathBuf;

use bytes::Bytes;

use super::{Error, PersistentState, Snapshot, SnapshotMeta, Storage};
use crate::core::{LogEntry, LogIndex, NodeId, Term};

/// What a simulated `fsync` does once it is triggered.
///
/// These model the worst-case `fsync`-failure behaviours Linux can exhibit,
/// which is why the storage contract treats a failed `fsync` as fatal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncFault {
    /// The sync errors and the not-yet-durable writes are lost (dropped dirty
    /// pages); every *later* sync then returns `Ok` although nothing more ever
    /// reaches the disk. This is the ext4 `data=ordered` worst case: it is what
    /// makes a failed `fsync` impossible to recover from, and therefore fatal.
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
        let base = staged_base(&self.staged);
        let keep = usize::try_from(from_index.get().saturating_sub(base + 1))
            .unwrap_or(usize::MAX)
            .min(self.staged.entries.len());
        self.staged.entries.truncate(keep);
        self.staged.entries.extend_from_slice(entries);
        self.sync()
    }

    fn persist_snapshot(&mut self, meta: SnapshotMeta, data: &[u8]) -> Result<(), Error> {
        // Drop the covered prefix of the staged log. `entries[0]` is at
        // `old_base + 1`; keep only the entries past `last_included_index`.
        let old_base = staged_base(&self.staged);
        let drop = usize::try_from(meta.last_included_index.get().saturating_sub(old_base))
            .unwrap_or(usize::MAX)
            .min(self.staged.entries.len());
        self.staged.entries.drain(..drop);
        self.staged.snapshot = Some(Snapshot {
            meta,
            data: Bytes::copy_from_slice(data),
        });
        self.sync()
    }
}

/// The global index of the entry just before `state.entries[0]`.
fn staged_base(state: &PersistentState) -> u64 {
    state
        .snapshot
        .as_ref()
        .map_or(0, |snap| snap.meta.last_included_index.get())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{MemStorage, SyncFault};
    use crate::core::{LogEntry, LogIndex, NodeId, Term};
    use crate::storage::{Error, PersistentState, SnapshotMeta, Storage};

    fn snap_meta(index: u64, term: u64) -> SnapshotMeta {
        SnapshotMeta {
            last_included_index: LogIndex::new(index),
            last_included_term: Term::new(term),
        }
    }

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
    fn snapshot_round_trips_and_drops_the_covered_prefix() {
        let mut store = MemStorage::new();
        ok(store.persist_log(
            LogIndex::new(1),
            &[
                entry(1, b"a"),
                entry(1, b"b"),
                entry(2, b"c"),
                entry(2, b"d"),
            ],
        ));
        ok(store.persist_snapshot(snap_meta(2, 1), b"snap-bytes"));

        let state = reloaded(&store);
        let Some(snapshot) = state.snapshot else {
            unreachable!("snapshot recovered");
        };
        assert_eq!(snapshot.meta, snap_meta(2, 1));
        assert_eq!(snapshot.data.as_ref(), b"snap-bytes");
        assert_eq!(state.entries, vec![entry(2, b"c"), entry(2, b"d")]);
    }

    #[test]
    fn persist_log_after_a_snapshot_is_relative_to_the_base() {
        let mut store = MemStorage::new();
        ok(store.persist_log(
            LogIndex::new(1),
            &[entry(1, b"a"), entry(1, b"b"), entry(1, b"c")],
        ));
        ok(store.persist_snapshot(snap_meta(2, 1), b"s"));

        // Splice at global index 3: keep entry 3, replace it.
        ok(store.persist_log(LogIndex::new(3), &[entry(4, b"x"), entry(4, b"y")]));

        assert_eq!(
            reloaded(&store).entries,
            vec![entry(4, b"x"), entry(4, b"y")],
        );
    }

    #[test]
    fn a_snapshot_ahead_of_the_whole_log_clears_it() {
        let mut store = MemStorage::new();
        ok(store.persist_log(LogIndex::new(1), &[entry(1, b"a"), entry(1, b"b")]));
        ok(store.persist_snapshot(snap_meta(9, 3), b"far"));

        let state = reloaded(&store);
        assert_eq!(state.snapshot.map(|s| s.meta), Some(snap_meta(9, 3)),);
        assert!(state.entries.is_empty());
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
