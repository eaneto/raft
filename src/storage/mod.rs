//! Durable storage for the bits of Raft state that must survive a crash:
//! `currentTerm`, `votedFor`, and the replicated log (Figure 2, "Persistent
//! state on all servers").
//!
//! The core ([`crate::core`]) never touches a disk; it returns
//! [`Effect::Persist`](crate::core::Effect::Persist) and
//! [`Effect::PersistLog`](crate::core::Effect::PersistLog) values, and the
//! driver hands those to a [`Storage`] implementation. Two implementations
//! live here:
//!
//! - [`FileStorage`] — the real thing. A checksummed, append-only log file and
//!   two redundant metadata files, each `fsync`ed, following the durability
//!   rules in `AGENTS.md` §8.
//! - [`MemStorage`] — an in-memory implementation with selectable `fsync`
//!   failure behaviour, for the deterministic simulator.
//!
//! ## Durability contract (`AGENTS.md` §8)
//!
//! - A failed `fsync` (file **or** directory) is **fatal**. A write method
//!   returning [`Error::Sync`] means the driver must log at `error` and
//!   terminate the process. It must never retry, and never treat a later
//!   successful `fsync` as evidence the earlier data reached the disk.
//! - Every write method here `fsync`s before it returns, so a driver that
//!   performs effects in order and only sends an RPC reply after the
//!   corresponding [`Storage`] call returns `Ok` satisfies "persist before
//!   reply".
//! - The log is a sequence of length-prefixed, CRC32C-checked records.
//!   Recovery replays records until the first one that is short or fails its
//!   checksum; a torn tail is **expected**, not corruption.
//! - `currentTerm` / `votedFor` are written to two independent files. If one
//!   fails its checksum on load the other is used; if both parse but disagree
//!   the more conservative value wins (higher term, "no vote").

mod crc32c;
mod file;
mod mem;

pub use crc32c::crc32c;
pub use file::FileStorage;
pub use mem::{MemStorage, SyncFault};

use std::fmt;
use std::io;
use std::path::PathBuf;

use crate::core::{LogEntry, LogIndex, NodeId, Term};

/// The persistent state as recovered from disk at startup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PersistentState {
    /// The last durable `currentTerm`; [`Term::ZERO`] for a fresh node.
    pub current_term: Term,
    /// The last durable `votedFor`, if any.
    pub voted_for: Option<NodeId>,
    /// The recovered log entries, index 1 first, torn tail already dropped.
    pub entries: Vec<LogEntry>,
}

impl PersistentState {
    /// The state of a node that has never persisted anything.
    #[must_use]
    pub const fn fresh() -> Self {
        Self {
            current_term: Term::ZERO,
            voted_for: None,
            entries: Vec::new(),
        }
    }
}

/// Durable storage for a single Raft server.
///
/// Every method that writes must make its change durable (`fsync` the file,
/// and the parent directory when a file was created or renamed) before
/// returning `Ok`. See the module docs for the failure contract.
pub trait Storage {
    /// Recovers the persistent state, dropping any torn tail from the log.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Io`] if the backing store cannot be read, or
    /// [`Error::Corrupt`] if the state is present but unusable (e.g. both
    /// metadata copies fail their checksum). On [`Error::Corrupt`] the driver
    /// may discard all Raft state and rejoin the cluster as a fresh follower
    /// (`AGENTS.md` §8 rule 7).
    fn load(&mut self) -> Result<PersistentState, Error>;

    /// Durably records `current_term` and `voted_for`.
    ///
    /// # Errors
    ///
    /// [`Error::Sync`] (fatal — see module docs) or [`Error::Io`].
    fn persist_metadata(
        &mut self,
        current_term: Term,
        voted_for: Option<NodeId>,
    ) -> Result<(), Error>;

    /// Makes the log equal to `keep the first from_index - 1 entries, then
    /// these` — the on-disk counterpart of
    /// [`Effect::PersistLog`](crate::core::Effect::PersistLog). `from_index` is
    /// 1-based and must be `>= 1` and `<= len + 1`.
    ///
    /// # Errors
    ///
    /// [`Error::Sync`] (fatal — see module docs) or [`Error::Io`].
    fn persist_log(&mut self, from_index: LogIndex, entries: &[LogEntry]) -> Result<(), Error>;
}

/// A storage failure.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// An `fsync` / `fdatasync` on a file or directory returned an error.
    ///
    /// **Fatal.** Per `AGENTS.md` §8 the driver must log this at `error` and
    /// terminate; it must never retry or trust a later successful sync.
    Sync {
        /// The file or directory whose sync failed.
        path: PathBuf,
        /// The underlying OS error.
        source: io::Error,
    },
    /// A read, write, open, rename, or `set_len` syscall failed.
    Io {
        /// The path being operated on.
        path: PathBuf,
        /// The underlying OS error.
        source: io::Error,
    },
    /// Persistent state is present but unusable: both metadata copies failed
    /// their checksum, or a file is malformed in a way recovery cannot treat
    /// as a torn tail. The node may discard all Raft state and rejoin as a
    /// fresh follower (`AGENTS.md` §8 rule 7).
    Corrupt {
        /// What was wrong.
        detail: String,
    },
    /// A single log record's payload does not fit the on-disk 4 GiB frame.
    /// A Raft command this large is a programming error upstream, not a disk
    /// fault.
    RecordTooLarge {
        /// The oversized payload length.
        bytes: usize,
    },
}

impl Error {
    /// Whether this error is the fatal `fsync`-failure case: the caller must
    /// terminate rather than continue.
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::Sync { .. })
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sync { path, source } => {
                write!(f, "fsync failed on {} (fatal): {source}", path.display())
            }
            Self::Io { path, source } => write!(f, "io error on {}: {source}", path.display()),
            Self::Corrupt { detail } => write!(f, "persistent state is corrupt: {detail}"),
            Self::RecordTooLarge { bytes } => {
                write!(
                    f,
                    "log record of {bytes} bytes exceeds the 4 GiB frame limit"
                )
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sync { source, .. } | Self::Io { source, .. } => Some(source),
            Self::Corrupt { .. } | Self::RecordTooLarge { .. } => None,
        }
    }
}
