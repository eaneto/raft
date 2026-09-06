//! Core domain types for the Raft state machine.
//!
//! These newtypes replace bare integers for the three identifiers that run
//! through the paper (Ongaro & Ousterhout, Figure 2): the term number, the log
//! index, and the server id. Wrapping them keeps the type checker honest — a
//! [`Term`] cannot be passed where a [`LogIndex`] is expected — and gives each
//! concept one place to state its invariants.
//!
//! All three are `Copy` and totally ordered. [`NodeId`] is `Ord` on purpose:
//! the driver iterates peers in sorted order so that behaviour never depends on
//! hash-map iteration order, which keeps runs deterministic and replayable.
//!
//! Constructing and comparing these types performs no IO, reads no clock,
//! spawns no threads, and draws no randomness.
//!
//! On top of the identifiers sit [`LogEntry`] and [`Log`], the replicated log
//! and its 1-based indexing. [`RaftNode::step`] is the core's whole public
//! surface: it takes an [`Input`] plus the current [`LogicalInstant`] and
//! returns the [`Effect`]s the driver must carry out. Nothing in here performs
//! IO, reads a real clock, spawns a thread, or draws randomness.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use bytes::Bytes;

mod message;

pub use message::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply, Message,
    RequestVoteArgs, RequestVoteReply,
};

/// A Raft term: a logical clock that increases monotonically over the life of
/// the cluster.
///
/// Terms are totally ordered and start at [`Term::ZERO`]. A node's current term
/// must never decrease, so this type offers [`Term::next`] but no way to go
/// backwards.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Term(u64);

impl Term {
    /// The term before any election has happened. A freshly initialised node is
    /// at term zero.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw term number.
    #[must_use]
    pub const fn new(term: u64) -> Self {
        Self(term)
    }

    /// The raw term number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next term, used when a follower or candidate starts an election.
    ///
    /// # Panics
    ///
    /// Panics on `u64` overflow. This cannot occur in practice: at one election
    /// per nanosecond, reaching `u64::MAX` would take roughly 585 years.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A position in the replicated log.
///
/// The log is 1-based, matching the paper. [`LogIndex::ZERO`] is the sentinel
/// "before the first entry": it is the `prevLogIndex` an `AppendEntries` carries
/// for an empty log, and the `lastApplied` / `commitIndex` of a node that has
/// applied nothing.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct LogIndex(u64);

impl LogIndex {
    /// The position before the first log entry.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw index.
    #[must_use]
    pub const fn new(index: u64) -> Self {
        Self(index)
    }

    /// The raw index.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next index.
    ///
    /// # Panics
    ///
    /// Panics on `u64` overflow, which cannot occur in practice (see
    /// [`Term::next`]).
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// The previous index, saturating at [`LogIndex::ZERO`] so that stepping
    /// back from an empty log stays at the sentinel.
    #[must_use]
    pub const fn prev(self) -> Self {
        Self(self.0.saturating_sub(1))
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The identifier of a server in the cluster.
///
/// Ordered so that peers can be iterated deterministically (sorted), never in
/// `HashMap` order — see the module docs.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct NodeId(u64);

impl NodeId {
    /// Wraps a raw server id.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// The raw server id.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The set of servers that vote in the current configuration.
///
/// A membership change appends a [`LogEntryKind::Config`] entry carrying the
/// new set; every server then uses the latest configuration present in its
/// log, even before that entry is committed (thesis §4.1). Backed by a
/// [`BTreeSet`] so iteration order never influences behaviour.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClusterConfig {
    voters: BTreeSet<NodeId>,
}

impl ClusterConfig {
    /// A configuration with exactly `voters`.
    #[must_use]
    pub fn new(voters: impl IntoIterator<Item = NodeId>) -> Self {
        Self {
            voters: voters.into_iter().collect(),
        }
    }

    /// The voting servers, in sorted order.
    #[must_use]
    pub const fn voters(&self) -> &BTreeSet<NodeId> {
        &self.voters
    }

    /// Whether `id` is a voter.
    #[must_use]
    pub fn contains(&self, id: NodeId) -> bool {
        self.voters.contains(&id)
    }

    /// The number of voters.
    #[must_use]
    pub fn len(&self) -> usize {
        self.voters.len()
    }

    /// Whether there are no voters.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.voters.is_empty()
    }

    /// A copy with `id` added as a voter.
    #[must_use]
    pub fn with_added(&self, id: NodeId) -> Self {
        let mut voters = self.voters.clone();
        voters.insert(id);
        Self { voters }
    }

    /// A copy with `id` removed.
    #[must_use]
    pub fn without(&self, id: NodeId) -> Self {
        let mut voters = self.voters.clone();
        voters.remove(&id);
        Self { voters }
    }
}

/// What a [`LogEntry`] carries.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LogEntryKind {
    /// An opaque client command for the application state machine. Raft never
    /// inspects these bytes.
    Command(Bytes),
    /// A cluster configuration — a membership change.
    Config(ClusterConfig),
}

/// One entry in the replicated log: a payload together with the [`Term`] of the
/// leader that first created it.
///
/// An entry does not carry its own index — the index is its 1-based position in
/// the [`Log`]. After log compaction the index of the first surviving entry is
/// tracked by the log, not by the entry.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
    /// The term of the leader that first appended this entry.
    pub term: Term,
    /// The entry's payload.
    pub kind: LogEntryKind,
}

impl LogEntry {
    /// A command entry for `command` at `term`.
    #[must_use]
    pub fn command(term: Term, command: impl Into<Bytes>) -> Self {
        Self {
            term,
            kind: LogEntryKind::Command(command.into()),
        }
    }

    /// A configuration entry for `config` at `term`.
    #[must_use]
    pub const fn config(term: Term, config: ClusterConfig) -> Self {
        Self {
            term,
            kind: LogEntryKind::Config(config),
        }
    }

    /// The command bytes, or `None` for a configuration entry.
    #[must_use]
    pub const fn command_bytes(&self) -> Option<&Bytes> {
        match &self.kind {
            LogEntryKind::Command(command) => Some(command),
            LogEntryKind::Config(_) => None,
        }
    }

    /// The configuration, or `None` for a command entry.
    #[must_use]
    pub const fn as_config(&self) -> Option<&ClusterConfig> {
        match &self.kind {
            LogEntryKind::Config(config) => Some(config),
            LogEntryKind::Command(_) => None,
        }
    }
}

/// The replicated log.
///
/// Indices are **1-based**, matching the paper: the first entry is at
/// `LogIndex::new(1)`, and [`LogIndex::ZERO`] means "before the first entry"
/// (the `prevLogIndex` of an `AppendEntries` that carries the whole log, and
/// the starting `commitIndex` / `lastApplied`).
///
/// After a snapshot, the entries up to and including some index are dropped and
/// replaced by that snapshot. The log then keeps a **compaction base** — the
/// index and term of the last entry the snapshot covers — and its in-memory
/// [`Vec`] holds only the entries *after* it. All the index-to-position
/// arithmetic, base included, lives here so the rest of the core reads like
/// Figure 2 and never has to think about the offset.
///
/// This is an in-memory view only; durability is the storage layer's job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Log {
    /// Index of the last entry covered by the most recent snapshot, or
    /// [`LogIndex::ZERO`] when the log has never been compacted. The first
    /// in-memory entry sits at `snapshot_last_index + 1`.
    snapshot_last_index: LogIndex,
    /// Term of the entry at `snapshot_last_index`; [`Term::ZERO`] when nothing
    /// has been compacted. Answers the `AppendEntries` consistency check when a
    /// leader's `prevLogIndex` lands exactly on the snapshot boundary.
    snapshot_last_term: Term,
    /// The entries after the compaction base, index `snapshot_last_index + 1`
    /// first.
    entries: Vec<LogEntry>,
}

impl Default for Log {
    fn default() -> Self {
        Self {
            snapshot_last_index: LogIndex::ZERO,
            snapshot_last_term: Term::ZERO,
            entries: Vec::new(),
        }
    }
}

impl Log {
    /// An empty, never-compacted log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A never-compacted log holding `entries`, index 1 first. Used by the
    /// driver to restore a log recovered from storage.
    #[must_use]
    pub const fn from_entries(entries: Vec<LogEntry>) -> Self {
        Self {
            snapshot_last_index: LogIndex::ZERO,
            snapshot_last_term: Term::ZERO,
            entries,
        }
    }

    /// A log restored behind a snapshot: `entries` (index
    /// `snapshot_last_index + 1` first) sit on top of a snapshot whose last
    /// covered entry is `(snapshot_last_index, snapshot_last_term)`.
    #[must_use]
    pub const fn from_snapshot(
        snapshot_last_index: LogIndex,
        snapshot_last_term: Term,
        entries: Vec<LogEntry>,
    ) -> Self {
        Self {
            snapshot_last_index,
            snapshot_last_term,
            entries,
        }
    }

    /// Whether the log represents no entries at all — never compacted and with
    /// nothing appended.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.snapshot_last_index.get() == 0
    }

    /// The highest index the log logically holds, counting entries folded into
    /// the snapshot. [`LogIndex::ZERO`] for a log that represents nothing.
    #[must_use]
    pub const fn len(&self) -> u64 {
        // usize -> u64 cannot lose bits on any platform Raft runs on.
        self.snapshot_last_index.get() + self.entries.len() as u64
    }

    /// The index of the compaction base: the last entry the most recent
    /// snapshot covers, or [`LogIndex::ZERO`] if the log has never been
    /// compacted.
    #[must_use]
    pub const fn snapshot_last_index(&self) -> LogIndex {
        self.snapshot_last_index
    }

    /// The term of the entry at [`Log::snapshot_last_index`].
    #[must_use]
    pub const fn snapshot_last_term(&self) -> Term {
        self.snapshot_last_term
    }

    /// The index of the last entry, or [`LogIndex::ZERO`] if the log is empty.
    #[must_use]
    pub const fn last_index(&self) -> LogIndex {
        LogIndex::new(self.len())
    }

    /// The term of the last entry: the last in-memory entry's, or the
    /// compaction base's when nothing sits above the snapshot, or
    /// [`Term::ZERO`] for an empty log.
    #[must_use]
    pub fn last_term(&self) -> Term {
        self.entries
            .last()
            .map_or(self.snapshot_last_term, |entry| entry.term)
    }

    /// The entry at `index`, or `None` if `index` is [`LogIndex::ZERO`], has
    /// been compacted into the snapshot, or is past the end of the log.
    #[must_use]
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        let position = index
            .get()
            .checked_sub(self.snapshot_last_index.get() + 1)?;
        let position = usize::try_from(position).ok()?;
        self.entries.get(position)
    }

    /// The term of the entry at `index`, or `None` if there is no such entry
    /// the log can still answer for.
    ///
    /// Used for the `AppendEntries` consistency check: a follower accepts new
    /// entries only when `term_at(prevLogIndex)` equals the leader's
    /// `prevLogTerm`. `term_at(snapshot_last_index)` returns the compaction
    /// base's term; a `prevLogIndex` strictly inside the snapshot returns
    /// `None`, which fails the check and makes the leader fall back to
    /// `InstallSnapshot`.
    #[must_use]
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index.get() != 0 && index == self.snapshot_last_index {
            return Some(self.snapshot_last_term);
        }
        self.get(index).map(|entry| entry.term)
    }

    /// The in-memory entries from `index` to the end of the log, as a slice.
    ///
    /// `index` is a global 1-based index: anything at or below
    /// `snapshot_last_index + 1` yields every in-memory entry, and an `index`
    /// past the end yields an empty slice. Used to build the `entries` an
    /// `AppendEntries` carries and the tail an [`Effect::PersistLog`] makes
    /// durable.
    #[must_use]
    pub fn entries_from(&self, index: LogIndex) -> &[LogEntry] {
        let start = index
            .get()
            .saturating_sub(self.snapshot_last_index.get() + 1);
        let start = usize::try_from(start).unwrap_or(usize::MAX);
        self.entries.get(start..).unwrap_or(&[])
    }

    /// Appends one entry to the end of the log.
    ///
    /// This is the only way the log grows. A leader only ever appends to its
    /// own log; it never rewrites existing entries (Leader Append-Only).
    pub fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    /// Drops every entry after `index`, keeping entries `1..=index`.
    ///
    /// `index` must be at or above the compaction base; the snapshot itself is
    /// never truncated. `truncate_after(snapshot_last_index)` drops every
    /// in-memory entry. An `index` at or past the current end is a no-op.
    pub fn truncate_after(&mut self, index: LogIndex) {
        debug_assert!(
            index.get() >= self.snapshot_last_index.get(),
            "truncate_after({}) is below the compaction base {}",
            index,
            self.snapshot_last_index,
        );
        let keep = index.get().saturating_sub(self.snapshot_last_index.get());
        if let Ok(keep) = usize::try_from(keep) {
            self.entries.truncate(keep);
        }
    }

    /// Compacts the log up to and including `up_to_index`, whose term is
    /// `up_to_term`: those entries are now represented by a snapshot, so drop
    /// them and move the compaction base forward.
    ///
    /// A no-op if `up_to_index` is at or below the current base. If it is past
    /// the last in-memory entry (installing a snapshot that is ahead of our
    /// whole log) every in-memory entry is dropped and the base jumps to
    /// `up_to_index`.
    pub fn compact(&mut self, up_to_index: LogIndex, up_to_term: Term) {
        if up_to_index.get() <= self.snapshot_last_index.get() {
            return;
        }
        let drop = up_to_index.get() - self.snapshot_last_index.get();
        let drop = usize::try_from(drop)
            .unwrap_or(usize::MAX)
            .min(self.entries.len());
        self.entries.drain(..drop);
        self.snapshot_last_index = up_to_index;
        self.snapshot_last_term = up_to_term;
    }
}

/// The role a server plays in the current term (paper §5.1), together with the
/// volatile state that only makes sense in that role.
///
/// A server is always in exactly one of these. It starts as [`Role::Follower`],
/// becomes a [`Role::Candidate`] when its election timer fires, and becomes
/// [`Role::Leader`] on winning an election. Keeping the per-role bookkeeping
/// inside the variant means a follower simply has no `match_index` to get wrong.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Role {
    /// Passive: responds to leaders and candidates, and starts an election if
    /// it stops hearing from a leader.
    Follower,
    /// Actively soliciting votes for its own term.
    Candidate {
        /// Servers that have granted a vote this term, including this one. A
        /// [`BTreeSet`] so the count is order-independent.
        votes_granted: BTreeSet<NodeId>,
    },
    /// Won the election for the current term; replicates the log and sends
    /// heartbeats. Both maps are keyed by peer id and reinitialised on
    /// election (Figure 2, "Volatile state on leaders"). While a server is
    /// being added they also carry an entry for that non-voting learner, which
    /// [`RaftNode::config`] does not yet list.
    Leader {
        /// For each peer, the index of the next log entry to send it.
        next_index: BTreeMap<NodeId, LogIndex>,
        /// For each peer, the highest log index known to be replicated on it.
        match_index: BTreeMap<NodeId, LogIndex>,
    },
}

/// A single-server membership change (thesis §4): one server added or removed
/// per configuration entry. Joint consensus is deliberately not used.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MembershipChange {
    /// Add `NodeId` as a voter, after it catches up as a non-voting learner.
    AddServer(NodeId),
    /// Remove `NodeId` from the configuration.
    RemoveServer(NodeId),
}

/// The leader's progress on the one membership change it has in flight.
#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingChange {
    change: MembershipChange,
    phase: ChangePhase,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ChangePhase {
    /// An `AddServer` is replicating to the new server before its configuration
    /// entry is appended. The round completes when the server's `matchIndex`
    /// reaches `round_target`; `ticks_left` heartbeats remain before the add is
    /// abandoned.
    ///
    /// DEVIATION: thesis §4.2.1 bounds catch-up by replication rounds each
    /// shorter than an election timeout. The pure core has no clock, so it
    /// counts heartbeat ticks instead.
    CatchingUp {
        round_target: LogIndex,
        ticks_left: u32,
    },
    /// The configuration entry is in the log at `config_index`, waiting to
    /// commit. On commit the change is done (and if it removed this leader, it
    /// steps down — thesis §4.2.2).
    Committing { config_index: LogIndex },
}

/// Heartbeat ticks a new server has to catch up in before an `AddServer` is
/// abandoned. See [`ChangePhase::CatchingUp`].
const CATCH_UP_TICKS: u32 = 40;

/// Recovers the single-server change that turns `before` into `after`, or
/// `None` if they differ by more than one server (which single-server changes
/// never produce).
fn diff_configs(before: &ClusterConfig, after: &ClusterConfig) -> Option<MembershipChange> {
    let added: Vec<NodeId> = after
        .voters()
        .difference(before.voters())
        .copied()
        .collect();
    let removed: Vec<NodeId> = before
        .voters()
        .difference(after.voters())
        .copied()
        .collect();
    match (added.as_slice(), removed.as_slice()) {
        ([id], []) => Some(MembershipChange::AddServer(*id)),
        ([], [id]) => Some(MembershipChange::RemoveServer(*id)),
        _ => None,
    }
}

/// A point on the driver's logical timeline, in milliseconds from an arbitrary
/// epoch.
///
/// The core never reads a real clock: the driver stamps every [`RaftNode::step`]
/// call with the current instant, and the core only ever *compares* instants.
/// The unit and epoch are the driver's business.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalInstant(u64);

impl LogicalInstant {
    /// The zero instant, for tests and for a freshly started driver.
    pub const START: Self = Self(0);

    /// Builds an instant from a millisecond count.
    #[must_use]
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    /// The millisecond count.
    #[must_use]
    pub const fn as_millis(self) -> u64 {
        self.0
    }
}

/// Something that drives the core forward: a peer message, a client request, or
/// a timer firing.
///
/// The driver builds these from real IO and hands them to [`RaftNode::step`]
/// one at a time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    /// A [`Message`] received from `from`. The sender is carried here rather
    /// than in the message so the Figure 2 reply types need no sender field.
    Deliver {
        /// The peer the message was received from.
        from: NodeId,
        /// The message itself.
        message: Message,
    },
    /// A client asks the cluster to append and eventually commit a command.
    /// Only a leader acts on it; a follower or candidate currently drops it
    /// (redirecting the client to the leader is future work).
    Propose {
        /// The opaque command bytes.
        command: Bytes,
    },
    /// The election timer expired: no leader contact within the randomized
    /// window the driver chose. A follower or candidate starts a new election.
    ElectionTimeout,
    /// The periodic heartbeat interval elapsed. Acted on only while
    /// [`Role::Leader`].
    HeartbeatTick,
    /// The driver has durably stored a snapshot received from the leader and
    /// restored the state machine from it (the response to a run of
    /// [`Effect::StoreSnapshotChunk`] ending with `done`). The core adopts the
    /// snapshot: it compacts its log to `last_included_index`, advances
    /// `commitIndex` / `lastApplied` there, and acknowledges the leader.
    SnapshotInstalled {
        /// The last log index the snapshot covers.
        last_included_index: LogIndex,
        /// Term of the entry at `last_included_index`.
        last_included_term: Term,
        /// The configuration in force at `last_included_index`.
        config: ClusterConfig,
    },
    /// The driver has durably stored a snapshot the state machine produced from
    /// its own applied entries; the core may now drop the covered log prefix.
    /// `up_to_index` is always `<= lastApplied`.
    CompactLog {
        /// The last log index the new snapshot covers.
        up_to_index: LogIndex,
    },
    /// An administrator asks the leader to add or remove one server. Ignored by
    /// any other role, and by a leader that already has a change in flight or a
    /// still-uncommitted configuration entry.
    ChangeMembership {
        /// The change to make.
        change: MembershipChange,
    },
}

/// A side effect the core needs the driver to perform.
///
/// The core never acts on the world itself; [`RaftNode::step`] returns these in
/// the order they must happen and the driver executes them. In particular,
/// every [`Effect::Persist`] and [`Effect::PersistLog`] must be durable before
/// the driver sends any [`Effect::SendRpc`] that follows it in the same batch:
/// a node must never tell a peer it granted a vote or stored an entry that a
/// crash could then make it forget (Figure 2, "Updated on stable storage
/// before responding to RPCs").
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Send `message` to peer `to`.
    SendRpc {
        /// The destination peer.
        to: NodeId,
        /// The message to send.
        message: Message,
    },
    /// Flush the persistent state to stable storage before continuing.
    ///
    /// The payload is the paper's "persistent state on all servers" minus the
    /// log; log durability has its own effect, [`Effect::PersistLog`].
    Persist {
        /// The term to persist.
        current_term: Term,
        /// The vote to persist.
        voted_for: Option<NodeId>,
    },
    /// Make the log durable from `from_index` onward before continuing.
    ///
    /// `entries` is the log's contents from `from_index` (1-based) to its end,
    /// in order. A leader append sets `from_index` to one past the previous
    /// last index, so `entries` is just the new tail; a follower splicing in a
    /// leader's entries may pass a lower `from_index` to overwrite conflicting
    /// entries. `from_index` is always `>= LogIndex::new(1)`.
    ///
    /// Like [`Effect::Persist`], this must be durable before any
    /// [`Effect::SendRpc`] later in the same batch, so a crash cannot make the
    /// node forget an entry it already acknowledged.
    PersistLog {
        /// 1-based index of the first entry in `entries`.
        from_index: LogIndex,
        /// The log from `from_index` to the end, in order.
        entries: Vec<LogEntry>,
    },
    /// Apply the committed entry at `index` to the application state machine.
    /// Emitted one entry at a time, in strictly increasing index order and
    /// never past `commitIndex`.
    ApplyToStateMachine {
        /// Index of the entry to apply.
        index: LogIndex,
        /// The entry's command bytes.
        command: Bytes,
    },
    /// Restart the election timer with a fresh randomized duration (the driver
    /// owns the range and the seeded RNG).
    ResetElectionTimer,
    /// Send the current snapshot to `to` as an `InstallSnapshot` chunk stream,
    /// because `to`'s next entry has been compacted into it.
    ///
    /// The pure core does not hold the snapshot bytes — the driver does — so it
    /// only names the recipient. The driver fills in `last_included_index`,
    /// its term, and the chunks from whatever it last persisted.
    SendSnapshot {
        /// The follower to bring up to date.
        to: NodeId,
    },
    /// Hand a received `InstallSnapshot` chunk to the driver to accumulate.
    ///
    /// The driver appends `data` at `offset` to its reassembly buffer; on the
    /// `done` chunk it persists the whole snapshot, restores the state machine
    /// from it, and feeds back [`Input::SnapshotInstalled`]. Nothing is
    /// acknowledged to the leader until that callback, so a crash mid-transfer
    /// cannot make the follower claim a snapshot it does not have.
    StoreSnapshotChunk {
        /// The last log index the snapshot covers.
        last_included_index: LogIndex,
        /// Term of the entry at `last_included_index`.
        last_included_term: Term,
        /// The configuration in force at `last_included_index`.
        config: ClusterConfig,
        /// Byte offset of `data` within the complete snapshot.
        offset: u64,
        /// Snapshot bytes beginning at `offset`.
        data: Vec<u8>,
        /// Whether `data` reaches the end of the snapshot.
        done: bool,
    },
    /// The active cluster configuration changed (a new one was appended, or one
    /// committed). The driver reconciles its transport connections to match,
    /// and a UI can observe membership.
    MembershipChanged {
        /// The configuration now in force.
        config: ClusterConfig,
    },
}

/// A single Raft server: the pure state machine.
///
/// All behaviour flows through [`RaftNode::step`]. The struct holds only the
/// state from Figure 2; the driver holds the clock, RNG, storage, transport,
/// and timers.
#[derive(Clone, Debug)]
pub struct RaftNode {
    id: NodeId,
    /// The active cluster configuration: the latest [`LogEntryKind::Config`]
    /// entry in the log, or `base_config` if the in-memory log holds none.
    /// Recomputed by [`RaftNode::recompute_config`] after any log change.
    config: ClusterConfig,
    /// The configuration in force before the first in-memory log entry — from
    /// the recovered snapshot, or the bootstrap set. The fallback for
    /// `config` once every `Config` entry has been compacted away.
    base_config: ClusterConfig,
    /// The other voters, sorted (a cache derived from `config`), so behaviour
    /// never depends on set iteration order.
    peers: Vec<NodeId>,

    // Persistent state (Figure 2) — the driver must have these durable before
    // replying to an RPC that depends on them.
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Log,

    // Volatile state (Figure 2).
    commit_index: LogIndex,
    last_applied: LogIndex,
    role: Role,
    /// The leader this node currently recognises for `current_term`, learned
    /// from an accepted `AppendEntries` or `InstallSnapshot`. Used to address
    /// the reply after the driver installs a received snapshot, and (later) to
    /// redirect clients.
    leader_id: Option<NodeId>,
    /// While this node leads and has a membership change under way, its
    /// progress on it. At most one change is in flight at a time.
    pending_change: Option<PendingChange>,
}

impl RaftNode {
    /// Creates a fresh follower: term 0, no vote, empty log, nothing committed.
    ///
    /// `peers` is the set of *other* servers; together with `id` they form the
    /// bootstrap configuration.
    #[must_use]
    pub fn new(id: NodeId, peers: impl IntoIterator<Item = NodeId>) -> Self {
        let mut voters: BTreeSet<NodeId> = peers.into_iter().collect();
        voters.insert(id);
        let config = ClusterConfig { voters };
        let mut node = Self {
            id,
            base_config: config.clone(),
            peers: Vec::new(),
            config,
            current_term: Term::ZERO,
            voted_for: None,
            log: Log::new(),
            commit_index: LogIndex::ZERO,
            last_applied: LogIndex::ZERO,
            role: Role::Follower,
            leader_id: None,
            pending_change: None,
        };
        node.recompute_config();
        node
    }

    /// Creates a server that is **not** in any bootstrap configuration: a
    /// brand-new node joining an existing cluster. It stays passive — it never
    /// starts an election — until a configuration entry that names it arrives
    /// from the leader (thesis §4.2.1: a new server joins as a non-voting
    /// learner).
    #[must_use]
    pub fn new_learner(id: NodeId) -> Self {
        let mut node = Self::new(id, std::iter::empty());
        node.base_config = ClusterConfig::new(std::iter::empty());
        node.recompute_config();
        node
    }

    /// Rebuilds a server from persistent state recovered at startup (Figure 2:
    /// `currentTerm`, `votedFor`, `log`), optionally sitting behind a snapshot.
    ///
    /// It comes up as a [`Role::Follower`]. `snapshot` is `Some((index, term))`
    /// when a snapshot was recovered: `entries` are then the log *after* that
    /// index, and `commitIndex` / `lastApplied` start there because the driver
    /// has already restored the state machine from the snapshot. Without a
    /// snapshot both start at zero and the restarted node re-learns its commit
    /// point from the leader and replays the committed prefix.
    ///
    /// `peers` is the bootstrap configuration; the active configuration is then
    /// recomputed from any `Config` entries in `entries` (a snapshot that
    /// carries its own configuration is threaded in with the storage snapshot
    /// format).
    #[must_use]
    pub fn from_state(
        id: NodeId,
        peers: impl IntoIterator<Item = NodeId>,
        current_term: Term,
        voted_for: Option<NodeId>,
        snapshot: Option<(LogIndex, Term, ClusterConfig)>,
        entries: Vec<LogEntry>,
    ) -> Self {
        let mut node = Self::new(id, peers);
        node.current_term = current_term;
        node.voted_for = voted_for;
        node.log = match snapshot {
            Some((index, term, config)) => {
                node.commit_index = index;
                node.last_applied = index;
                node.base_config = config;
                Log::from_snapshot(index, term, entries)
            }
            None => Log::from_entries(entries),
        };
        node.recompute_config();
        node
    }

    /// Re-derives the active configuration: the latest [`LogEntryKind::Config`]
    /// entry still in the in-memory log, or `base_config` if there is none. The
    /// sorted peer cache follows.
    fn recompute_config(&mut self) {
        self.config = self
            .log
            .entries_from(LogIndex::new(1))
            .iter()
            .rev()
            .find_map(LogEntry::as_config)
            .cloned()
            .unwrap_or_else(|| self.base_config.clone());
        self.peers = self
            .config
            .voters()
            .iter()
            .copied()
            .filter(|&voter| voter != self.id)
            .collect();
    }

    /// Advances the state machine by one input and returns the effects the
    /// driver must perform, in order.
    ///
    /// `now` is the driver's current [`LogicalInstant`]. This timer model keeps
    /// election deadlines in the driver, so the core does not consult `now`
    /// yet; it stays in the signature for the features that will (pre-vote, the
    /// leader-disruption fix).
    #[must_use]
    pub fn step(&mut self, input: Input, now: LogicalInstant) -> Vec<Effect> {
        let _ = now;
        match input {
            Input::Deliver { from, message } => match message {
                Message::RequestVote(args) => self.handle_request_vote(args),
                Message::RequestVoteReply(reply) => self.handle_request_vote_reply(from, reply),
                Message::AppendEntries(args) => self.handle_append_entries(&args),
                Message::AppendEntriesReply(reply) => self.handle_append_entries_reply(from, reply),
                Message::InstallSnapshot(args) => self.handle_install_snapshot(&args),
                Message::InstallSnapshotReply(reply) => {
                    self.handle_install_snapshot_reply(from, reply)
                }
            },
            Input::ElectionTimeout => self.handle_election_timeout(),
            Input::HeartbeatTick => self.handle_heartbeat_tick(),
            Input::Propose { command } => self.handle_propose(command),
            Input::SnapshotInstalled {
                last_included_index,
                last_included_term,
                config,
            } => self.handle_snapshot_installed(last_included_index, last_included_term, config),
            Input::CompactLog { up_to_index } => self.handle_compact_log(up_to_index),
            Input::ChangeMembership { change } => self.handle_change_membership(change),
        }
    }

    /// Handles an incoming `RequestVote` (Figure 2).
    ///
    /// Steps down first if the candidate's term is newer, rejects outright if
    /// it is older, then grants the vote only when this server has not already
    /// voted for someone else this term and the candidate's log is at least as
    /// up-to-date as ours (§5.4.1). Any change to `currentTerm` / `votedFor` is
    /// emitted as [`Effect::Persist`] before the reply (persist-before-reply).
    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> Vec<Effect> {
        let before = (self.current_term, self.voted_for);
        let mut effects = Vec::new();

        if args.term > self.current_term {
            self.become_follower(args.term);
        }

        if args.term < self.current_term {
            effects.push(self.request_vote_reply(args.candidate_id, false));
            return effects;
        }

        let voted_elsewhere = matches!(self.voted_for, Some(id) if id != args.candidate_id);
        let granted = !voted_elsewhere
            && self.candidate_log_is_up_to_date(args.last_log_index, args.last_log_term);
        if granted {
            self.voted_for = Some(args.candidate_id);
        }

        self.persist_if_changed(before, &mut effects);
        if granted {
            // Granting a vote counts as hearing from a viable leader-to-be.
            effects.push(Effect::ResetElectionTimer);
        }
        effects.push(self.request_vote_reply(args.candidate_id, granted));
        effects
    }

    /// Handles a `RequestVoteReply` while we are (or were) a candidate.
    ///
    /// A reply carrying a newer term makes us a follower. Otherwise, a granted
    /// vote for our current term is tallied, and reaching a quorum promotes us
    /// to leader.
    fn handle_request_vote_reply(&mut self, from: NodeId, reply: RequestVoteReply) -> Vec<Effect> {
        let before = (self.current_term, self.voted_for);
        let mut effects = Vec::new();

        if reply.term > self.current_term {
            self.become_follower(reply.term);
            self.persist_if_changed(before, &mut effects);
            return effects;
        }

        if reply.term < self.current_term {
            return effects;
        }

        if reply.vote_granted {
            if let Role::Candidate { votes_granted } = &mut self.role {
                votes_granted.insert(from);
            }
            effects.extend(self.promote_if_quorum());
        }
        effects
    }

    /// Handles an incoming `AppendEntries` (Figure 2), including the receiver
    /// rules 3–5: splice conflicting entries, append new ones, and advance
    /// `commitIndex` from the leader's.
    ///
    /// Reception from the current leader (term `>=` ours) resets the election
    /// timer whether or not the consistency check passes — diverging logs do
    /// not make the leader illegitimate.
    fn handle_append_entries(&mut self, args: &AppendEntriesArgs) -> Vec<Effect> {
        let before = (self.current_term, self.voted_for);
        let mut effects = Vec::new();

        // Receiver rule 1: reply false to a leader from an older term, and do
        // not treat it as leader contact.
        if args.term < self.current_term {
            effects.push(self.append_entries_reply(args.leader_id, false, LogIndex::ZERO));
            return effects;
        }

        // A newer term, or the same term while we still think we are a
        // candidate: the sender is the leader for this term, so step down.
        if args.term > self.current_term || self.is_candidate() {
            self.become_follower(args.term);
        }
        self.leader_id = Some(args.leader_id);
        self.persist_if_changed(before, &mut effects);

        // Receiver rule 2: the log must contain `prev_log_index` with a
        // matching term, or this is the empty-log base case.
        let consistent = args.prev_log_index == LogIndex::ZERO
            || self.log.term_at(args.prev_log_index) == Some(args.prev_log_term);
        if !consistent {
            effects.push(Effect::ResetElectionTimer);
            effects.push(self.append_entries_reply(args.leader_id, false, LogIndex::ZERO));
            return effects;
        }

        // The last index this RPC vouches for: everything up to and including
        // it now matches the leader (paper's "index of last new entry").
        // usize -> u64 cannot lose bits on any platform Raft runs on.
        let last_covered = LogIndex::new(args.prev_log_index.get() + args.entries.len() as u64);

        // Receiver rules 3 & 4: splice in the entries that differ.
        if let Some(from_index) = self.splice_entries(args.prev_log_index, &args.entries) {
            // A spliced-in or truncated-away `Config` entry changes membership.
            let old_config = self.config.clone();
            self.recompute_config();
            if self.config != old_config {
                effects.push(Effect::MembershipChanged {
                    config: self.config.clone(),
                });
            }
            effects.push(Effect::PersistLog {
                from_index,
                entries: self.log.entries_from(from_index).to_vec(),
            });
        }

        // Receiver rule 5: adopt the leader's commit point, bounded by what
        // this RPC actually covered.
        if args.leader_commit > self.commit_index {
            self.commit_index = args.leader_commit.min(last_covered);
            self.apply_committed(&mut effects);
        }

        effects.push(Effect::ResetElectionTimer);
        effects.push(self.append_entries_reply(args.leader_id, true, last_covered));
        effects
    }

    /// Applies `AppendEntries` receiver rules 3 & 4. Walks `entries` against
    /// the log from just after `prev_log_index`; at the first index whose term
    /// disagrees (or that the log lacks) it truncates the tail and appends the
    /// remainder. Entries already present with a matching term are left as they
    /// are, so a delayed or duplicated RPC never rewinds the log (Log
    /// Matching, Leader Append-Only on the leader's behalf).
    ///
    /// Returns `Some(from_index)` — the 1-based index of the first entry
    /// written — when the log changed, or `None` when every entry was already
    /// present.
    fn splice_entries(
        &mut self,
        prev_log_index: LogIndex,
        entries: &[LogEntry],
    ) -> Option<LogIndex> {
        let mut index = prev_log_index;
        for (offset, entry) in entries.iter().enumerate() {
            index = index.next();
            if self.log.term_at(index) == Some(entry.term) {
                continue;
            }
            // Conflict, or past our end: drop anything from `index` on and
            // take the leader's version of this suffix.
            self.log.truncate_after(index.prev());
            for new in &entries[offset..] {
                self.log.append(new.clone());
            }
            return Some(index);
        }
        None
    }

    /// Emits [`Effect::ApplyToStateMachine`] for every **command** entry from
    /// `last_applied + 1` through `commit_index`, one at a time in index order,
    /// advancing `last_applied` as it goes so it never overtakes `commit_index`.
    /// Configuration entries advance `last_applied` too but are not handed to
    /// the application state machine — they took effect the moment they were
    /// appended. Stops if an index is somehow missing rather than inventing a
    /// command.
    fn apply_committed(&mut self, effects: &mut Vec<Effect>) {
        while self.last_applied < self.commit_index {
            let next = self.last_applied.next();
            let Some(entry) = self.log.get(next) else {
                break;
            };
            let command = entry.command_bytes().cloned();
            self.last_applied = next;
            if let Some(command) = command {
                effects.push(Effect::ApplyToStateMachine {
                    index: next,
                    command,
                });
            }
        }
    }

    /// Handles an `AppendEntriesReply` from `from` (Figure 2, leader side).
    ///
    /// A reply from a newer term steps us down. Otherwise, while we are still
    /// the leader for the reply's term: `success` moves `match_index[from]` /
    /// `next_index[from]` forward and may advance `commit_index` (§5.4.2);
    /// failure decrements `next_index[from]` and retries straight away.
    fn handle_append_entries_reply(
        &mut self,
        from: NodeId,
        reply: AppendEntriesReply,
    ) -> Vec<Effect> {
        let before = (self.current_term, self.voted_for);
        let mut effects = Vec::new();

        if reply.term > self.current_term {
            self.become_follower(reply.term);
            self.persist_if_changed(before, &mut effects);
            return effects;
        }

        // A stale reply, or one meant for a leadership we no longer hold.
        if reply.term < self.current_term || !self.is_leader() {
            return effects;
        }

        if reply.success {
            self.record_match(from, reply.match_index);
            self.maybe_advance_commit_index(&mut effects);
            self.advance_catch_up(from, &mut effects);
        } else {
            self.back_off(from);
            effects.push(self.replicate_to(from));
        }
        effects
    }

    /// Handles an incoming `InstallSnapshot` chunk (Figure 13, follower side).
    ///
    /// Term rules match `AppendEntries`: a chunk from an older term is rejected
    /// and is not leader contact; a newer term (or the same term while still a
    /// candidate) steps this node down. The chunk itself is handed to the
    /// driver via [`Effect::StoreSnapshotChunk`]; the leader is not
    /// acknowledged until the driver has the whole snapshot durable and calls
    /// back [`Input::SnapshotInstalled`], so a crash mid-transfer cannot make
    /// this node claim a snapshot it does not have. A snapshot this node has
    /// already committed past is acknowledged at once without reinstalling.
    fn handle_install_snapshot(&mut self, args: &InstallSnapshotArgs) -> Vec<Effect> {
        let before = (self.current_term, self.voted_for);
        let mut effects = Vec::new();

        if args.term < self.current_term {
            effects.push(self.install_snapshot_reply(args.leader_id, args.last_included_index));
            return effects;
        }

        if args.term > self.current_term || self.is_candidate() {
            self.become_follower(args.term);
        }
        self.leader_id = Some(args.leader_id);
        self.persist_if_changed(before, &mut effects);

        effects.push(Effect::ResetElectionTimer);

        // Already have this prefix committed: keep our log and ack immediately.
        if args.last_included_index <= self.commit_index {
            effects.push(self.install_snapshot_reply(args.leader_id, args.last_included_index));
            return effects;
        }

        effects.push(Effect::StoreSnapshotChunk {
            last_included_index: args.last_included_index,
            last_included_term: args.last_included_term,
            config: args.config.clone(),
            offset: args.offset,
            data: args.data.clone(),
            done: args.done,
        });
        effects
    }

    /// Handles an `InstallSnapshotReply` from `from` (Figure 13, leader side).
    ///
    /// A newer term steps us down. Otherwise, while we still lead for the
    /// reply's term, the follower now holds the snapshot through
    /// `last_included_index`: record that as its `matchIndex`, which also pulls
    /// its `nextIndex` past the compacted region so the next round is a normal
    /// `AppendEntries`, and re-check whether that advances `commitIndex`.
    fn handle_install_snapshot_reply(
        &mut self,
        from: NodeId,
        reply: InstallSnapshotReply,
    ) -> Vec<Effect> {
        let before = (self.current_term, self.voted_for);
        let mut effects = Vec::new();

        if reply.term > self.current_term {
            self.become_follower(reply.term);
            self.persist_if_changed(before, &mut effects);
            return effects;
        }

        if reply.term < self.current_term || !self.is_leader() {
            return effects;
        }

        self.record_match(from, reply.last_included_index);
        self.maybe_advance_commit_index(&mut effects);
        effects
    }

    /// Handles [`Input::SnapshotInstalled`]: the driver has a snapshot from the
    /// leader durable and has restored the state machine from it. Adopt it —
    /// compact the log to `last_included_index`, move `commitIndex` /
    /// `lastApplied` up to it — and acknowledge the leader (persist-before-reply:
    /// this is the first point at which the snapshot is on stable storage).
    fn handle_snapshot_installed(
        &mut self,
        last_included_index: LogIndex,
        last_included_term: Term,
        config: ClusterConfig,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();

        self.log.compact(last_included_index, last_included_term);
        // The snapshot's configuration becomes the floor once its entries are
        // gone from the log.
        self.base_config = config;
        self.recompute_config();
        self.commit_index = self.commit_index.max(last_included_index);
        if self.last_applied < last_included_index {
            self.last_applied = last_included_index;
        }
        // Normally a no-op: only runs if a concurrent AppendEntries had already
        // pushed commitIndex past the snapshot.
        self.apply_committed(&mut effects);

        if let Some(leader) = self.leader_id {
            effects.push(self.install_snapshot_reply(leader, last_included_index));
        }
        effects
    }

    /// Handles [`Input::CompactLog`]: the driver has durably snapshotted the
    /// state machine through `up_to_index`, so drop that prefix of the log.
    /// Bounded by `lastApplied` — never compact an entry that has not been
    /// applied — and the term comes from the log, which still holds it.
    fn handle_compact_log(&mut self, up_to_index: LogIndex) -> Vec<Effect> {
        if up_to_index > self.log.snapshot_last_index()
            && up_to_index <= self.last_applied
            && let Some(term) = self.log.term_at(up_to_index)
        {
            self.log.compact(up_to_index, term);
            self.recompute_config();
        }
        Vec::new()
    }

    /// Handles [`Input::ChangeMembership`] (thesis §4). A no-op unless this
    /// server leads, has no change already in flight, and the latest
    /// configuration entry is committed. `RemoveServer` appends the new
    /// configuration at once; `AddServer` first replicates to the new server
    /// as a non-voting learner (see [`ChangePhase::CatchingUp`]).
    fn handle_change_membership(&mut self, change: MembershipChange) -> Vec<Effect> {
        if !self.is_leader()
            || self.pending_change.is_some()
            || self.latest_config_index() > self.commit_index
        {
            return Vec::new();
        }

        match change {
            MembershipChange::AddServer(id) => {
                if id == self.id || self.config.contains(id) {
                    return Vec::new();
                }
                let next = self.log.last_index().next();
                if let Role::Leader {
                    next_index,
                    match_index,
                } = &mut self.role
                {
                    next_index.insert(id, next);
                    match_index.insert(id, LogIndex::ZERO);
                }
                self.pending_change = Some(PendingChange {
                    change,
                    phase: ChangePhase::CatchingUp {
                        round_target: self.log.last_index(),
                        ticks_left: CATCH_UP_TICKS,
                    },
                });
                vec![self.replicate_to(id)]
            }
            MembershipChange::RemoveServer(id) => {
                if !self.config.contains(id) {
                    return Vec::new();
                }
                let new_config = self.config.without(id);
                self.append_config(change, new_config)
            }
        }
    }

    /// The index of the last configuration entry in the log, or the compaction
    /// base (whose configuration is `base_config`) if there is none in memory.
    fn latest_config_index(&self) -> LogIndex {
        let base = self.log.snapshot_last_index().get();
        match self
            .log
            .entries_from(LogIndex::new(1))
            .iter()
            .enumerate()
            .rev()
            .find(|(_, entry)| entry.as_config().is_some())
        {
            Some((pos, _)) => LogIndex::new(base + pos as u64 + 1),
            None => self.log.snapshot_last_index(),
        }
    }

    /// Appends `new_config` as a configuration entry, replicates it, and marks
    /// the change as waiting to commit. Shared by `RemoveServer` and by the
    /// promotion of a caught-up `AddServer` learner.
    fn append_config(
        &mut self,
        change: MembershipChange,
        new_config: ClusterConfig,
    ) -> Vec<Effect> {
        let from_index = self.log.last_index().next();
        self.log
            .append(LogEntry::config(self.current_term, new_config.clone()));
        let config_index = self.log.last_index();
        self.recompute_config();
        self.sync_replication_maps();
        self.pending_change = Some(PendingChange {
            change,
            phase: ChangePhase::Committing { config_index },
        });

        let mut effects = vec![
            Effect::PersistLog {
                from_index,
                entries: self.log.entries_from(from_index).to_vec(),
            },
            Effect::MembershipChanged { config: new_config },
        ];
        effects.extend(self.replicate_to_peers());
        self.maybe_advance_commit_index(&mut effects);
        effects
    }

    /// Advances a pending `AddServer`'s catch-up when the new server acks. When
    /// its `matchIndex` reaches the current log end, it is promoted (its
    /// configuration entry is appended); if a round completes but the log has
    /// grown, another round starts.
    fn advance_catch_up(&mut self, from: NodeId, effects: &mut Vec<Effect>) {
        let Some(PendingChange {
            change: MembershipChange::AddServer(id),
            phase: ChangePhase::CatchingUp { round_target, .. },
        }) = &self.pending_change
        else {
            return;
        };
        let (id, round_target) = (*id, *round_target);
        if from != id {
            return;
        }
        let matched = match &self.role {
            Role::Leader { match_index, .. } => {
                match_index.get(&id).copied().unwrap_or(LogIndex::ZERO)
            }
            _ => return,
        };
        if matched < round_target {
            return;
        }

        if matched >= self.log.last_index() {
            let new_config = self.config.with_added(id);
            effects.extend(self.append_config(MembershipChange::AddServer(id), new_config));
        } else if let Some(PendingChange {
            phase: ChangePhase::CatchingUp { round_target, .. },
            ..
        }) = &mut self.pending_change
        {
            *round_target = self.log.last_index();
        }
    }

    /// Abandons a stuck `AddServer`: drops the pending change and the learner's
    /// replication state.
    fn abort_catch_up(&mut self) {
        if let Some(PendingChange {
            change: MembershipChange::AddServer(id),
            ..
        }) = self.pending_change.take()
            && let Role::Leader {
                next_index,
                match_index,
            } = &mut self.role
        {
            next_index.remove(&id);
            match_index.remove(&id);
        }
    }

    /// On commit of a pending configuration entry: clear the change, and if the
    /// new configuration no longer includes this leader, step it down (thesis
    /// §4.2.2).
    fn check_config_committed(&mut self, effects: &mut Vec<Effect>) {
        let Some(PendingChange {
            change,
            phase: ChangePhase::Committing { config_index },
        }) = &self.pending_change
        else {
            return;
        };
        if *config_index > self.commit_index {
            return;
        }
        let change = *change;
        self.pending_change = None;
        // Now that a removal is durable everywhere it matters, stop replicating
        // to the removed peer.
        if let MembershipChange::RemoveServer(id) = change
            && let Role::Leader {
                next_index,
                match_index,
            } = &mut self.role
        {
            next_index.remove(&id);
            match_index.remove(&id);
        }
        effects.push(Effect::MembershipChanged {
            config: self.config.clone(),
        });
        if !self.config.contains(self.id) && self.is_leader() {
            self.leader_id = None;
            self.role = Role::Follower;
        }
    }

    /// Reconciles the leader's `next_index` / `match_index` with the current
    /// configuration after it changes: drops entries for servers no longer
    /// voting (a removed peer), keeps a promoted learner's, and seeds any new
    /// voter.
    fn sync_replication_maps(&mut self) {
        let mut keep: BTreeSet<NodeId> = self
            .config
            .voters()
            .iter()
            .copied()
            .filter(|&voter| voter != self.id)
            .collect();
        // Keep replicating to a peer being removed until the removal commits,
        // so it learns it is no longer in the cluster (and does not sit there
        // timing out and disrupting elections).
        if let Some(PendingChange {
            change: MembershipChange::RemoveServer(id),
            phase: ChangePhase::Committing { .. },
        }) = &self.pending_change
            && *id != self.id
        {
            keep.insert(*id);
        }
        let next = self.log.last_index().next();
        if let Role::Leader {
            next_index,
            match_index,
        } = &mut self.role
        {
            next_index.retain(|peer, _| keep.contains(peer));
            match_index.retain(|peer, _| keep.contains(peer));
            for &voter in &keep {
                next_index.entry(voter).or_insert(next);
                match_index.entry(voter).or_insert(LogIndex::ZERO);
            }
        }
    }

    /// Records a successful ack: `match_index[peer]` only moves forward (a
    /// reordered older ack cannot pull it back) and `next_index[peer]` follows.
    fn record_match(&mut self, peer: NodeId, matched: LogIndex) {
        if let Role::Leader {
            next_index,
            match_index,
        } = &mut self.role
        {
            if let Some(m) = match_index.get_mut(&peer) {
                *m = matched.max(*m);
            }
            if let Some(n) = next_index.get_mut(&peer) {
                *n = matched.next().max(*n);
            }
        }
    }

    /// Figure 2: on a failed `AppendEntries`, step `next_index` for the peer
    /// back by one (never below 1) so the next attempt probes further back.
    fn back_off(&mut self, peer: NodeId) {
        if let Role::Leader { next_index, .. } = &mut self.role
            && let Some(n) = next_index.get_mut(&peer)
        {
            *n = n.prev().max(LogIndex::new(1));
        }
    }

    /// §5.4.2: advance `commit_index` to the highest index stored on a
    /// majority (this leader included) — but only when that entry is from the
    /// **current** term, so a leader never commits an earlier-term entry by
    /// vote count alone (paper Figure 8). Older entries below it commit
    /// indirectly, via [`RaftNode::apply_committed`]. Emits the resulting
    /// applies.
    fn maybe_advance_commit_index(&mut self, effects: &mut Vec<Effect>) {
        let majority_matched = {
            let Role::Leader { match_index, .. } = &self.role else {
                return;
            };
            // Only voters count: a still-catching-up learner has a
            // `match_index` entry but is not in the configuration.
            let mut matched: Vec<LogIndex> = match_index
                .iter()
                .filter(|(peer, _)| self.config.contains(**peer))
                .map(|(_, index)| *index)
                .collect();
            matched.push(self.log.last_index()); // our own log end
            matched.sort_unstable_by_key(|&index| std::cmp::Reverse(index));
            // Sorted high -> low, position `quorum - 1` is the largest index
            // that at least `quorum` servers hold.
            matched[self.quorum() - 1]
        };

        if majority_matched > self.commit_index
            && self.log.term_at(majority_matched) == Some(self.current_term)
        {
            self.commit_index = majority_matched;
            self.apply_committed(effects);
            self.check_config_committed(effects);
        }
    }

    /// Handles the election timer firing: a follower or candidate starts a new
    /// election for `currentTerm + 1` (paper §5.2).
    ///
    /// Term monotonicity holds by construction — [`RaftNode::become_candidate`]
    /// only ever calls [`Term::next`], and a failed election just leaves us a
    /// candidate at the higher term until the timer fires again.
    fn handle_election_timeout(&mut self) -> Vec<Effect> {
        // A leader never re-elects, and a server absent from its own
        // configuration (a not-yet-added learner, or one just removed) stays
        // passive so it cannot disrupt the cluster it is joining or leaving.
        if self.is_leader() || !self.config.contains(self.id) {
            return Vec::new();
        }

        self.become_candidate();

        let mut effects = Vec::new();
        // currentTerm and votedFor both just changed; make them durable before
        // asking anyone for a vote.
        effects.push(Effect::Persist {
            current_term: self.current_term,
            voted_for: self.voted_for,
        });
        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();
        for &peer in &self.peers {
            effects.push(Effect::SendRpc {
                to: peer,
                message: Message::RequestVote(RequestVoteArgs {
                    term: self.current_term,
                    candidate_id: self.id,
                    last_log_index,
                    last_log_term,
                }),
            });
        }
        effects.push(Effect::ResetElectionTimer);
        // A single-node cluster reaches quorum on its own vote right away.
        effects.extend(self.promote_if_quorum());
        effects
    }

    /// Handles the heartbeat interval elapsing: a leader re-sends
    /// `AppendEntries` to reassert authority (carrying any entries a peer, or a
    /// catching-up learner, has yet to acknowledge); every other role ignores
    /// it. Each tick also spends one of a pending `AddServer`'s catch-up
    /// budget, abandoning it if the new server never keeps up.
    fn handle_heartbeat_tick(&mut self) -> Vec<Effect> {
        if !self.is_leader() {
            return Vec::new();
        }
        if let Some(PendingChange {
            phase: ChangePhase::CatchingUp { ticks_left, .. },
            ..
        }) = &mut self.pending_change
        {
            *ticks_left = ticks_left.saturating_sub(1);
            if *ticks_left == 0 {
                self.abort_catch_up();
            }
        }
        self.replicate_to_peers()
    }

    /// Handles a client proposal (Figure 2, "Clients of Raft").
    ///
    /// Only a leader acts: it appends one entry for `command` at its current
    /// term, makes the new tail durable with [`Effect::PersistLog`], then
    /// sends every peer the entries it is missing. Any other role drops the
    /// proposal — redirecting the client to the leader is future work.
    ///
    /// A majority still has to store the entry before it commits (§5.4.2), so
    /// on a multi-node cluster nothing is applied until the acks come back. A
    /// lone leader is its own majority, so `maybe_advance_commit_index` commits
    /// and applies the entry in this same step.
    fn handle_propose(&mut self, command: Bytes) -> Vec<Effect> {
        if !self.is_leader() {
            return Vec::new();
        }

        let from_index = self.log.last_index().next();
        self.log
            .append(LogEntry::command(self.current_term, command));

        let mut effects = vec![Effect::PersistLog {
            from_index,
            entries: self.log.entries_from(from_index).to_vec(),
        }];
        effects.extend(self.replicate_to_peers());
        self.maybe_advance_commit_index(&mut effects);
        effects
    }

    /// Adopts `term` as `currentTerm` and reverts to [`Role::Follower`].
    ///
    /// Clears `votedFor` only when the term actually advances, so a same-term
    /// step-down (e.g. a candidate conceding to a leader) needs no new fsync.
    /// When the term advances the recognised leader is forgotten too; the
    /// caller sets it again if this step-down was triggered by that leader's
    /// own RPC. Callers pass a `term` that is `>=` the current one.
    fn become_follower(&mut self, term: Term) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
            self.leader_id = None;
        }
        self.pending_change = None;
        self.role = Role::Follower;
    }

    /// Starts a candidacy: bump the term, vote for self, record that one vote.
    fn become_candidate(&mut self) {
        self.current_term = self.current_term.next();
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.pending_change = None;
        let mut votes_granted = BTreeSet::new();
        votes_granted.insert(self.id);
        self.role = Role::Candidate { votes_granted };
    }

    /// Becomes leader for the current term and reinitialises the per-peer
    /// replication bookkeeping (Figure 2).
    fn become_leader(&mut self) {
        let next = self.log.last_index().next();
        let next_index = self.peers.iter().map(|&peer| (peer, next)).collect();
        let match_index = self
            .peers
            .iter()
            .map(|&peer| (peer, LogIndex::ZERO))
            .collect();
        self.leader_id = Some(self.id);
        self.role = Role::Leader {
            next_index,
            match_index,
        };
        self.adopt_pending_config_change();
    }

    /// If the log ends with an uncommitted configuration entry, a previous
    /// leader started a membership change that this one must finish. Rebuild
    /// the pending change and keep replicating to the affected server (a
    /// removed peer or a just-promoted learner) so it learns of the change and
    /// stops disrupting elections.
    fn adopt_pending_config_change(&mut self) {
        let config_index = self.latest_config_index();
        if config_index.get() == 0 || config_index <= self.commit_index {
            return;
        }
        let previous = self.config_before(config_index);
        let Some(change) = diff_configs(&previous, &self.config) else {
            return;
        };
        let affected = match change {
            MembershipChange::AddServer(id) | MembershipChange::RemoveServer(id) => id,
        };
        if affected != self.id {
            let next = self.log.last_index().next();
            if let Role::Leader {
                next_index,
                match_index,
            } = &mut self.role
            {
                next_index.entry(affected).or_insert(next);
                match_index.entry(affected).or_insert(LogIndex::ZERO);
            }
        }
        self.pending_change = Some(PendingChange {
            change,
            phase: ChangePhase::Committing { config_index },
        });
    }

    /// The configuration in force just before `config_index`: the previous
    /// configuration entry in the log, or `base_config` if there is none.
    fn config_before(&self, config_index: LogIndex) -> ClusterConfig {
        let base = self.log.snapshot_last_index().get();
        self.log
            .entries_from(LogIndex::new(1))
            .iter()
            .enumerate()
            .rev()
            .find(|(pos, entry)| {
                entry.as_config().is_some() && base + *pos as u64 + 1 < config_index.get()
            })
            .and_then(|(_, entry)| entry.as_config().cloned())
            .unwrap_or_else(|| self.base_config.clone())
    }

    /// If we are a candidate holding a quorum of votes, become leader and
    /// return the initial `AppendEntries` to every peer — empty while the log
    /// has nothing past their `next_index` — that stop peers from starting
    /// their own elections.
    fn promote_if_quorum(&mut self) -> Vec<Effect> {
        let has_quorum = match &self.role {
            Role::Candidate { votes_granted } => votes_granted.len() >= self.quorum(),
            Role::Follower | Role::Leader { .. } => false,
        };
        if !has_quorum {
            return Vec::new();
        }

        self.become_leader();
        self.replicate_to_peers()
    }

    /// Replicates to every peer: an `AppendEntries` carrying the entries it is
    /// missing according to its `next_index`, or an [`Effect::SendSnapshot`]
    /// when that entry has been compacted away. With every peer caught up this
    /// is a batch of empty heartbeats; after a fresh `Propose` it carries the
    /// new tail. Used on winning an election, on `HeartbeatTick`, and on
    /// `Propose`.
    fn replicate_to_peers(&self) -> Vec<Effect> {
        match &self.role {
            Role::Leader { next_index, .. } => {
                // Voters plus any catching-up learner (which has a `next_index`
                // entry but is not yet in the configuration).
                next_index
                    .keys()
                    .map(|&peer| self.replicate_to(peer))
                    .collect()
            }
            Role::Follower | Role::Candidate { .. } => Vec::new(),
        }
    }

    /// Builds the replication effect for one peer.
    ///
    /// If the peer's `next_index` still points into the log, that is an
    /// `AppendEntries`: the entry just before `next_index` is the §5.3
    /// consistency-check anchor (`prev_log_index` / `prev_log_term`), and
    /// everything from `next_index` to the end of the log rides along as
    /// `entries`. If `next_index` has fallen to or below the compaction base,
    /// the leader no longer has `prev_log_term`, so it sends the snapshot
    /// instead ([`Effect::SendSnapshot`]).
    fn replicate_to(&self, peer: NodeId) -> Effect {
        let next = self.next_index_for(peer);
        if next <= self.log.snapshot_last_index() {
            return Effect::SendSnapshot { to: peer };
        }
        let prev_log_index = next.prev();
        let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(Term::ZERO);
        Effect::SendRpc {
            to: peer,
            message: Message::AppendEntries(AppendEntriesArgs {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries: self.log.entries_from(next).to_vec(),
                leader_commit: self.commit_index,
            }),
        }
    }

    /// This leader's `next_index` for `peer`.
    ///
    /// [`RaftNode::become_leader`] seeds one entry per peer, so on the leader
    /// path the map always contains `peer`. The fallback — one past the last
    /// log index, the value `become_leader` itself uses — keeps the function
    /// total for any off-path caller a future step might add.
    fn next_index_for(&self, peer: NodeId) -> LogIndex {
        match &self.role {
            Role::Leader { next_index, .. } => next_index
                .get(&peer)
                .copied()
                .unwrap_or_else(|| self.log.last_index().next()),
            Role::Follower | Role::Candidate { .. } => self.log.last_index().next(),
        }
    }

    /// The §5.4.1 "up-to-date" test: the candidate's last log entry is newer by
    /// term, or the same term and at least as long.
    fn candidate_log_is_up_to_date(&self, last_log_index: LogIndex, last_log_term: Term) -> bool {
        let our_term = self.log.last_term();
        last_log_term > our_term
            || (last_log_term == our_term && last_log_index >= self.log.last_index())
    }

    /// Pushes an [`Effect::Persist`] iff `currentTerm` or `votedFor` differs
    /// from `before`. Call it before pushing any reply that depends on them.
    fn persist_if_changed(&self, before: (Term, Option<NodeId>), effects: &mut Vec<Effect>) {
        if (self.current_term, self.voted_for) != before {
            effects.push(Effect::Persist {
                current_term: self.current_term,
                voted_for: self.voted_for,
            });
        }
    }

    /// Builds a `RequestVoteReply` effect addressed to `candidate`.
    const fn request_vote_reply(&self, candidate: NodeId, vote_granted: bool) -> Effect {
        Effect::SendRpc {
            to: candidate,
            message: Message::RequestVoteReply(RequestVoteReply {
                term: self.current_term,
                vote_granted,
            }),
        }
    }

    /// Builds an `AppendEntriesReply` effect addressed to `leader`.
    ///
    /// `match_index` is the highest index the follower has stored for this
    /// leader on `success`, and [`LogIndex::ZERO`] otherwise.
    const fn append_entries_reply(
        &self,
        leader: NodeId,
        success: bool,
        match_index: LogIndex,
    ) -> Effect {
        Effect::SendRpc {
            to: leader,
            message: Message::AppendEntriesReply(AppendEntriesReply {
                term: self.current_term,
                success,
                match_index,
            }),
        }
    }

    /// Builds an `InstallSnapshotReply` effect addressed to `leader`, echoing
    /// the `last_included_index` the follower now holds.
    const fn install_snapshot_reply(
        &self,
        leader: NodeId,
        last_included_index: LogIndex,
    ) -> Effect {
        Effect::SendRpc {
            to: leader,
            message: Message::InstallSnapshotReply(InstallSnapshotReply {
                term: self.current_term,
                last_included_index,
            }),
        }
    }

    /// Number of voting servers in the active configuration.
    fn cluster_size(&self) -> usize {
        self.config.len()
    }

    /// Votes needed to win an election or commit an entry: a strict majority of
    /// the active configuration. An empty configuration (a learner) has no
    /// reachable quorum.
    fn quorum(&self) -> usize {
        if self.config.is_empty() {
            return usize::MAX;
        }
        self.cluster_size() / 2 + 1
    }

    /// This server's id.
    #[must_use]
    pub const fn id(&self) -> NodeId {
        self.id
    }

    /// The other voters in the active configuration, sorted.
    #[must_use]
    pub fn peers(&self) -> &[NodeId] {
        &self.peers
    }

    /// The active cluster configuration — the latest one in the log, committed
    /// or not.
    #[must_use]
    pub const fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// The current role and its per-role volatile state.
    #[must_use]
    pub const fn role(&self) -> &Role {
        &self.role
    }

    /// Whether this server is currently a follower.
    #[must_use]
    pub const fn is_follower(&self) -> bool {
        matches!(self.role, Role::Follower)
    }

    /// Whether this server is currently a candidate.
    #[must_use]
    pub const fn is_candidate(&self) -> bool {
        matches!(self.role, Role::Candidate { .. })
    }

    /// Whether this server is currently the leader.
    #[must_use]
    pub const fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader { .. })
    }

    /// The current term.
    #[must_use]
    pub const fn current_term(&self) -> Term {
        self.current_term
    }

    /// The candidate this server has voted for in the current term, if any.
    #[must_use]
    pub const fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// The replicated log.
    #[must_use]
    pub const fn log(&self) -> &Log {
        &self.log
    }

    /// The highest log index known to be committed.
    #[must_use]
    pub const fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    /// The highest log index applied to the application state machine.
    #[must_use]
    pub const fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// The index of the last entry folded into the most recent snapshot, or
    /// [`LogIndex::ZERO`] if the log has never been compacted.
    #[must_use]
    pub const fn snapshot_last_index(&self) -> LogIndex {
        self.log.snapshot_last_index()
    }

    /// The leader this node currently recognises for [`RaftNode::current_term`],
    /// if it has heard from one since the term began.
    #[must_use]
    pub const fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{
        AppendEntriesArgs, AppendEntriesReply, CATCH_UP_TICKS, ClusterConfig, Effect, Input,
        InstallSnapshotArgs, InstallSnapshotReply, Log, LogEntry, LogIndex, LogicalInstant,
        MembershipChange, Message, NodeId, RaftNode, RequestVoteArgs, RequestVoteReply, Role, Term,
    };

    /// The core does not consult the clock under this timer model, so every
    /// test steps at the same instant.
    const NOW: LogicalInstant = LogicalInstant::START;

    fn entry(term: u64) -> LogEntry {
        LogEntry::command(Term::new(term), Bytes::from_static(b"cmd"))
    }

    fn entry_cmd(term: u64, command: &'static [u8]) -> LogEntry {
        LogEntry::command(Term::new(term), Bytes::from_static(command))
    }

    fn node(id: u64, peers: &[u64]) -> RaftNode {
        RaftNode::new(NodeId::new(id), peers.iter().copied().map(NodeId::new))
    }

    fn request_vote(term: u64, candidate: u64, last_log_index: u64, last_log_term: u64) -> Message {
        Message::RequestVote(RequestVoteArgs {
            term: Term::new(term),
            candidate_id: NodeId::new(candidate),
            last_log_index: LogIndex::new(last_log_index),
            last_log_term: Term::new(last_log_term),
        })
    }

    fn vote_reply(term: u64, granted: bool) -> Message {
        Message::RequestVoteReply(RequestVoteReply {
            term: Term::new(term),
            vote_granted: granted,
        })
    }

    fn heartbeat(term: u64, leader: u64, prev_log_index: u64, prev_log_term: u64) -> Message {
        Message::AppendEntries(AppendEntriesArgs {
            term: Term::new(term),
            leader_id: NodeId::new(leader),
            prev_log_index: LogIndex::new(prev_log_index),
            prev_log_term: Term::new(prev_log_term),
            entries: Vec::new(),
            leader_commit: LogIndex::ZERO,
        })
    }

    /// A failed/heartbeat `AppendEntriesReply` effect: `match_index` is zero.
    fn append_reply(to: u64, term: u64, success: bool) -> Effect {
        Effect::SendRpc {
            to: NodeId::new(to),
            message: Message::AppendEntriesReply(AppendEntriesReply {
                term: Term::new(term),
                success,
                match_index: LogIndex::ZERO,
            }),
        }
    }

    /// A successful `AppendEntriesReply` effect carrying `matched`.
    fn ack(to: u64, term: u64, matched: u64) -> Effect {
        Effect::SendRpc {
            to: NodeId::new(to),
            message: Message::AppendEntriesReply(AppendEntriesReply {
                term: Term::new(term),
                success: true,
                match_index: LogIndex::new(matched),
            }),
        }
    }

    /// An incoming `AppendEntriesReply` message.
    fn ae_reply(term: u64, success: bool, matched: u64) -> Message {
        Message::AppendEntriesReply(AppendEntriesReply {
            term: Term::new(term),
            success,
            match_index: LogIndex::new(matched),
        })
    }

    fn append_entries(
        term: u64,
        leader: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> Message {
        Message::AppendEntries(AppendEntriesArgs {
            term: Term::new(term),
            leader_id: NodeId::new(leader),
            prev_log_index: LogIndex::new(prev_log_index),
            prev_log_term: Term::new(prev_log_term),
            entries,
            leader_commit: LogIndex::new(leader_commit),
        })
    }

    fn send(to: u64, message: Message) -> Effect {
        Effect::SendRpc {
            to: NodeId::new(to),
            message,
        }
    }

    fn install_snapshot(
        term: u64,
        leader: u64,
        last_included_index: u64,
        last_included_term: u64,
        done: bool,
    ) -> Message {
        Message::InstallSnapshot(InstallSnapshotArgs {
            term: Term::new(term),
            leader_id: NodeId::new(leader),
            last_included_index: LogIndex::new(last_included_index),
            last_included_term: Term::new(last_included_term),
            config: config(&[1, 2, 3]),
            offset: 0,
            data: b"snap".to_vec(),
            done,
        })
    }

    fn snapshot_reply_msg(term: u64, last_included_index: u64) -> Message {
        Message::InstallSnapshotReply(InstallSnapshotReply {
            term: Term::new(term),
            last_included_index: LogIndex::new(last_included_index),
        })
    }

    fn snapshot_reply(to: u64, term: u64, last_included_index: u64) -> Effect {
        Effect::SendRpc {
            to: NodeId::new(to),
            message: snapshot_reply_msg(term, last_included_index),
        }
    }

    fn deliver(from: u64, message: Message) -> Input {
        Input::Deliver {
            from: NodeId::new(from),
            message,
        }
    }

    fn propose(command: &'static [u8]) -> Input {
        Input::Propose {
            command: Bytes::from_static(command),
        }
    }

    /// Steps the node for its side effects on state, discarding the effects.
    /// Used to arrange a node into a role before the call under test.
    fn drive(n: &mut RaftNode, input: Input) {
        let _ = n.step(input, NOW);
    }

    #[test]
    fn term_starts_at_zero_and_only_advances() {
        assert_eq!(Term::ZERO.get(), 0);
        assert_eq!(Term::ZERO.next(), Term::new(1));
        assert_eq!(Term::new(1).next().next(), Term::new(3));
    }

    #[test]
    fn terms_are_totally_ordered() {
        assert!(Term::ZERO < Term::new(1));
        assert!(Term::new(5) > Term::new(4));

        let mut terms = [Term::new(3), Term::ZERO, Term::new(1)];
        terms.sort();
        assert_eq!(terms, [Term::ZERO, Term::new(1), Term::new(3)]);
    }

    #[test]
    fn log_index_arithmetic_saturates_at_zero() {
        assert_eq!(LogIndex::ZERO.prev(), LogIndex::ZERO);
        assert_eq!(LogIndex::ZERO.next(), LogIndex::new(1));
        assert_eq!(LogIndex::new(1).prev(), LogIndex::ZERO);
        assert_eq!(LogIndex::new(10).next(), LogIndex::new(11));
    }

    #[test]
    fn node_ids_sort_by_value() {
        let mut ids = [NodeId::new(3), NodeId::new(1), NodeId::new(2)];
        ids.sort();
        assert_eq!(ids, [NodeId::new(1), NodeId::new(2), NodeId::new(3)]);
    }

    #[test]
    fn display_is_the_bare_number() {
        assert_eq!(Term::new(7).to_string(), "7");
        assert_eq!(LogIndex::new(7).to_string(), "7");
        assert_eq!(NodeId::new(7).to_string(), "7");
    }

    #[test]
    fn empty_log_reports_zero_sentinels() {
        let log = Log::new();
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
        assert_eq!(log.last_index(), LogIndex::ZERO);
        assert_eq!(log.last_term(), Term::ZERO);
        assert_eq!(log.get(LogIndex::ZERO), None);
        assert_eq!(log.get(LogIndex::new(1)), None);
        assert_eq!(log.term_at(LogIndex::new(1)), None);
    }

    #[test]
    fn append_grows_the_log_and_indexing_is_one_based() {
        let mut log = Log::new();
        log.append(entry(1));
        log.append(entry(2));

        assert_eq!(log.len(), 2);
        assert_eq!(log.last_index(), LogIndex::new(2));
        assert_eq!(log.last_term(), Term::new(2));

        assert_eq!(log.get(LogIndex::ZERO), None);
        assert_eq!(log.get(LogIndex::new(1)), Some(&entry(1)));
        assert_eq!(log.get(LogIndex::new(2)), Some(&entry(2)));
        assert_eq!(log.get(LogIndex::new(3)), None);

        assert_eq!(log.term_at(LogIndex::new(2)), Some(Term::new(2)));
        assert_eq!(log.term_at(LogIndex::new(3)), None);
    }

    #[test]
    fn truncate_after_keeps_the_prefix_up_to_and_including_the_index() {
        let mut log = Log::new();
        log.append(entry(1));
        log.append(entry(2));
        log.append(entry(3));

        log.truncate_after(LogIndex::new(2));
        assert_eq!(log.len(), 2);
        assert_eq!(log.last_index(), LogIndex::new(2));
        assert_eq!(log.get(LogIndex::new(3)), None);
    }

    #[test]
    fn truncate_after_zero_empties_the_log() {
        let mut log = Log::new();
        log.append(entry(1));

        log.truncate_after(LogIndex::ZERO);
        assert!(log.is_empty());
    }

    #[test]
    fn truncate_after_past_the_end_is_a_no_op() {
        let mut log = Log::new();
        log.append(entry(1));

        log.truncate_after(LogIndex::new(9));
        assert_eq!(log.len(), 1);
    }

    #[test]
    fn entries_from_returns_the_tail_slice() {
        let empty: &[LogEntry] = &[];

        let mut log = Log::new();
        assert_eq!(log.entries_from(LogIndex::new(1)), empty);

        log.append(entry(1));
        log.append(entry(2));
        log.append(entry(3));

        assert_eq!(log.entries_from(LogIndex::ZERO).len(), 3);
        assert_eq!(log.entries_from(LogIndex::new(1)).len(), 3);
        assert_eq!(log.entries_from(LogIndex::new(3)), &[entry(3)]);
        assert_eq!(log.entries_from(LogIndex::new(4)), empty);
        assert_eq!(log.entries_from(LogIndex::new(99)), empty);
    }

    #[test]
    fn compact_moves_the_base_and_keeps_the_suffix() {
        let mut log = Log::new();
        log.append(entry(1)); // index 1
        log.append(entry(2)); // index 2
        log.append(entry(3)); // index 3
        log.append(entry(4)); // index 4

        log.compact(LogIndex::new(2), Term::new(2));

        // Indices are unchanged; the first two entries are now the snapshot.
        assert_eq!(log.snapshot_last_index(), LogIndex::new(2));
        assert_eq!(log.snapshot_last_term(), Term::new(2));
        assert_eq!(log.last_index(), LogIndex::new(4));
        assert_eq!(log.len(), 4);
        assert!(!log.is_empty());

        assert_eq!(log.get(LogIndex::new(1)), None); // compacted away
        assert_eq!(log.get(LogIndex::new(2)), None);
        assert_eq!(log.get(LogIndex::new(3)), Some(&entry(3)));
        assert_eq!(log.get(LogIndex::new(4)), Some(&entry(4)));

        // The base answers for its own index; a hole inside the snapshot does not.
        assert_eq!(log.term_at(LogIndex::new(2)), Some(Term::new(2)));
        assert_eq!(log.term_at(LogIndex::new(1)), None);
        assert_eq!(log.term_at(LogIndex::new(3)), Some(Term::new(3)));

        assert_eq!(log.entries_from(LogIndex::new(1)), &[entry(3), entry(4)]);
        assert_eq!(log.entries_from(LogIndex::new(3)), &[entry(3), entry(4)]);
        assert_eq!(log.entries_from(LogIndex::new(4)), &[entry(4)]);
    }

    #[test]
    fn compact_past_the_whole_log_drops_every_entry() {
        let mut log = Log::new();
        log.append(entry(1));
        log.append(entry(2));

        log.compact(LogIndex::new(5), Term::new(4));

        assert_eq!(log.snapshot_last_index(), LogIndex::new(5));
        assert_eq!(log.last_index(), LogIndex::new(5));
        assert_eq!(log.last_term(), Term::new(4));
        assert_eq!(log.entries_from(LogIndex::new(1)), &[] as &[LogEntry]);
        assert_eq!(log.term_at(LogIndex::new(5)), Some(Term::new(4)));

        // Appending resumes at base + 1.
        log.append(entry(6));
        assert_eq!(log.get(LogIndex::new(6)), Some(&entry(6)));
        assert_eq!(log.last_index(), LogIndex::new(6));
    }

    #[test]
    fn compact_at_or_below_the_base_is_a_no_op() {
        let mut log = Log::new();
        log.append(entry(1));
        log.append(entry(2));
        log.append(entry(3));
        log.compact(LogIndex::new(2), Term::new(2));

        log.compact(LogIndex::new(1), Term::new(1));
        log.compact(LogIndex::new(2), Term::new(2));

        assert_eq!(log.snapshot_last_index(), LogIndex::new(2));
        assert_eq!(log.entries_from(LogIndex::new(1)), &[entry(3)]);
    }

    #[test]
    fn truncate_after_is_relative_to_the_compaction_base() {
        let mut log = Log::new();
        for term in 1..=5 {
            log.append(entry(term));
        }
        log.compact(LogIndex::new(3), Term::new(3));

        log.truncate_after(LogIndex::new(4));
        assert_eq!(log.last_index(), LogIndex::new(4));
        assert_eq!(log.entries_from(LogIndex::new(1)), &[entry(4)]);

        // Down to the base drops every in-memory entry but keeps the snapshot.
        log.truncate_after(LogIndex::new(3));
        assert!(log.entries_from(LogIndex::new(1)).is_empty());
        assert_eq!(log.last_index(), LogIndex::new(3));
        assert_eq!(log.last_term(), Term::new(3));
    }

    #[test]
    fn from_snapshot_restores_the_base_and_the_tail() {
        let log = Log::from_snapshot(LogIndex::new(10), Term::new(4), vec![entry(5), entry(5)]);
        assert_eq!(log.snapshot_last_index(), LogIndex::new(10));
        assert_eq!(log.last_index(), LogIndex::new(12));
        assert_eq!(log.get(LogIndex::new(11)), Some(&entry(5)));
        assert_eq!(log.term_at(LogIndex::new(10)), Some(Term::new(4)));
        assert_eq!(log.term_at(LogIndex::new(9)), None);
    }

    #[test]
    fn new_node_is_a_follower_at_term_zero_with_an_empty_log() {
        let node = RaftNode::new(NodeId::new(1), [NodeId::new(2), NodeId::new(3)]);
        assert_eq!(node.id(), NodeId::new(1));
        assert!(node.is_follower());
        assert_eq!(node.current_term(), Term::ZERO);
        assert_eq!(node.voted_for(), None);
        assert!(node.log().is_empty());
        assert_eq!(node.commit_index(), LogIndex::ZERO);
        assert_eq!(node.last_applied(), LogIndex::ZERO);
    }

    #[test]
    fn new_node_drops_self_from_peers_then_sorts_and_dedups() {
        let node = RaftNode::new(
            NodeId::new(2),
            [
                NodeId::new(3),
                NodeId::new(2),
                NodeId::new(1),
                NodeId::new(3),
            ],
        );
        assert_eq!(node.peers(), [NodeId::new(1), NodeId::new(3)]);
    }

    #[test]
    fn election_timeout_starts_a_candidacy_and_solicits_votes() {
        let mut n = node(1, &[2, 3]);

        let effects = n.step(Input::ElectionTimeout, NOW);

        assert!(n.is_candidate());
        assert_eq!(n.current_term(), Term::new(1));
        assert_eq!(n.voted_for(), Some(NodeId::new(1)));
        assert!(matches!(
            n.role(),
            Role::Candidate { votes_granted } if votes_granted.len() == 1
        ));

        assert_eq!(
            effects,
            vec![
                Effect::Persist {
                    current_term: Term::new(1),
                    voted_for: Some(NodeId::new(1)),
                },
                Effect::SendRpc {
                    to: NodeId::new(2),
                    message: request_vote(1, 1, 0, 0),
                },
                Effect::SendRpc {
                    to: NodeId::new(3),
                    message: request_vote(1, 1, 0, 0),
                },
                Effect::ResetElectionTimer,
            ],
        );
    }

    #[test]
    fn a_failed_election_leaves_the_term_raised_not_rolled_back() {
        let mut n = node(1, &[2, 3]);

        drive(&mut n, Input::ElectionTimeout);
        drive(&mut n, Input::ElectionTimeout);

        assert_eq!(n.current_term(), Term::new(2));
        assert!(n.is_candidate());
    }

    #[test]
    fn single_node_cluster_elects_itself_immediately() {
        let mut n = node(1, &[]);

        let effects = n.step(Input::ElectionTimeout, NOW);

        assert!(n.is_leader());
        assert_eq!(n.current_term(), Term::new(1));
        assert_eq!(
            effects,
            vec![
                Effect::Persist {
                    current_term: Term::new(1),
                    voted_for: Some(NodeId::new(1)),
                },
                Effect::ResetElectionTimer,
            ],
        );
    }

    #[test]
    fn candidate_wins_on_a_quorum_and_sends_heartbeats() {
        let mut n = node(1, &[2, 3]);
        drive(&mut n, Input::ElectionTimeout);

        let effects = n.step(deliver(2, vote_reply(1, true)), NOW);

        assert!(n.is_leader());
        let heartbeat = |to: u64| Effect::SendRpc {
            to: NodeId::new(to),
            message: Message::AppendEntries(AppendEntriesArgs {
                term: Term::new(1),
                leader_id: NodeId::new(1),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::ZERO,
                entries: Vec::new(),
                leader_commit: LogIndex::ZERO,
            }),
        };
        assert_eq!(effects, vec![heartbeat(2), heartbeat(3)]);
    }

    #[test]
    fn votes_arriving_after_the_win_are_ignored() {
        let mut n = node(1, &[2, 3]);
        drive(&mut n, Input::ElectionTimeout);
        drive(&mut n, deliver(2, vote_reply(1, true)));
        assert!(n.is_leader());

        let effects = n.step(deliver(3, vote_reply(1, true)), NOW);

        assert!(effects.is_empty());
        assert!(n.is_leader());
    }

    #[test]
    fn vote_is_granted_to_an_up_to_date_candidate_in_a_newer_term() {
        let mut n = node(1, &[2, 3]);

        let effects = n.step(deliver(2, request_vote(3, 2, 0, 0)), NOW);

        assert!(n.is_follower());
        assert_eq!(n.current_term(), Term::new(3));
        assert_eq!(n.voted_for(), Some(NodeId::new(2)));
        assert_eq!(
            effects,
            vec![
                Effect::Persist {
                    current_term: Term::new(3),
                    voted_for: Some(NodeId::new(2)),
                },
                Effect::ResetElectionTimer,
                Effect::SendRpc {
                    to: NodeId::new(2),
                    message: vote_reply(3, true),
                },
            ],
        );
    }

    #[test]
    fn vote_is_denied_for_a_stale_term_without_touching_state() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(5);

        let effects = n.step(deliver(2, request_vote(1, 2, 0, 0)), NOW);

        assert_eq!(n.current_term(), Term::new(5));
        assert_eq!(n.voted_for(), None);
        assert_eq!(
            effects,
            vec![Effect::SendRpc {
                to: NodeId::new(2),
                message: vote_reply(5, false),
            }],
        );
    }

    #[test]
    fn vote_is_denied_when_already_cast_for_another_candidate_this_term() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(2);
        n.voted_for = Some(NodeId::new(2));

        let effects = n.step(deliver(3, request_vote(2, 3, 0, 0)), NOW);

        assert_eq!(n.voted_for(), Some(NodeId::new(2)));
        assert_eq!(
            effects,
            vec![Effect::SendRpc {
                to: NodeId::new(3),
                message: vote_reply(2, false),
            }],
        );
    }

    #[test]
    fn a_newer_term_steps_us_down_even_when_the_vote_is_denied() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);
        n.log.append(entry(1)); // our last entry: index 1, term 1

        // Candidate 2 is in term 4 but its log is empty, so it is not
        // up-to-date: deny the vote, but still adopt the newer term.
        let effects = n.step(deliver(2, request_vote(4, 2, 0, 0)), NOW);

        assert!(n.is_follower());
        assert_eq!(n.current_term(), Term::new(4));
        assert_eq!(n.voted_for(), None);
        assert_eq!(
            effects,
            vec![
                Effect::Persist {
                    current_term: Term::new(4),
                    voted_for: None,
                },
                Effect::SendRpc {
                    to: NodeId::new(2),
                    message: vote_reply(4, false),
                },
            ],
        );
    }

    #[test]
    fn a_reply_with_a_newer_term_makes_a_leader_step_down() {
        let mut n = node(1, &[2, 3]);
        drive(&mut n, Input::ElectionTimeout);
        drive(&mut n, deliver(2, vote_reply(1, true)));
        assert!(n.is_leader());

        let effects = n.step(deliver(3, vote_reply(6, false)), NOW);

        assert!(n.is_follower());
        assert_eq!(n.current_term(), Term::new(6));
        assert_eq!(n.voted_for(), None);
        assert_eq!(
            effects,
            vec![Effect::Persist {
                current_term: Term::new(6),
                voted_for: None,
            }],
        );
    }

    #[test]
    fn append_entries_from_the_current_leader_resets_the_timer_and_acks() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);

        let effects = n.step(deliver(2, heartbeat(1, 2, 0, 0)), NOW);

        assert!(n.is_follower());
        assert_eq!(n.current_term(), Term::new(1));
        assert_eq!(
            effects,
            vec![Effect::ResetElectionTimer, append_reply(2, 1, true)],
        );
    }

    #[test]
    fn append_entries_with_a_newer_term_steps_a_candidate_down() {
        let mut n = node(1, &[2, 3]);
        drive(&mut n, Input::ElectionTimeout); // candidate, term 1
        drive(&mut n, Input::ElectionTimeout); // candidate, term 2

        let effects = n.step(deliver(3, heartbeat(5, 3, 0, 0)), NOW);

        assert!(n.is_follower());
        assert_eq!(n.current_term(), Term::new(5));
        assert_eq!(n.voted_for(), None);
        assert_eq!(
            effects,
            vec![
                Effect::Persist {
                    current_term: Term::new(5),
                    voted_for: None,
                },
                Effect::ResetElectionTimer,
                append_reply(3, 5, true),
            ],
        );
    }

    #[test]
    fn same_term_append_entries_makes_a_candidate_yield_without_a_new_fsync() {
        let mut n = node(1, &[2, 3]);
        drive(&mut n, Input::ElectionTimeout); // candidate, term 1, voted for self

        let effects = n.step(deliver(2, heartbeat(1, 2, 0, 0)), NOW);

        assert!(n.is_follower());
        assert_eq!(n.voted_for(), Some(NodeId::new(1))); // vote kept, term unchanged
        assert_eq!(
            effects,
            vec![Effect::ResetElectionTimer, append_reply(2, 1, true)],
        );
    }

    #[test]
    fn append_entries_from_a_stale_leader_is_rejected_and_is_not_contact() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(5);

        let effects = n.step(deliver(2, heartbeat(2, 2, 0, 0)), NOW);

        assert_eq!(n.current_term(), Term::new(5));
        // No ResetElectionTimer: a stale leader does not count as contact.
        assert_eq!(effects, vec![append_reply(2, 5, false)]);
    }

    #[test]
    fn append_entries_failing_the_consistency_check_acks_false_but_still_resets() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);
        n.log.append(entry(1)); // index 1, term 1

        // Leader claims prev entry at index 1 has term 2; ours has term 1.
        let effects = n.step(deliver(2, heartbeat(1, 2, 1, 2)), NOW);

        assert_eq!(
            effects,
            vec![Effect::ResetElectionTimer, append_reply(2, 1, false)],
        );
    }

    #[test]
    fn heartbeat_tick_makes_the_leader_rebroadcast_append_entries() {
        let mut n = node(1, &[2, 3]);
        drive(&mut n, Input::ElectionTimeout);
        drive(&mut n, deliver(2, vote_reply(1, true)));
        assert!(n.is_leader());

        let effects = n.step(Input::HeartbeatTick, NOW);

        let beat = |to: u64| Effect::SendRpc {
            to: NodeId::new(to),
            message: Message::AppendEntries(AppendEntriesArgs {
                term: Term::new(1),
                leader_id: NodeId::new(1),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::ZERO,
                entries: Vec::new(),
                leader_commit: LogIndex::ZERO,
            }),
        };
        assert_eq!(effects, vec![beat(2), beat(3)]);
    }

    #[test]
    fn heartbeat_tick_is_a_no_op_for_a_follower() {
        let mut n = node(1, &[2, 3]);

        assert!(n.step(Input::HeartbeatTick, NOW).is_empty());
        assert!(n.is_follower());
    }

    #[test]
    fn propose_on_a_non_leader_is_dropped() {
        let mut n = node(1, &[2, 3]);

        assert!(n.step(propose(b"cmd"), NOW).is_empty());
        assert!(n.log().is_empty());
        assert!(n.is_follower());

        drive(&mut n, Input::ElectionTimeout); // candidate
        assert!(n.step(propose(b"cmd"), NOW).is_empty());
        assert!(n.log().is_empty());
    }

    #[test]
    fn propose_on_a_leader_appends_persists_then_replicates() {
        let mut n = node(1, &[2, 3]);
        drive(&mut n, Input::ElectionTimeout);
        drive(&mut n, deliver(2, vote_reply(1, true)));
        assert!(n.is_leader());

        let effects = n.step(propose(b"cmd"), NOW);

        assert_eq!(n.log().len(), 1);
        assert_eq!(n.log().get(LogIndex::new(1)), Some(&entry(1)));

        let tail = vec![entry(1)];
        assert_eq!(
            effects,
            vec![
                Effect::PersistLog {
                    from_index: LogIndex::new(1),
                    entries: tail.clone(),
                },
                send(2, append_entries(1, 1, 0, 0, tail.clone(), 0)),
                send(3, append_entries(1, 1, 0, 0, tail, 0)),
            ],
        );
    }

    #[test]
    fn persist_log_precedes_the_replicating_send_rpcs() {
        let mut n = node(1, &[2, 3]);
        drive(&mut n, Input::ElectionTimeout);
        drive(&mut n, deliver(2, vote_reply(1, true)));

        let effects = n.step(propose(b"cmd"), NOW);

        let persist_at = effects
            .iter()
            .position(|e| matches!(e, Effect::PersistLog { .. }));
        let first_send = effects
            .iter()
            .position(|e| matches!(e, Effect::SendRpc { .. }));
        assert_eq!(persist_at, Some(0));
        assert!(persist_at < first_send);
    }

    #[test]
    fn a_second_proposal_replicates_the_whole_unacked_tail() {
        let mut n = node(1, &[2, 3]);
        drive(&mut n, Input::ElectionTimeout);
        drive(&mut n, deliver(2, vote_reply(1, true)));

        drive(&mut n, propose(b"cmd"));
        let effects = n.step(propose(b"cmd"), NOW);

        // No ack has come back, so `next_index` for both peers is still 1: the
        // PersistLog covers only the new entry, but the AppendEntries carries
        // the whole uncommitted tail. Nothing commits — the leader is one of
        // three.
        assert_eq!(n.log().len(), 2);
        assert_eq!(n.commit_index(), LogIndex::ZERO);
        assert_eq!(
            effects,
            vec![
                Effect::PersistLog {
                    from_index: LogIndex::new(2),
                    entries: vec![entry(1)],
                },
                send(2, append_entries(1, 1, 0, 0, vec![entry(1), entry(1)], 0)),
                send(3, append_entries(1, 1, 0, 0, vec![entry(1), entry(1)], 0)),
            ],
        );
    }

    #[test]
    fn heartbeat_tick_carries_entries_a_peer_has_not_acked() {
        let mut n = node(1, &[2, 3]);
        drive(&mut n, Input::ElectionTimeout);
        drive(&mut n, deliver(2, vote_reply(1, true)));
        drive(&mut n, propose(b"cmd"));

        let effects = n.step(Input::HeartbeatTick, NOW);

        assert_eq!(
            effects,
            vec![
                send(2, append_entries(1, 1, 0, 0, vec![entry(1)], 0)),
                send(3, append_entries(1, 1, 0, 0, vec![entry(1)], 0)),
            ],
        );
    }

    // --- 5b: follower splice (AppendEntries receiver rules 3-5) ------------

    #[test]
    fn append_entries_appends_new_entries_to_an_empty_log() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);

        let entries = vec![entry_cmd(1, b"a"), entry_cmd(1, b"b")];
        let effects = n.step(
            deliver(2, append_entries(1, 2, 0, 0, entries.clone(), 0)),
            NOW,
        );

        assert_eq!(n.log().entries_from(LogIndex::new(1)), entries.as_slice());
        assert_eq!(
            effects,
            vec![
                Effect::PersistLog {
                    from_index: LogIndex::new(1),
                    entries,
                },
                Effect::ResetElectionTimer,
                ack(2, 1, 2),
            ],
        );
    }

    #[test]
    fn append_entries_truncates_a_conflicting_tail_then_appends() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(3);
        n.log.append(entry(1)); // index 1, term 1
        n.log.append(entry(2)); // index 2, term 2
        n.log.append(entry(3)); // index 3, term 3

        // The leader agrees up to index 2, then has term 2 at index 3 where we
        // have term 3: index 3 is a conflict and must be replaced.
        let incoming = vec![entry(2), entry_cmd(2, b"z")];
        let effects = n.step(deliver(2, append_entries(3, 2, 1, 1, incoming, 0)), NOW);

        assert_eq!(n.log().len(), 3);
        assert_eq!(n.log().get(LogIndex::new(2)), Some(&entry(2))); // untouched
        assert_eq!(n.log().get(LogIndex::new(3)), Some(&entry_cmd(2, b"z")));
        assert_eq!(
            effects,
            vec![
                Effect::PersistLog {
                    from_index: LogIndex::new(3),
                    entries: vec![entry_cmd(2, b"z")],
                },
                Effect::ResetElectionTimer,
                ack(2, 3, 3),
            ],
        );
    }

    #[test]
    fn a_duplicate_append_entries_does_not_rewrite_the_log() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);
        n.log.append(entry_cmd(1, b"a"));
        n.log.append(entry_cmd(1, b"b"));

        let dup = vec![entry_cmd(1, b"a"), entry_cmd(1, b"b")];
        let effects = n.step(deliver(2, append_entries(1, 2, 0, 0, dup, 0)), NOW);

        // Every entry was already present: no splice, no PersistLog.
        assert_eq!(n.log().len(), 2);
        assert_eq!(effects, vec![Effect::ResetElectionTimer, ack(2, 1, 2)]);
    }

    #[test]
    fn append_entries_advances_commit_index_and_emits_applies_in_order() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);

        let entries = vec![entry_cmd(1, b"a"), entry_cmd(1, b"b")];
        let effects = n.step(
            deliver(2, append_entries(1, 2, 0, 0, entries.clone(), 2)),
            NOW,
        );

        assert_eq!(n.commit_index(), LogIndex::new(2));
        assert_eq!(n.last_applied(), LogIndex::new(2));
        assert_eq!(
            effects,
            vec![
                Effect::PersistLog {
                    from_index: LogIndex::new(1),
                    entries,
                },
                Effect::ApplyToStateMachine {
                    index: LogIndex::new(1),
                    command: Bytes::from_static(b"a"),
                },
                Effect::ApplyToStateMachine {
                    index: LogIndex::new(2),
                    command: Bytes::from_static(b"b"),
                },
                Effect::ResetElectionTimer,
                ack(2, 1, 2),
            ],
        );
    }

    #[test]
    fn leader_commit_is_clamped_to_what_the_rpc_covered() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);

        let effects = n.step(
            deliver(2, append_entries(1, 2, 0, 0, vec![entry_cmd(1, b"a")], 9)),
            NOW,
        );

        assert_eq!(n.commit_index(), LogIndex::new(1));
        assert_eq!(n.last_applied(), LogIndex::new(1));
        assert_eq!(
            effects,
            vec![
                Effect::PersistLog {
                    from_index: LogIndex::new(1),
                    entries: vec![entry_cmd(1, b"a")],
                },
                Effect::ApplyToStateMachine {
                    index: LogIndex::new(1),
                    command: Bytes::from_static(b"a"),
                },
                Effect::ResetElectionTimer,
                ack(2, 1, 1),
            ],
        );
    }

    // --- 5b: leader ack handling -----------------------------------------

    #[test]
    fn a_successful_ack_from_one_peer_commits_in_a_three_node_cluster() {
        let mut n = node(1, &[2, 3]);
        drive(&mut n, Input::ElectionTimeout);
        drive(&mut n, deliver(2, vote_reply(1, true)));
        drive(&mut n, propose(b"x"));
        assert_eq!(n.commit_index(), LogIndex::ZERO); // leader alone: not yet

        let effects = n.step(deliver(2, ae_reply(1, true, 1)), NOW);

        // Peer 2 plus the leader is a majority of three.
        assert_eq!(n.commit_index(), LogIndex::new(1));
        assert_eq!(n.last_applied(), LogIndex::new(1));
        assert_eq!(
            effects,
            vec![Effect::ApplyToStateMachine {
                index: LogIndex::new(1),
                command: Bytes::from_static(b"x"),
            }],
        );
    }

    #[test]
    fn a_failed_ack_backs_off_next_index_and_retries_immediately() {
        let mut n = node(1, &[2, 3]);
        // Two entries already on this node before it wins, so as leader its
        // `next_index` for every peer starts at 3.
        n.log.append(entry(1));
        n.log.append(entry(1));
        n.current_term = Term::new(1);
        drive(&mut n, Input::ElectionTimeout); // term 2, candidate
        drive(&mut n, deliver(2, vote_reply(2, true))); // leader, term 2

        let effects = n.step(deliver(2, ae_reply(2, false, 0)), NOW);

        // next_index[2] 3 -> 2: retry anchors at index 1, carrying index 2 on.
        assert_eq!(
            effects,
            vec![send(2, append_entries(2, 1, 1, 1, vec![entry(1)], 0))],
        );

        // A second failure walks it back once more: anchor at 0, whole log.
        let effects = n.step(deliver(2, ae_reply(2, false, 0)), NOW);
        assert_eq!(
            effects,
            vec![send(
                2,
                append_entries(2, 1, 0, 0, vec![entry(1), entry(1)], 0)
            )],
        );
    }

    #[test]
    fn an_ack_from_a_newer_term_makes_the_leader_step_down() {
        let mut n = node(1, &[2, 3]);
        drive(&mut n, Input::ElectionTimeout);
        drive(&mut n, deliver(2, vote_reply(1, true)));
        assert!(n.is_leader());

        let effects = n.step(deliver(3, ae_reply(7, false, 0)), NOW);

        assert!(n.is_follower());
        assert_eq!(n.current_term(), Term::new(7));
        assert_eq!(n.voted_for(), None);
        assert_eq!(
            effects,
            vec![Effect::Persist {
                current_term: Term::new(7),
                voted_for: None,
            }],
        );
    }

    #[test]
    fn a_stale_ack_is_ignored() {
        let mut n = node(1, &[2, 3]);
        n.log.append(entry(1));
        n.current_term = Term::new(1);
        drive(&mut n, Input::ElectionTimeout); // term 2
        drive(&mut n, deliver(2, vote_reply(2, true))); // leader, term 2

        let effects = n.step(deliver(2, ae_reply(1, true, 1)), NOW);

        assert!(effects.is_empty());
        assert_eq!(n.commit_index(), LogIndex::ZERO);
        assert!(n.is_leader());
    }

    #[test]
    fn a_prior_term_entry_commits_only_behind_a_current_term_entry() {
        let mut n = node(1, &[2, 3]);
        n.log.append(entry_cmd(1, b"old")); // index 1, term 1
        n.current_term = Term::new(1);
        drive(&mut n, Input::ElectionTimeout); // term 2, candidate
        drive(&mut n, deliver(2, vote_reply(2, true))); // leader, term 2

        // The term-1 entry reaches a majority, but a leader never commits an
        // earlier term's entry by count alone (paper Figure 8).
        let effects = n.step(deliver(2, ae_reply(2, true, 1)), NOW);
        assert_eq!(n.commit_index(), LogIndex::ZERO);
        assert!(effects.is_empty());

        // A current-term entry at index 2 now reaches a majority.
        drive(&mut n, propose(b"new"));
        let effects = n.step(deliver(2, ae_reply(2, true, 2)), NOW);

        // Committing index 2 carries index 1 with it, applied in order.
        assert_eq!(n.commit_index(), LogIndex::new(2));
        assert_eq!(n.last_applied(), LogIndex::new(2));
        assert_eq!(
            effects,
            vec![
                Effect::ApplyToStateMachine {
                    index: LogIndex::new(1),
                    command: Bytes::from_static(b"old"),
                },
                Effect::ApplyToStateMachine {
                    index: LogIndex::new(2),
                    command: Bytes::from_static(b"new"),
                },
            ],
        );
    }

    #[test]
    fn a_lone_leader_commits_and_applies_a_proposal_in_one_step() {
        let mut n = node(1, &[]);
        drive(&mut n, Input::ElectionTimeout); // sole vote -> leader, term 1

        let effects = n.step(propose(b"solo"), NOW);

        assert_eq!(n.commit_index(), LogIndex::new(1));
        assert_eq!(n.last_applied(), LogIndex::new(1));
        assert_eq!(
            effects,
            vec![
                Effect::PersistLog {
                    from_index: LogIndex::new(1),
                    entries: vec![entry_cmd(1, b"solo")],
                },
                Effect::ApplyToStateMachine {
                    index: LogIndex::new(1),
                    command: Bytes::from_static(b"solo"),
                },
            ],
        );
    }

    // --- 8: snapshotting / InstallSnapshot ------------------------------------

    /// Arranges `n` as leader for term 2 with a five-entry term-1 log, then
    /// compacts it up to `up_to` so the compaction base is non-zero.
    fn leader_with_compacted_log(up_to: u64) -> RaftNode {
        let mut n = node(1, &[2, 3]);
        for _ in 0..5 {
            n.log.append(entry(1));
        }
        n.current_term = Term::new(1);
        drive(&mut n, Input::ElectionTimeout); // term 2, candidate
        drive(&mut n, deliver(2, vote_reply(2, true))); // leader, term 2
        n.commit_index = LogIndex::new(5);
        n.last_applied = LogIndex::new(5);
        drive(
            &mut n,
            Input::CompactLog {
                up_to_index: LogIndex::new(up_to),
            },
        );
        assert_eq!(n.snapshot_last_index(), LogIndex::new(up_to));
        n
    }

    #[test]
    fn compact_log_input_trims_the_core_log_up_to_last_applied() {
        let mut n = node(1, &[2, 3]);
        for _ in 0..8 {
            n.log.append(entry(1));
        }
        n.current_term = Term::new(1);
        n.commit_index = LogIndex::new(5);
        n.last_applied = LogIndex::new(5);

        let effects = n.step(
            Input::CompactLog {
                up_to_index: LogIndex::new(5),
            },
            NOW,
        );

        assert!(effects.is_empty());
        assert_eq!(n.snapshot_last_index(), LogIndex::new(5));
        assert_eq!(n.log().get(LogIndex::new(5)), None); // folded away
        assert_eq!(n.log().get(LogIndex::new(6)), Some(&entry(1)));
        assert_eq!(n.log().last_index(), LogIndex::new(8));
    }

    #[test]
    fn compact_log_past_last_applied_is_ignored() {
        let mut n = node(1, &[2, 3]);
        for _ in 0..8 {
            n.log.append(entry(1));
        }
        n.current_term = Term::new(1);
        n.last_applied = LogIndex::new(3);

        drive(
            &mut n,
            Input::CompactLog {
                up_to_index: LogIndex::new(5),
            },
        );

        assert_eq!(n.snapshot_last_index(), LogIndex::ZERO);
    }

    #[test]
    fn a_leader_sends_a_snapshot_once_a_peer_falls_behind_the_base() {
        let mut n = leader_with_compacted_log(3); // base at index 3

        // Peer 3 rejects: nextIndex walks 6 -> 5 -> 4 -> 3. At 3 (== base) the
        // leader can no longer build prevLogTerm, so it sends the snapshot.
        let mut last = Vec::new();
        for _ in 0..3 {
            last = n.step(deliver(3, ae_reply(2, false, 0)), NOW);
        }
        assert_eq!(last, vec![Effect::SendSnapshot { to: NodeId::new(3) }]);
    }

    #[test]
    fn a_heartbeat_tick_sends_a_snapshot_to_a_peer_behind_the_base() {
        let mut n = leader_with_compacted_log(3);
        if let Role::Leader { next_index, .. } = &mut n.role {
            next_index.insert(NodeId::new(3), LogIndex::new(1));
        }

        let effects = n.step(Input::HeartbeatTick, NOW);

        assert!(effects.contains(&Effect::SendSnapshot { to: NodeId::new(3) }));
    }

    #[test]
    fn an_install_snapshot_reply_advances_next_index_past_the_compacted_region() {
        let mut n = leader_with_compacted_log(3);
        if let Role::Leader { next_index, .. } = &mut n.role {
            next_index.insert(NodeId::new(2), LogIndex::new(1));
        }

        drive(&mut n, deliver(2, snapshot_reply_msg(2, 4)));

        if let Role::Leader {
            next_index,
            match_index,
        } = n.role()
        {
            assert_eq!(next_index[&NodeId::new(2)], LogIndex::new(5));
            assert_eq!(match_index[&NodeId::new(2)], LogIndex::new(4));
        } else {
            unreachable!("still a leader");
        }
    }

    #[test]
    fn install_snapshot_stores_the_chunk_and_defers_the_reply() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);

        let effects = n.step(deliver(2, install_snapshot(2, 2, 5, 1, true)), NOW);

        assert!(n.is_follower());
        assert_eq!(n.current_term(), Term::new(2));
        assert_eq!(n.leader_id(), Some(NodeId::new(2)));
        assert_eq!(
            effects,
            vec![
                Effect::Persist {
                    current_term: Term::new(2),
                    voted_for: None,
                },
                Effect::ResetElectionTimer,
                Effect::StoreSnapshotChunk {
                    last_included_index: LogIndex::new(5),
                    last_included_term: Term::new(1),
                    config: config(&[1, 2, 3]),
                    offset: 0,
                    data: b"snap".to_vec(),
                    done: true,
                },
            ],
        );
        // No reply, and nothing adopted, until the driver confirms it durable.
        assert_eq!(n.commit_index(), LogIndex::ZERO);
        assert_eq!(n.snapshot_last_index(), LogIndex::ZERO);
    }

    #[test]
    fn snapshot_installed_adopts_the_snapshot_and_acks_the_leader() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);
        drive(&mut n, deliver(2, install_snapshot(2, 2, 5, 3, true)));

        let effects = n.step(
            Input::SnapshotInstalled {
                last_included_index: LogIndex::new(5),
                last_included_term: Term::new(3),
                config: config(&[1, 2, 3, 4]),
            },
            NOW,
        );

        assert_eq!(n.commit_index(), LogIndex::new(5));
        assert_eq!(n.last_applied(), LogIndex::new(5));
        assert_eq!(n.snapshot_last_index(), LogIndex::new(5));
        assert_eq!(n.log().term_at(LogIndex::new(5)), Some(Term::new(3)));
        // The snapshot's configuration is adopted.
        assert_eq!(n.config(), &config(&[1, 2, 3, 4]));
        assert_eq!(effects, vec![snapshot_reply(2, 2, 5)]);
    }

    #[test]
    fn install_snapshot_from_a_stale_leader_is_rejected_and_is_not_contact() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(5);

        let effects = n.step(deliver(2, install_snapshot(2, 2, 9, 2, true)), NOW);

        assert_eq!(n.current_term(), Term::new(5));
        assert_eq!(n.snapshot_last_index(), LogIndex::ZERO);
        // Only the rejecting reply -- no ResetElectionTimer.
        assert_eq!(effects, vec![snapshot_reply(2, 5, 9)]);
    }

    #[test]
    fn install_snapshot_already_covered_by_commit_index_acks_without_storing() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(2);
        for _ in 0..10 {
            n.log.append(entry(1));
        }
        n.commit_index = LogIndex::new(10);

        let effects = n.step(deliver(2, install_snapshot(2, 2, 5, 1, true)), NOW);

        assert_eq!(
            effects,
            vec![Effect::ResetElectionTimer, snapshot_reply(2, 2, 5)],
        );
        assert_eq!(n.snapshot_last_index(), LogIndex::ZERO);
    }

    #[test]
    fn an_install_snapshot_reply_from_a_newer_term_steps_the_leader_down() {
        let mut n = leader_with_compacted_log(3);

        let effects = n.step(deliver(2, snapshot_reply_msg(9, 3)), NOW);

        assert!(n.is_follower());
        assert_eq!(n.current_term(), Term::new(9));
        assert_eq!(
            effects,
            vec![Effect::Persist {
                current_term: Term::new(9),
                voted_for: None,
            }],
        );
    }

    // --- 9: cluster configuration as a log entry ----------------------------

    fn config(ids: &[u64]) -> ClusterConfig {
        ClusterConfig::new(ids.iter().copied().map(NodeId::new))
    }

    #[test]
    fn cluster_config_add_remove_and_membership() {
        let base = config(&[1, 2, 3]);
        assert_eq!(base.len(), 3);
        assert!(base.contains(NodeId::new(2)));
        assert!(!base.contains(NodeId::new(9)));

        let grown = base.with_added(NodeId::new(4));
        assert_eq!(grown.len(), 4);
        assert!(grown.contains(NodeId::new(4)));

        let shrunk = base.without(NodeId::new(2));
        assert_eq!(
            shrunk.voters().iter().copied().collect::<Vec<_>>(),
            vec![NodeId::new(1), NodeId::new(3),]
        );
    }

    #[test]
    fn a_new_node_takes_its_bootstrap_configuration() {
        let n = node(1, &[2, 3]);
        assert_eq!(n.config(), &config(&[1, 2, 3]));
        assert_eq!(n.peers(), [NodeId::new(2), NodeId::new(3)]);
    }

    #[test]
    fn a_config_entry_in_the_log_becomes_the_active_configuration() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);
        n.log
            .append(LogEntry::config(Term::new(1), config(&[1, 2, 3, 4, 5])));
        n.recompute_config();

        assert_eq!(n.config(), &config(&[1, 2, 3, 4, 5]));
        assert_eq!(
            n.peers(),
            [
                NodeId::new(2),
                NodeId::new(3),
                NodeId::new(4),
                NodeId::new(5),
            ]
        );
        assert_eq!(n.cluster_size(), 5);
        assert_eq!(n.quorum(), 3);

        // Truncating the entry away reverts to the bootstrap set.
        n.log.truncate_after(LogIndex::ZERO);
        n.recompute_config();
        assert_eq!(n.config(), &config(&[1, 2, 3]));
    }

    // --- 10: single-server membership changes -----------------------------

    /// A three-node leader for term 2 with one committed command at index 1.
    fn three_node_leader() -> RaftNode {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);
        drive(&mut n, Input::ElectionTimeout); // term 2, candidate
        drive(&mut n, deliver(2, vote_reply(2, true))); // leader, term 2
        drive(&mut n, propose(b"c0"));
        drive(&mut n, deliver(2, ae_reply(2, true, 1))); // commit index 1
        assert_eq!(n.commit_index(), LogIndex::new(1));
        n
    }

    fn change(c: MembershipChange) -> Input {
        Input::ChangeMembership { change: c }
    }

    #[test]
    fn add_server_catches_up_then_appends_and_commits_a_config_entry() {
        let mut n = three_node_leader();
        let new = NodeId::new(4);

        // The add begins as catch-up: a learner entry, no config entry yet.
        let effects = n.step(change(MembershipChange::AddServer(new)), NOW);
        assert!(matches!(
            effects.as_slice(),
            [Effect::SendRpc { to, .. }] if *to == new
        ));
        assert!(!n.config().contains(new));
        if let Role::Leader { next_index, .. } = n.role() {
            assert!(next_index.contains_key(&new));
        } else {
            unreachable!("still leader");
        }

        // The new server acks up to the leader's last index -> promotion.
        let last = n.log().last_index().get();
        drive(&mut n, deliver(4, ae_reply(2, true, last)));
        assert!(n.config().contains(new));
        assert_eq!(n.config().len(), 4);

        // A majority of the *new* configuration commits the config entry.
        let config_index = n.log().last_index().get();
        drive(&mut n, deliver(2, ae_reply(2, true, config_index)));
        drive(&mut n, deliver(3, ae_reply(2, true, config_index)));
        assert!(n.commit_index().get() >= config_index);
        // The change is finished.
        drive(&mut n, Input::HeartbeatTick);
        drive(&mut n, deliver(4, ae_reply(2, true, config_index)));
    }

    #[test]
    fn remove_server_appends_a_config_entry_immediately() {
        let mut n = node(1, &[2, 3, 4, 5]);
        n.current_term = Term::new(1);
        drive(&mut n, Input::ElectionTimeout);
        drive(&mut n, deliver(2, vote_reply(2, true)));
        drive(&mut n, deliver(3, vote_reply(2, true)));
        assert!(n.is_leader());

        let effects = n.step(change(MembershipChange::RemoveServer(NodeId::new(5))), NOW);

        assert_eq!(n.config().len(), 4);
        assert!(!n.config().contains(NodeId::new(5)));
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::MembershipChanged { config } if !config.contains(NodeId::new(5))
        )));
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::PersistLog { .. }))
        );
    }

    #[test]
    fn a_leader_that_removes_itself_steps_down_when_the_change_commits() {
        let mut n = three_node_leader();

        drive(
            &mut n,
            change(MembershipChange::RemoveServer(NodeId::new(1))),
        );
        assert!(n.is_leader()); // still leading until the entry commits
        let config_index = n.log().last_index().get();

        drive(&mut n, deliver(2, ae_reply(2, true, config_index)));
        drive(&mut n, deliver(3, ae_reply(2, true, config_index)));

        assert!(n.is_follower());
        assert!(!n.config().contains(NodeId::new(1)));
    }

    #[test]
    fn only_one_membership_change_at_a_time() {
        let mut n = three_node_leader();
        drive(&mut n, change(MembershipChange::AddServer(NodeId::new(4))));

        // A second change while the first is in flight is ignored.
        let effects = n.step(change(MembershipChange::RemoveServer(NodeId::new(3))), NOW);
        assert!(effects.is_empty());
        assert!(n.config().contains(NodeId::new(3)));
        assert_eq!(n.config().len(), 3);
    }

    #[test]
    fn a_stuck_add_server_is_abandoned_after_the_tick_budget() {
        let mut n = three_node_leader();
        let new = NodeId::new(4);
        drive(&mut n, change(MembershipChange::AddServer(new)));

        for _ in 0..CATCH_UP_TICKS {
            drive(&mut n, Input::HeartbeatTick);
        }

        assert_eq!(n.config().len(), 3);
        if let Role::Leader { next_index, .. } = n.role() {
            assert!(!next_index.contains_key(&new));
        } else {
            unreachable!("still leader");
        }
        // A fresh change can start again.
        let effects = n.step(change(MembershipChange::RemoveServer(NodeId::new(3))), NOW);
        assert!(!effects.is_empty());
    }

    #[test]
    fn a_new_leader_adopts_an_uncommitted_configuration_change() {
        // Node 1 leads {1,2,3}, appends RemoveServer(3), then loses leadership
        // before the entry commits. A follower elected next must finish it.
        let mut a = three_node_leader();
        drive(
            &mut a,
            change(MembershipChange::RemoveServer(NodeId::new(3))),
        );
        let log = a.log().entries_from(LogIndex::new(1)).to_vec();
        let term = a.current_term();

        let mut b = node(2, &[1, 3]);
        b.current_term = term;
        for entry in log {
            b.log.append(entry);
        }
        b.recompute_config();
        // It has the config entry but has not committed it.
        assert_eq!(b.config().len(), 2);
        assert!(b.pending_change.is_none());

        b.become_leader();

        // The new leader picks the change back up and keeps talking to node 3.
        assert!(matches!(
            b.pending_change,
            Some(super::PendingChange {
                change: MembershipChange::RemoveServer(id),
                ..
            }) if id == NodeId::new(3)
        ));
        if let Role::Leader { next_index, .. } = b.role() {
            assert!(next_index.contains_key(&NodeId::new(3)));
        } else {
            unreachable!("still leader");
        }
    }

    #[test]
    fn a_server_absent_from_its_configuration_stays_passive() {
        // A brand-new learner: not in any configuration.
        let mut n = RaftNode::new_learner(NodeId::new(4));
        assert!(!n.config().contains(NodeId::new(4)));

        let effects = n.step(Input::ElectionTimeout, NOW);
        assert!(effects.is_empty());
        assert!(n.is_follower());
        assert_eq!(n.current_term(), Term::ZERO);

        // Once a configuration entry names it, it participates normally.
        n.log
            .append(LogEntry::config(Term::new(1), config(&[1, 2, 3, 4])));
        n.recompute_config();
        drive(&mut n, Input::ElectionTimeout);
        assert!(n.is_candidate());
    }

    #[test]
    fn a_catching_up_learner_does_not_count_toward_quorum() {
        let mut n = three_node_leader();
        let new = NodeId::new(4);
        drive(&mut n, change(MembershipChange::AddServer(new)));

        drive(&mut n, propose(b"c1")); // index 2
        let idx = n.log().last_index().get();

        // Only the learner acks: {1} of {1,2,3} is not a majority.
        drive(&mut n, deliver(4, ae_reply(2, true, idx)));
        assert_eq!(n.commit_index(), LogIndex::new(1));

        // A real voter acks -> now committed.
        drive(&mut n, deliver(2, ae_reply(2, true, idx)));
        assert_eq!(n.commit_index().get(), idx);
    }

    #[test]
    fn a_committed_config_entry_advances_last_applied_without_an_apply_effect() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);
        n.log.append(entry_cmd(1, b"cmd")); // index 1
        n.log
            .append(LogEntry::config(Term::new(1), config(&[1, 2, 3, 4]))); // index 2
        n.recompute_config();
        n.commit_index = LogIndex::new(2);

        let mut effects = Vec::new();
        n.apply_committed(&mut effects);

        assert_eq!(n.last_applied(), LogIndex::new(2));
        assert_eq!(
            effects,
            vec![Effect::ApplyToStateMachine {
                index: LogIndex::new(1),
                command: Bytes::from_static(b"cmd"),
            }],
        );
    }
}
