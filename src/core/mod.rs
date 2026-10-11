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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;

use bytes::Bytes;

mod message;

pub use message::{
    AppendEntriesArgs, AppendEntriesReply, Conflict, InstallSnapshotArgs, InstallSnapshotReply,
    Message, PreVoteArgs, PreVoteReply, RequestVoteArgs, RequestVoteReply, TimeoutNowArgs,
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

/// Which pre-vote straw poll a [`PreVoteArgs`] belongs to (thesis §9.6).
///
/// Not a paper concept. A pre-vote round deliberately leaves `currentTerm`
/// alone, so consecutive rounds at the same term are otherwise
/// indistinguishable: a "yes" a peer sent in one round — formed when it had
/// heard no leader — could be delivered late and counted in a later round,
/// where the peer may well have a leader again. That would promote the
/// pre-candidate on consent it no longer has, and the resulting term increment
/// unseats a healthy leader (any higher term reaches it through
/// `AppendEntriesReply`, Figure 2). Numbering the rounds is what makes a grant
/// spendable only in the round that solicited it.
///
/// It counts rounds for the life of one server process, not per role: a
/// pre-candidate that yields to a leader and later polls again must not reuse a
/// number, or a straggling reply from the first stint would match the second.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct PreVoteRound(u64);

impl PreVoteRound {
    /// Before any round has been started. No pre-vote ever carries this, so a
    /// reply bearing it can never be mistaken for a live one.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw round number.
    #[must_use]
    pub const fn new(round: u64) -> Self {
        Self(round)
    }

    /// The next round.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// The raw round number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Numbers a leader's rounds of `AppendEntries` within its term, so that it
/// can tell which replies answer an RPC sent *after* a given moment (thesis
/// §6.4).
///
/// To serve a read, the leader must confirm it was still leader at some point
/// after the read arrived. An ack proves only that the follower had not moved
/// to a newer term when it handled the RPC, so it counts only for an RPC sent
/// after the read: the leader starts a new round when a read arrives, every
/// `AppendEntries` carries the current round, and every reply echoes it. Any
/// reply at the leader's term counts, success or not.
///
/// Rounds restart at zero with each leadership. Replies are matched to a
/// leadership by term, and a server leads at most once per term.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct HeartbeatRound(u64);

impl HeartbeatRound {
    /// The round a leadership starts in, before any read has asked for one.
    pub const ZERO: Self = Self(0);

    /// Wraps a raw round number.
    #[must_use]
    pub const fn new(round: u64) -> Self {
        Self(round)
    }

    /// The next round.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// The raw round number.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// The driver's handle for one linearizable read ([`Input::ReadIndex`]),
/// echoed by [`Effect::ReadReady`] or [`Effect::ReadFailed`]. The core never
/// interprets it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadId(u64);

impl ReadId {
    /// Wraps a raw id.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// The raw id.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for PreVoteRound {
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
    /// The empty entry a new leader appends at the start of its term (thesis
    /// §6.4). Committing it is how the leader learns which entries are
    /// committed: until an entry of its own term commits, its `commitIndex`
    /// may lag the cluster's. Never handed to the application state machine.
    Noop,
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

    /// A new leader's empty entry at `term`.
    #[must_use]
    pub const fn noop(term: Term) -> Self {
        Self {
            term,
            kind: LogEntryKind::Noop,
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

    /// The command bytes, or `None` for a configuration or no-op entry.
    #[must_use]
    pub const fn command_bytes(&self) -> Option<&Bytes> {
        match &self.kind {
            LogEntryKind::Command(command) => Some(command),
            LogEntryKind::Config(_) | LogEntryKind::Noop => None,
        }
    }

    /// The configuration, or `None` for a command or no-op entry.
    #[must_use]
    pub const fn as_config(&self) -> Option<&ClusterConfig> {
        match &self.kind {
            LogEntryKind::Config(config) => Some(config),
            LogEntryKind::Command(_) | LogEntryKind::Noop => None,
        }
    }

    /// The payload's size in bytes, as counted against
    /// [`ReplicationConfig::max_append_bytes`]: the command's length, or eight
    /// bytes per voter for a configuration, nothing for a no-op. Framing
    /// overhead is not counted.
    fn payload_len(&self) -> usize {
        match &self.kind {
            LogEntryKind::Command(command) => command.len(),
            LogEntryKind::Config(config) => config.len() * size_of::<u64>(),
            LogEntryKind::Noop => 0,
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

    /// The last index of an `AppendEntries` batch starting at `index`: as many
    /// entries as fit in `max_bytes` of payload, but always at least one, so
    /// an entry larger than the cap still goes out on its own. `index.prev()`
    /// (an empty batch) when there is nothing at or past `index`.
    fn batch_end(&self, index: LogIndex, max_bytes: usize) -> LogIndex {
        let mut bytes = 0usize;
        let mut count = 0u64;
        for entry in self.entries_from(index) {
            let size = entry.payload_len();
            if count > 0 && bytes.saturating_add(size) > max_bytes {
                break;
            }
            bytes = bytes.saturating_add(size);
            count += 1;
        }
        LogIndex::new(index.get() + count).prev()
    }

    /// The first index of the run of `term_at(index)` entries that ends at
    /// `index`, walking back no further than the compaction base. `index`
    /// itself when the log cannot answer for it.
    fn term_start(&self, index: LogIndex) -> LogIndex {
        let Some(term) = self.term_at(index) else {
            return index;
        };
        // `term_at` answers `None` below the compaction base, which ends the
        // walk there.
        let mut start = index;
        while self.term_at(start.prev()) == Some(term) {
            start = start.prev();
        }
        start
    }

    /// The highest index holding an entry of `term`, or `None` if the log
    /// (including its compaction base) has none. Terms never decrease along
    /// the log, so a run of `term` ends just before the first newer entry,
    /// which a binary search finds.
    fn last_index_of_term(&self, term: Term) -> Option<LogIndex> {
        let up_to_term = self.entries.partition_point(|entry| entry.term <= term);
        // usize -> u64 cannot lose bits on any platform Raft runs on.
        let index = LogIndex::new(self.snapshot_last_index.get() + up_to_term as u64);
        (self.term_at(index) == Some(term)).then_some(index)
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
/// A server is always in exactly one of these. It starts as [`Role::Follower`];
/// when its election timer fires it becomes a [`Role::PreCandidate`] and runs a
/// pre-vote straw poll (thesis §9.6), only promoting to [`Role::Candidate`] —
/// and only then incrementing its term — once a majority say they would vote
/// for it. Winning the real election makes it [`Role::Leader`]. Keeping the
/// per-role bookkeeping inside the variant means a follower simply has no
/// `match_index` to get wrong.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Role {
    /// Passive: responds to leaders and candidates, and starts an election if
    /// it stops hearing from a leader.
    Follower,
    /// Running a pre-vote round (thesis §9.6): asking peers whether they *would*
    /// grant a vote, without having incremented `currentTerm` or voted for
    /// itself. A server stuck here — partitioned away, or removed and not yet
    /// aware — never inflates its term, so it cannot disrupt the cluster when
    /// it reconnects.
    PreCandidate {
        /// Servers that said they would grant a vote, including this one. A
        /// [`BTreeSet`] so the count is order-independent.
        pre_votes_granted: BTreeSet<NodeId>,
    },
    /// Actively soliciting votes for its own term.
    Candidate {
        /// Servers that have granted a vote this term, including this one. A
        /// [`BTreeSet`] so the count is order-independent.
        votes_granted: BTreeSet<NodeId>,
    },
    /// Won the election for the current term; replicates the log and sends
    /// heartbeats.
    Leader {
        /// Replication progress for each peer, reinitialised on election
        /// (Figure 2, "Volatile state on leaders"). While a server is being
        /// added it also carries an entry for that non-voting learner, which
        /// [`RaftNode::config`] does not yet list.
        progress: BTreeMap<NodeId, Progress>,
        /// The leadership transfer this leader is running, if any (thesis
        /// §3.10). While it is `Some` the leader accepts no new proposals.
        transfer: Option<LeadershipTransfer>,
        /// The round every `AppendEntries` now carries; bumped by each read.
        heartbeat_round: HeartbeatRound,
    },
}

/// A leader's replication bookkeeping for one peer: Figure 2's "Volatile
/// state on leaders", plus the flow control that pipelines `AppendEntries`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Progress {
    /// The index of the next log entry to send the peer. While
    /// [`Flow::Replicate`] it runs ahead of any ack: it is moved past each
    /// batch as the batch is sent.
    pub next_index: LogIndex,
    /// The highest log index known to be replicated on the peer.
    pub match_index: LogIndex,
    /// How eagerly the leader may send to the peer.
    pub flow: Flow,
    /// The newest [`HeartbeatRound`] the peer has echoed back.
    pub acked_round: HeartbeatRound,
}

impl Progress {
    /// Progress for a peer nothing is yet known about: probe from `next_index`
    /// (one past the leader's last entry), with nothing matched.
    const fn new(next_index: LogIndex) -> Self {
        Self {
            next_index,
            match_index: LogIndex::ZERO,
            flow: Flow::Probe { paused: false },
            acked_round: HeartbeatRound::ZERO,
        }
    }
}

/// A read the leader has accepted but not yet answered (thesis §6.4).
#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingRead {
    id: ReadId,
    /// The first round sent after the read arrived; a quorum must echo it.
    round: HeartbeatRound,
    /// `commitIndex` once an entry of the leader's term has committed (no
    /// sooner: until then it may lag the cluster's). The read is served from
    /// a state machine that has applied at least this far.
    read_index: Option<LogIndex>,
    /// Heartbeat ticks left before the read is failed; see [`READ_TICKS`].
    ticks_left: u32,
}

/// Flow control for one peer's `AppendEntries` stream.
///
/// Figure 2 leaves the send schedule open. Sending the whole unacknowledged
/// tail on every proposal re-sends the same entries over and over while acks
/// are in flight, and waiting for each ack before sending more caps throughput
/// at one batch per round trip. Like etcd/raft, the leader instead **probes**
/// until it learns where the peer's log matches its own, then **pipelines**:
/// it sends each new batch as soon as it exists, advancing `next_index`
/// without waiting, with at most [`ReplicationConfig::max_inflight`] batches
/// unacknowledged.
///
/// None of this affects safety: the §5.3 consistency check on every
/// `AppendEntries` is what keeps logs matching, so a wrong guess here only
/// costs a rejection or a redundant resend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Flow {
    /// Where the peer's log diverges from ours is unknown: one `AppendEntries`
    /// at a time, and `next_index` moves only on a reply — back by one on a
    /// rejection (Figure 2), forward on success. Every peer starts here when a
    /// leader is elected, and returns here after any rejection.
    Probe {
        /// An `AppendEntries` has been sent and not yet answered, so another
        /// waits for the reply or the next heartbeat (which re-sends it in
        /// case it was lost).
        paused: bool,
    },
    /// The peer's log matches ours up to `match_index`: stream new entries
    /// without waiting for acks.
    Replicate {
        /// The last index carried by each unacknowledged batch, oldest first.
        /// An ack frees every batch at or below the index it confirms. When
        /// full, the leader sends nothing but heartbeats; a heartbeat that
        /// finds the peer missing a lost batch is rejected, which drops the
        /// peer back to [`Flow::Probe`] to resend it.
        inflight: VecDeque<LogIndex>,
    },
    /// `next_index` fell to or below the compaction base, so the peer has been
    /// sent the snapshot ([`Effect::SendSnapshot`]). Nothing else goes to it —
    /// not even heartbeats, since the snapshot chunks are leader contact —
    /// until its `InstallSnapshotReply` switches it to [`Flow::Replicate`]
    /// (or to [`Flow::Probe`], which sends the newer snapshot, if the leader
    /// has compacted past the installed one meanwhile), or until `ticks_left`
    /// runs out and it drops back to [`Flow::Probe`], which sends the snapshot
    /// again on the assumption it was lost. Stale `AppendEntries` replies do
    /// not end the wait.
    Snapshot {
        /// Heartbeat ticks left before the snapshot is presumed lost.
        ///
        /// DEVIATION: a real-time retry timeout would fit better, but the pure
        /// core has no clock, so it counts heartbeat ticks instead; see
        /// [`ReplicationConfig::snapshot_retry_ticks`].
        ticks_left: u32,
    },
}

/// Tunables for how a leader streams its log to followers. None of them
/// affects safety, only throughput and message sizes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReplicationConfig {
    /// The most `AppendEntries` batches a leader keeps unacknowledged per peer
    /// while pipelining (see [`Flow::Replicate`]). Values below 1 act as 1.
    pub max_inflight: usize,
    /// The most entry payload, in bytes, one `AppendEntries` carries (see
    /// [`LogEntry`]'s payload sizes). A single entry larger than this is still
    /// sent, alone, so no entry can stall replication.
    pub max_append_bytes: usize,
    /// Heartbeat ticks a leader waits for an `InstallSnapshotReply` before it
    /// presumes the snapshot lost and sends it again (see [`Flow::Snapshot`]).
    /// Set it above the time a whole snapshot takes to transfer and install.
    /// Values below 1 act as 1.
    pub snapshot_retry_ticks: u32,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            max_inflight: 8,
            max_append_bytes: 1024 * 1024,
            // One second at the default 50 ms heartbeat.
            snapshot_retry_ticks: 20,
        }
    }
}

/// A leader's progress on handing leadership to another server (thesis §3.10).
///
/// The leader stops taking new proposals, brings `target`'s log up to date
/// through ordinary replication, and once `target` holds the whole log sends it
/// a [`Message::TimeoutNow`] so it starts an election at once. Kept inside
/// [`Role::Leader`], so a leader that steps down for any reason drops the
/// transfer with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeadershipTransfer {
    /// The voter leadership is being handed to.
    pub target: NodeId,
    /// Heartbeat ticks left before the transfer is abandoned and the leader
    /// takes proposals again.
    ///
    /// DEVIATION: thesis §3.10 aborts a transfer that has not completed within
    /// an election timeout. The pure core has no clock, so it counts heartbeat
    /// ticks instead; see [`TRANSFER_TICKS`].
    pub ticks_left: u32,
}

/// Heartbeat ticks a leadership transfer has to complete in before it is
/// abandoned.
///
/// With the default 50 ms heartbeat and 150–300 ms election timeout
/// that is one maximal election timeout, the bound thesis §3.10 asks for. See
/// [`LeadershipTransfer::ticks_left`].
pub const TRANSFER_TICKS: u32 = 6;

/// Heartbeat ticks a read has to be confirmed in before the leader gives up
/// on it with [`Effect::ReadFailed`].
///
/// A leader cut off from a quorum never confirms a read, and without
/// check-quorum it does not step down on its own either, so the read would
/// otherwise wait for the partition to heal. With the default 50 ms heartbeat
/// and 150–300 ms election timeout this is one maximal election timeout.
///
/// DEVIATION: thesis §6.4 sets no bound on a read; the pure core has no clock,
/// so the bound is counted in heartbeat ticks.
pub const READ_TICKS: u32 = 6;

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
    /// Clients ask the cluster to append and eventually commit commands, in
    /// order. Only a leader acts on it, and not while it is transferring
    /// leadership; anyone else currently drops it (redirecting the client to
    /// the leader is future work). A driver that hands over every proposal
    /// queued since its last step pays one [`Effect::PersistLog`] (one
    /// `fsync`) and one round of `AppendEntries` for the lot.
    Propose {
        /// The opaque command bytes, one log entry each. Empty is a no-op.
        commands: Vec<Bytes>,
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
    /// any other role, by a leader that already has a change in flight or a
    /// still-uncommitted configuration entry, and by one transferring
    /// leadership.
    ChangeMembership {
        /// The change to make.
        change: MembershipChange,
    },
    /// An administrator asks the leader to hand leadership to `target` (thesis
    /// §3.10). Ignored by any other role, and by a leader that is already
    /// transferring, has a membership change in flight, or is asked to transfer
    /// to itself or to a server that is not a voter.
    TransferLeadership {
        /// The voter that should become leader.
        target: NodeId,
    },
    /// A client wants a linearizable read (thesis §6.4, `ReadIndex`). The
    /// leader answers with [`Effect::ReadReady`] once it has confirmed it
    /// still leads and its state machine has caught up to the read's index —
    /// or [`Effect::ReadFailed`] if it is not leader, stops leading first, or
    /// cannot confirm the read within [`READ_TICKS`] heartbeats. Nothing is
    /// written to the log.
    ReadIndex {
        /// The driver's handle for the read, echoed in the answer.
        id: ReadId,
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
    /// The read `id` is safe to serve now: query the state machine as it
    /// stands. The state machine has applied at least `read_index`, and this
    /// effect follows every [`Effect::ApplyToStateMachine`] of the same step,
    /// so performing effects in order is enough.
    ReadReady {
        /// The handle from [`Input::ReadIndex`].
        id: ReadId,
        /// The leader's `commitIndex` the read is linearized at.
        read_index: LogIndex,
    },
    /// The read `id` cannot be served by this node: it is not the leader,
    /// stopped leading before the read was confirmed, or could not confirm it
    /// within [`READ_TICKS`] heartbeats. The client may retry, ideally against
    /// the current leader.
    ReadFailed {
        /// The handle from [`Input::ReadIndex`].
        id: ReadId,
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
    /// Whether this node has heard from a current-term leader since its
    /// election timer last fired. The driver only delivers
    /// [`Input::ElectionTimeout`] after a full randomized timeout with no
    /// [`Effect::ResetElectionTimer`], so while this is `true` a leader was
    /// alive within the last minimum election timeout. Used to withhold
    /// (pre-)votes from a would-be candidate while a leader still holds office
    /// (thesis §4.2.3 / §9.6). Set on accepted `AppendEntries` /
    /// `InstallSnapshot`; cleared on timeout and on entering a (pre-)candidacy.
    heard_from_leader: bool,
    /// The pre-vote round this node last started (thesis §9.6). Written **only**
    /// by [`RaftNode::become_pre_candidate`], which bumps it in the same breath
    /// as it creates the fresh tally, so the two can never disagree; a reply
    /// carrying any other round belongs to a round that is over and is dropped.
    pre_vote_round: PreVoteRound,
    /// While this node leads and has a membership change under way, its
    /// progress on it. At most one change is in flight at a time.
    pending_change: Option<PendingChange>,
    /// How this node streams its log while it leads.
    replication: ReplicationConfig,
    /// Reads accepted while leading and not yet answered, oldest first.
    reads: Vec<PendingRead>,
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
            heard_from_leader: false,
            pre_vote_round: PreVoteRound::ZERO,
            pending_change: None,
            replication: ReplicationConfig::default(),
            reads: Vec::new(),
        };
        node.recompute_config();
        node
    }

    /// Replaces the default [`ReplicationConfig`].
    #[must_use]
    pub const fn with_replication(mut self, replication: ReplicationConfig) -> Self {
        self.replication = replication;
        self
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
    /// election deadlines in the driver and derives "a leader is still alive"
    /// from whether [`Input::ElectionTimeout`] has fired
    /// (`RaftNode::heard_from_leader`), so the core never has to compare
    /// instants; `now` stays in the signature for a future feature that will.
    #[must_use]
    pub fn step(&mut self, input: Input, now: LogicalInstant) -> Vec<Effect> {
        let _ = now;
        let mut effects = self.dispatch(input);
        self.settle_reads(&mut effects);
        effects
    }

    /// Routes one input to its handler.
    fn dispatch(&mut self, input: Input) -> Vec<Effect> {
        match input {
            Input::Deliver { from, message } => match message {
                Message::RequestVote(args) => self.handle_request_vote(args),
                Message::RequestVoteReply(reply) => self.handle_request_vote_reply(from, reply),
                Message::PreVote(args) => self.handle_pre_vote(&args),
                Message::PreVoteReply(reply) => self.handle_pre_vote_reply(from, reply),
                Message::AppendEntries(args) => self.handle_append_entries(&args),
                Message::AppendEntriesReply(reply) => self.handle_append_entries_reply(from, reply),
                Message::InstallSnapshot(args) => self.handle_install_snapshot(&args),
                Message::InstallSnapshotReply(reply) => {
                    self.handle_install_snapshot_reply(from, reply)
                }
                Message::TimeoutNow(args) => self.handle_timeout_now(args),
            },
            Input::ElectionTimeout => self.handle_election_timeout(),
            Input::HeartbeatTick => self.handle_heartbeat_tick(),
            Input::Propose { commands } => self.handle_propose(commands),
            Input::SnapshotInstalled {
                last_included_index,
                last_included_term,
                config,
            } => self.handle_snapshot_installed(last_included_index, last_included_term, config),
            Input::CompactLog { up_to_index } => self.handle_compact_log(up_to_index),
            Input::ChangeMembership { change } => self.handle_change_membership(change),
            Input::TransferLeadership { target } => self.handle_transfer_leadership(target),
            Input::ReadIndex { id } => self.handle_read_index(id),
        }
    }

    /// Handles [`Input::ReadIndex`] (thesis §6.4). A non-leader fails the read
    /// at once. A leader starts a new [`HeartbeatRound`] and sends it to every
    /// peer straight away; [`RaftNode::settle_reads`] answers the read once a
    /// quorum has echoed that round.
    fn handle_read_index(&mut self, id: ReadId) -> Vec<Effect> {
        let Role::Leader {
            heartbeat_round, ..
        } = &mut self.role
        else {
            return vec![Effect::ReadFailed { id }];
        };
        *heartbeat_round = heartbeat_round.next();
        self.reads.push(PendingRead {
            id,
            round: *heartbeat_round,
            read_index: None,
            ticks_left: READ_TICKS,
        });
        self.replicate_to_peers(true)
    }

    /// Runs after every input: answers each pending read that has become
    /// servable, and fails them all once this node no longer leads — however
    /// it stopped, so no step-down path can strand a read.
    ///
    /// A read takes `commitIndex` as its read index only once an entry of this
    /// leader's term has committed (until then `commitIndex` may lag the
    /// cluster's). It is ready when a quorum has echoed its round and
    /// `lastApplied` has reached its read index.
    fn settle_reads(&mut self, effects: &mut Vec<Effect>) {
        if self.reads.is_empty() {
            return;
        }
        if !self.is_leader() {
            effects.extend(
                self.reads
                    .drain(..)
                    .map(|read| Effect::ReadFailed { id: read.id }),
            );
            return;
        }
        if self.committed_in_current_term() {
            let commit_index = self.commit_index;
            for read in &mut self.reads {
                read.read_index.get_or_insert(commit_index);
            }
        }
        let confirmed = self.confirmed_round();
        let last_applied = self.last_applied;
        self.reads.retain(|read| match read.read_index {
            Some(read_index) if read.round <= confirmed && read_index <= last_applied => {
                effects.push(Effect::ReadReady {
                    id: read.id,
                    read_index,
                });
                false
            }
            _ => true,
        });
    }

    /// The newest [`HeartbeatRound`] a quorum of voters has echoed, this
    /// leader counting as having echoed its own current round. Zero when not
    /// leading.
    fn confirmed_round(&self) -> HeartbeatRound {
        let Role::Leader {
            progress,
            heartbeat_round,
            ..
        } = &self.role
        else {
            return HeartbeatRound::ZERO;
        };
        // Only voters count, as for commitment.
        let mut acked: Vec<HeartbeatRound> = progress
            .iter()
            .filter(|(peer, _)| self.config.contains(**peer))
            .map(|(_, p)| p.acked_round)
            .collect();
        acked.push(*heartbeat_round);
        acked.sort_unstable_by_key(|&round| std::cmp::Reverse(round));
        acked[self.quorum() - 1]
    }

    /// Handles an incoming `RequestVote` (Figure 2).
    ///
    /// Disregards the request outright — no term adoption, no vote, no timer
    /// reset — while this server is still hearing from a leader (thesis
    /// §4.2.3): a server that was removed or partitioned away, and whose own
    /// `RequestVote` therefore carries an inflated term, must not be able to
    /// force a sitting leader out. (Pre-vote already stops the term inflating
    /// in the first place; this covers a straggler that missed the leader's
    /// last heartbeat but whose peers did not.) The exception is a request
    /// flagged `leadership_transfer`: that leader sent the candidate a
    /// `TimeoutNow` precisely so it would be replaced (thesis §3.10).
    /// Otherwise: steps down if the candidate's term is newer, rejects outright if it is older, then grants
    /// the vote only when this server has not already voted for someone else
    /// this term and the candidate's log is at least as up-to-date as ours
    /// (§5.4.1). Any change to `currentTerm` / `votedFor` is emitted as
    /// [`Effect::Persist`] before the reply (persist-before-reply).
    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> Vec<Effect> {
        let before = (self.current_term, self.voted_for);
        let mut effects = Vec::new();

        // §4.2.3: while a leader is still in contact it outranks any lone
        // campaigner. Reply with our unchanged term so a genuinely newer
        // candidate learns nothing it can use, and retry once our own timer
        // fires and clears `heard_from_leader`.
        if self.heard_from_leader && !args.leadership_transfer {
            effects.push(self.request_vote_reply(args.candidate_id, false));
            return effects;
        }

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

    /// Handles an incoming `PreVote` straw poll (thesis §9.6).
    ///
    /// This is a query, not a vote: it never changes `currentTerm`, `votedFor`,
    /// the role, or anything on disk, so a partitioned or removed server that
    /// keeps asking can never disrupt the cluster. The reply says whether this
    /// server *would* grant a real vote right now — it would, when it is
    /// neither a leader itself nor hearing from one (thesis §4.2.3 / §9.6), the
    /// would-be term in `args.term` is not stale, and the pre-candidate's log
    /// is at least as up-to-date as ours (§5.4.1). The reply always carries our
    /// real `currentTerm` so a pre-candidate a term behind can catch up.
    ///
    /// A leader must refuse: in a three-node cluster a single peer that can
    /// still reach the leader would otherwise get the leader's "yes" plus its
    /// own and reach a quorum, promote to a real candidate, and unseat the very
    /// leader it just polled.
    ///
    /// `args.round` is echoed untouched so the pre-candidate can tell which of
    /// its rounds this answer belongs to; this server never interprets it.
    fn handle_pre_vote(&self, args: &PreVoteArgs) -> Vec<Effect> {
        let would_grant = !self.is_leader()
            && !self.heard_from_leader
            && args.term >= self.current_term
            && self.candidate_log_is_up_to_date(args.last_log_index, args.last_log_term);
        vec![Effect::SendRpc {
            to: args.candidate_id,
            message: Message::PreVoteReply(PreVoteReply {
                term: self.current_term,
                vote_granted: would_grant,
                round: args.round,
            }),
        }]
    }

    /// Handles a `PreVoteReply` while we are still pre-campaigning.
    ///
    /// A reply from a genuinely newer term steps us down (and is the one way a
    /// pre-vote round touches persistent state). That check comes first and
    /// ignores the round: a newer term is real news whichever poll carried it.
    /// Otherwise a "yes" is tallied only if it answers the round we are
    /// *currently* running — a peer that consented while it heard no leader may
    /// well have one again by the next round, and spending that stale consent
    /// would promote us on support we no longer have (see [`PreVoteRound`]).
    /// A majority of live "yes"es promotes us out of the straw poll into a real
    /// election via [`RaftNode::promote_pre_candidate_if_quorum`].
    fn handle_pre_vote_reply(&mut self, from: NodeId, reply: PreVoteReply) -> Vec<Effect> {
        let before = (self.current_term, self.voted_for);
        let mut effects = Vec::new();

        if reply.term > self.current_term {
            self.become_follower(reply.term);
            self.persist_if_changed(before, &mut effects);
            return effects;
        }

        // A stale responder, a reply to a round we have already left behind, or
        // one that outlived the pre-candidacy itself.
        if reply.term < self.current_term
            || reply.round != self.pre_vote_round
            || !self.is_pre_candidate()
        {
            return effects;
        }

        if reply.vote_granted {
            if let Role::PreCandidate { pre_votes_granted } = &mut self.role {
                pre_votes_granted.insert(from);
            }
            effects.extend(self.promote_pre_candidate_if_quorum());
        }
        effects
    }

    /// Handles a `TimeoutNow` from a leader handing leadership to this server
    /// (thesis §3.10): start a real election at once, as if the election timer
    /// had fired, but without the pre-vote round — the leader has already
    /// checked this server holds its whole log, and its peers would refuse a
    /// pre-vote while they still hear from that leader. The `RequestVote`s it
    /// sends carry `leadership_transfer` so they are not refused for the same
    /// reason.
    ///
    /// A `TimeoutNow` from an older term is stale — most likely a duplicate
    /// arriving after the election it asked for — and is dropped. A newer term
    /// is adopted first. A leader, or a server absent from its own
    /// configuration, does not campaign.
    fn handle_timeout_now(&mut self, args: TimeoutNowArgs) -> Vec<Effect> {
        let before = (self.current_term, self.voted_for);
        if args.term < self.current_term {
            return Vec::new();
        }
        if args.term > self.current_term {
            self.become_follower(args.term);
        }
        if self.is_leader() || !self.config.contains(self.id) {
            let mut effects = Vec::new();
            self.persist_if_changed(before, &mut effects);
            return effects;
        }
        self.start_real_election(true)
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
            effects.push(self.append_entries_reply(args, false, LogIndex::ZERO, None));
            return effects;
        }

        // A newer term, or the same term while we still think we are a
        // (pre-)candidate: the sender is the leader for this term, so step down
        // and abandon any campaign.
        if args.term > self.current_term || self.is_candidate() || self.is_pre_candidate() {
            self.become_follower(args.term);
        }
        self.leader_id = Some(args.leader_id);
        // A live leader for our term: withhold (pre-)votes until our election
        // timer next fires (thesis §4.2.3 / §9.6).
        self.heard_from_leader = true;
        self.persist_if_changed(before, &mut effects);

        // Receiver rule 2: the log must contain `prev_log_index` with a
        // matching term, or this is the empty-log base case. An anchor below
        // our snapshot also matches: a snapshot only covers committed entries,
        // which every current leader holds (Leader Completeness), so by Log
        // Matching our prefix up to it is the leader's. Only a delayed or
        // duplicated RPC from before we compacted is anchored there.
        let consistent = args.prev_log_index == LogIndex::ZERO
            || args.prev_log_index < self.log.snapshot_last_index()
            || self.log.term_at(args.prev_log_index) == Some(args.prev_log_term);
        if !consistent {
            let conflict = self.conflict_at(args.prev_log_index);
            effects.push(Effect::ResetElectionTimer);
            effects.push(self.append_entries_reply(args, false, LogIndex::ZERO, Some(conflict)));
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
        // this RPC actually covered. Compare *after* bounding: an RPC anchored
        // below our `commitIndex` covers less than we already know is
        // committed, and `commitIndex` never moves back (§9.9).
        let leader_commit = args.leader_commit.min(last_covered);
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit;
            self.apply_committed(&mut effects);
        }

        effects.push(Effect::ResetElectionTimer);
        effects.push(self.append_entries_reply(args, true, last_covered, None));
        effects
    }

    /// The hint for a rejected `AppendEntries` anchored at `prev_log_index`
    /// (paper §5.3): our term there and the first index we hold at that term,
    /// so the leader can step back over the whole term at once; or, when our
    /// log ends before `prev_log_index`, just past our last entry.
    fn conflict_at(&self, prev_log_index: LogIndex) -> Conflict {
        let term = self.log.term_at(prev_log_index);
        let index = if term.is_some() {
            self.log.term_start(prev_log_index)
        } else {
            self.log.last_index().next()
        };
        Conflict { term, index }
    }

    /// Applies `AppendEntries` receiver rules 3 & 4. Walks `entries` against
    /// the log from just after `prev_log_index`; at the first index whose term
    /// disagrees (or that the log lacks) it truncates the tail and appends the
    /// remainder. Entries already present with a matching term are left as they
    /// are, so a delayed or duplicated RPC never rewinds the log (Log
    /// Matching, Leader Append-Only on the leader's behalf).
    ///
    /// Entries at or below the compaction base are skipped: the snapshot
    /// already holds them, committed, so they cannot conflict, and the
    /// snapshot itself is never truncated.
    ///
    /// Returns `Some(from_index)` — the 1-based index of the first entry
    /// written — when the log changed, or `None` when every entry was already
    /// present.
    fn splice_entries(
        &mut self,
        prev_log_index: LogIndex,
        entries: &[LogEntry],
    ) -> Option<LogIndex> {
        let base = self.log.snapshot_last_index();
        let mut index = prev_log_index;
        for (offset, entry) in entries.iter().enumerate() {
            index = index.next();
            if index <= base || self.log.term_at(index) == Some(entry.term) {
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
    /// `next_index[from]` forward, may advance `commit_index` (§5.4.2), and
    /// sends `from` whatever it is still missing; failure steps
    /// `next_index[from]` back — over a whole conflicting term when the reply
    /// carries a hint — and retries straight away.
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

        // Any reply at our term shows `from` still followed us when it
        // handled that round (thesis §6.4).
        if let Role::Leader { progress, .. } = &mut self.role
            && let Some(p) = progress.get_mut(&from)
        {
            p.acked_round = p.acked_round.max(reply.heartbeat_round);
        }

        if reply.success {
            self.record_match(from, reply.match_index);
            self.maybe_advance_commit_index(&mut effects);
            self.advance_catch_up(from, &mut effects);
            effects.extend(self.timeout_now_if_caught_up(from));
        } else {
            self.back_off(from, reply.conflict);
        }
        effects.extend(self.replicate_to(from, false));
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

        if args.term > self.current_term || self.is_candidate() || self.is_pre_candidate() {
            self.become_follower(args.term);
        }
        self.leader_id = Some(args.leader_id);
        self.heard_from_leader = true;
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
        // The snapshot was installed, so the wait is over even if we have
        // compacted past it since: probe again, which sends the newer one.
        if let Role::Leader { progress, .. } = &mut self.role
            && let Some(p) = progress.get_mut(&from)
            && matches!(p.flow, Flow::Snapshot { .. })
        {
            p.flow = Flow::Probe { paused: false };
        }
        self.maybe_advance_commit_index(&mut effects);
        effects.extend(self.timeout_now_if_caught_up(from));
        effects.extend(self.replicate_to(from, false));
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
        self.last_applied = self.last_applied.max(last_included_index);
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
            // The snapshot replaces every `Config` entry up to `up_to_index`,
            // so the fallback configuration must become the one in force
            // there — not stay the bootstrap set.
            self.base_config = self.config_at(up_to_index);
            self.log.compact(up_to_index, term);
            self.recompute_config();
        }
        Vec::new()
    }

    /// Handles [`Input::ChangeMembership`] (thesis §4). A no-op unless this
    /// server leads, has no change already in flight, and has committed an
    /// entry of its own term. `RemoveServer` appends the new configuration at
    /// once; `AddServer` first replicates to the new server as a non-voting
    /// learner (see [`ChangePhase::CatchingUp`]).
    ///
    /// Together those also mean the latest configuration entry is committed:
    /// this leader's own is in flight until it commits, and a predecessor's
    /// sits below the no-op, so it commits no later than the no-op does.
    fn handle_change_membership(&mut self, change: MembershipChange) -> Vec<Effect> {
        if !self.is_leader()
            || self.is_transferring()
            || self.pending_change.is_some()
            || !self.committed_in_current_term()
        {
            return Vec::new();
        }

        match change {
            MembershipChange::AddServer(id) => {
                if id == self.id || self.config.contains(id) {
                    return Vec::new();
                }
                let next = self.log.last_index().next();
                if let Role::Leader { progress, .. } = &mut self.role {
                    progress.insert(id, Progress::new(next));
                }
                self.pending_change = Some(PendingChange {
                    change,
                    phase: ChangePhase::CatchingUp {
                        round_target: self.log.last_index(),
                        ticks_left: CATCH_UP_TICKS,
                    },
                });
                self.replicate_to(id, true)
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

    /// Handles [`Input::TransferLeadership`] (thesis §3.10). A no-op unless this
    /// server leads, is not already transferring, has no membership change in
    /// flight (one could remove `target` under us), and `target` is a voter
    /// other than itself. Otherwise the leader stops accepting proposals and
    /// either sends `target` a [`Message::TimeoutNow`] straight away, when it
    /// already holds the whole log, or replicates to it and waits for the ack
    /// that says it does.
    fn handle_transfer_leadership(&mut self, target: NodeId) -> Vec<Effect> {
        if !self.is_leader()
            || self.is_transferring()
            || self.pending_change.is_some()
            || target == self.id
            || !self.config.contains(target)
        {
            return Vec::new();
        }
        if let Role::Leader { transfer, .. } = &mut self.role {
            *transfer = Some(LeadershipTransfer {
                target,
                ticks_left: TRANSFER_TICKS,
            });
        }
        self.timeout_now_if_caught_up(target).map_or_else(
            || self.replicate_to(target, true),
            |timeout_now| vec![timeout_now],
        )
    }

    /// While transferring leadership to `peer`, the [`Message::TimeoutNow`] to
    /// send it once its `matchIndex` has reached the end of our log — the point
    /// at which it would win an election (§5.4.1). `None` otherwise.
    ///
    /// Called on every ack from the target, not just the first that reaches
    /// the end, so a `TimeoutNow` the network loses is sent again on the next
    /// heartbeat's ack. A duplicate is harmless: by the time it lands the
    /// target has moved to a newer term and drops it as stale.
    fn timeout_now_if_caught_up(&self, peer: NodeId) -> Option<Effect> {
        let Role::Leader {
            transfer: Some(LeadershipTransfer { target, .. }),
            ..
        } = &self.role
        else {
            return None;
        };
        let matched = self.match_index_for(peer);
        (peer == *target && matched >= self.log.last_index()).then(|| Effect::SendRpc {
            to: peer,
            message: Message::TimeoutNow(TimeoutNowArgs {
                term: self.current_term,
                leader_id: self.id,
            }),
        })
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
        // The peers' progress stays as it is: a promoted learner
        // already has entries, and a peer being removed keeps its own until the
        // removal commits (`check_config_committed` drops them), so it is sent
        // the entry that removes it and does not sit there timing out.
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
        effects.extend(self.replicate_to_peers(false));
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
        let matched = self.match_index_for(id);
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
            && let Role::Leader { progress, .. } = &mut self.role
        {
            progress.remove(&id);
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
            && let Role::Leader { progress, .. } = &mut self.role
        {
            progress.remove(&id);
        }
        effects.push(Effect::MembershipChanged {
            config: self.config.clone(),
        });
        if !self.config.contains(self.id) && self.is_leader() {
            self.leader_id = None;
            self.role = Role::Follower;
        }
    }

    /// Records a successful ack: `match_index[peer]` only moves forward (a
    /// reordered older ack cannot pull it back) and `next_index[peer]` follows.
    /// The peer's log is now known to match ours up to there, so it streams
    /// ([`Flow::Replicate`]), with every batch the ack covers freed. A peer
    /// waiting on a snapshot keeps waiting unless the ack carries it past the
    /// compaction base: a stale `AppendEntries` ack must not trigger a resend.
    fn record_match(&mut self, peer: NodeId, matched: LogIndex) {
        let base = self.log.snapshot_last_index();
        if let Role::Leader { progress, .. } = &mut self.role
            && let Some(p) = progress.get_mut(&peer)
        {
            p.match_index = matched.max(p.match_index);
            p.next_index = matched.next().max(p.next_index);
            match &mut p.flow {
                Flow::Replicate { inflight } => {
                    while inflight.front().is_some_and(|&last| last <= p.match_index) {
                        inflight.pop_front();
                    }
                }
                Flow::Snapshot { .. } if p.next_index <= base => {}
                Flow::Probe { .. } | Flow::Snapshot { .. } => {
                    p.flow = Flow::Replicate {
                        inflight: VecDeque::new(),
                    };
                }
            }
        }
    }

    /// On a failed `AppendEntries` the peer goes back to [`Flow::Probe`],
    /// unpaused so the retry goes out at once. A peer that was already probing
    /// steps `next_index` back by one (Figure 2, never below 1) to probe
    /// further back; one that was streaming restarts just past `match_index`,
    /// the last point it acknowledged.
    ///
    /// A `conflict` hint (paper §5.3) can only pull that retry point further
    /// back. With a conflicting term we hold too, the follower's entries of it
    /// may match ours, so resume just past our last entry of that term;
    /// otherwise the whole run is foreign and we resume at the follower's
    /// `conflict.index` (or, for a short log, just past its end). Taking the
    /// lower of the two rules means a stale or duplicated hint costs at most a
    /// retransmission and never skips an entry the follower lacks.
    ///
    /// Probing is not floored at `match_index`: a follower that lost its
    /// storage (or a stale rejection) can send `next_index` below it, and a
    /// floor there would leave such a follower rejecting the same probe
    /// forever.
    ///
    /// A peer waiting on a snapshot ignores the rejection: it can only answer
    /// an `AppendEntries` sent before the snapshot, which the snapshot
    /// supersedes.
    fn back_off(&mut self, peer: NodeId, conflict: Option<Conflict>) {
        let hinted = conflict.map(|c| {
            c.term
                .and_then(|term| self.log.last_index_of_term(term))
                .map_or(c.index, LogIndex::next)
        });
        if let Role::Leader { progress, .. } = &mut self.role
            && let Some(p) = progress.get_mut(&peer)
        {
            let mut retry_from = match p.flow {
                Flow::Probe { .. } => p.next_index.prev(),
                Flow::Replicate { .. } => p.match_index.next(),
                Flow::Snapshot { .. } => return,
            };
            if let Some(hinted) = hinted {
                retry_from = retry_from.min(hinted);
            }
            p.next_index = retry_from.max(LogIndex::new(1));
            p.flow = Flow::Probe { paused: false };
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
            let Role::Leader { progress, .. } = &self.role else {
                return;
            };
            // Only voters count: a still-catching-up learner has a
            // progress entry but is not in the configuration.
            let mut matched: Vec<LogIndex> = progress
                .iter()
                .filter(|(peer, _)| self.config.contains(**peer))
                .map(|(_, p)| p.match_index)
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

    /// Handles the election timer firing: a follower or (pre-)candidate starts a
    /// fresh **pre-vote** round (thesis §9.6). It asks every peer whether they
    /// would vote for it at `currentTerm + 1` *without* incrementing its own
    /// term or voting for itself; only [`RaftNode::promote_pre_candidate_if_quorum`],
    /// on a majority of "yes" replies, does that and starts the real election.
    ///
    /// A server that keeps timing out in isolation therefore never inflates its
    /// term, so it cannot force a sitting leader to step down when it reconnects.
    /// Term monotonicity still holds by construction —
    /// [`RaftNode::become_candidate`] only ever calls [`Term::next`].
    fn handle_election_timeout(&mut self) -> Vec<Effect> {
        // A leader never re-elects, and a server absent from its own
        // configuration (a not-yet-added learner, or one just removed) stays
        // passive so it cannot disrupt the cluster it is joining or leaving.
        if self.is_leader() || !self.config.contains(self.id) {
            return Vec::new();
        }

        self.become_pre_candidate();

        // A pre-vote round changes nothing on disk, so there is nothing to
        // persist before asking.
        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();
        let mut effects: Vec<Effect> = self
            .peers
            .iter()
            .map(|&peer| Effect::SendRpc {
                to: peer,
                message: Message::PreVote(PreVoteArgs {
                    // The term we *would* campaign in, not one we have adopted.
                    term: self.current_term.next(),
                    candidate_id: self.id,
                    last_log_index,
                    last_log_term,
                    round: self.pre_vote_round,
                }),
            })
            .collect();
        effects.push(Effect::ResetElectionTimer);
        // A lone voter wins its own pre-vote (and then its own election).
        effects.extend(self.promote_pre_candidate_if_quorum());
        effects
    }

    /// Handles the heartbeat interval elapsing: a leader re-sends
    /// `AppendEntries` to reassert authority (carrying any entries a peer, or a
    /// catching-up learner, has yet to acknowledge); every other role ignores
    /// it. Each tick also spends one of a pending `AddServer`'s catch-up
    /// budget, abandoning it if the new server never keeps up, and one of a
    /// leadership transfer's, abandoning the transfer — and taking proposals
    /// again — if the target has not taken over by then (thesis §3.10).
    fn handle_heartbeat_tick(&mut self) -> Vec<Effect> {
        if !self.is_leader() {
            return Vec::new();
        }
        if let Role::Leader { transfer, .. } = &mut self.role
            && let Some(pending) = transfer
        {
            pending.ticks_left = pending.ticks_left.saturating_sub(1);
            if pending.ticks_left == 0 {
                *transfer = None;
            }
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
        self.expire_snapshot_waits();
        let mut effects = self.expire_reads();
        effects.extend(self.replicate_to_peers(true));
        effects
    }

    /// Counts a heartbeat tick against every pending read and fails those
    /// whose [`READ_TICKS`] budget has run out.
    fn expire_reads(&mut self) -> Vec<Effect> {
        let mut failed = Vec::new();
        self.reads.retain_mut(|read| {
            read.ticks_left = read.ticks_left.saturating_sub(1);
            if read.ticks_left == 0 {
                failed.push(Effect::ReadFailed { id: read.id });
            }
            read.ticks_left > 0
        });
        failed
    }

    /// Handles client proposals (Figure 2, "Clients of Raft").
    ///
    /// Only a leader acts: it appends one entry per command, in order, at its
    /// current term, makes the new tail durable with a single
    /// [`Effect::PersistLog`], then sends every peer the entries it is missing.
    /// Any other role drops the
    /// proposal — redirecting the client to the leader is future work — and so
    /// does a leader transferring leadership (thesis §3.10), so the target can
    /// catch up with a log that has stopped growing.
    ///
    /// A majority still has to store the entry before it commits (§5.4.2), so
    /// on a multi-node cluster nothing is applied until the acks come back. A
    /// lone leader is its own majority, so `maybe_advance_commit_index` commits
    /// and applies the entry in this same step.
    fn handle_propose(&mut self, commands: Vec<Bytes>) -> Vec<Effect> {
        if !self.is_leader() || self.is_transferring() || commands.is_empty() {
            return Vec::new();
        }

        let from_index = self.log.last_index().next();
        for command in commands {
            self.log
                .append(LogEntry::command(self.current_term, command));
        }

        let mut effects = vec![Effect::PersistLog {
            from_index,
            entries: self.log.entries_from(from_index).to_vec(),
        }];
        effects.extend(self.replicate_to_peers(false));
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
            // A new term we did not reach via a leader's RPC: assume no leader
            // until one contacts us. `handle_append_entries` /
            // `handle_install_snapshot` set this back to `true` right after
            // calling us when the step-down *was* a leader's doing.
            self.heard_from_leader = false;
        }
        self.pending_change = None;
        self.role = Role::Follower;
    }

    /// Starts a pre-vote round (thesis §9.6): unlike [`RaftNode::become_candidate`]
    /// this does **not** touch `currentTerm` or `votedFor`, so a server that
    /// keeps pre-voting in isolation never inflates its term. Every
    /// [`Input::ElectionTimeout`] begins a fresh round with the pre-candidate's
    /// own straw-poll vote already counted.
    ///
    /// Numbering the round and emptying the tally happen together and nowhere
    /// else, which is what stops a "yes" from one round being counted in the
    /// next ([`PreVoteRound`]).
    fn become_pre_candidate(&mut self) {
        self.leader_id = None;
        self.heard_from_leader = false;
        self.pending_change = None;
        self.pre_vote_round = self.pre_vote_round.next();
        let mut pre_votes_granted = BTreeSet::new();
        pre_votes_granted.insert(self.id);
        self.role = Role::PreCandidate { pre_votes_granted };
    }

    /// Starts a candidacy: bump the term, vote for self, record that one vote.
    fn become_candidate(&mut self) {
        self.current_term = self.current_term.next();
        self.voted_for = Some(self.id);
        self.leader_id = None;
        self.heard_from_leader = false;
        self.pending_change = None;
        let mut votes_granted = BTreeSet::new();
        votes_granted.insert(self.id);
        self.role = Role::Candidate { votes_granted };
    }

    /// Becomes leader for the current term and reinitialises the per-peer
    /// replication bookkeeping (Figure 2).
    fn become_leader(&mut self) {
        let next = self.log.last_index().next();
        let progress = self
            .peers
            .iter()
            .map(|&peer| (peer, Progress::new(next)))
            .collect();
        self.leader_id = Some(self.id);
        self.role = Role::Leader {
            progress,
            transfer: None,
            heartbeat_round: HeartbeatRound::ZERO,
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
            if let Role::Leader { progress, .. } = &mut self.role {
                progress
                    .entry(affected)
                    .or_insert_with(|| Progress::new(next));
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
        self.config_at(config_index.prev())
    }

    /// If we are a candidate holding a quorum of votes, become leader and
    /// return the initial `AppendEntries` to every peer — empty while the log
    /// has nothing past their `next_index` — that stop peers from starting
    /// their own elections.
    fn promote_if_quorum(&mut self) -> Vec<Effect> {
        let has_quorum = match &self.role {
            Role::Candidate { votes_granted } => votes_granted.len() >= self.quorum(),
            Role::Follower | Role::PreCandidate { .. } | Role::Leader { .. } => false,
        };
        if !has_quorum {
            return Vec::new();
        }

        self.become_leader();
        // Open the term with a no-op (thesis §6.4): `next_index` was set just
        // past the old log, so the opening heartbeats already carry it.
        let from_index = self.log.last_index().next();
        self.log.append(LogEntry::noop(self.current_term));
        let mut effects = vec![Effect::PersistLog {
            from_index,
            entries: self.log.entries_from(from_index).to_vec(),
        }];
        effects.extend(self.replicate_to_peers(true));
        // A single-server cluster commits it on the spot.
        self.maybe_advance_commit_index(&mut effects);
        effects
    }

    /// Whether an entry of `currentTerm` has committed — on a leader, its
    /// opening no-op. Terms never decrease along the log and a leader's own
    /// entries sit at its tail, so that is exactly when the entry at
    /// `commitIndex` is from this term. Until then a new leader's
    /// `commitIndex` may lag the cluster's.
    fn committed_in_current_term(&self) -> bool {
        self.log.term_at(self.commit_index) == Some(self.current_term)
    }

    /// If we are a pre-candidate whose straw poll has reached a quorum of "yes"
    /// replies, the cluster would elect us: now actually start the election
    /// with [`RaftNode::start_real_election`]. Otherwise nothing happens and we
    /// keep collecting replies until the next timeout starts a fresh round.
    fn promote_pre_candidate_if_quorum(&mut self) -> Vec<Effect> {
        let has_quorum = match &self.role {
            Role::PreCandidate { pre_votes_granted } => pre_votes_granted.len() >= self.quorum(),
            Role::Follower | Role::Candidate { .. } | Role::Leader { .. } => false,
        };
        if !has_quorum {
            return Vec::new();
        }
        self.start_real_election(false)
    }

    /// Increments the term, votes for self, makes both durable, and solicits
    /// real votes from every peer. Reached from a won pre-vote round (thesis
    /// §9.6), so by the time the term moves a majority has already said it
    /// would elect us — or from a leader's `TimeoutNow` (thesis §3.10), in
    /// which case `leadership_transfer` is set on every `RequestVote`.
    fn start_real_election(&mut self, leadership_transfer: bool) -> Vec<Effect> {
        self.become_candidate();

        // currentTerm and votedFor both just changed; make them durable before
        // asking anyone for a vote.
        let mut effects = vec![Effect::Persist {
            current_term: self.current_term,
            voted_for: self.voted_for,
        }];
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
                    leadership_transfer,
                }),
            });
        }
        effects.push(Effect::ResetElectionTimer);
        // A single-node cluster reaches quorum on its own vote right away.
        effects.extend(self.promote_if_quorum());
        effects
    }

    /// Spends one heartbeat tick of every [`Flow::Snapshot`] wait. A peer whose
    /// budget runs out drops back to [`Flow::Probe`], so the replication that
    /// follows sends it the snapshot again.
    fn expire_snapshot_waits(&mut self) {
        if let Role::Leader { progress, .. } = &mut self.role {
            for p in progress.values_mut() {
                if let Flow::Snapshot { ticks_left } = &mut p.flow {
                    *ticks_left = ticks_left.saturating_sub(1);
                    if *ticks_left == 0 {
                        p.flow = Flow::Probe { paused: false };
                    }
                }
            }
        }
    }

    /// Replicates to every peer (see [`RaftNode::replicate_to`]). With
    /// `force` (on winning an election and on `HeartbeatTick`) every peer is
    /// sent something, a heartbeat at least; without it (on `Propose` and on a
    /// new configuration entry) only peers whose flow control has room are.
    fn replicate_to_peers(&mut self, force: bool) -> Vec<Effect> {
        let peers: Vec<NodeId> = match &self.role {
            // Voters plus any catching-up learner (which has a progress entry
            // but is not yet in the configuration).
            Role::Leader { progress, .. } => progress.keys().copied().collect(),
            Role::Follower | Role::PreCandidate { .. } | Role::Candidate { .. } => Vec::new(),
        };
        peers
            .into_iter()
            .flat_map(|peer| self.replicate_to(peer, force))
            .collect()
    }

    /// Builds the replication effects for one peer that its flow control
    /// allows, and records the sends in its [`Progress`].
    ///
    /// If the peer's `next_index` still points into the log, that is an
    /// `AppendEntries`: the entry just before `next_index` is the §5.3
    /// consistency-check anchor (`prev_log_index` / `prev_log_term`), and the
    /// entries from `next_index` on ride along as `entries`, up to
    /// [`ReplicationConfig::max_append_bytes`] of them. If `next_index` has
    /// fallen to or below the compaction base, the leader no longer has
    /// `prev_log_term`, so it sends the snapshot instead
    /// ([`Effect::SendSnapshot`]) and the peer waits in [`Flow::Snapshot`],
    /// sent nothing at all — even with `force` — until that resolves.
    ///
    /// A [`Flow::Probe`] peer gets one `AppendEntries`, and nothing while it
    /// is waiting on a reply. A [`Flow::Replicate`] peer gets successive
    /// batches until it has the whole log or its window is full, and nothing
    /// when it has no room or nothing new. With `force` a peer that would get
    /// nothing still gets an `AppendEntries` — an empty heartbeat in the
    /// `Replicate` case — so a heartbeat always goes out.
    fn replicate_to(&mut self, peer: NodeId, force: bool) -> Vec<Effect> {
        let max_inflight = self.replication.max_inflight.max(1);
        let max_bytes = self.replication.max_append_bytes;
        let snapshot_retry_ticks = self.replication.snapshot_retry_ticks.max(1);
        let last_index = self.log.last_index();
        let mut effects = Vec::new();
        while let Role::Leader { progress, .. } = &mut self.role
            && let Some(p) = progress.get_mut(&peer)
        {
            if p.next_index <= self.log.snapshot_last_index() {
                if !matches!(p.flow, Flow::Snapshot { .. }) {
                    p.flow = Flow::Snapshot {
                        ticks_left: snapshot_retry_ticks,
                    };
                    effects.push(Effect::SendSnapshot { to: peer });
                }
                break;
            }

            let next = p.next_index;
            let must_send = force && effects.is_empty();
            let (batch_end, more) = match &mut p.flow {
                Flow::Probe { paused } => {
                    if *paused && !must_send {
                        break;
                    }
                    *paused = true;
                    (self.log.batch_end(next, max_bytes), false)
                }
                Flow::Replicate { inflight } => {
                    if next <= last_index && inflight.len() < max_inflight {
                        let end = self.log.batch_end(next, max_bytes);
                        inflight.push_back(end);
                        p.next_index = end.next();
                        (end, true)
                    } else if must_send {
                        (next.prev(), false)
                    } else {
                        break;
                    }
                }
                // Only entered with `next_index` at or below the base, and
                // left (by `record_match`) as soon as it is past it, so the
                // check above has already handled it.
                Flow::Snapshot { .. } => break,
            };
            effects.push(self.append_entries(peer, next, batch_end));
            if !more {
                break;
            }
        }
        effects
    }

    /// An `AppendEntries` for `peer` carrying entries `from..=to` (none when
    /// `to < from`), anchored on the entry just before `from`.
    fn append_entries(&self, peer: NodeId, from: LogIndex, to: LogIndex) -> Effect {
        let prev_log_index = from.prev();
        let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(Term::ZERO);
        let count =
            usize::try_from(to.get().saturating_sub(prev_log_index.get())).unwrap_or(usize::MAX);
        let entries = self.log.entries_from(from);
        Effect::SendRpc {
            to: peer,
            message: Message::AppendEntries(AppendEntriesArgs {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index,
                prev_log_term,
                entries: entries[..count.min(entries.len())].to_vec(),
                leader_commit: self.commit_index,
                heartbeat_round: self.heartbeat_round(),
            }),
        }
    }

    /// The round this leader's `AppendEntries` carry; zero when not leading.
    const fn heartbeat_round(&self) -> HeartbeatRound {
        match &self.role {
            Role::Leader {
                heartbeat_round, ..
            } => *heartbeat_round,
            Role::Follower | Role::PreCandidate { .. } | Role::Candidate { .. } => {
                HeartbeatRound::ZERO
            }
        }
    }

    /// This leader's `match_index` for `peer`, or zero when it is not leading
    /// or has no progress for `peer`.
    fn match_index_for(&self, peer: NodeId) -> LogIndex {
        match &self.role {
            Role::Leader { progress, .. } => progress
                .get(&peer)
                .map_or(LogIndex::ZERO, |p| p.match_index),
            Role::Follower | Role::PreCandidate { .. } | Role::Candidate { .. } => LogIndex::ZERO,
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
    /// leader on `success`, and [`LogIndex::ZERO`] otherwise; `conflict` is
    /// the back-off hint on a failed consistency check. The reply goes to
    /// `args`' leader and echoes its heartbeat round.
    const fn append_entries_reply(
        &self,
        args: &AppendEntriesArgs,
        success: bool,
        match_index: LogIndex,
        conflict: Option<Conflict>,
    ) -> Effect {
        Effect::SendRpc {
            to: args.leader_id,
            message: Message::AppendEntriesReply(AppendEntriesReply {
                term: self.current_term,
                success,
                match_index,
                conflict,
                heartbeat_round: args.heartbeat_round,
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

    /// The configuration in force at log index `index`: the latest
    /// [`LogEntryKind::Config`] entry at or below it, or the configuration the
    /// log was compacted (or bootstrapped) with if there is none in memory.
    ///
    /// This is the configuration a snapshot covering `index` must carry
    /// (thesis §5.1); a later, possibly uncommitted, `Config` entry does not
    /// belong in it.
    #[must_use]
    pub fn config_at(&self, index: LogIndex) -> ClusterConfig {
        let base = self.log.snapshot_last_index().get();
        self.log
            .entries_from(LogIndex::new(1))
            .iter()
            .enumerate()
            .rev()
            .find(|(pos, entry)| entry.as_config().is_some() && base + (*pos as u64) < index.get())
            .and_then(|(_, entry)| entry.as_config().cloned())
            .unwrap_or_else(|| self.base_config.clone())
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

    /// Whether this server is currently a pre-candidate: running the pre-vote
    /// straw poll (thesis §9.6) but not yet campaigning for real.
    #[must_use]
    pub const fn is_pre_candidate(&self) -> bool {
        matches!(self.role, Role::PreCandidate { .. })
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

    /// Whether this server leads and is handing leadership to another server
    /// (thesis §3.10). A transferring leader accepts no proposals.
    #[must_use]
    pub const fn is_transferring(&self) -> bool {
        matches!(
            self.role,
            Role::Leader {
                transfer: Some(_),
                ..
            }
        )
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
        AppendEntriesArgs, AppendEntriesReply, CATCH_UP_TICKS, ClusterConfig, Conflict, Effect,
        Flow, HeartbeatRound, Input, InstallSnapshotArgs, InstallSnapshotReply, Log, LogEntry,
        LogIndex, LogicalInstant, MembershipChange, Message, NodeId, PreVoteArgs, PreVoteReply,
        PreVoteRound, Progress, READ_TICKS, RaftNode, ReadId, ReplicationConfig, RequestVoteArgs,
        RequestVoteReply, Role, TRANSFER_TICKS, Term, TimeoutNowArgs,
    };

    /// The core does not consult the clock under this timer model, so every
    /// test steps at the same instant.
    const NOW: LogicalInstant = LogicalInstant::START;

    fn entry(term: u64) -> LogEntry {
        LogEntry::command(Term::new(term), Bytes::from_static(b"cmd"))
    }

    fn noop(term: u64) -> LogEntry {
        LogEntry::noop(Term::new(term))
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
            leadership_transfer: false,
        })
    }

    /// A `RequestVote` from a candidate a leader handed off to (thesis §3.10).
    fn transfer_vote(
        term: u64,
        candidate: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Message {
        Message::RequestVote(RequestVoteArgs {
            term: Term::new(term),
            candidate_id: NodeId::new(candidate),
            last_log_index: LogIndex::new(last_log_index),
            last_log_term: Term::new(last_log_term),
            leadership_transfer: true,
        })
    }

    fn vote_reply(term: u64, granted: bool) -> Message {
        Message::RequestVoteReply(RequestVoteReply {
            term: Term::new(term),
            vote_granted: granted,
        })
    }

    /// A `PreVote` for round 1 — the round a node's first timeout starts.
    fn pre_vote(term: u64, candidate: u64, last_log_index: u64, last_log_term: u64) -> Message {
        pre_vote_in(1, term, candidate, last_log_index, last_log_term)
    }

    fn pre_vote_in(
        round: u64,
        term: u64,
        candidate: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Message {
        Message::PreVote(PreVoteArgs {
            term: Term::new(term),
            candidate_id: NodeId::new(candidate),
            last_log_index: LogIndex::new(last_log_index),
            last_log_term: Term::new(last_log_term),
            round: PreVoteRound::new(round),
        })
    }

    /// A `PreVoteReply` answering round 1.
    fn pre_vote_reply(term: u64, granted: bool) -> Message {
        pre_vote_reply_in(1, term, granted)
    }

    fn pre_vote_reply_in(round: u64, term: u64, granted: bool) -> Message {
        Message::PreVoteReply(PreVoteReply {
            term: Term::new(term),
            vote_granted: granted,
            round: PreVoteRound::new(round),
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
            heartbeat_round: HeartbeatRound::ZERO,
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
                conflict: None,
                heartbeat_round: HeartbeatRound::ZERO,
            }),
        }
    }

    /// A failed `AppendEntriesReply` effect carrying a conflict hint.
    fn rejection(to: u64, term: u64, conflict_term: Option<u64>, conflict_index: u64) -> Effect {
        send(to, rejected(term, conflict_term, conflict_index))
    }

    /// An `AppendEntriesReply` message rejecting with a conflict hint.
    fn rejected(term: u64, conflict_term: Option<u64>, conflict_index: u64) -> Message {
        Message::AppendEntriesReply(AppendEntriesReply {
            term: Term::new(term),
            success: false,
            match_index: LogIndex::ZERO,
            conflict: Some(Conflict {
                term: conflict_term.map(Term::new),
                index: LogIndex::new(conflict_index),
            }),
            heartbeat_round: HeartbeatRound::ZERO,
        })
    }

    /// A successful `AppendEntriesReply` effect carrying `matched`.
    fn ack(to: u64, term: u64, matched: u64) -> Effect {
        Effect::SendRpc {
            to: NodeId::new(to),
            message: Message::AppendEntriesReply(AppendEntriesReply {
                term: Term::new(term),
                success: true,
                match_index: LogIndex::new(matched),
                conflict: None,
                heartbeat_round: HeartbeatRound::ZERO,
            }),
        }
    }

    /// An incoming `AppendEntriesReply` message answering heartbeat `round`.
    fn ae_reply_in_round(term: u64, success: bool, matched: u64, round: u64) -> Message {
        Message::AppendEntriesReply(AppendEntriesReply {
            term: Term::new(term),
            success,
            match_index: LogIndex::new(matched),
            conflict: None,
            heartbeat_round: HeartbeatRound::new(round),
        })
    }

    /// An incoming `AppendEntriesReply` message.
    fn ae_reply(term: u64, success: bool, matched: u64) -> Message {
        Message::AppendEntriesReply(AppendEntriesReply {
            term: Term::new(term),
            success,
            match_index: LogIndex::new(matched),
            conflict: None,
            heartbeat_round: HeartbeatRound::ZERO,
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
            heartbeat_round: HeartbeatRound::ZERO,
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
            commands: vec![Bytes::from_static(command)],
        }
    }

    /// Steps the node for its side effects on state, discarding the effects.
    /// Used to arrange a node into a role before the call under test.
    fn drive(n: &mut RaftNode, input: Input) {
        let _ = n.step(input, NOW);
    }

    /// Drives `n` through an election timeout and a winning pre-vote round,
    /// leaving it a real [`Role::Candidate`] at `currentTerm + 1` (a single-node
    /// cluster runs straight through to [`Role::Leader`]). This is what a bare
    /// `Input::ElectionTimeout` did before pre-vote (thesis §9.6) existed, so
    /// tests that only need a campaigning node use it and stay concise.
    fn time_out(n: &mut RaftNode) {
        let would_be_term = n.current_term().get();
        drive(n, Input::ElectionTimeout);
        // Answer the round the timeout just opened, whichever number that is.
        let round = n.pre_vote_round.get();
        let voters: Vec<u64> = n.peers().iter().map(|peer| peer.get()).collect();
        for voter in voters {
            if !n.is_pre_candidate() {
                break;
            }
            drive(
                n,
                deliver(voter, pre_vote_reply_in(round, would_be_term, true)),
            );
        }
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
        assert_eq!(PreVoteRound::new(7).to_string(), "7");
    }

    #[test]
    fn pre_vote_rounds_count_up_from_zero() {
        assert_eq!(PreVoteRound::ZERO.get(), 0);
        assert_eq!(PreVoteRound::ZERO.next().next().get(), 2);
    }

    #[test]
    fn a_logical_instant_round_trips_its_millis() {
        assert_eq!(LogicalInstant::START.as_millis(), 0);
        assert_eq!(LogicalInstant::from_millis(42).as_millis(), 42);
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

        // No entries in memory, but the snapshot still represents five.
        assert!(!log.is_empty());
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
    fn a_second_compaction_drops_only_the_entries_past_the_first() {
        let mut log = Log::new();
        for term in 1..=6 {
            log.append(entry(term));
        }
        log.compact(LogIndex::new(2), Term::new(2));

        log.compact(LogIndex::new(4), Term::new(4));

        assert_eq!(log.snapshot_last_index(), LogIndex::new(4));
        assert_eq!(log.entries_from(LogIndex::new(1)), &[entry(5), entry(6)]);
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
    fn term_start_walks_back_to_the_first_entry_of_the_term() {
        let mut log = Log::new();
        for term in [1, 2, 2, 3] {
            log.append(entry(term));
        }
        assert_eq!(log.term_start(LogIndex::new(3)), LogIndex::new(2));
        assert_eq!(log.term_start(LogIndex::new(2)), LogIndex::new(2));
        assert_eq!(log.term_start(LogIndex::new(1)), LogIndex::new(1));
        assert_eq!(log.term_start(LogIndex::new(4)), LogIndex::new(4));
        // Past the end: nothing to walk back from.
        assert_eq!(log.term_start(LogIndex::new(9)), LogIndex::new(9));
    }

    #[test]
    fn term_start_stops_at_the_snapshot_base() {
        let log = Log::from_snapshot(LogIndex::new(10), Term::new(4), vec![entry(4), entry(4)]);
        assert_eq!(log.term_start(LogIndex::new(12)), LogIndex::new(10));
        assert_eq!(log.term_start(LogIndex::new(10)), LogIndex::new(10));
    }

    #[test]
    fn last_index_of_term_finds_the_end_of_a_run() {
        let mut log = Log::new();
        for term in [1, 1, 3, 3, 5] {
            log.append(entry(term));
        }
        assert_eq!(log.last_index_of_term(Term::new(1)), Some(LogIndex::new(2)));
        assert_eq!(log.last_index_of_term(Term::new(3)), Some(LogIndex::new(4)));
        assert_eq!(log.last_index_of_term(Term::new(5)), Some(LogIndex::new(5)));
        assert_eq!(log.last_index_of_term(Term::new(2)), None);
        assert_eq!(log.last_index_of_term(Term::new(6)), None);
        assert_eq!(Log::new().last_index_of_term(Term::ZERO), None);
    }

    #[test]
    fn last_index_of_term_sees_the_snapshot_base_but_not_below_it() {
        let log = Log::from_snapshot(LogIndex::new(10), Term::new(4), vec![entry(5)]);
        assert_eq!(
            log.last_index_of_term(Term::new(4)),
            Some(LogIndex::new(10))
        );
        assert_eq!(log.last_index_of_term(Term::new(3)), None);
        let bare = Log::from_snapshot(LogIndex::new(10), Term::new(4), vec![]);
        assert_eq!(
            bare.last_index_of_term(Term::new(4)),
            Some(LogIndex::new(10))
        );
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
    fn exactly_one_role_predicate_holds() {
        let roles = |n: &RaftNode| {
            [
                n.is_follower(),
                n.is_pre_candidate(),
                n.is_candidate(),
                n.is_leader(),
            ]
        };
        let mut n = node(1, &[2, 3]);
        assert_eq!(roles(&n), [true, false, false, false]);
        drive(&mut n, Input::ElectionTimeout);
        assert_eq!(roles(&n), [false, true, false, false]);
        drive(&mut n, deliver(2, pre_vote_reply(0, true)));
        assert_eq!(roles(&n), [false, false, true, false]);
        drive(&mut n, deliver(2, vote_reply(1, true)));
        assert_eq!(roles(&n), [false, false, false, true]);
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
    fn election_timeout_starts_a_pre_vote_without_touching_the_term() {
        let mut n = node(1, &[2, 3]);

        let effects = n.step(Input::ElectionTimeout, NOW);

        // No term bump, no self-vote, nothing to persist: just a straw poll.
        assert!(n.is_pre_candidate());
        assert_eq!(n.current_term(), Term::ZERO);
        assert_eq!(n.voted_for(), None);
        assert!(matches!(
            n.role(),
            Role::PreCandidate { pre_votes_granted } if pre_votes_granted.len() == 1
        ));

        assert_eq!(
            effects,
            vec![
                Effect::SendRpc {
                    to: NodeId::new(2),
                    message: pre_vote(1, 1, 0, 0),
                },
                Effect::SendRpc {
                    to: NodeId::new(3),
                    message: pre_vote(1, 1, 0, 0),
                },
                Effect::ResetElectionTimer,
            ],
        );
    }

    #[test]
    fn a_quorum_of_pre_votes_starts_the_real_election() {
        let mut n = node(1, &[2, 3]);
        drive(&mut n, Input::ElectionTimeout);

        let effects = n.step(deliver(2, pre_vote_reply(0, true)), NOW);

        assert!(n.is_candidate());
        assert_eq!(n.current_term(), Term::new(1));
        assert_eq!(n.voted_for(), Some(NodeId::new(1)));
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
    fn a_lost_pre_vote_round_leaves_the_term_untouched() {
        let mut n = node(1, &[2, 3]);

        // Peers refuse (they still hear a leader): no promotion, no term bump.
        drive(&mut n, Input::ElectionTimeout);
        drive(&mut n, deliver(2, pre_vote_reply(0, false)));
        drive(&mut n, deliver(3, pre_vote_reply(0, false)));
        assert!(n.is_pre_candidate());
        assert_eq!(n.current_term(), Term::ZERO);

        // The next timeout just starts another round, still at term 0.
        drive(&mut n, Input::ElectionTimeout);
        assert!(n.is_pre_candidate());
        assert_eq!(n.current_term(), Term::ZERO);

        // Once a round wins, the term finally advances and does not roll back.
        drive(&mut n, deliver(2, pre_vote_reply_in(2, 0, true)));
        assert!(n.is_candidate());
        assert_eq!(n.current_term(), Term::new(1));
        drive(&mut n, Input::ElectionTimeout); // real election failed: re-poll
        assert!(n.is_pre_candidate());
        assert_eq!(n.current_term(), Term::new(1));
    }

    #[test]
    fn a_grant_from_an_earlier_round_cannot_be_spent_in_a_later_one() {
        // Five voters, so a quorum of 3 needs two grants beyond our own and a
        // round can genuinely end short.
        let mut n = node(1, &[2, 3, 4, 5]);
        n.current_term = Term::new(5);

        drive(&mut n, Input::ElectionTimeout); // round 1
        drive(&mut n, deliver(2, pre_vote_reply_in(1, 5, true))); // {1,2}
        assert!(n.is_pre_candidate(), "two of five is not a quorum");

        drive(&mut n, Input::ElectionTimeout); // round 2, tally emptied
        drive(&mut n, deliver(3, pre_vote_reply_in(2, 5, true))); // {1,3}
        assert!(n.is_pre_candidate());

        // Node 2's round-1 "yes" finally lands. It was formed when node 2 heard
        // no leader; by now it may well hear one again, so it must not count.
        drive(&mut n, deliver(2, pre_vote_reply_in(1, 5, true)));
        assert!(
            n.is_pre_candidate(),
            "a stale round-1 grant was spent in round 2",
        );
        assert_eq!(
            n.current_term(),
            Term::new(5),
            "term inflated on stale consent"
        );

        // The same peer answering the *current* round does count.
        drive(&mut n, deliver(2, pre_vote_reply_in(2, 5, true)));
        assert!(n.is_candidate());
        assert_eq!(n.current_term(), Term::new(6));
    }

    #[test]
    fn a_pre_vote_grant_from_a_peer_a_term_behind_is_not_counted() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(5);
        drive(&mut n, Input::ElectionTimeout); // round 1

        // Node 2 lags at term 4. Its "yes" carries its own term, so it is left
        // for a later round, by which time our reply will have caught it up.
        drive(&mut n, deliver(2, pre_vote_reply(4, true)));

        assert!(n.is_pre_candidate());
        assert_eq!(n.current_term(), Term::new(5));
    }

    #[test]
    fn a_pre_vote_reply_from_a_newer_term_steps_the_pre_candidate_down() {
        let mut n = node(1, &[2, 3]);
        drive(&mut n, Input::ElectionTimeout);
        assert!(n.is_pre_candidate());

        let effects = n.step(deliver(2, pre_vote_reply(7, false)), NOW);

        assert!(n.is_follower());
        assert_eq!(n.current_term(), Term::new(7));
        assert_eq!(
            effects,
            vec![Effect::Persist {
                current_term: Term::new(7),
                voted_for: None,
            }],
        );
    }

    #[test]
    fn a_pre_vote_is_granted_when_no_leader_is_known_and_the_log_is_current() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(4);

        // Would-be term 5, empty log matches ours: grant, and change nothing.
        let effects = n.step(deliver(2, pre_vote(5, 2, 0, 0)), NOW);

        assert_eq!(n.current_term(), Term::new(4));
        assert_eq!(n.voted_for(), None);
        assert!(n.is_follower());
        assert_eq!(
            effects,
            vec![Effect::SendRpc {
                to: NodeId::new(2),
                message: pre_vote_reply(4, true),
            }],
        );
    }

    #[test]
    fn a_pre_vote_is_refused_while_a_leader_is_active() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(4);
        // A heartbeat from the term-4 leader: we now hear a leader.
        drive(&mut n, deliver(3, heartbeat(4, 3, 0, 0)));

        let effects = n.step(deliver(2, pre_vote(5, 2, 0, 0)), NOW);

        assert_eq!(
            effects,
            vec![Effect::SendRpc {
                to: NodeId::new(2),
                message: pre_vote_reply(4, false),
            }],
        );

        // After our own timer fires we no longer shield the leader.
        drive(&mut n, Input::ElectionTimeout);
        let effects = n.step(deliver(2, pre_vote(5, 2, 0, 0)), NOW);
        assert_eq!(effects, vec![send(2, pre_vote_reply(4, true))]);
    }

    #[test]
    fn a_leader_refuses_pre_votes_so_a_lone_peer_cannot_unseat_it() {
        let mut n = node(1, &[2, 3]);
        time_out(&mut n);
        drive(&mut n, deliver(2, vote_reply(1, true)));
        assert!(n.is_leader());

        // Node 3, cut off from node 2 but still able to reach the leader, keeps
        // pre-voting. If the leader granted, {3, leader} would be a quorum of 3.
        let effects = n.step(deliver(3, pre_vote(2, 3, 0, 0)), NOW);

        assert!(n.is_leader());
        assert_eq!(n.current_term(), Term::new(1));
        assert_eq!(effects, vec![send(3, pre_vote_reply(1, false))]);
    }

    #[test]
    fn an_append_entries_reverts_a_pre_candidate_to_a_follower() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(4);
        drive(&mut n, Input::ElectionTimeout);
        assert!(n.is_pre_candidate());

        // The leader was alive after all: yield, and shield it from then on.
        let effects = n.step(deliver(3, heartbeat(4, 3, 0, 0)), NOW);

        assert!(n.is_follower());
        assert_eq!(n.current_term(), Term::new(4));
        assert_eq!(n.leader_id(), Some(NodeId::new(3)));
        // A pre-vote round writes nothing, so conceding one costs no fsync.
        assert_eq!(
            effects,
            vec![Effect::ResetElectionTimer, append_reply(3, 4, true)],
        );
        assert_eq!(
            n.step(deliver(2, pre_vote(5, 2, 0, 0)), NOW),
            vec![send(2, pre_vote_reply(4, false))],
        );
    }

    #[test]
    fn an_install_snapshot_reverts_a_pre_candidate_to_a_follower() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(4);
        drive(&mut n, Input::ElectionTimeout);
        assert!(n.is_pre_candidate());

        drive(&mut n, deliver(3, install_snapshot(4, 3, 2, 4, true)));

        assert!(n.is_follower());
        assert_eq!(n.current_term(), Term::new(4));
        assert_eq!(n.leader_id(), Some(NodeId::new(3)));
    }

    #[test]
    fn a_whole_pre_vote_round_persists_nothing() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(4);

        let mut effects = n.step(Input::ElectionTimeout, NOW);
        effects.extend(n.step(deliver(2, pre_vote_reply(4, false)), NOW));
        effects.extend(n.step(deliver(3, pre_vote_reply(4, false)), NOW));

        assert!(n.is_pre_candidate());
        assert_eq!(n.current_term(), Term::new(4));
        assert_eq!(n.voted_for(), None);
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::Persist { .. } | Effect::PersistLog { .. })),
            "a losing pre-vote round must not touch stable storage: {effects:?}",
        );
    }

    #[test]
    fn a_pre_vote_is_refused_for_a_stale_log_or_a_stale_would_be_term() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(4);
        n.log.append(entry(4)); // our last entry: index 1, term 4

        // Up-to-date term but the pre-candidate's log is shorter.
        let stale_log = n.step(deliver(2, pre_vote(5, 2, 0, 0)), NOW);
        assert_eq!(stale_log, vec![send(2, pre_vote_reply(4, false))]);

        // Fresh enough log, but the would-be term is behind ours.
        let stale_term = n.step(deliver(2, pre_vote(3, 2, 1, 4)), NOW);
        assert_eq!(stale_term, vec![send(2, pre_vote_reply(4, false))]);

        // Control: fresh log, current would-be term -> granted.
        let ok = n.step(deliver(2, pre_vote(5, 2, 1, 4)), NOW);
        assert_eq!(ok, vec![send(2, pre_vote_reply(4, true))]);
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
                // Pre-vote reset, then the real election's persist + reset,
                // then the opening no-op, which commits on the spot.
                Effect::ResetElectionTimer,
                Effect::Persist {
                    current_term: Term::new(1),
                    voted_for: Some(NodeId::new(1)),
                },
                Effect::ResetElectionTimer,
                Effect::PersistLog {
                    from_index: LogIndex::new(1),
                    entries: vec![noop(1)],
                },
            ],
        );
        assert_eq!(n.commit_index(), LogIndex::new(1));
        // The no-op is not the application's: nothing is applied.
        assert_eq!(n.last_applied(), LogIndex::new(1));
    }

    #[test]
    fn candidate_wins_on_a_quorum_and_opens_its_term_with_a_noop() {
        let mut n = node(1, &[2, 3]);
        time_out(&mut n);

        let effects = n.step(deliver(2, vote_reply(1, true)), NOW);

        assert!(n.is_leader());
        assert_eq!(
            effects,
            vec![
                Effect::PersistLog {
                    from_index: LogIndex::new(1),
                    entries: vec![noop(1)],
                },
                send(2, append_entries(1, 1, 0, 0, vec![noop(1)], 0)),
                send(3, append_entries(1, 1, 0, 0, vec![noop(1)], 0)),
            ],
        );
        // Not committed until a peer acks it.
        assert_eq!(n.commit_index(), LogIndex::ZERO);
    }

    #[test]
    fn votes_arriving_after_the_win_are_ignored() {
        let mut n = node(1, &[2, 3]);
        time_out(&mut n);
        drive(&mut n, deliver(2, vote_reply(1, true)));
        assert!(n.is_leader());

        let effects = n.step(deliver(3, vote_reply(1, true)), NOW);

        assert!(effects.is_empty());
        assert!(n.is_leader());
    }

    #[test]
    fn a_vote_granted_in_an_earlier_term_does_not_count_toward_a_later_one() {
        let mut n = node(1, &[2, 3]);
        time_out(&mut n); // candidate, term 1
        time_out(&mut n); // candidate again, term 2

        let effects = n.step(deliver(2, vote_reply(1, true)), NOW);

        assert!(effects.is_empty());
        assert!(n.is_candidate());
        assert_eq!(n.current_term(), Term::new(2));
    }

    #[test]
    fn a_same_term_request_vote_does_not_unseat_a_leader_or_a_candidate() {
        let mut leader = node(1, &[2, 3]);
        time_out(&mut leader); // term 1
        drive(&mut leader, deliver(2, vote_reply(1, true)));
        assert!(leader.is_leader());

        let mut candidate = node(1, &[2, 3]);
        time_out(&mut candidate); // term 1
        assert!(candidate.is_candidate());

        for n in [&mut leader, &mut candidate] {
            // Node 3 campaigned in term 1 too; its request can arrive late.
            let effects = n.step(deliver(3, request_vote(1, 3, 0, 0)), NOW);
            assert_eq!(effects, vec![send(3, vote_reply(1, false))]);
            assert!(!n.is_follower());
            assert_eq!(n.voted_for(), Some(NodeId::new(1)));
        }
        assert!(leader.is_leader());
        assert!(candidate.is_candidate());
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
    fn a_follower_hearing_a_leader_disregards_a_request_vote() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(4);
        drive(&mut n, deliver(3, heartbeat(4, 3, 0, 0))); // leader 3 is in contact

        // A higher-term candidate campaigns: disregarded outright (§4.2.3).
        let effects = n.step(deliver(2, request_vote(9, 2, 0, 0)), NOW);

        assert!(n.is_follower());
        assert_eq!(n.current_term(), Term::new(4)); // term NOT adopted
        assert_eq!(n.voted_for(), None);
        assert_eq!(
            effects,
            vec![Effect::SendRpc {
                to: NodeId::new(2),
                message: vote_reply(4, false),
            }],
        );
    }

    #[test]
    fn once_its_own_timer_fires_a_follower_processes_a_request_vote() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(4);
        drive(&mut n, deliver(3, heartbeat(4, 3, 0, 0)));

        // Its election timer fires: it no longer shields the old leader.
        drive(&mut n, Input::ElectionTimeout);
        assert!(n.is_pre_candidate());

        let effects = n.step(deliver(2, request_vote(9, 2, 0, 0)), NOW);

        assert!(n.is_follower());
        assert_eq!(n.current_term(), Term::new(9));
        assert_eq!(n.voted_for(), Some(NodeId::new(2)));
        assert_eq!(
            effects,
            vec![
                Effect::Persist {
                    current_term: Term::new(9),
                    voted_for: Some(NodeId::new(2)),
                },
                Effect::ResetElectionTimer,
                Effect::SendRpc {
                    to: NodeId::new(2),
                    message: vote_reply(9, true),
                },
            ],
        );
    }

    #[test]
    fn a_reply_with_a_newer_term_makes_a_leader_step_down() {
        let mut n = node(1, &[2, 3]);
        time_out(&mut n);
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
        time_out(&mut n); // candidate, term 1
        time_out(&mut n); // candidate, term 2

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
        time_out(&mut n); // candidate, term 1, voted for self

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
            vec![Effect::ResetElectionTimer, rejection(2, 1, Some(1), 1),],
        );
    }

    #[test]
    fn a_rejection_names_the_first_index_of_the_conflicting_term() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(3);
        for term in [1, 2, 2, 2] {
            n.log.append(entry(term));
        }

        // The leader's entry 4 is from term 3; ours is from term 2, which we
        // have held since index 2.
        let effects = n.step(deliver(2, heartbeat(3, 2, 4, 3)), NOW);

        assert!(effects.contains(&rejection(2, 3, Some(2), 2)));
    }

    #[test]
    fn a_rejection_from_a_short_log_points_just_past_its_end() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);
        n.log.append(entry(1));
        n.log.append(entry(1));

        let effects = n.step(deliver(2, heartbeat(1, 2, 7, 1)), NOW);

        assert!(effects.contains(&rejection(2, 1, None, 3)));
    }

    #[test]
    fn a_rejection_hint_stops_at_the_snapshot_base() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(3);
        n.log = Log::from_snapshot(LogIndex::new(3), Term::new(2), vec![entry(2), entry(2)]);

        // Term 2 runs back into the snapshot; the hint goes no lower than it.
        let effects = n.step(deliver(2, heartbeat(3, 2, 5, 3)), NOW);

        assert!(effects.contains(&rejection(2, 3, Some(2), 3)));
    }

    #[test]
    fn heartbeat_tick_makes_the_leader_rebroadcast_append_entries() {
        let mut n = node(1, &[2, 3]);
        time_out(&mut n);
        drive(&mut n, deliver(2, vote_reply(1, true)));
        assert!(n.is_leader());

        let effects = n.step(Input::HeartbeatTick, NOW);

        // Nobody has acked the opening no-op yet, so the beat carries it again.
        assert_eq!(
            effects,
            vec![
                send(2, append_entries(1, 1, 0, 0, vec![noop(1)], 0)),
                send(3, append_entries(1, 1, 0, 0, vec![noop(1)], 0)),
            ],
        );
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

        time_out(&mut n); // candidate
        assert!(n.step(propose(b"cmd"), NOW).is_empty());
        assert!(n.log().is_empty());
    }

    /// Node 1 of {1, 2, 3}, leader for term 1 whose log is just its opening
    /// no-op at index 1, which both peers have acked — so it is committed and
    /// both peers are streaming ([`Flow::Replicate`]) with nothing in flight.
    fn acked_leader() -> RaftNode {
        let mut n = node(1, &[2, 3]);
        time_out(&mut n);
        drive(&mut n, deliver(2, vote_reply(1, true)));
        drive(&mut n, deliver(2, ae_reply(1, true, 1)));
        drive(&mut n, deliver(3, ae_reply(1, true, 1)));
        assert_eq!(n.commit_index(), LogIndex::new(1));
        assert!(n.is_leader());
        n
    }

    #[test]
    fn propose_on_a_leader_appends_persists_then_replicates() {
        let mut n = acked_leader();

        let effects = n.step(propose(b"cmd"), NOW);

        assert_eq!(n.log().len(), 2);
        assert_eq!(n.log().get(LogIndex::new(2)), Some(&entry(1)));

        let tail = vec![entry(1)];
        assert_eq!(
            effects,
            vec![
                Effect::PersistLog {
                    from_index: LogIndex::new(2),
                    entries: tail.clone(),
                },
                send(2, append_entries(1, 1, 1, 1, tail.clone(), 1)),
                send(3, append_entries(1, 1, 1, 1, tail, 1)),
            ],
        );
    }

    #[test]
    fn persist_log_precedes_the_replicating_send_rpcs() {
        let mut n = acked_leader();

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
    fn a_second_proposal_pipelines_only_the_new_entry() {
        let mut n = acked_leader();

        drive(&mut n, propose(b"cmd"));
        let effects = n.step(propose(b"cmd"), NOW);

        // No ack has come back, but both peers are streaming: the first batch
        // already moved `next_index` to 3, so the second carries only the new
        // entry, anchored on the first. Nothing new commits — the leader is
        // one of three.
        assert_eq!(n.log().len(), 3);
        assert_eq!(n.commit_index(), LogIndex::new(1));
        assert_eq!(
            effects,
            vec![
                Effect::PersistLog {
                    from_index: LogIndex::new(3),
                    entries: vec![entry(1)],
                },
                send(2, append_entries(1, 1, 2, 1, vec![entry(1)], 1)),
                send(3, append_entries(1, 1, 2, 1, vec![entry(1)], 1)),
            ],
        );
    }

    #[test]
    fn a_batch_of_proposals_is_persisted_and_sent_together() {
        let mut n = acked_leader();

        let effects = n.step(
            Input::Propose {
                commands: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            },
            NOW,
        );

        let tail = vec![entry_cmd(1, b"a"), entry_cmd(1, b"b")];
        assert_eq!(
            effects,
            vec![
                Effect::PersistLog {
                    from_index: LogIndex::new(2),
                    entries: tail.clone(),
                },
                send(2, append_entries(1, 1, 1, 1, tail.clone(), 1)),
                send(3, append_entries(1, 1, 1, 1, tail, 1)),
            ],
        );
    }

    #[test]
    fn an_empty_proposal_is_a_no_op() {
        let mut n = acked_leader();

        assert!(n.step(Input::Propose { commands: vec![] }, NOW).is_empty());
        assert_eq!(n.log().len(), 1); // just the opening no-op
    }

    #[test]
    fn a_proposal_waits_while_a_probe_is_unanswered() {
        let mut n = node(1, &[2, 3]);
        time_out(&mut n);
        drive(&mut n, deliver(2, vote_reply(1, true)));

        // The opening heartbeats are probes nobody has answered yet, so the
        // new entry is only persisted; the heartbeat tick or the probe's reply
        // carries it out.
        let effects = n.step(propose(b"cmd"), NOW);

        assert_eq!(
            effects,
            vec![Effect::PersistLog {
                from_index: LogIndex::new(2),
                entries: vec![entry(1)],
            }],
        );
    }

    /// The [`Progress`] this leader holds for `peer`.
    #[track_caller]
    fn progress_of(n: &RaftNode, peer: u64) -> Progress {
        let Role::Leader { progress, .. } = n.role() else {
            unreachable!("not a leader");
        };
        progress[&NodeId::new(peer)].clone()
    }

    /// Just the `SendRpc` effects, in order.
    fn rpcs(effects: Vec<Effect>) -> Vec<Effect> {
        effects
            .into_iter()
            .filter(|e| matches!(e, Effect::SendRpc { .. }))
            .collect()
    }

    /// Default replication limits, but at most `max_inflight` batches.
    fn window(max_inflight: usize) -> ReplicationConfig {
        ReplicationConfig {
            max_inflight,
            ..ReplicationConfig::default()
        }
    }

    /// At most `max_inflight` batches of at most `max_append_bytes` each.
    fn limits(max_inflight: usize, max_append_bytes: usize) -> ReplicationConfig {
        ReplicationConfig {
            max_inflight,
            max_append_bytes,
            ..ReplicationConfig::default()
        }
    }

    fn streaming(inflight: &[u64]) -> Flow {
        Flow::Replicate {
            inflight: inflight.iter().copied().map(LogIndex::new).collect(),
        }
    }

    #[test]
    fn a_new_leader_probes_every_peer() {
        let mut n = node(1, &[2, 3]);
        time_out(&mut n);
        drive(&mut n, deliver(2, vote_reply(1, true)));

        // The opening heartbeat is the first probe, now awaiting its reply.
        assert_eq!(progress_of(&n, 2).flow, Flow::Probe { paused: true });
        assert_eq!(progress_of(&n, 3).flow, Flow::Probe { paused: true });
    }

    #[test]
    fn a_successful_ack_switches_a_probing_peer_to_streaming() {
        let n = acked_leader();

        assert_eq!(progress_of(&n, 2).flow, streaming(&[]));
        assert_eq!(progress_of(&n, 3).flow, streaming(&[]));
    }

    #[test]
    fn each_streamed_batch_is_tracked_until_acked() {
        let mut n = acked_leader();
        drive(&mut n, propose(b"cmd"));
        drive(&mut n, propose(b"cmd"));

        assert_eq!(progress_of(&n, 2).next_index, LogIndex::new(4));
        assert_eq!(progress_of(&n, 2).flow, streaming(&[2, 3]));

        // An ack frees exactly the batches it covers.
        drive(&mut n, deliver(2, ae_reply(1, true, 2)));
        assert_eq!(progress_of(&n, 2).match_index, LogIndex::new(2));
        assert_eq!(progress_of(&n, 2).flow, streaming(&[3]));
    }

    #[test]
    fn a_full_window_holds_proposals_until_an_ack_frees_a_slot() {
        let mut n = acked_leader().with_replication(window(2));
        drive(&mut n, propose(b"cmd"));
        drive(&mut n, propose(b"cmd"));

        // Two batches in flight: the third proposal is persisted but not sent.
        let effects = n.step(propose(b"cmd"), NOW);
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::SendRpc { .. })),
            "sent past a full window: {effects:?}",
        );

        // Node 2 acks the first batch (committing it); the freed slot carries
        // entry 4 at once.
        let effects = rpcs(n.step(deliver(2, ae_reply(1, true, 2)), NOW));
        assert_eq!(
            effects,
            vec![send(2, append_entries(1, 1, 3, 1, vec![entry(1)], 2))],
        );
        assert_eq!(progress_of(&n, 2).flow, streaming(&[3, 4]));
    }

    #[test]
    fn an_ack_with_entries_still_unsent_sends_them_all_in_one_batch() {
        let mut n = acked_leader().with_replication(window(1));
        drive(&mut n, propose(b"cmd"));
        drive(&mut n, propose(b"cmd"));
        drive(&mut n, propose(b"cmd"));

        let effects = rpcs(n.step(deliver(2, ae_reply(1, true, 2)), NOW));

        assert_eq!(
            effects,
            vec![send(
                2,
                append_entries(1, 1, 2, 1, vec![entry(1), entry(1)], 2)
            )],
        );
    }

    #[test]
    fn a_batch_holds_as_many_entries_as_fit_the_byte_cap() {
        let mut log = Log::new();
        for command in [&b"aa"[..], b"bb", b"cc", b"dd"] {
            log.append(entry_cmd(1, command));
        }

        assert_eq!(log.batch_end(LogIndex::new(1), 4), LogIndex::new(2));
        assert_eq!(log.batch_end(LogIndex::new(1), 5), LogIndex::new(2));
        assert_eq!(log.batch_end(LogIndex::new(2), 6), LogIndex::new(4));
        assert_eq!(
            log.batch_end(LogIndex::new(1), usize::MAX),
            LogIndex::new(4)
        );
    }

    #[test]
    fn a_batch_always_holds_at_least_one_entry() {
        let mut log = Log::new();
        log.append(entry_cmd(1, b"larger than the cap"));
        log.append(entry_cmd(1, b"x"));

        assert_eq!(log.batch_end(LogIndex::new(1), 0), LogIndex::new(1));
        assert_eq!(log.batch_end(LogIndex::new(1), 3), LogIndex::new(1));
    }

    #[test]
    fn a_batch_past_the_end_of_the_log_is_empty() {
        let mut log = Log::new();
        log.append(entry(1));

        assert_eq!(log.batch_end(LogIndex::new(2), 1024), LogIndex::new(1));
    }

    #[test]
    fn a_config_entry_counts_eight_bytes_per_voter() {
        let config = ClusterConfig::new([1, 2, 3].map(NodeId::new));
        let mut log = Log::new();
        log.append(LogEntry::config(Term::new(1), config));
        log.append(entry_cmd(1, b"x"));

        assert_eq!(log.batch_end(LogIndex::new(1), 24), LogIndex::new(1));
        assert_eq!(log.batch_end(LogIndex::new(1), 25), LogIndex::new(2));
    }

    #[test]
    fn a_probe_carries_a_single_capped_batch() {
        let mut n = node(1, &[2, 3]).with_replication(limits(8, 3));
        time_out(&mut n);
        drive(&mut n, deliver(2, vote_reply(1, true)));
        drive(&mut n, propose(b"cmd")); // held: the opening probe is unanswered
        drive(&mut n, propose(b"cmd"));

        let effects = n.step(Input::HeartbeatTick, NOW);

        // "cmd" is three bytes, so a batch holds the (empty) no-op and one
        // command.
        assert_eq!(
            effects,
            vec![
                send(2, append_entries(1, 1, 0, 0, vec![noop(1), entry(1)], 0)),
                send(3, append_entries(1, 1, 0, 0, vec![noop(1), entry(1)], 0)),
            ],
        );
    }

    #[test]
    fn a_streaming_peer_that_is_behind_gets_capped_batches_until_the_window_fills() {
        let mut n = acked_leader().with_replication(limits(2, 3));
        for _ in 0..5 {
            drive(&mut n, propose(b"cmd"));
        }
        // Entries 2 and 3 went out one per batch; 4, 5 and 6 are held.
        assert_eq!(progress_of(&n, 2).flow, streaming(&[2, 3]));

        let effects = rpcs(n.step(deliver(2, ae_reply(1, true, 3)), NOW));

        // Both slots free up: two one-entry batches, and entry 6 waits.
        assert_eq!(
            effects,
            vec![
                send(2, append_entries(1, 1, 3, 1, vec![entry(1)], 3)),
                send(2, append_entries(1, 1, 4, 1, vec![entry(1)], 3)),
            ],
        );
        assert_eq!(progress_of(&n, 2).flow, streaming(&[4, 5]));
        assert_eq!(progress_of(&n, 2).next_index, LogIndex::new(6));
    }

    #[test]
    fn an_entry_larger_than_the_cap_is_still_replicated() {
        let mut n = acked_leader().with_replication(limits(8, 1));

        let effects = rpcs(n.step(propose(b"cmd"), NOW));

        assert_eq!(
            effects,
            vec![
                send(2, append_entries(1, 1, 1, 1, vec![entry(1)], 1)),
                send(3, append_entries(1, 1, 1, 1, vec![entry(1)], 1)),
            ],
        );
    }

    #[test]
    fn a_heartbeat_to_a_full_window_is_empty_and_anchored_past_the_batches() {
        let mut n = acked_leader().with_replication(window(1));
        drive(&mut n, propose(b"cmd"));
        drive(&mut n, propose(b"cmd")); // held: the window is full

        let effects = n.step(Input::HeartbeatTick, NOW);

        // Anchored on entry 2, the last one sent: a peer that lost it rejects,
        // and the rejection makes the leader resend.
        assert_eq!(
            effects,
            vec![
                send(2, append_entries(1, 1, 2, 1, vec![], 1)),
                send(3, append_entries(1, 1, 2, 1, vec![], 1)),
            ],
        );
    }

    #[test]
    fn a_heartbeat_to_a_caught_up_streaming_peer_is_empty() {
        let mut n = acked_leader();
        drive(&mut n, propose(b"cmd"));
        drive(&mut n, deliver(2, ae_reply(1, true, 2)));

        let effects = n.step(Input::HeartbeatTick, NOW);

        assert!(effects.contains(&send(2, append_entries(1, 1, 2, 1, vec![], 2))));
    }

    #[test]
    fn a_rejection_while_streaming_resends_from_just_past_match_index() {
        let mut n = acked_leader();
        drive(&mut n, propose(b"cmd"));
        drive(&mut n, deliver(2, ae_reply(1, true, 2)));
        drive(&mut n, propose(b"cmd"));
        drive(&mut n, propose(b"cmd"));
        assert_eq!(progress_of(&n, 2).next_index, LogIndex::new(5));

        // Node 2 lost both batches and rejected a heartbeat anchored at 4.
        let effects = n.step(deliver(2, ae_reply(1, false, 0)), NOW);

        assert_eq!(
            effects,
            vec![send(
                2,
                append_entries(1, 1, 2, 1, vec![entry(1), entry(1)], 2)
            )],
        );
        assert_eq!(progress_of(&n, 2).next_index, LogIndex::new(3));
        assert_eq!(progress_of(&n, 2).flow, Flow::Probe { paused: true });
    }

    #[test]
    fn a_probe_steps_back_past_match_index_for_a_peer_that_lost_its_log() {
        let mut n = acked_leader();
        drive(&mut n, propose(b"cmd"));
        drive(&mut n, deliver(2, ae_reply(1, true, 2)));
        drive(&mut n, deliver(2, ae_reply(1, false, 0))); // now probing from 3

        // Node 2 was wiped and rejects again: the probe goes back over entry 2
        // even though node 2 once acked it, and resends it.
        let effects = n.step(deliver(2, ae_reply(1, false, 0)), NOW);

        assert_eq!(
            effects,
            vec![send(2, append_entries(1, 1, 1, 1, vec![entry(1)], 2))],
        );
        assert_eq!(progress_of(&n, 2).next_index, LogIndex::new(2));
    }

    #[test]
    fn a_reply_from_a_peer_without_progress_sends_nothing() {
        let mut n = acked_leader();
        if let Role::Leader { progress, .. } = &mut n.role {
            progress.remove(&NodeId::new(3));
        }

        assert!(n.step(deliver(3, ae_reply(1, false, 0)), NOW).is_empty());
        assert!(n.step(deliver(3, ae_reply(1, true, 0)), NOW).is_empty());
    }

    #[test]
    fn heartbeat_tick_carries_entries_a_peer_has_not_acked() {
        let mut n = node(1, &[2, 3]);
        time_out(&mut n);
        drive(&mut n, deliver(2, vote_reply(1, true)));
        drive(&mut n, propose(b"cmd"));

        let effects = n.step(Input::HeartbeatTick, NOW);

        assert_eq!(
            effects,
            vec![
                send(2, append_entries(1, 1, 0, 0, vec![noop(1), entry(1)], 0)),
                send(3, append_entries(1, 1, 0, 0, vec![noop(1), entry(1)], 0)),
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

    #[test]
    fn an_rpc_anchored_below_commit_index_does_not_move_it_back() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);
        let entries = (0..5).map(|_| entry(1)).collect();
        drive(&mut n, deliver(2, append_entries(1, 2, 0, 0, entries, 5)));
        assert_eq!(n.commit_index(), LogIndex::new(5));

        // Anchored at 2 with nothing attached: it vouches only through 2.
        let effects = n.step(deliver(2, append_entries(1, 2, 2, 1, Vec::new(), 6)), NOW);

        assert_eq!(n.commit_index(), LogIndex::new(5));
        assert_eq!(n.last_applied(), LogIndex::new(5));
        assert_eq!(effects, vec![Effect::ResetElectionTimer, ack(2, 1, 2)]);
    }

    // --- 5b: leader ack handling -----------------------------------------

    #[test]
    fn a_successful_ack_from_one_peer_commits_in_a_three_node_cluster() {
        let mut n = node(1, &[2, 3]);
        time_out(&mut n);
        drive(&mut n, deliver(2, vote_reply(1, true)));
        drive(&mut n, propose(b"x"));
        assert_eq!(n.commit_index(), LogIndex::ZERO); // leader alone: not yet

        let effects = n.step(deliver(2, ae_reply(1, true, 2)), NOW);

        // Peer 2 plus the leader is a majority of three: the no-op and "x"
        // commit, and only "x" is applied.
        assert_eq!(n.commit_index(), LogIndex::new(2));
        assert_eq!(n.last_applied(), LogIndex::new(2));
        assert_eq!(
            effects,
            vec![Effect::ApplyToStateMachine {
                index: LogIndex::new(2),
                command: Bytes::from_static(b"x"),
            }],
        );
    }

    #[test]
    fn a_failed_ack_backs_off_next_index_and_retries_immediately() {
        let mut n = node(1, &[2, 3]);
        // Two entries already on this node before it wins, so as leader its
        // `next_index` for every peer starts at 3, where its no-op goes.
        n.log.append(entry(1));
        n.log.append(entry(1));
        n.current_term = Term::new(1);
        time_out(&mut n); // term 2, candidate
        drive(&mut n, deliver(2, vote_reply(2, true))); // leader, term 2

        let effects = n.step(deliver(2, ae_reply(2, false, 0)), NOW);

        // next_index[2] 3 -> 2: retry anchors at index 1, carrying index 2 on.
        assert_eq!(
            effects,
            vec![send(
                2,
                append_entries(2, 1, 1, 1, vec![entry(1), noop(2)], 0)
            )],
        );

        // A second failure walks it back once more: anchor at 0, whole log.
        let effects = n.step(deliver(2, ae_reply(2, false, 0)), NOW);
        assert_eq!(
            effects,
            vec![send(
                2,
                append_entries(2, 1, 0, 0, vec![entry(1), entry(1), noop(2)], 0)
            )],
        );
    }

    #[test]
    fn an_ack_from_a_newer_term_makes_the_leader_step_down() {
        let mut n = node(1, &[2, 3]);
        time_out(&mut n);
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
        time_out(&mut n); // term 2
        drive(&mut n, deliver(2, vote_reply(2, true))); // leader, term 2

        let effects = n.step(deliver(2, ae_reply(1, true, 1)), NOW);

        assert!(effects.is_empty());
        assert_eq!(n.commit_index(), LogIndex::ZERO);
        assert!(n.is_leader());
        // It vouches for node 2's log in a term we no longer lead.
        if let Role::Leader { progress, .. } = n.role() {
            assert_eq!(progress[&NodeId::new(2)].match_index, LogIndex::ZERO);
        } else {
            unreachable!("still a leader");
        }
    }

    #[test]
    fn a_prior_term_entry_commits_only_behind_a_current_term_entry() {
        let mut n = node(1, &[2, 3]);
        n.log.append(entry_cmd(1, b"old")); // index 1, term 1
        n.current_term = Term::new(1);
        time_out(&mut n); // term 2, candidate
        drive(&mut n, deliver(2, vote_reply(2, true))); // leader, term 2

        // The term-1 entry reaches a majority, but a leader never commits an
        // earlier term's entry by count alone (paper Figure 8).
        let effects = n.step(deliver(2, ae_reply(2, true, 1)), NOW);
        assert_eq!(n.commit_index(), LogIndex::ZERO);
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::ApplyToStateMachine { .. })),
            "applied an uncommitted entry: {effects:?}",
        );

        // The leader's own no-op at index 2 now reaches a majority.
        let effects = n.step(deliver(2, ae_reply(2, true, 2)), NOW);

        // Committing index 2 carries index 1 with it; the no-op itself is not
        // the application's.
        assert_eq!(n.commit_index(), LogIndex::new(2));
        assert_eq!(n.last_applied(), LogIndex::new(2));
        assert_eq!(
            effects,
            vec![Effect::ApplyToStateMachine {
                index: LogIndex::new(1),
                command: Bytes::from_static(b"old"),
            }],
        );
    }

    #[test]
    fn a_lone_leader_commits_and_applies_a_proposal_in_one_step() {
        let mut n = node(1, &[]);
        time_out(&mut n); // sole vote -> leader, term 1, no-op committed

        let effects = n.step(propose(b"solo"), NOW);

        assert_eq!(n.commit_index(), LogIndex::new(2));
        assert_eq!(n.last_applied(), LogIndex::new(2));
        assert_eq!(
            effects,
            vec![
                Effect::PersistLog {
                    from_index: LogIndex::new(2),
                    entries: vec![entry_cmd(1, b"solo")],
                },
                Effect::ApplyToStateMachine {
                    index: LogIndex::new(2),
                    command: Bytes::from_static(b"solo"),
                },
            ],
        );
    }

    // --- 8: snapshotting / InstallSnapshot ------------------------------------

    /// Arranges `n` as leader for term 2 with a five-entry term-1 log (plus
    /// its term-2 no-op at index 6), then compacts it up to `up_to` so the
    /// compaction base is non-zero.
    fn leader_with_compacted_log(up_to: u64) -> RaftNode {
        let mut n = node(1, &[2, 3]);
        for _ in 0..5 {
            n.log.append(entry(1));
        }
        n.current_term = Term::new(1);
        time_out(&mut n); // term 2, candidate
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

    /// Node 1 of {1, 2, 3}, a term-1 follower of node 2 holding entries 1–5,
    /// all committed and applied, with 1–3 compacted into a snapshot.
    fn follower_behind_a_snapshot() -> RaftNode {
        let mut n = node(1, &[2, 3]);
        for _ in 0..5 {
            n.log.append(entry(1));
        }
        n.current_term = Term::new(1);
        n.commit_index = LogIndex::new(5);
        n.last_applied = LogIndex::new(5);
        drive(
            &mut n,
            Input::CompactLog {
                up_to_index: LogIndex::new(3),
            },
        );
        n
    }

    #[test]
    fn config_at_is_the_latest_config_entry_at_or_below_the_index() {
        let mut n = node(1, &[2, 3]);
        n.log.append(entry(1)); // 1
        n.log
            .append(LogEntry::config(Term::new(1), config(&[1, 2, 3, 4]))); // 2
        n.log.append(entry(1)); // 3
        n.log
            .append(LogEntry::config(Term::new(1), config(&[1, 2, 4]))); // 4

        assert_eq!(n.config_at(LogIndex::new(1)), config(&[1, 2, 3]));
        assert_eq!(n.config_at(LogIndex::new(2)), config(&[1, 2, 3, 4]));
        assert_eq!(n.config_at(LogIndex::new(3)), config(&[1, 2, 3, 4]));
        assert_eq!(n.config_at(LogIndex::new(4)), config(&[1, 2, 4]));
    }

    #[test]
    fn compacting_past_a_config_entry_keeps_that_configuration() {
        let mut n = node(1, &[2, 3]);
        n.log.append(entry(1)); // 1
        n.log
            .append(LogEntry::config(Term::new(1), config(&[1, 2]))); // 2
        n.log.append(entry(1)); // 3
        n.log
            .append(LogEntry::config(Term::new(1), config(&[1, 2, 5]))); // 4, later
        n.current_term = Term::new(1);
        n.commit_index = LogIndex::new(3);
        n.last_applied = LogIndex::new(3);

        drive(
            &mut n,
            Input::CompactLog {
                up_to_index: LogIndex::new(3),
            },
        );

        // The removal of node 3 is folded into the snapshot, not reverted to
        // the bootstrap set; the later entry is still in force on top of it.
        assert_eq!(n.config_at(LogIndex::new(3)), config(&[1, 2]));
        assert_eq!(n.config(), &config(&[1, 2, 5]));

        // Once the later entry is compacted too, it becomes the fallback.
        n.commit_index = LogIndex::new(4);
        n.last_applied = LogIndex::new(4);
        drive(
            &mut n,
            Input::CompactLog {
                up_to_index: LogIndex::new(4),
            },
        );
        assert_eq!(n.log().snapshot_last_index(), n.log().last_index());
        assert_eq!(n.config(), &config(&[1, 2, 5]));
    }

    #[test]
    fn a_learner_that_compacts_away_its_admission_keeps_its_configuration() {
        let mut n = RaftNode::new_learner(NodeId::new(4));
        drive(
            &mut n,
            deliver(
                1,
                append_entries(
                    1,
                    1,
                    0,
                    0,
                    vec![
                        entry(1),
                        LogEntry::config(Term::new(1), config(&[1, 2, 3, 4])),
                    ],
                    2,
                ),
            ),
        );
        assert!(n.config().contains(NodeId::new(4)));

        drive(
            &mut n,
            Input::CompactLog {
                up_to_index: LogIndex::new(2),
            },
        );

        // Before the fix this fell back to the learner's empty bootstrap set,
        // leaving a voter (or leader) with no quorum at all.
        assert_eq!(n.config(), &config(&[1, 2, 3, 4]));
    }

    #[test]
    fn a_stale_append_entries_anchored_below_the_snapshot_leaves_the_log_intact() {
        let mut n = follower_behind_a_snapshot();
        let log_before = n.log().clone();

        // A delayed RPC from before the compaction: anchored at 0, carrying
        // entries the snapshot already holds. Treating the snapshot as a
        // conflict would truncate below the base and append entry 1 at 4.
        let effects = n.step(
            deliver(2, append_entries(1, 2, 0, 0, vec![entry(1), entry(1)], 2)),
            NOW,
        );

        assert_eq!(n.log(), &log_before);
        assert_eq!(effects.last(), Some(&ack(2, 1, 2)));
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::PersistLog { .. }))
        );
    }

    #[test]
    fn an_append_entries_straddling_the_snapshot_appends_only_past_it() {
        let mut n = follower_behind_a_snapshot();

        let effects = n.step(
            deliver(
                2,
                append_entries(1, 2, 1, 1, vec![entry(1); 5], 5), // entries 2..=6
            ),
            NOW,
        );

        assert_eq!(n.log().snapshot_last_index(), LogIndex::new(3));
        assert_eq!(n.log().last_index(), LogIndex::new(6));
        assert!(effects.contains(&Effect::PersistLog {
            from_index: LogIndex::new(6),
            entries: vec![entry(1)],
        }));
        assert_eq!(effects.last(), Some(&ack(2, 1, 6)));
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

    /// A term-4 leader whose log holds terms `[1, 1, 1, 3, 3]`, so every
    /// peer starts probing at `nextIndex` 6.
    fn leader_over_two_terms() -> RaftNode {
        let mut n = node(1, &[2, 3]);
        for term in [1, 1, 1, 3, 3] {
            n.log.append(entry(term));
        }
        n.current_term = Term::new(3);
        time_out(&mut n); // term 4
        drive(&mut n, deliver(2, vote_reply(4, true)));
        assert!(n.is_leader());
        assert_eq!(progress_of(&n, 2).next_index, LogIndex::new(6));
        n
    }

    /// The `prev_log_index` of every `AppendEntries` in `effects`.
    fn anchors(effects: &[Effect]) -> Vec<u64> {
        effects
            .iter()
            .filter_map(|e| match e {
                Effect::SendRpc {
                    message: Message::AppendEntries(args),
                    ..
                } => Some(args.prev_log_index.get()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_hint_for_a_term_the_leader_lacks_skips_the_whole_run() {
        let mut n = leader_over_two_terms();

        // Peer 2 holds term 2 from index 4 on; we never had term 2.
        let effects = n.step(deliver(2, rejected(4, Some(2), 4)), NOW);

        assert_eq!(progress_of(&n, 2).next_index, LogIndex::new(4));
        assert_eq!(anchors(&effects), vec![3]);
    }

    #[test]
    fn a_hint_for_a_term_the_leader_holds_resumes_past_its_own_run() {
        let mut n = leader_over_two_terms();

        // Peer 2's entry 5 is from term 1, which it has held since index 1;
        // our term-1 run ends at 3, so its first three entries may match.
        let effects = n.step(deliver(2, rejected(4, Some(1), 1)), NOW);

        assert_eq!(progress_of(&n, 2).next_index, LogIndex::new(4));
        assert_eq!(anchors(&effects), vec![3]);
    }

    #[test]
    fn a_hint_from_a_short_log_resumes_just_past_its_end() {
        let mut n = leader_over_two_terms();

        let effects = n.step(deliver(2, rejected(4, None, 2)), NOW);

        assert_eq!(progress_of(&n, 2).next_index, LogIndex::new(2));
        assert_eq!(anchors(&effects), vec![1]);
    }

    #[test]
    fn a_hint_never_moves_next_index_past_the_plain_back_off() {
        let mut n = leader_over_two_terms();

        // A stale or bogus hint pointing past the probe is ignored: the probe
        // still steps back by one.
        drive(&mut n, deliver(2, rejected(4, None, 9)));
        assert_eq!(progress_of(&n, 2).next_index, LogIndex::new(5));

        // Same for a term run we end later than the probe point.
        drive(&mut n, deliver(2, rejected(4, Some(3), 4)));
        assert_eq!(progress_of(&n, 2).next_index, LogIndex::new(4));
    }

    #[test]
    fn a_hint_below_the_snapshot_base_sends_the_snapshot() {
        let mut n = leader_with_compacted_log(3); // base at index 3

        let effects = n.step(deliver(3, rejected(2, None, 2)), NOW);

        assert_eq!(effects, vec![Effect::SendSnapshot { to: NodeId::new(3) }]);
    }

    #[test]
    fn a_heartbeat_tick_sends_a_snapshot_to_a_peer_behind_the_base() {
        let mut n = leader_with_compacted_log(3);
        if let Role::Leader { progress, .. } = &mut n.role {
            progress.insert(NodeId::new(3), Progress::new(LogIndex::new(1)));
        }

        let effects = n.step(Input::HeartbeatTick, NOW);

        assert!(effects.contains(&Effect::SendSnapshot { to: NodeId::new(3) }));
    }

    /// A term-2 leader with entries 1–3 compacted (of 6) whose peer 3 has
    /// just been sent the snapshot by a heartbeat, with a retry budget of
    /// `retry_ticks`.
    fn leader_awaiting_a_snapshot_ack(retry_ticks: u32) -> RaftNode {
        let mut n = leader_with_compacted_log(3).with_replication(ReplicationConfig {
            snapshot_retry_ticks: retry_ticks,
            ..ReplicationConfig::default()
        });
        if let Role::Leader { progress, .. } = &mut n.role {
            progress.insert(NodeId::new(3), Progress::new(LogIndex::new(1)));
        }
        let effects = n.step(Input::HeartbeatTick, NOW);
        assert!(effects.contains(&Effect::SendSnapshot { to: NodeId::new(3) }));
        n
    }

    fn sends_to(effects: &[Effect], peer: u64) -> bool {
        effects.iter().any(|e| match e {
            Effect::SendRpc { to, .. } | Effect::SendSnapshot { to } => *to == NodeId::new(peer),
            _ => false,
        })
    }

    #[test]
    fn a_peer_sent_the_snapshot_waits_for_its_reply() {
        let mut n = leader_awaiting_a_snapshot_ack(3);
        assert_eq!(progress_of(&n, 3).flow, Flow::Snapshot { ticks_left: 3 });

        // Neither heartbeats nor proposals send it anything more.
        assert!(!sends_to(&n.step(Input::HeartbeatTick, NOW), 3));
        assert!(!sends_to(&n.step(propose(b"cmd"), NOW), 3));
        assert_eq!(progress_of(&n, 3).flow, Flow::Snapshot { ticks_left: 2 });
    }

    #[test]
    fn a_snapshot_with_no_reply_is_sent_again_once_the_budget_runs_out() {
        let mut n = leader_awaiting_a_snapshot_ack(2);

        assert!(!sends_to(&n.step(Input::HeartbeatTick, NOW), 3));
        let effects = n.step(Input::HeartbeatTick, NOW);

        assert!(effects.contains(&Effect::SendSnapshot { to: NodeId::new(3) }));
        assert_eq!(progress_of(&n, 3).flow, Flow::Snapshot { ticks_left: 2 });
    }

    #[test]
    fn a_snapshot_reply_switches_the_peer_to_streaming_the_rest() {
        let mut n = leader_awaiting_a_snapshot_ack(3);

        let effects = n.step(deliver(3, snapshot_reply_msg(2, 3)), NOW);

        assert_eq!(
            effects,
            vec![send(
                3,
                append_entries(2, 1, 3, 1, vec![entry(1), entry(1), noop(2)], 5)
            )],
        );
        assert_eq!(progress_of(&n, 3).flow, streaming(&[6]));
    }

    #[test]
    fn stale_append_entries_replies_do_not_disturb_a_snapshot_wait() {
        let mut n = leader_awaiting_a_snapshot_ack(3);

        // A rejection and an ack, both from before the snapshot went out.
        assert!(n.step(deliver(3, ae_reply(2, false, 0)), NOW).is_empty());
        assert!(rpcs(n.step(deliver(3, ae_reply(2, true, 1)), NOW)).is_empty());

        assert_eq!(progress_of(&n, 3).flow, Flow::Snapshot { ticks_left: 3 });
        assert_eq!(progress_of(&n, 3).match_index, LogIndex::new(1));
    }

    #[test]
    fn a_peer_compacted_past_again_mid_wait_is_sent_the_newer_snapshot() {
        let mut n = leader_awaiting_a_snapshot_ack(3);
        drive(
            &mut n,
            Input::CompactLog {
                up_to_index: LogIndex::new(5),
            },
        );

        // The reply covers only the old snapshot, still behind the new base.
        let effects = n.step(deliver(3, snapshot_reply_msg(2, 3)), NOW);

        assert_eq!(effects, vec![Effect::SendSnapshot { to: NodeId::new(3) }]);
        assert_eq!(progress_of(&n, 3).flow, Flow::Snapshot { ticks_left: 3 });
    }

    #[test]
    fn an_install_snapshot_reply_advances_next_index_past_the_compacted_region() {
        let mut n = leader_with_compacted_log(3);
        if let Role::Leader { progress, .. } = &mut n.role {
            progress.insert(NodeId::new(2), Progress::new(LogIndex::new(1)));
        }

        let effects = n.step(deliver(2, snapshot_reply_msg(2, 4)), NOW);

        // Entry 5 and the leader's no-op, all that is past the snapshot, are
        // streamed straight after.
        assert_eq!(
            effects,
            vec![send(
                2,
                append_entries(2, 1, 4, 1, vec![entry(1), noop(2)], 5)
            )],
        );
        if let Role::Leader { progress, .. } = n.role() {
            assert_eq!(progress[&NodeId::new(2)].next_index, LogIndex::new(7));
            assert_eq!(progress[&NodeId::new(2)].match_index, LogIndex::new(4));
        } else {
            unreachable!("still a leader");
        }
    }

    #[test]
    fn a_stale_install_snapshot_reply_is_ignored() {
        let mut n = leader_with_compacted_log(3); // leader, term 2
        if let Role::Leader { progress, .. } = &mut n.role {
            progress.insert(NodeId::new(2), Progress::new(LogIndex::new(1)));
        }

        let effects = n.step(deliver(2, snapshot_reply_msg(1, 4)), NOW);

        assert!(effects.is_empty());
        if let Role::Leader { progress, .. } = n.role() {
            assert_eq!(progress[&NodeId::new(2)].next_index, LogIndex::new(1));
            assert_eq!(progress[&NodeId::new(2)].match_index, LogIndex::ZERO);
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
        assert!(config(&[]).is_empty());
        let base = config(&[1, 2, 3]);
        assert!(!base.is_empty());
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

    /// A three-node leader for term 2 whose opening no-op, at index 1, is
    /// committed — so it may start a membership change.
    fn three_node_leader() -> RaftNode {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);
        time_out(&mut n); // term 2, candidate
        drive(&mut n, deliver(2, vote_reply(2, true))); // leader, term 2
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
        if let Role::Leader { progress, .. } = n.role() {
            assert!(progress.contains_key(&new));
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

    /// The single-server membership bug (Ongaro, raft-dev 2015): a leader
    /// whose predecessor left an uncommitted config entry on a minority could
    /// otherwise append a change whose majority does not overlap that one's.
    /// The fix is to change membership only once an entry of the leader's own
    /// term — its opening no-op — has committed.
    #[test]
    fn a_new_leader_changes_membership_only_once_its_noop_commits() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(1);
        time_out(&mut n); // term 2, candidate
        drive(&mut n, deliver(2, vote_reply(2, true))); // leader, term 2

        let remove = change(MembershipChange::RemoveServer(NodeId::new(3)));
        assert!(n.step(remove.clone(), NOW).is_empty());
        assert_eq!(n.config().len(), 3);
        assert!(n.pending_change.is_none());

        drive(&mut n, deliver(2, ae_reply(2, true, 1))); // the no-op commits
        let effects = n.step(remove, NOW);

        assert_eq!(n.config().len(), 2);
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::PersistLog { .. }))
        );
    }

    #[test]
    fn remove_server_appends_a_config_entry_immediately() {
        let mut n = node(1, &[2, 3, 4, 5]);
        n.current_term = Term::new(1);
        time_out(&mut n);
        drive(&mut n, deliver(2, vote_reply(2, true)));
        drive(&mut n, deliver(3, vote_reply(2, true)));
        assert!(n.is_leader());
        // Two acks of the opening no-op commit it: a quorum of five.
        drive(&mut n, deliver(2, ae_reply(2, true, 1)));
        drive(&mut n, deliver(3, ae_reply(2, true, 1)));

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
    fn a_removed_peer_is_replicated_to_until_its_removal_commits() {
        let mut n = three_node_leader();
        let removed = NodeId::new(3);
        let sends_to = |effects: &[Effect], peer: NodeId| {
            effects.iter().any(|e| {
                matches!(e, Effect::SendRpc { to, message: Message::AppendEntries(_) } if *to == peer)
            })
        };

        // The config entry that removes node 3 goes to node 3 as well.
        drive(&mut n, deliver(3, ae_reply(2, true, 1)));
        let effects = n.step(change(MembershipChange::RemoveServer(removed)), NOW);
        assert!(!n.config().contains(removed));
        assert!(sends_to(&effects, removed));
        assert!(sends_to(&n.step(Input::HeartbeatTick, NOW), removed));

        // Once the removal commits on {1, 2}, node 3 is dropped.
        let config_index = n.log().last_index().get();
        drive(&mut n, deliver(2, ae_reply(2, true, config_index)));
        assert_eq!(n.commit_index().get(), config_index);
        assert!(!sends_to(&n.step(Input::HeartbeatTick, NOW), removed));
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
    fn adding_a_current_member_is_a_no_op() {
        let mut n = three_node_leader();

        let effects = n.step(change(MembershipChange::AddServer(NodeId::new(2))), NOW);

        assert!(effects.is_empty());
        assert!(n.pending_change.is_none());
    }

    #[test]
    fn an_add_server_keeps_catching_up_until_its_budget_runs_out() {
        let mut n = three_node_leader();
        drive(&mut n, change(MembershipChange::AddServer(NodeId::new(4))));

        for _ in 1..CATCH_UP_TICKS {
            drive(&mut n, Input::HeartbeatTick);
        }

        assert!(n.pending_change.is_some(), "abandoned before the last tick");
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
        if let Role::Leader { progress, .. } = n.role() {
            assert!(!progress.contains_key(&new));
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
        if let Role::Leader { progress, .. } = b.role() {
            assert!(progress.contains_key(&NodeId::new(3)));
        } else {
            unreachable!("still leader");
        }
    }

    /// Node 2 with `log` and `commit_index`, elected leader of term 2 by fiat:
    /// arranges exactly the state `become_leader` inherits from a predecessor.
    fn leader_inheriting(log: Log, commit_index: u64) -> RaftNode {
        let mut n = node(2, &[1, 3]);
        n.current_term = Term::new(2);
        n.log = log;
        n.commit_index = LogIndex::new(commit_index);
        n.recompute_config();
        n.become_leader();
        n
    }

    fn pending(n: &RaftNode) -> Option<(MembershipChange, LogIndex)> {
        match n.pending_change {
            Some(super::PendingChange {
                change,
                phase: super::ChangePhase::Committing { config_index },
            }) => Some((change, config_index)),
            _ => None,
        }
    }

    #[test]
    fn a_new_leader_adopts_an_uncommitted_add_server() {
        let mut log = Log::new();
        log.append(LogEntry::config(Term::new(1), config(&[1, 2, 3, 4])));

        let n = leader_inheriting(log, 0);

        assert_eq!(
            pending(&n),
            Some((
                MembershipChange::AddServer(NodeId::new(4)),
                LogIndex::new(1)
            ))
        );
        if let Role::Leader { progress, .. } = n.role() {
            assert!(progress.contains_key(&NodeId::new(4)));
        } else {
            unreachable!("still leader");
        }
    }

    #[test]
    fn a_new_leader_diffs_against_the_previous_config_entry() {
        // Bootstrap {1,2,3} -> {1,2,3,4} (committed) -> {1,2,4} (not yet).
        // Only the last step is in flight; diffing against the bootstrap set
        // would see two changes at once.
        let mut log = Log::new();
        log.append(LogEntry::config(Term::new(1), config(&[1, 2, 3, 4])));
        log.append(LogEntry::config(Term::new(1), config(&[1, 2, 4])));

        let n = leader_inheriting(log.clone(), 1);
        assert_eq!(
            pending(&n),
            Some((
                MembershipChange::RemoveServer(NodeId::new(3)),
                LogIndex::new(2)
            ))
        );

        // With that entry committed too there is nothing left to finish.
        let n = leader_inheriting(log, 2);
        assert!(n.pending_change.is_none());
    }

    #[test]
    fn a_new_leader_finds_its_config_entry_past_the_compaction_base() {
        // Indices 1..=3 are in the snapshot; the entries are 4, 5 and 6.
        let log = Log::from_snapshot(
            LogIndex::new(3),
            Term::new(1),
            vec![
                entry(1),
                entry(1),
                LogEntry::config(Term::new(1), config(&[1, 2])),
            ],
        );

        let n = leader_inheriting(log, 5);

        assert_eq!(
            pending(&n),
            Some((
                MembershipChange::RemoveServer(NodeId::new(3)),
                LogIndex::new(6)
            ))
        );
    }

    #[test]
    fn a_learner_that_overtakes_its_round_target_is_promoted() {
        let mut n = three_node_leader();
        let new = NodeId::new(4);
        drive(&mut n, change(MembershipChange::AddServer(new))); // target: index 1
        drive(&mut n, propose(b"c1")); // index 2, sent to the learner too

        // The learner acks the whole log in one go, past the round's target.
        drive(&mut n, deliver(4, ae_reply(2, true, 2)));

        assert!(n.config().contains(new));
    }

    #[test]
    fn a_removal_finishes_only_when_its_own_entry_commits() {
        let mut n = three_node_leader();
        drive(&mut n, propose(b"c1")); // index 2
        drive(
            &mut n,
            change(MembershipChange::RemoveServer(NodeId::new(3))),
        ); // index 3
        let removal = Some((
            MembershipChange::RemoveServer(NodeId::new(3)),
            LogIndex::new(3),
        ));

        // Committing the entry *before* the config entry is not enough.
        let effects = n.step(deliver(2, ae_reply(2, true, 2)), NOW);
        assert_eq!(n.commit_index(), LogIndex::new(2));
        assert_eq!(pending(&n), removal);
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::MembershipChanged { .. }))
        );

        let effects = n.step(deliver(2, ae_reply(2, true, 3)), NOW);
        assert!(n.pending_change.is_none());
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::MembershipChanged { .. }))
        );
        // Removing someone else does not make the leader step down.
        assert!(n.is_leader());
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
        time_out(&mut n);
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

    // --- leadership transfer (thesis §3.10) --------------------------------

    fn transfer_to(target: u64) -> Input {
        Input::TransferLeadership {
            target: NodeId::new(target),
        }
    }

    fn timeout_now_args(term: u64, leader: u64) -> TimeoutNowArgs {
        TimeoutNowArgs {
            term: Term::new(term),
            leader_id: NodeId::new(leader),
        }
    }

    fn timeout_now(to: u64, term: u64, leader: u64) -> Effect {
        send(to, Message::TimeoutNow(timeout_now_args(term, leader)))
    }

    #[test]
    fn a_transfer_to_a_caught_up_voter_sends_timeout_now_at_once() {
        let mut n = three_node_leader(); // node 2 has acked index 1

        let effects = n.step(transfer_to(2), NOW);

        assert_eq!(effects, vec![timeout_now(2, 2, 1)]);
        assert!(n.is_transferring());
        assert!(
            n.is_leader(),
            "the leader keeps office until it hears a newer term"
        );
    }

    #[test]
    fn a_transfer_to_a_lagging_voter_replicates_first() {
        let mut n = three_node_leader(); // node 3 has acked nothing

        let effects = n.step(transfer_to(3), NOW);

        assert_eq!(
            effects,
            vec![send(3, append_entries(2, 1, 0, 0, vec![noop(2)], 1))],
        );
        assert!(n.is_transferring());
    }

    #[test]
    fn timeout_now_goes_out_only_once_the_target_holds_the_whole_log() {
        let mut n = three_node_leader();
        drive(&mut n, deliver(2, ae_reply(2, true, 1))); // node 2 stays current
        drive(&mut n, propose(b"c1")); // index 2
        drive(&mut n, transfer_to(3));

        // An ack short of the log end is not enough.
        let effects = n.step(deliver(3, ae_reply(2, true, 1)), NOW);
        assert!(!effects.contains(&timeout_now(3, 2, 1)));

        // Reaching the end is.
        let effects = n.step(deliver(3, ae_reply(2, true, 2)), NOW);
        assert!(effects.contains(&timeout_now(3, 2, 1)));
    }

    #[test]
    fn an_ack_from_another_peer_does_not_send_timeout_now() {
        let mut n = three_node_leader();
        drive(&mut n, transfer_to(3));

        let effects = n.step(deliver(2, ae_reply(2, true, 1)), NOW);

        assert!(effects.iter().all(|e| !matches!(
            e,
            Effect::SendRpc {
                message: Message::TimeoutNow(_),
                ..
            }
        )));
    }

    #[test]
    fn every_ack_from_a_caught_up_target_resends_timeout_now() {
        let mut n = three_node_leader();
        drive(&mut n, transfer_to(2)); // first TimeoutNow, assume lost

        // The next heartbeat's ack brings another.
        let effects = n.step(deliver(2, ae_reply(2, true, 1)), NOW);

        assert!(effects.contains(&timeout_now(2, 2, 1)));
    }

    #[test]
    fn an_install_snapshot_reply_can_complete_the_catch_up() {
        let mut n = leader_with_compacted_log(5);
        // Fold the no-op in too, so the whole log is the snapshot.
        n.commit_index = LogIndex::new(6);
        n.last_applied = LogIndex::new(6);
        drive(
            &mut n,
            Input::CompactLog {
                up_to_index: LogIndex::new(6),
            },
        );
        if let Role::Leader { progress, .. } = &mut n.role {
            progress.insert(NodeId::new(2), Progress::new(LogIndex::new(1)));
        }
        drive(&mut n, transfer_to(2));

        let effects = n.step(deliver(2, snapshot_reply_msg(2, 6)), NOW);

        assert!(effects.contains(&timeout_now(2, 2, 1)));
    }

    #[test]
    fn a_transferring_leader_drops_proposals() {
        let mut n = three_node_leader();
        drive(&mut n, transfer_to(3));
        let before = n.log().last_index();

        let effects = n.step(propose(b"late"), NOW);

        assert!(effects.is_empty());
        assert_eq!(n.log().last_index(), before);
    }

    #[test]
    fn a_transferring_leader_starts_no_membership_change() {
        let mut n = three_node_leader();
        drive(&mut n, transfer_to(3));

        let effects = n.step(change(MembershipChange::RemoveServer(NodeId::new(2))), NOW);

        assert!(effects.is_empty());
        assert!(n.pending_change.is_none());
    }

    #[test]
    fn a_transfer_is_refused_to_self_a_non_voter_or_a_second_target() {
        let mut n = three_node_leader();

        for target in [1, 4] {
            assert!(n.step(transfer_to(target), NOW).is_empty());
            assert!(!n.is_transferring());
        }

        drive(&mut n, transfer_to(3));
        assert!(n.step(transfer_to(2), NOW).is_empty());
        assert!(matches!(
            n.role(),
            Role::Leader { transfer: Some(t), .. } if t.target == NodeId::new(3)
        ));
    }

    #[test]
    fn a_transfer_is_refused_during_a_membership_change() {
        let mut n = three_node_leader();
        drive(&mut n, change(MembershipChange::AddServer(NodeId::new(4))));

        assert!(n.step(transfer_to(2), NOW).is_empty());
        assert!(!n.is_transferring());
    }

    #[test]
    fn a_transfer_is_ignored_by_a_non_leader() {
        let mut n = node(1, &[2, 3]);

        assert!(n.step(transfer_to(2), NOW).is_empty());
        assert!(!n.is_transferring());
    }

    #[test]
    fn stepping_down_ends_the_transfer() {
        let mut n = three_node_leader();
        drive(&mut n, transfer_to(2));

        drive(&mut n, deliver(2, vote_reply(3, false))); // term 3 exists

        assert!(n.is_follower());
        assert!(!n.is_transferring());
    }

    #[test]
    fn timeout_now_starts_a_real_election_without_a_pre_vote() {
        let mut n = node(2, &[1, 3]);
        n.current_term = Term::new(2);
        drive(&mut n, deliver(1, heartbeat(2, 1, 0, 0))); // leader 1 in contact

        let effects = n.step(deliver(1, Message::TimeoutNow(timeout_now_args(2, 1))), NOW);

        assert!(n.is_candidate());
        assert_eq!(n.current_term(), Term::new(3));
        assert_eq!(n.voted_for(), Some(NodeId::new(2)));
        assert_eq!(
            effects,
            vec![
                Effect::Persist {
                    current_term: Term::new(3),
                    voted_for: Some(NodeId::new(2)),
                },
                send(1, transfer_vote(3, 2, 0, 0)),
                send(3, transfer_vote(3, 2, 0, 0)),
                Effect::ResetElectionTimer,
            ],
        );
    }

    #[test]
    fn timeout_now_from_a_newer_term_is_adopted_before_campaigning() {
        let mut n = node(2, &[1, 3]);
        n.current_term = Term::new(2);

        drive(
            &mut n,
            deliver(1, Message::TimeoutNow(timeout_now_args(5, 1))),
        );

        assert!(n.is_candidate());
        assert_eq!(n.current_term(), Term::new(6));
    }

    #[test]
    fn a_stale_timeout_now_is_dropped() {
        let mut n = node(2, &[1, 3]);
        n.current_term = Term::new(4);

        let effects = n.step(deliver(1, Message::TimeoutNow(timeout_now_args(3, 1))), NOW);

        assert!(effects.is_empty());
        assert!(n.is_follower());
        assert_eq!(n.current_term(), Term::new(4));
    }

    #[test]
    fn a_leader_does_not_campaign_on_a_same_term_timeout_now() {
        let mut n = three_node_leader(); // term 2

        let effects = n.step(deliver(2, Message::TimeoutNow(timeout_now_args(2, 2))), NOW);

        assert!(effects.is_empty());
        assert!(n.is_leader());
        assert_eq!(n.current_term(), Term::new(2));
    }

    #[test]
    fn a_server_outside_its_configuration_does_not_campaign_on_timeout_now() {
        let mut n = RaftNode::new_learner(NodeId::new(4));

        let effects = n.step(deliver(1, Message::TimeoutNow(timeout_now_args(3, 1))), NOW);

        // It adopts the newer term, durably, but stays passive.
        assert_eq!(
            effects,
            vec![Effect::Persist {
                current_term: Term::new(3),
                voted_for: None,
            }],
        );
        assert!(n.is_follower());
    }

    #[test]
    fn a_follower_hearing_a_leader_grants_a_transfer_vote() {
        let mut n = node(1, &[2, 3]);
        n.current_term = Term::new(4);
        drive(&mut n, deliver(3, heartbeat(4, 3, 0, 0))); // leader 3 in contact

        let effects = n.step(deliver(2, transfer_vote(5, 2, 0, 0)), NOW);

        assert_eq!(n.current_term(), Term::new(5));
        assert_eq!(n.voted_for(), Some(NodeId::new(2)));
        assert!(effects.contains(&send(2, vote_reply(5, true))));
    }

    #[test]
    fn a_transfer_vote_still_needs_an_up_to_date_log() {
        let mut n = node(1, &[2, 3]);
        n.log.append(entry(4));
        n.current_term = Term::new(4);
        drive(&mut n, deliver(3, heartbeat(4, 3, 1, 4)));

        let effects = n.step(deliver(2, transfer_vote(5, 2, 0, 0)), NOW);

        assert_eq!(n.voted_for(), None);
        assert!(effects.contains(&send(2, vote_reply(5, false))));
    }

    #[test]
    fn a_transfer_survives_until_its_last_tick() {
        let mut n = three_node_leader();
        drive(&mut n, transfer_to(3));

        for _ in 1..TRANSFER_TICKS {
            drive(&mut n, Input::HeartbeatTick);
        }

        assert!(n.is_transferring(), "abandoned before the last tick");
    }

    #[test]
    fn a_stalled_transfer_is_abandoned_and_proposals_resume() {
        let mut n = three_node_leader();
        drive(&mut n, transfer_to(3));

        for _ in 0..TRANSFER_TICKS {
            drive(&mut n, Input::HeartbeatTick);
        }

        assert!(n.is_leader());
        assert!(!n.is_transferring());
        let before = n.log().last_index();
        drive(&mut n, propose(b"after"));
        assert_eq!(n.log().last_index(), before.next());
        // And a fresh transfer may start.
        assert!(!n.step(transfer_to(2), NOW).is_empty());
    }

    // --- Phase 2: ReadIndex (thesis §6.4) ------------------------------------

    fn read(id: u64) -> Input {
        Input::ReadIndex {
            id: ReadId::new(id),
        }
    }

    fn read_ready(id: u64, read_index: u64) -> Effect {
        Effect::ReadReady {
            id: ReadId::new(id),
            read_index: LogIndex::new(read_index),
        }
    }

    fn read_failed(id: u64) -> Effect {
        Effect::ReadFailed {
            id: ReadId::new(id),
        }
    }

    /// The `heartbeat_round` of every `AppendEntries` in `effects`.
    fn rounds(effects: &[Effect]) -> Vec<u64> {
        effects
            .iter()
            .filter_map(|e| match e {
                Effect::SendRpc {
                    message: Message::AppendEntries(args),
                    ..
                } => Some(args.heartbeat_round.get()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_read_id_round_trips_its_raw_value() {
        assert_eq!(ReadId::new(7).get(), 7);
        assert_eq!(HeartbeatRound::new(3).get(), 3);
        assert_eq!(HeartbeatRound::ZERO.next(), HeartbeatRound::new(1));
    }

    #[test]
    fn a_follower_fails_a_read_at_once() {
        let mut n = node(1, &[2, 3]);

        assert_eq!(n.step(read(7), NOW), vec![read_failed(7)]);
    }

    #[test]
    fn a_read_starts_a_new_round_sent_to_every_peer_at_once() {
        let mut n = acked_leader();

        let effects = n.step(read(7), NOW);

        assert_eq!(rounds(&effects), vec![1, 1]);
        assert!(!effects.contains(&read_ready(7, 1)));
    }

    #[test]
    fn a_quorum_echoing_the_reads_round_serves_it() {
        let mut n = acked_leader();
        drive(&mut n, read(7));

        let effects = n.step(deliver(2, ae_reply_in_round(1, true, 1, 1)), NOW);

        assert_eq!(effects, vec![read_ready(7, 1)]);
    }

    #[test]
    fn an_ack_for_a_round_before_the_read_does_not_confirm_it() {
        let mut n = acked_leader();
        drive(&mut n, read(7));

        // An AppendEntries sent before the read arrived proves nothing about
        // leadership after it.
        let effects = n.step(deliver(2, ae_reply_in_round(1, true, 1, 0)), NOW);

        assert!(!effects.contains(&read_ready(7, 1)));
    }

    #[test]
    fn a_rejection_at_the_leaders_term_still_confirms_a_read() {
        let mut n = acked_leader();
        drive(&mut n, read(7));

        let effects = n.step(deliver(2, ae_reply_in_round(1, false, 0, 1)), NOW);

        assert!(effects.contains(&read_ready(7, 1)));
    }

    #[test]
    fn later_proposals_carry_the_latest_round() {
        let mut n = acked_leader();
        drive(&mut n, read(7));
        drive(&mut n, read(8));

        let effects = n.step(propose(b"cmd"), NOW);

        assert_eq!(rounds(&effects), vec![2, 2]);
    }

    #[test]
    fn each_read_waits_for_its_own_round() {
        let mut n = acked_leader();
        drive(&mut n, read(7)); // round 1
        drive(&mut n, read(8)); // round 2

        let effects = n.step(deliver(2, ae_reply_in_round(1, true, 1, 1)), NOW);
        assert_eq!(effects, vec![read_ready(7, 1)]);

        let effects = n.step(deliver(3, ae_reply_in_round(1, true, 1, 2)), NOW);
        assert_eq!(effects, vec![read_ready(8, 1)]);
    }

    #[test]
    fn a_read_waits_for_the_leaders_noop_to_commit() {
        let mut n = node(1, &[2, 3]);
        time_out(&mut n);
        drive(&mut n, deliver(2, vote_reply(1, true))); // leader, no-op unacked
        drive(&mut n, read(7));

        // Leadership is confirmed, but commitIndex may still lag the
        // cluster's until the no-op commits.
        let effects = n.step(deliver(2, ae_reply_in_round(1, false, 0, 1)), NOW);
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::ReadReady { .. })),
            "served a read before the no-op committed: {effects:?}",
        );

        let effects = n.step(deliver(2, ae_reply_in_round(1, true, 1, 1)), NOW);
        assert!(effects.contains(&read_ready(7, 1)));
    }

    #[test]
    fn a_read_waits_until_the_state_machine_reaches_its_index() {
        let mut n = acked_leader();
        // As if index 1 were committed but not yet applied.
        n.last_applied = LogIndex::ZERO;
        drive(&mut n, read(7));

        let effects = n.step(deliver(2, ae_reply_in_round(1, true, 1, 1)), NOW);

        assert!(effects.is_empty(), "{effects:?}");
    }

    #[test]
    fn a_single_server_serves_a_read_at_once() {
        let mut n = node(1, &[]);
        drive(&mut n, Input::ElectionTimeout); // leader, no-op committed

        assert_eq!(n.step(read(7), NOW), vec![read_ready(7, 1)]);
    }

    #[test]
    fn a_leader_that_steps_down_fails_its_pending_reads() {
        let mut n = acked_leader();
        drive(&mut n, read(7));
        drive(&mut n, read(8));

        let effects = n.step(deliver(3, ae_reply(5, false, 0)), NOW);

        assert!(n.is_follower());
        assert!(effects.ends_with(&[read_failed(7), read_failed(8)]));
        // Nothing is left to answer later.
        drive(&mut n, deliver(2, ae_reply_in_round(1, true, 1, 2)));
        assert!(n.reads.is_empty());
    }

    #[test]
    fn a_read_no_quorum_confirms_fails_after_its_tick_budget() {
        let mut n = acked_leader();
        drive(&mut n, read(7));

        for _ in 1..READ_TICKS {
            let effects = n.step(Input::HeartbeatTick, NOW);
            assert!(!effects.contains(&read_failed(7)));
        }
        let effects = n.step(Input::HeartbeatTick, NOW);

        assert!(effects.contains(&read_failed(7)));
        assert!(n.is_leader());
        assert!(n.reads.is_empty());
    }

    #[test]
    fn a_read_confirmed_in_time_is_not_failed_later() {
        let mut n = acked_leader();
        drive(&mut n, read(7));
        drive(&mut n, read(8));
        for _ in 1..READ_TICKS {
            drive(&mut n, Input::HeartbeatTick);
        }

        // Read 7 is confirmed with a tick to spare; read 8 never is.
        let effects = n.step(deliver(2, ae_reply_in_round(1, true, 1, 1)), NOW);
        assert_eq!(effects, vec![read_ready(7, 1)]);
        let effects = n.step(Input::HeartbeatTick, NOW);
        assert!(effects.contains(&read_failed(8)));
        assert!(!effects.contains(&read_failed(7)));
    }

    #[test]
    fn only_voters_confirm_a_read() {
        let mut n = acked_leader();
        if let Role::Leader { progress, .. } = &mut n.role {
            progress.insert(NodeId::new(4), Progress::new(LogIndex::new(2)));
        }
        drive(&mut n, read(7));

        // Node 4 is a learner, not in the configuration.
        let effects = n.step(deliver(4, ae_reply_in_round(1, true, 1, 1)), NOW);

        assert!(!effects.contains(&read_ready(7, 1)));
    }

    #[test]
    fn a_follower_echoes_the_round_it_was_sent() {
        let mut n = node(1, &[2, 3]);
        let Message::AppendEntries(mut args) = heartbeat(1, 2, 0, 0) else {
            unreachable!("heartbeat builds an AppendEntries");
        };
        args.heartbeat_round = HeartbeatRound::new(9);

        let effects = n.step(deliver(2, Message::AppendEntries(args)), NOW);

        assert!(effects.contains(&send(2, ae_reply_in_round(1, true, 0, 9))));
    }
}
