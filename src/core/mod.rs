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

use std::fmt;

use bytes::Bytes;

mod message;

pub use message::{
    AppendEntriesArgs, AppendEntriesReply, Message, RequestVoteArgs, RequestVoteReply,
};

/// A Raft term: a logical clock that increases monotonically over the life of
/// the cluster.
///
/// Terms are totally ordered and start at [`Term::ZERO`]. A node's current term
/// must never decrease, so this type offers [`Term::next`] but no way to go
/// backwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

/// One entry in the replicated log: an opaque client command together with the
/// [`Term`] of the leader that first created it.
///
/// An entry does not carry its own index — the index is its 1-based position in
/// the [`Log`]. After log compaction the index of the first surviving entry is
/// tracked by the log, not by the entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    /// The term of the leader that first appended this entry.
    pub term: Term,
    /// The command to hand to the application state machine once the entry is
    /// committed. Raft never inspects these bytes.
    pub command: Bytes,
}

/// The replicated log.
///
/// Indices are **1-based**, matching the paper: the first entry is at
/// `LogIndex::new(1)`, and [`LogIndex::ZERO`] means "before the first entry"
/// (the `prevLogIndex` of an `AppendEntries` that carries the whole log, and
/// the starting `commitIndex` / `lastApplied`). All the index-to-position
/// arithmetic lives here so the rest of the core reads like Figure 2.
///
/// This is an in-memory view only; durability is the storage layer's job.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    /// An empty log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the log holds no entries.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The number of entries in the log.
    #[must_use]
    pub const fn len(&self) -> u64 {
        // usize -> u64 cannot lose bits on any platform Raft runs on.
        self.entries.len() as u64
    }

    /// The index of the last entry, or [`LogIndex::ZERO`] if the log is empty.
    #[must_use]
    pub const fn last_index(&self) -> LogIndex {
        LogIndex::new(self.len())
    }

    /// The term of the last entry, or [`Term::ZERO`] if the log is empty.
    #[must_use]
    pub fn last_term(&self) -> Term {
        self.entries.last().map_or(Term::ZERO, |entry| entry.term)
    }

    /// The entry at `index`, or `None` if `index` is [`LogIndex::ZERO`] or past
    /// the end of the log.
    #[must_use]
    pub fn get(&self, index: LogIndex) -> Option<&LogEntry> {
        let one_based = index.get();
        if one_based == 0 {
            return None;
        }
        let position = usize::try_from(one_based - 1).ok()?;
        self.entries.get(position)
    }

    /// The term of the entry at `index`, or `None` if there is no such entry.
    ///
    /// Used for the `AppendEntries` consistency check: a follower accepts new
    /// entries only when `term_at(prevLogIndex)` equals the leader's
    /// `prevLogTerm`.
    #[must_use]
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        self.get(index).map(|entry| entry.term)
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
    /// `truncate_after(LogIndex::ZERO)` empties the log. An `index` at or past
    /// the current end is a no-op.
    pub fn truncate_after(&mut self, index: LogIndex) {
        if let Ok(keep) = usize::try_from(index.get()) {
            self.entries.truncate(keep);
        }
    }
}

/// The role a server plays in the current term (paper §5.1).
///
/// A server is always in exactly one of these. It starts as [`Role::Follower`],
/// becomes a [`Role::Candidate`] when its election timer fires, and becomes
/// [`Role::Leader`] on winning an election.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Passive: responds to leaders and candidates, and starts an election if
    /// it stops hearing from a leader.
    Follower,
    /// Actively soliciting votes for its own term.
    Candidate,
    /// Won the election for the current term; replicates the log and sends
    /// heartbeats.
    Leader,
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
    /// Only a leader acts on it; other roles reject or redirect (a later step).
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
}

/// A side effect the core needs the driver to perform.
///
/// The core never acts on the world itself; [`RaftNode::step`] returns these in
/// the order they must happen and the driver executes them. In particular,
/// every [`Effect::Persist`] must be durable before the driver sends any
/// [`Effect::SendRpc`] that follows it in the same batch (persist-before-reply,
/// §8).
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
    /// log; log durability gets its own effect in the storage step.
    Persist {
        /// The term to persist.
        current_term: Term,
        /// The vote to persist.
        voted_for: Option<NodeId>,
    },
    /// Apply the committed entry at `index` to the application state machine.
    /// Emitted one entry at a time, in index order (Applied-in-order, §9.10).
    ApplyToStateMachine {
        /// Index of the entry to apply.
        index: LogIndex,
        /// The entry's command bytes.
        command: Bytes,
    },
    /// Restart the election timer with a fresh randomized duration (the driver
    /// owns the range and the seeded RNG).
    ResetElectionTimer,
}

/// A single Raft server: the pure state machine.
///
/// All behaviour flows through [`RaftNode::step`]. The struct holds only the
/// state from Figure 2; the driver holds the clock, RNG, storage, transport,
/// and timers.
#[derive(Clone, Debug)]
pub struct RaftNode {
    id: NodeId,
    /// Other servers in the cluster, sorted and deduplicated so that iteration
    /// order never influences behaviour.
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
}

impl RaftNode {
    /// Creates a fresh follower: term 0, no vote, empty log, nothing committed.
    ///
    /// `peers` is the set of *other* servers; `id` is removed from it if
    /// present, and the rest is sorted and deduplicated.
    #[must_use]
    pub fn new(id: NodeId, peers: impl IntoIterator<Item = NodeId>) -> Self {
        let mut peers: Vec<NodeId> = peers.into_iter().filter(|peer| *peer != id).collect();
        peers.sort_unstable();
        peers.dedup();
        Self {
            id,
            peers,
            current_term: Term::ZERO,
            voted_for: None,
            log: Log::new(),
            commit_index: LogIndex::ZERO,
            last_applied: LogIndex::ZERO,
            role: Role::Follower,
        }
    }

    /// Advances the state machine by one input and returns the effects the
    /// driver must perform, in order.
    ///
    /// `now` is the driver's current [`LogicalInstant`]; the core compares
    /// timeouts against it but never reads a clock of its own.
    #[must_use]
    pub fn step(&mut self, input: Input, now: LogicalInstant) -> Vec<Effect> {
        // Skeleton: the boundary is in place but no input does anything yet.
        // Leader election will handle `Deliver(RequestVote*)` and
        // `ElectionTimeout`; log replication will handle `Deliver(AppendEntries*)`,
        // `HeartbeatTick`, and `Propose`.
        let _ = (input, now);
        Vec::new()
    }

    /// This server's id.
    #[must_use]
    pub const fn id(&self) -> NodeId {
        self.id
    }

    /// The other servers in the cluster, sorted.
    #[must_use]
    pub fn peers(&self) -> &[NodeId] {
        &self.peers
    }

    /// The current role.
    #[must_use]
    pub const fn role(&self) -> Role {
        self.role
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
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{
        Input, Log, LogEntry, LogIndex, LogicalInstant, Message, NodeId, RaftNode, RequestVoteArgs,
        Role, Term,
    };

    fn entry(term: u64) -> LogEntry {
        LogEntry {
            term: Term::new(term),
            command: Bytes::from_static(b"cmd"),
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
    fn new_node_is_a_follower_at_term_zero_with_an_empty_log() {
        let node = RaftNode::new(NodeId::new(1), [NodeId::new(2), NodeId::new(3)]);
        assert_eq!(node.id(), NodeId::new(1));
        assert_eq!(node.role(), Role::Follower);
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
    fn step_is_a_no_op_for_every_input_in_the_skeleton() {
        let mut node = RaftNode::new(NodeId::new(1), [NodeId::new(2), NodeId::new(3)]);
        let now = LogicalInstant::START;

        assert!(node.step(Input::ElectionTimeout, now).is_empty());
        assert!(node.step(Input::HeartbeatTick, now).is_empty());
        assert!(
            node.step(
                Input::Propose {
                    command: Bytes::from_static(b"x"),
                },
                now,
            )
            .is_empty()
        );

        let vote = Message::RequestVote(RequestVoteArgs {
            term: Term::new(1),
            candidate_id: NodeId::new(2),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::ZERO,
        });
        assert!(
            node.step(
                Input::Deliver {
                    from: NodeId::new(2),
                    message: vote,
                },
                now,
            )
            .is_empty()
        );

        // State is untouched.
        assert_eq!(node.current_term(), Term::ZERO);
        assert_eq!(node.role(), Role::Follower);
    }
}
