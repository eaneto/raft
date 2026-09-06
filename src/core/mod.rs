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
    /// election (Figure 2, "Volatile state on leaders").
    Leader {
        /// For each peer, the index of the next log entry to send it.
        next_index: BTreeMap<NodeId, LogIndex>,
        /// For each peer, the highest log index known to be replicated on it.
        match_index: BTreeMap<NodeId, LogIndex>,
    },
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
                // Leader-side ack handling lands with log replication.
                Message::AppendEntriesReply(_) => Vec::new(),
            },
            Input::ElectionTimeout => self.handle_election_timeout(),
            Input::HeartbeatTick => self.handle_heartbeat_tick(),
            // Filled in by log replication.
            Input::Propose { .. } => Vec::new(),
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

    /// Handles an incoming `AppendEntries` (Figure 2).
    ///
    /// This step covers the term and role rules and the §5.3 log-matching
    /// *check*; it does not yet truncate, append, or advance `commitIndex`
    /// (Figure 2 receiver rules 3–5), which arrive with log replication. A
    /// heartbeat carries no entries, so those rules are inert here.
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
            effects.push(self.append_entries_reply(args.leader_id, false));
            return effects;
        }

        // A newer term, or the same term while we still think we are a
        // candidate: the sender is the leader for this term, so step down.
        if args.term > self.current_term || self.is_candidate() {
            self.become_follower(args.term);
        }

        let consistent = args.prev_log_index == LogIndex::ZERO
            || self.log.term_at(args.prev_log_index) == Some(args.prev_log_term);

        self.persist_if_changed(before, &mut effects);
        effects.push(Effect::ResetElectionTimer);
        effects.push(self.append_entries_reply(args.leader_id, consistent));
        effects
    }

    /// Handles the election timer firing: a follower or candidate starts a new
    /// election for `currentTerm + 1` (paper §5.2).
    ///
    /// Term monotonicity holds by construction — [`RaftNode::become_candidate`]
    /// only ever calls [`Term::next`], and a failed election just leaves us a
    /// candidate at the higher term until the timer fires again.
    fn handle_election_timeout(&mut self) -> Vec<Effect> {
        if self.is_leader() {
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

    /// Handles the heartbeat interval elapsing: a leader re-broadcasts
    /// `AppendEntries` to reassert authority; every other role ignores it.
    fn handle_heartbeat_tick(&self) -> Vec<Effect> {
        if self.is_leader() {
            self.broadcast_append_entries()
        } else {
            Vec::new()
        }
    }

    /// Adopts `term` as `currentTerm` and reverts to [`Role::Follower`].
    ///
    /// Clears `votedFor` only when the term actually advances, so a same-term
    /// step-down (e.g. a candidate conceding to a leader) needs no new fsync.
    /// Callers pass a `term` that is `>=` the current one.
    fn become_follower(&mut self, term: Term) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
    }

    /// Starts a candidacy: bump the term, vote for self, record that one vote.
    fn become_candidate(&mut self) {
        self.current_term = self.current_term.next();
        self.voted_for = Some(self.id);
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
        self.role = Role::Leader {
            next_index,
            match_index,
        };
    }

    /// If we are a candidate holding a quorum of votes, become leader and
    /// return the initial empty `AppendEntries` heartbeats that stop peers from
    /// starting their own elections.
    fn promote_if_quorum(&mut self) -> Vec<Effect> {
        let has_quorum = match &self.role {
            Role::Candidate { votes_granted } => votes_granted.len() >= self.quorum(),
            Role::Follower | Role::Leader { .. } => false,
        };
        if !has_quorum {
            return Vec::new();
        }

        self.become_leader();
        self.broadcast_append_entries()
    }

    /// Sends an `AppendEntries` to every peer.
    ///
    /// For now the payload is always an empty heartbeat; log replication will
    /// make it per-peer, carrying the entries from `next_index` onward.
    fn broadcast_append_entries(&self) -> Vec<Effect> {
        let prev_log_index = self.log.last_index();
        let prev_log_term = self.log.last_term();
        self.peers
            .iter()
            .map(|&peer| Effect::SendRpc {
                to: peer,
                message: Message::AppendEntries(AppendEntriesArgs {
                    term: self.current_term,
                    leader_id: self.id,
                    prev_log_index,
                    prev_log_term,
                    entries: Vec::new(),
                    leader_commit: self.commit_index,
                }),
            })
            .collect()
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
    const fn append_entries_reply(&self, leader: NodeId, success: bool) -> Effect {
        Effect::SendRpc {
            to: leader,
            message: Message::AppendEntriesReply(AppendEntriesReply {
                term: self.current_term,
                success,
            }),
        }
    }

    /// Number of servers in the cluster (peers plus self).
    const fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    /// Votes needed to win an election or commit an entry: a strict majority.
    const fn quorum(&self) -> usize {
        self.cluster_size() / 2 + 1
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
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{
        AppendEntriesArgs, AppendEntriesReply, Effect, Input, Log, LogEntry, LogIndex,
        LogicalInstant, Message, NodeId, RaftNode, RequestVoteArgs, RequestVoteReply, Role, Term,
    };

    /// The core does not consult the clock under this timer model, so every
    /// test steps at the same instant.
    const NOW: LogicalInstant = LogicalInstant::START;

    fn entry(term: u64) -> LogEntry {
        LogEntry {
            term: Term::new(term),
            command: Bytes::from_static(b"cmd"),
        }
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

    fn append_reply(to: u64, term: u64, success: bool) -> Effect {
        Effect::SendRpc {
            to: NodeId::new(to),
            message: Message::AppendEntriesReply(AppendEntriesReply {
                term: Term::new(term),
                success,
            }),
        }
    }

    fn deliver(from: u64, message: Message) -> Input {
        Input::Deliver {
            from: NodeId::new(from),
            message,
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
}
