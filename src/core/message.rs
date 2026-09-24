//! The RPC messages the core exchanges with its peers.
//!
//! These mirror the two RPCs of Figure 2 — a request and a reply for each —
//! expressed with the core's newtypes. They are plain data: the core produces
//! and consumes them, and a later transport layer serializes them. No wire
//! format is fixed here.
//!
//! The Figure 2 reply structs have no sender field. The core learns who a
//! message came from via [`Input::Deliver`](super::Input::Deliver)'s `from`,
//! which the transport always knows, so the messages stay faithful to the
//! paper.

use super::{ClusterConfig, LogEntry, LogIndex, NodeId, PreVoteRound, Term};

/// Arguments for the `RequestVote` RPC (Figure 2), sent by a candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RequestVoteArgs {
    /// The candidate's term.
    pub term: Term,
    /// The candidate requesting the vote.
    pub candidate_id: NodeId,
    /// Index of the candidate's last log entry (§5.4.1).
    pub last_log_index: LogIndex,
    /// Term of the candidate's last log entry (§5.4.1).
    pub last_log_term: Term,
    /// Set when the candidate is campaigning because the leader told it to
    /// with a [`Message::TimeoutNow`] (thesis §3.10). A voter still hearing
    /// from that leader would otherwise disregard the request (thesis
    /// §4.2.3); this flag says the leader itself wants to be replaced.
    pub leadership_transfer: bool,
}

/// Reply to a `RequestVote` RPC (Figure 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RequestVoteReply {
    /// The responder's `currentTerm`, so a stale candidate can step down.
    pub term: Term,
    /// Whether the vote was granted.
    pub vote_granted: bool,
}

/// Arguments for the `PreVote` RPC (thesis §9.6), sent by a *pre-candidate*
/// before it dares increment its term.
///
/// It mirrors [`RequestVoteArgs`] — same up-to-date check, same candidate id —
/// but asks a hypothetical question rather than requesting a commitment, and
/// answering it changes nothing on the recipient. The two differences from a
/// real `RequestVote` are that `term` is a term nobody has adopted, and the
/// extra `round`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PreVoteArgs {
    /// The term the pre-candidate *would* campaign in: its `currentTerm + 1`.
    /// It has **not** adopted this term, and will not unless the poll succeeds.
    pub term: Term,
    /// The pre-candidate running the poll.
    pub candidate_id: NodeId,
    /// Index of the pre-candidate's last log entry (§5.4.1).
    pub last_log_index: LogIndex,
    /// Term of the pre-candidate's last log entry (§5.4.1).
    pub last_log_term: Term,
    /// Which of the pre-candidate's rounds this is. Echoed verbatim in the
    /// reply so a "yes" cannot be spent in a later round — see
    /// [`PreVoteRound`].
    pub round: PreVoteRound,
}

/// Reply to a `PreVote` RPC (thesis §9.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PreVoteReply {
    /// The responder's real `currentTerm` — not the hypothetical one it was
    /// asked about — so a pre-candidate left behind can catch up.
    pub term: Term,
    /// Whether the responder *would* grant a real vote right now.
    pub vote_granted: bool,
    /// The `round` from the request, echoed unchanged.
    pub round: PreVoteRound,
}

/// Arguments for the `AppendEntries` RPC (Figure 2), sent by the leader both to
/// replicate entries and, with `entries` empty, as a heartbeat.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AppendEntriesArgs {
    /// The leader's term.
    pub term: Term,
    /// The leader, so a follower can redirect clients to it.
    pub leader_id: NodeId,
    /// Index of the log entry immediately preceding the new ones.
    pub prev_log_index: LogIndex,
    /// Term of the `prev_log_index` entry.
    pub prev_log_term: Term,
    /// Entries to store, in index order; empty for a heartbeat.
    pub entries: Vec<LogEntry>,
    /// The leader's `commitIndex`.
    pub leader_commit: LogIndex,
}

/// Reply to an `AppendEntries` RPC (Figure 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AppendEntriesReply {
    /// The responder's `currentTerm`, so a stale leader can step down.
    pub term: Term,
    /// `true` if the follower's log contained an entry matching
    /// `prev_log_index` and `prev_log_term`.
    pub success: bool,
    /// On `success`, the highest log index the follower has now stored for
    /// this leader — `prev_log_index + entries.len()`. The leader copies it
    /// straight into `matchIndex` for this peer, which is why a bare
    /// term/success reply is not enough (this mirrors `mmatchIndex` in
    /// Ongaro's TLA+ spec). Meaningless and set to [`LogIndex::ZERO`] when
    /// `success` is `false`.
    pub match_index: LogIndex,
}

/// Arguments for the `InstallSnapshot` RPC (Figure 13), sent by the leader to a
/// follower whose next entry has been compacted into the leader's snapshot.
///
/// The snapshot bytes are transferred as an ordered sequence of chunks; only
/// the final chunk carries `done = true`. The follower reassembles them and,
/// once it has the whole snapshot durable, replies exactly once.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InstallSnapshotArgs {
    /// The leader's term.
    pub term: Term,
    /// The leader, so a follower can redirect clients to it.
    pub leader_id: NodeId,
    /// The snapshot replaces every log entry up to and including this index.
    pub last_included_index: LogIndex,
    /// Term of the entry at `last_included_index`.
    pub last_included_term: Term,
    /// The cluster configuration in force at `last_included_index`, so a
    /// follower that installs this snapshot keeps its membership even after
    /// every configuration entry has been compacted away.
    pub config: ClusterConfig,
    /// Byte offset of `data` within the complete snapshot.
    pub offset: u64,
    /// Snapshot bytes beginning at `offset`.
    pub data: Vec<u8>,
    /// Whether `data` reaches the end of the snapshot.
    pub done: bool,
}

/// Reply to an `InstallSnapshot` RPC (Figure 13).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InstallSnapshotReply {
    /// The responder's `currentTerm`, so a stale leader can step down.
    pub term: Term,
    /// The `last_included_index` the follower installed, echoed from the
    /// request. Figure 13's reply carries only the term because there the
    /// leader remembers what it sent; here the pure core handles the reply
    /// without having sent the chunks itself (the driver holds the bytes), so
    /// the follower echoes the index and the leader sets `matchIndex` from it.
    pub last_included_index: LogIndex,
}

/// Arguments for the `TimeoutNow` RPC (thesis §3.10), sent by a leader that is
/// handing leadership over, once the target holds the leader's whole log.
///
/// It asks the target to start an election at once, as if its election timer
/// had fired, skipping the pre-vote round. It has no reply: the leader learns
/// the outcome from the new term the target's election brings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TimeoutNowArgs {
    /// The leader's term.
    pub term: Term,
    /// The leader handing over.
    pub leader_id: NodeId,
}

/// A message between peers: the four RPCs of Figure 2, the `PreVote` straw poll
/// (thesis §9.6), `InstallSnapshot` (Figure 13), and `TimeoutNow` (thesis
/// §3.10).
///
/// The core emits these inside [`Effect::SendRpc`](super::Effect::SendRpc) and
/// receives them inside [`Input::Deliver`](super::Input::Deliver).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Message {
    /// A candidate soliciting a vote.
    RequestVote(RequestVoteArgs),
    /// A response to [`Message::RequestVote`].
    RequestVoteReply(RequestVoteReply),
    /// A pre-candidate's straw poll before it increments its term (thesis
    /// §9.6): "if I started an election now, would you vote for me?".
    /// Answering one never changes the recipient's `currentTerm`, `votedFor`,
    /// or role.
    PreVote(PreVoteArgs),
    /// A response to [`Message::PreVote`]: `vote_granted` means the responder
    /// *would* grant a real vote now.
    PreVoteReply(PreVoteReply),
    /// A leader replicating entries, or a heartbeat when `entries` is empty.
    AppendEntries(AppendEntriesArgs),
    /// A response to [`Message::AppendEntries`].
    AppendEntriesReply(AppendEntriesReply),
    /// One chunk of a leader's snapshot, for a follower too far behind to catch
    /// up from the log alone.
    InstallSnapshot(InstallSnapshotArgs),
    /// A response to [`Message::InstallSnapshot`].
    InstallSnapshotReply(InstallSnapshotReply),
    /// A leader transferring leadership telling its caught-up target to start
    /// an election now.
    TimeoutNow(TimeoutNowArgs),
}
