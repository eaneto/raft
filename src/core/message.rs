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

use super::{LogEntry, LogIndex, NodeId, Term};

/// Arguments for the `RequestVote` RPC (Figure 2), sent by a candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestVoteArgs {
    /// The candidate's term.
    pub term: Term,
    /// The candidate requesting the vote.
    pub candidate_id: NodeId,
    /// Index of the candidate's last log entry (§5.4.1).
    pub last_log_index: LogIndex,
    /// Term of the candidate's last log entry (§5.4.1).
    pub last_log_term: Term,
}

/// Reply to a `RequestVote` RPC (Figure 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestVoteReply {
    /// The responder's `currentTerm`, so a stale candidate can step down.
    pub term: Term,
    /// Whether the vote was granted.
    pub vote_granted: bool,
}

/// Arguments for the `AppendEntries` RPC (Figure 2), sent by the leader both to
/// replicate entries and, with `entries` empty, as a heartbeat.
#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

/// A message between peers: one of the four in Figure 2.
///
/// The core emits these inside [`Effect::SendRpc`](super::Effect::SendRpc) and
/// receives them inside [`Input::Deliver`](super::Input::Deliver).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// A candidate soliciting a vote.
    RequestVote(RequestVoteArgs),
    /// A response to [`Message::RequestVote`].
    RequestVoteReply(RequestVoteReply),
    /// A leader replicating entries, or a heartbeat when `entries` is empty.
    AppendEntries(AppendEntriesArgs),
    /// A response to [`Message::AppendEntries`].
    AppendEntriesReply(AppendEntriesReply),
}
