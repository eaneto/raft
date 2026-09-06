//! Shared deterministic-simulation harness.
//!
//! [`Sim`] drives a whole cluster on one thread against a logical clock: a
//! simulated network (drop / duplicate / jitter-reorder / partition), a
//! logical clock, and an in-harness snapshot round-trip. It re-checks the Raft
//! safety properties after every core step. `tests/simulation.rs` uses it for
//! curated seed batteries; `tests/proptest.rs` for random schedules.

// The `pub` surface here is for the sibling test files that `mod`-include this
// harness; it has no external consumers, and not every test binary uses every
// helper.
#![allow(dead_code, unreachable_pub)]

use std::collections::BTreeMap;

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use raft::core::{
    Effect, Input, InstallSnapshotArgs, LogIndex, LogicalInstant, MembershipChange, Message,
    NodeId, RaftNode, Term,
};

// Timer and network constants, in logical milliseconds. The heartbeat period
// sits comfortably below the election-timeout floor, so a healthy leader is
// never unseated on a reliable network.
pub const ELECTION_MIN: u64 = 150;
pub const ELECTION_MAX: u64 = 300;
pub const HEARTBEAT_PERIOD: u64 = 50;
pub const NET_DELAY: u64 = 10;
/// Sentinel deadline meaning "not armed": a leader runs no election timer.
pub const NEVER: u64 = u64::MAX;

/// How the simulated network misbehaves for a run.
///
/// [`RELIABLE`] is in-order, lossless, single-partition, and draws no
/// randomness of its own, so the reliable-network tests stay bit-for-bit
/// reproducible even as the faulty paths are added around them.
#[derive(Clone, Copy)]
pub struct Net {
    /// Per-mille chance each sent message is dropped outright.
    pub drop_permille: u32,
    /// Per-mille chance a non-dropped message is also delivered a second time.
    pub dup_permille: u32,
    /// Extra delay, `0..=jitter`, added to `NET_DELAY` per message. Non-zero
    /// jitter lets later messages overtake earlier ones (reordering).
    pub jitter: u64,
    /// Re-draw the partition this often (logical ms); `0` never partitions.
    pub repartition_every: u64,
}

pub const RELIABLE: Net = Net {
    drop_permille: 0,
    dup_permille: 0,
    jitter: 0,
    repartition_every: 0,
};

/// A node's snapshot in the harness: `(last_index, last_term, applied-prefix)`.
/// The applied prefix stands in for real serialized state-machine bytes.
pub type SimSnapshot = (u64, u64, Vec<(u64, Bytes)>);

/// A message in flight on the simulated network.
pub struct Envelope {
    pub deliver_at: u64,
    pub from: NodeId,
    pub to: NodeId,
    pub message: Message,
}

/// A whole cluster driven on one thread against a logical clock.
pub struct Sim {
    pub seed: u64,
    pub now: u64,
    pub ids: Vec<NodeId>,
    pub nodes: Vec<RaftNode>,
    pub election_deadline: Vec<u64>,
    pub heartbeat_deadline: Vec<u64>,
    pub inflight: Vec<Envelope>,
    pub net: Net,
    /// Partition group per node; two nodes exchange messages only when their
    /// groups match. All-zero means one connected network.
    pub groups: Vec<usize>,
    pub next_net_change: u64,
    /// Per node, the `(index, command)` pairs the core told it to apply, in
    /// order. `step` asserts index order as it fills this. Also stands in for
    /// the state-machine state: a snapshot is a prefix of this list.
    pub applied: Vec<Vec<(u64, Bytes)>>,
    /// Compact a node's log once `lastApplied` runs this many entries past its
    /// last snapshot. `None` disables compaction (the default).
    pub snapshot_threshold: Option<u64>,
    /// Per node, the snapshot it currently holds. Mirrors the driver's cached
    /// snapshot, and is what a `SendSnapshot` effect streams to a follower.
    pub snapshot: Vec<Option<SimSnapshot>>,
    pub rng: StdRng,

    // --- invariant bookkeeping (Raft safety properties) ---
    pub prev_commit: Vec<u64>,
    pub prev_applied: Vec<u64>,
    /// index -> (entry term, command, term it was first observed committed in).
    pub committed: BTreeMap<u64, (u64, Bytes, u64)>,
    /// term -> the one node ever seen leading it (Election Safety).
    pub leader_of_term: BTreeMap<u64, usize>,
    pub saw_leader: bool,
}

impl Sim {
    pub fn new(node_count: usize, seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let ids: Vec<NodeId> = (1..=node_count).map(|i| NodeId::new(i as u64)).collect();
        let nodes: Vec<RaftNode> = ids
            .iter()
            .map(|&id| RaftNode::new(id, ids.iter().copied().filter(|&peer| peer != id)))
            .collect();
        let election_deadline = (0..node_count)
            .map(|_| rng.gen_range(ELECTION_MIN..ELECTION_MAX))
            .collect();
        let heartbeat_deadline = vec![HEARTBEAT_PERIOD; node_count];
        Self {
            seed,
            now: 0,
            ids,
            nodes,
            election_deadline,
            heartbeat_deadline,
            inflight: Vec::new(),
            net: RELIABLE,
            groups: vec![0; node_count],
            next_net_change: NEVER,
            applied: vec![Vec::new(); node_count],
            snapshot_threshold: None,
            snapshot: vec![None; node_count],
            rng,
            prev_commit: vec![0; node_count],
            prev_applied: vec![0; node_count],
            committed: BTreeMap::new(),
            leader_of_term: BTreeMap::new(),
            saw_leader: false,
        }
    }

    /// Switches on the given network faults. Chainable after [`Sim::new`].
    pub const fn with_net(mut self, net: Net) -> Self {
        self.net = net;
        self.next_net_change = if net.repartition_every > 0 {
            net.repartition_every
        } else {
            NEVER
        };
        self
    }

    /// Compacts every node's log once `lastApplied` reaches `threshold` entries
    /// past its last snapshot. Chainable after [`Sim::new`].
    pub const fn with_snapshot_threshold(mut self, threshold: u64) -> Self {
        self.snapshot_threshold = Some(threshold);
        self
    }

    pub fn index_of(&self, id: NodeId) -> usize {
        // Every routed id was built in `new`, so the fallback is unreachable.
        self.ids.iter().position(|&known| known == id).unwrap_or(0)
    }

    pub fn connected(&self, from: NodeId, to: NodeId) -> bool {
        self.groups[self.index_of(from)] == self.groups[self.index_of(to)]
    }

    pub fn next_event_time(&self) -> u64 {
        let timers = self
            .election_deadline
            .iter()
            .copied()
            .chain(self.heartbeat_deadline.iter().copied());
        let messages = self.inflight.iter().map(|envelope| envelope.deliver_at);
        timers
            .chain(messages)
            .chain(std::iter::once(self.next_net_change))
            .min()
            .unwrap_or(NEVER)
    }

    /// Runs the event loop until logical time `end` (inclusive), advancing the
    /// clock only to times where something actually happens.
    pub fn run_until(&mut self, end: u64) {
        loop {
            let now = self.next_event_time();
            if now > end {
                self.now = end;
                return;
            }
            self.now = now;

            if self.next_net_change == now {
                self.shuffle_partition();
                self.next_net_change = if self.net.repartition_every > 0 {
                    now + self.net.repartition_every
                } else {
                    NEVER
                };
            }

            for i in 0..self.nodes.len() {
                if self.election_deadline[i] == now {
                    self.step(i, Input::ElectionTimeout);
                }
                if self.heartbeat_deadline[i] == now {
                    self.heartbeat_deadline[i] = now + HEARTBEAT_PERIOD;
                    self.step(i, Input::HeartbeatTick);
                }
            }

            let (due, pending): (Vec<Envelope>, Vec<Envelope>) = self
                .inflight
                .drain(..)
                .partition(|envelope| envelope.deliver_at == now);
            self.inflight = pending;
            for envelope in due {
                // A partition drops in-flight messages across the cut.
                if !self.connected(envelope.from, envelope.to) {
                    continue;
                }
                let i = self.index_of(envelope.to);
                self.step(
                    i,
                    Input::Deliver {
                        from: envelope.from,
                        message: envelope.message,
                    },
                );
            }

            self.check_invariants();
        }
    }

    /// Steps node `i` with `input` and routes the effects it returns.
    pub fn step(&mut self, i: usize, input: Input) {
        let effects = self.nodes[i].step(input, LogicalInstant::from_millis(self.now));
        let mut followups: Vec<Input> = Vec::new();
        for effect in effects {
            match effect {
                Effect::SendRpc { to, message } => self.send(i, to, message),
                Effect::ResetElectionTimer => {
                    self.election_deadline[i] = self.arm_election();
                }
                Effect::ApplyToStateMachine { index, command } => {
                    // Configuration entries advance lastApplied without an apply
                    // effect, so applied indices may skip -- but never repeat or
                    // go backwards.
                    let last_seen = self.applied[i].last().map_or(0, |(idx, _)| *idx);
                    assert!(
                        index.get() > last_seen,
                        "seed={}: node {i} applied index {} out of order (after {last_seen})",
                        self.seed,
                        index.get(),
                    );
                    self.applied[i].push((index.get(), command));
                }
                // The simulator has no durability model, so persistence
                // effects are dropped; membership is read straight off the
                // nodes, so the announcement is informational.
                Effect::Persist { .. }
                | Effect::PersistLog { .. }
                | Effect::MembershipChanged { .. } => {}
                // The leader streams its whole snapshot as one `InstallSnapshot`
                // message (offset 0, done); the network can still drop it.
                Effect::SendSnapshot { to } => self.send_snapshot(i, to),
                // Every chunk the harness produces is terminal: restore the
                // "state machine" from the applied prefix it carries and tell
                // the core so it adopts the snapshot and acks the leader.
                Effect::StoreSnapshotChunk {
                    last_included_index,
                    last_included_term,
                    config,
                    data,
                    ..
                } => {
                    let prefix = decode_applied(&data);
                    self.applied[i].clone_from(&prefix);
                    self.snapshot[i] =
                        Some((last_included_index.get(), last_included_term.get(), prefix));
                    followups.push(Input::SnapshotInstalled {
                        last_included_index,
                        last_included_term,
                        config,
                    });
                }
            }
        }

        for followup in followups {
            self.step(i, followup);
        }
        self.maybe_compact(i);

        // A leader runs no election timer; a voter has one armed; a server not
        // in its own configuration (a learner, or one just removed) is passive.
        // The core emits no explicit "stop timer", so reconcile against role
        // and membership.
        if self.nodes[i].is_leader() {
            self.election_deadline[i] = NEVER;
        } else if self.nodes[i].config().contains(self.ids[i]) {
            if self.election_deadline[i] == NEVER || self.election_deadline[i] <= self.now {
                self.election_deadline[i] = self.arm_election();
            }
        } else {
            self.election_deadline[i] = NEVER;
        }
    }

    /// Streams node `i`'s snapshot to `to` as a single `InstallSnapshot`
    /// message (the harness does not model chunk boundaries).
    pub fn send_snapshot(&mut self, i: usize, to: NodeId) {
        let Some((last_index, last_term, prefix)) = self.snapshot[i].clone() else {
            return; // the leader has nothing to serve yet
        };
        let message = Message::InstallSnapshot(InstallSnapshotArgs {
            term: self.nodes[i].current_term(),
            leader_id: self.nodes[i].id(),
            last_included_index: LogIndex::new(last_index),
            last_included_term: Term::new(last_term),
            config: self.nodes[i].config().clone(),
            offset: 0,
            data: encode_applied(&prefix),
            done: true,
        });
        self.send(i, to, message);
    }

    /// Mirrors the driver's compaction trigger: once node `i` has applied
    /// `snapshot_threshold` entries past its last snapshot, snapshot the
    /// applied prefix and have the core drop the covered log.
    pub fn maybe_compact(&mut self, i: usize) {
        let Some(threshold) = self.snapshot_threshold else {
            return;
        };
        let last_applied = self.nodes[i].last_applied();
        let base = self.nodes[i].snapshot_last_index().get();
        if last_applied.get().saturating_sub(base) < threshold.max(1) {
            return;
        }
        let Some(term) = self.nodes[i].log().term_at(last_applied) else {
            return;
        };
        let prefix: Vec<(u64, Bytes)> = self.applied[i]
            .iter()
            .take_while(|(idx, _)| *idx <= last_applied.get())
            .cloned()
            .collect();
        self.snapshot[i] = Some((last_applied.get(), term.get(), prefix));
        let _ = self.nodes[i].step(
            Input::CompactLog {
                up_to_index: last_applied,
            },
            LogicalInstant::from_millis(self.now),
        );
    }

    /// Queues a message on the network, applying drop / jitter / duplication.
    pub fn send(&mut self, from_idx: usize, to: NodeId, message: Message) {
        if self.net.drop_permille > 0 && self.rng.gen_ratio(self.net.drop_permille, 1000) {
            return;
        }
        let from = self.nodes[from_idx].id();
        let first = self.now + NET_DELAY + self.jitter();
        let duplicate =
            self.net.dup_permille > 0 && self.rng.gen_ratio(self.net.dup_permille, 1000);
        let second = duplicate.then(|| self.now + NET_DELAY + self.jitter());

        self.inflight.push(Envelope {
            deliver_at: first,
            from,
            to,
            message: message.clone(),
        });
        if let Some(deliver_at) = second {
            self.inflight.push(Envelope {
                deliver_at,
                from,
                to,
                message,
            });
        }
    }

    pub fn jitter(&mut self) -> u64 {
        if self.net.jitter > 0 {
            self.rng.gen_range(0..=self.net.jitter)
        } else {
            0
        }
    }

    pub fn arm_election(&mut self) -> u64 {
        self.now + self.rng.gen_range(ELECTION_MIN..ELECTION_MAX)
    }

    /// Redraws the partition: mostly a full heal, otherwise a random two-way
    /// split with both sides non-empty.
    pub fn shuffle_partition(&mut self) {
        let n = self.nodes.len();
        if n < 2 || self.rng.gen_ratio(3, 5) {
            self.groups = vec![0; n];
            return;
        }
        loop {
            let split: Vec<usize> = (0..n)
                .map(|_| usize::from(self.rng.gen_bool(0.5)))
                .collect();
            if split.contains(&0) && split.contains(&1) {
                self.groups = split;
                return;
            }
        }
    }

    pub fn set_groups(&mut self, groups: Vec<usize>) {
        assert_eq!(groups.len(), self.nodes.len());
        self.groups = groups;
    }

    /// Models the cluster-recovery path: a node whose persistent state failed
    /// to load discards **all** Raft state and rejoins as a fresh follower. The
    /// cross-time safety invariants (committed-entry agreement, Log Matching,
    /// Leader Completeness) are deliberately *not* reset, so the rejoining
    /// node is still held to them; only this node's own monotonic-progress
    /// baseline restarts, because it is a new incarnation.
    pub fn wipe_node(&mut self, i: usize) {
        let peers = self.ids.iter().copied().filter(|&id| id != self.ids[i]);
        self.nodes[i] = RaftNode::new(self.ids[i], peers);
        self.applied[i].clear();
        self.snapshot[i] = None;
        self.prev_commit[i] = 0;
        self.prev_applied[i] = 0;
        self.election_deadline[i] = self.arm_election();
        self.heartbeat_deadline[i] = self.now + HEARTBEAT_PERIOD;
    }

    /// Brings a brand-new passive learner online and asks the sole leader to
    /// add it. All the per-node parallel state grows by one.
    pub fn add_server(&mut self, new_id: u64) {
        let id = NodeId::new(new_id);
        self.ids.push(id);
        self.nodes.push(RaftNode::new_learner(id));
        self.election_deadline.push(NEVER); // passive until it joins
        self.heartbeat_deadline.push(self.now + HEARTBEAT_PERIOD);
        self.groups.push(0);
        self.applied.push(Vec::new());
        self.snapshot.push(None);
        self.prev_commit.push(0);
        self.prev_applied.push(0);

        let leader = self.sole_leader();
        assert!(
            leader.is_some(),
            "seed={}: no leader to add through",
            self.seed
        );
        self.step(
            leader.unwrap_or(0),
            Input::ChangeMembership {
                change: MembershipChange::AddServer(id),
            },
        );
        self.check_invariants();
    }

    /// Asks the sole leader to remove the server with id `victim_id`.
    pub fn remove_server(&mut self, victim_id: u64) {
        let leader = self.sole_leader();
        assert!(
            leader.is_some(),
            "seed={}: no leader to remove through",
            self.seed,
        );
        self.step(
            leader.unwrap_or(0),
            Input::ChangeMembership {
                change: MembershipChange::RemoveServer(NodeId::new(victim_id)),
            },
        );
        self.check_invariants();
    }

    /// Whether every currently-connected node that considers itself a voter
    /// agrees `id` is (or is not) a voter. Nodes cut off by a partition are
    /// skipped: without pre-vote (Phase 2) a removed node that never hears the
    /// change keeps its stale view.
    pub fn all_agree_member(&self, id: u64, expected: bool) -> bool {
        let id = NodeId::new(id);
        self.nodes
            .iter()
            .enumerate()
            .filter(|(i, node)| self.groups[*i] == 0 && node.config().contains(node.id()))
            .all(|(_, node)| node.config().contains(id) == expected)
    }

    pub fn leaders(&self) -> Vec<usize> {
        (0..self.nodes.len())
            .filter(|&i| self.nodes[i].is_leader())
            .collect()
    }

    /// The single current leader, or `None` if there is not exactly one.
    pub fn sole_leader(&self) -> Option<usize> {
        match self.leaders().as_slice() {
            [only] => Some(*only),
            _ => None,
        }
    }

    /// A current leader whose partition group is `group`, if any.
    pub fn leader_in_group(&self, group: usize) -> Option<usize> {
        self.leaders()
            .into_iter()
            .find(|&i| self.groups[i] == group)
    }

    /// Runs in short slices until there is exactly one leader, up to `budget`
    /// logical ms from now. Returns that leader; asserts one appears.
    pub fn run_until_leader(&mut self, budget: u64) -> usize {
        let deadline = self.now + budget;
        while self.now < deadline {
            if let Some(leader) = self.sole_leader() {
                return leader;
            }
            self.run_until(self.now + HEARTBEAT_PERIOD);
        }
        let found = self.sole_leader();
        assert!(
            found.is_some(),
            "seed={}: no sole leader within {budget}ms",
            self.seed,
        );
        found.unwrap_or(0)
    }

    /// Feeds `command` to the sole current leader as a client proposal.
    pub fn propose_to_leader(&mut self, command: &[u8]) {
        let leader = self.sole_leader();
        assert!(
            leader.is_some(),
            "seed={}: no sole leader to accept a proposal",
            self.seed,
        );
        self.propose_at(leader.unwrap_or(0), command);
    }

    /// Feeds `command` to node `idx`, which must currently believe it leads.
    pub fn propose_at(&mut self, idx: usize, command: &[u8]) {
        assert!(
            self.nodes[idx].is_leader(),
            "seed={}: node {idx} is not a leader",
            self.seed,
        );
        self.step(
            idx,
            Input::Propose {
                command: Bytes::copy_from_slice(command),
            },
        );
        self.check_invariants();
    }

    pub fn applied_commands(&self, i: usize) -> Vec<Bytes> {
        self.applied[i].iter().map(|(_, cmd)| cmd.clone()).collect()
    }

    /// Node `i`'s in-memory log entries as `(global index, term, command)`.
    /// After compaction the first entry sits at `snapshot_last_index + 1`.
    pub fn log_entries(&self, i: usize) -> Vec<(u64, u64, Bytes)> {
        let base = self.nodes[i].log().snapshot_last_index().get();
        self.nodes[i]
            .log()
            .entries_from(LogIndex::new(1))
            .iter()
            .enumerate()
            .map(|(pos, entry)| {
                (
                    base + pos as u64 + 1,
                    entry.term.get(),
                    entry.command_bytes().cloned().unwrap_or_default(),
                )
            })
            .collect()
    }

    /// Re-checks the Raft safety properties against the whole cluster. Called
    /// after every core step, so any test that returns has held them.
    pub fn check_invariants(&mut self) {
        self.check_monotonic_progress();
        self.check_one_leader_per_term();
        self.record_and_check_committed();
        self.check_leader_completeness();
        self.check_applied_agreement();
        self.check_log_matching();
    }

    /// §9.9 / §9.10: `commitIndex` and `lastApplied` never go backwards, and a
    /// node never applies past what it has committed.
    pub fn check_monotonic_progress(&mut self) {
        let seed = self.seed;
        for i in 0..self.nodes.len() {
            let commit = self.nodes[i].commit_index().get();
            let applied = self.nodes[i].last_applied().get();
            assert!(
                commit >= self.prev_commit[i],
                "seed={seed}: node {i} commit_index regressed {} -> {commit}",
                self.prev_commit[i],
            );
            assert!(
                applied >= self.prev_applied[i],
                "seed={seed}: node {i} last_applied regressed {} -> {applied}",
                self.prev_applied[i],
            );
            assert!(
                applied <= commit,
                "seed={seed}: node {i} last_applied {applied} exceeds commit_index {commit}",
            );
            self.prev_commit[i] = commit;
            self.prev_applied[i] = applied;
        }
    }

    /// §9.1: at most one leader per term, across the whole run.
    pub fn check_one_leader_per_term(&mut self) {
        let seed = self.seed;
        for i in 0..self.nodes.len() {
            if !self.nodes[i].is_leader() {
                continue;
            }
            self.saw_leader = true;
            let term = self.nodes[i].current_term().get();
            match self.leader_of_term.get(&term) {
                Some(&j) => {
                    assert!(
                        j == i,
                        "seed={seed}: nodes {j} and {i} both led term {term}"
                    );
                }
                None => {
                    self.leader_of_term.insert(term, i);
                }
            }
        }
    }

    /// §9.5: every entry a node counts as committed agrees, at that index, with
    /// what any other node ever committed there.
    pub fn record_and_check_committed(&mut self) {
        let seed = self.seed;
        for i in 0..self.nodes.len() {
            let commit_term = self.nodes[i].current_term().get();
            let commit = self.nodes[i].commit_index().get();
            // Entries folded into this node's snapshot are committed by
            // definition; their agreement was checked before they were compacted.
            for (idx, term, command) in self.log_entries(i) {
                if idx > commit {
                    break;
                }
                match self.committed.get(&idx) {
                    Some((seen_term, seen_command, _)) => assert!(
                        *seen_term == term && *seen_command == command,
                        "seed={seed}: node {i} has committed entry {idx} = ({term}, {command:?}) \
                         but ({seen_term}, {seen_command:?}) was committed there earlier",
                    ),
                    None => {
                        self.committed.insert(idx, (term, command, commit_term));
                    }
                }
            }
        }
    }

    /// §9.4: a current leader holds every entry committed in an earlier or
    /// equal term (Leader Completeness, projected onto the live leaders).
    pub fn check_leader_completeness(&self) {
        let seed = self.seed;
        for i in 0..self.nodes.len() {
            if !self.nodes[i].is_leader() {
                continue;
            }
            let leader_term = self.nodes[i].current_term().get();
            let base = self.nodes[i].log().snapshot_last_index().get();
            let log = self.log_entries(i);
            for (&idx, (term, command, commit_term)) in &self.committed {
                if *commit_term > leader_term {
                    continue;
                }
                // A committed entry folded into the leader's own snapshot is
                // held by definition.
                if idx <= base {
                    continue;
                }
                let held = log.iter().find(|(g, ..)| *g == idx);
                assert!(
                    held.is_some(),
                    "seed={seed}: leader {i} (term {leader_term}) is missing committed entry {idx}",
                );
                if let Some((_, held_term, held_command)) = held {
                    assert!(
                        held_term == term && held_command == command,
                        "seed={seed}: leader {i} entry {idx} differs from what was committed there",
                    );
                }
            }
        }
    }

    /// §9.5: applied sequences agree on their common prefix.
    pub fn check_applied_agreement(&self) {
        let seed = self.seed;
        for left in 0..self.nodes.len() {
            for right in (left + 1)..self.nodes.len() {
                let common = self.applied[left].len().min(self.applied[right].len());
                assert!(
                    self.applied[left][..common] == self.applied[right][..common],
                    "seed={seed}: nodes {left} and {right} applied different commands \
                     in their common prefix",
                );
            }
        }
    }

    /// §9.3: Log Matching. Over the range both nodes still hold in memory, past
    /// the longest identical prefix no shared index may carry the same term.
    /// (Anything below either node's compaction base was checked before it was
    /// compacted.)
    pub fn check_log_matching(&self) {
        let seed = self.seed;
        for left in 0..self.nodes.len() {
            for right in (left + 1)..self.nodes.len() {
                let log_l = self.log_entries(left);
                let log_r = self.log_entries(right);
                let base_l = self.nodes[left].log().snapshot_last_index().get();
                let base_r = self.nodes[right].log().snapshot_last_index().get();
                let lo = base_l.max(base_r) + 1;
                let hi = log_l
                    .last()
                    .map_or(0, |(g, ..)| *g)
                    .min(log_r.last().map_or(0, |(g, ..)| *g));

                let at = |log: &[(u64, u64, Bytes)], g: u64| {
                    log.iter()
                        .find(|(gg, ..)| *gg == g)
                        .map(|(_, t, c)| (*t, c.clone()))
                };
                let mut still_matching = true;
                for g in lo..=hi {
                    let (Some(l), Some(r)) = (at(&log_l, g), at(&log_r, g)) else {
                        continue;
                    };
                    if still_matching && l == r {
                        continue;
                    }
                    still_matching = false;
                    assert!(
                        l.0 != r.0,
                        "seed={seed}: Log Matching broke at index {g} between nodes {left} and {right}",
                    );
                }
            }
        }
    }
}

/// Serializes an applied prefix so it can ride inside an `InstallSnapshot`
/// message's `data`, standing in for real state-machine bytes.
pub fn encode_applied(prefix: &[(u64, Bytes)]) -> Vec<u8> {
    let owned: Vec<(u64, Vec<u8>)> = prefix.iter().map(|(i, c)| (*i, c.to_vec())).collect();
    bincode::serialize(&owned).unwrap_or_default()
}

pub fn decode_applied(data: &[u8]) -> Vec<(u64, Bytes)> {
    let owned: Vec<(u64, Vec<u8>)> = bincode::deserialize(data).unwrap_or_default();
    owned
        .into_iter()
        .map(|(i, c)| (i, Bytes::from(c)))
        .collect()
}

pub const JITTER5: Net = Net {
    drop_permille: 0,
    dup_permille: 0,
    jitter: 5,
    repartition_every: 0,
};

/// The seeds a test runs: just `$SEED` when it is set (for replaying a
/// failure), otherwise the given battery.
pub fn seeds(battery: &[u64]) -> Vec<u64> {
    std::env::var("SEED")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .map_or_else(|| battery.to_vec(), |seed| vec![seed])
}
