//! Deterministic simulation tests.
//!
//! Every test here is reproducible from a single `u64` seed: a simulated
//! network and clock, with no wall-clock time, real threads, or `HashMap`
//! iteration order leaking into behaviour. A failing assertion carries its seed
//! so it can be replayed with `SEED=<n> just sim-seed`.
//!
//! The [`Sim`] event loop re-checks the Raft safety properties after every core
//! step, so any unreliable-network test that returns at all has held Election
//! Safety, Log Matching, Leader Completeness, State Machine Safety, and
//! `commitIndex` / `lastApplied` monotonicity for its whole run.

use std::collections::BTreeMap;

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use raft::core::{
    Effect, Input, InstallSnapshotArgs, LogIndex, LogicalInstant, Message, NodeId, RaftNode, Term,
};

// Timer and network constants, in logical milliseconds. The heartbeat period
// sits comfortably below the election-timeout floor, so a healthy leader is
// never unseated on a reliable network.
const ELECTION_MIN: u64 = 150;
const ELECTION_MAX: u64 = 300;
const HEARTBEAT_PERIOD: u64 = 50;
const NET_DELAY: u64 = 10;
/// Sentinel deadline meaning "not armed": a leader runs no election timer.
const NEVER: u64 = u64::MAX;

/// How the simulated network misbehaves for a run.
///
/// [`RELIABLE`] is in-order, lossless, single-partition, and draws no
/// randomness of its own, so the reliable-network tests stay bit-for-bit
/// reproducible even as the faulty paths are added around them.
#[derive(Clone, Copy)]
struct Net {
    /// Per-mille chance each sent message is dropped outright.
    drop_permille: u32,
    /// Per-mille chance a non-dropped message is also delivered a second time.
    dup_permille: u32,
    /// Extra delay, `0..=jitter`, added to `NET_DELAY` per message. Non-zero
    /// jitter lets later messages overtake earlier ones (reordering).
    jitter: u64,
    /// Re-draw the partition this often (logical ms); `0` never partitions.
    repartition_every: u64,
}

const RELIABLE: Net = Net {
    drop_permille: 0,
    dup_permille: 0,
    jitter: 0,
    repartition_every: 0,
};

/// A node's snapshot in the harness: `(last_index, last_term, applied-prefix)`.
/// The applied prefix stands in for real serialized state-machine bytes.
type SimSnapshot = (u64, u64, Vec<(u64, Bytes)>);

/// A message in flight on the simulated network.
struct Envelope {
    deliver_at: u64,
    from: NodeId,
    to: NodeId,
    message: Message,
}

/// A whole cluster driven on one thread against a logical clock.
struct Sim {
    seed: u64,
    now: u64,
    ids: Vec<NodeId>,
    nodes: Vec<RaftNode>,
    election_deadline: Vec<u64>,
    heartbeat_deadline: Vec<u64>,
    inflight: Vec<Envelope>,
    net: Net,
    /// Partition group per node; two nodes exchange messages only when their
    /// groups match. All-zero means one connected network.
    groups: Vec<usize>,
    next_net_change: u64,
    /// Per node, the `(index, command)` pairs the core told it to apply, in
    /// order. `step` asserts index order as it fills this. Also stands in for
    /// the state-machine state: a snapshot is a prefix of this list.
    applied: Vec<Vec<(u64, Bytes)>>,
    /// Compact a node's log once `lastApplied` runs this many entries past its
    /// last snapshot. `None` disables compaction (the default).
    snapshot_threshold: Option<u64>,
    /// Per node, the snapshot it currently holds. Mirrors the driver's cached
    /// snapshot, and is what a `SendSnapshot` effect streams to a follower.
    snapshot: Vec<Option<SimSnapshot>>,
    rng: StdRng,

    // --- invariant bookkeeping (Raft safety properties) ---
    prev_commit: Vec<u64>,
    prev_applied: Vec<u64>,
    /// index -> (entry term, command, term it was first observed committed in).
    committed: BTreeMap<u64, (u64, Bytes, u64)>,
    /// term -> the one node ever seen leading it (Election Safety).
    leader_of_term: BTreeMap<u64, usize>,
    saw_leader: bool,
}

impl Sim {
    fn new(node_count: usize, seed: u64) -> Self {
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
    const fn with_net(mut self, net: Net) -> Self {
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
    const fn with_snapshot_threshold(mut self, threshold: u64) -> Self {
        self.snapshot_threshold = Some(threshold);
        self
    }

    fn index_of(&self, id: NodeId) -> usize {
        // Every routed id was built in `new`, so the fallback is unreachable.
        self.ids.iter().position(|&known| known == id).unwrap_or(0)
    }

    fn connected(&self, from: NodeId, to: NodeId) -> bool {
        self.groups[self.index_of(from)] == self.groups[self.index_of(to)]
    }

    fn next_event_time(&self) -> u64 {
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
    fn run_until(&mut self, end: u64) {
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
    fn step(&mut self, i: usize, input: Input) {
        let effects = self.nodes[i].step(input, LogicalInstant::from_millis(self.now));
        let mut followups: Vec<Input> = Vec::new();
        for effect in effects {
            match effect {
                Effect::SendRpc { to, message } => self.send(i, to, message),
                Effect::ResetElectionTimer => {
                    self.election_deadline[i] = self.arm_election();
                }
                Effect::ApplyToStateMachine { index, command } => {
                    let expected = self.applied[i].len() as u64 + 1;
                    assert_eq!(
                        index.get(),
                        expected,
                        "seed={}: node {i} applied index {} out of order (expected {expected})",
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

        // A leader runs no election timer; every other role has one armed. The
        // core emits no explicit "stop timer", so reconcile against the role.
        if self.nodes[i].is_leader() {
            self.election_deadline[i] = NEVER;
        } else if self.election_deadline[i] == NEVER {
            self.election_deadline[i] = self.arm_election();
        }
    }

    /// Streams node `i`'s snapshot to `to` as a single `InstallSnapshot`
    /// message (the harness does not model chunk boundaries).
    fn send_snapshot(&mut self, i: usize, to: NodeId) {
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
    fn maybe_compact(&mut self, i: usize) {
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
    fn send(&mut self, from_idx: usize, to: NodeId, message: Message) {
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

    fn jitter(&mut self) -> u64 {
        if self.net.jitter > 0 {
            self.rng.gen_range(0..=self.net.jitter)
        } else {
            0
        }
    }

    fn arm_election(&mut self) -> u64 {
        self.now + self.rng.gen_range(ELECTION_MIN..ELECTION_MAX)
    }

    /// Redraws the partition: mostly a full heal, otherwise a random two-way
    /// split with both sides non-empty.
    fn shuffle_partition(&mut self) {
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

    fn set_groups(&mut self, groups: Vec<usize>) {
        assert_eq!(groups.len(), self.nodes.len());
        self.groups = groups;
    }

    /// Models the cluster-recovery path: a node whose persistent state failed
    /// to load discards **all** Raft state and rejoins as a fresh follower. The
    /// cross-time safety invariants (committed-entry agreement, Log Matching,
    /// Leader Completeness) are deliberately *not* reset, so the rejoining
    /// node is still held to them; only this node's own monotonic-progress
    /// baseline restarts, because it is a new incarnation.
    fn wipe_node(&mut self, i: usize) {
        let peers = self.ids.iter().copied().filter(|&id| id != self.ids[i]);
        self.nodes[i] = RaftNode::new(self.ids[i], peers);
        self.applied[i].clear();
        self.snapshot[i] = None;
        self.prev_commit[i] = 0;
        self.prev_applied[i] = 0;
        self.election_deadline[i] = self.arm_election();
        self.heartbeat_deadline[i] = self.now + HEARTBEAT_PERIOD;
    }

    fn leaders(&self) -> Vec<usize> {
        (0..self.nodes.len())
            .filter(|&i| self.nodes[i].is_leader())
            .collect()
    }

    /// The single current leader, or `None` if there is not exactly one.
    fn sole_leader(&self) -> Option<usize> {
        match self.leaders().as_slice() {
            [only] => Some(*only),
            _ => None,
        }
    }

    /// A current leader whose partition group is `group`, if any.
    fn leader_in_group(&self, group: usize) -> Option<usize> {
        self.leaders()
            .into_iter()
            .find(|&i| self.groups[i] == group)
    }

    /// Runs in short slices until there is exactly one leader, up to `budget`
    /// logical ms from now. Returns that leader; asserts one appears.
    fn run_until_leader(&mut self, budget: u64) -> usize {
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
    fn propose_to_leader(&mut self, command: &[u8]) {
        let leader = self.sole_leader();
        assert!(
            leader.is_some(),
            "seed={}: no sole leader to accept a proposal",
            self.seed,
        );
        self.propose_at(leader.unwrap_or(0), command);
    }

    /// Feeds `command` to node `idx`, which must currently believe it leads.
    fn propose_at(&mut self, idx: usize, command: &[u8]) {
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

    fn applied_commands(&self, i: usize) -> Vec<Bytes> {
        self.applied[i].iter().map(|(_, cmd)| cmd.clone()).collect()
    }

    /// Node `i`'s in-memory log entries as `(global index, term, command)`.
    /// After compaction the first entry sits at `snapshot_last_index + 1`.
    fn log_entries(&self, i: usize) -> Vec<(u64, u64, Bytes)> {
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
    fn check_invariants(&mut self) {
        self.check_monotonic_progress();
        self.check_one_leader_per_term();
        self.record_and_check_committed();
        self.check_leader_completeness();
        self.check_applied_agreement();
        self.check_log_matching();
    }

    /// §9.9 / §9.10: `commitIndex` and `lastApplied` never go backwards, and a
    /// node never applies past what it has committed.
    fn check_monotonic_progress(&mut self) {
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
    fn check_one_leader_per_term(&mut self) {
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
    fn record_and_check_committed(&mut self) {
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
    fn check_leader_completeness(&self) {
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
    fn check_applied_agreement(&self) {
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
    fn check_log_matching(&self) {
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
fn encode_applied(prefix: &[(u64, Bytes)]) -> Vec<u8> {
    let owned: Vec<(u64, Vec<u8>)> = prefix.iter().map(|(i, c)| (*i, c.to_vec())).collect();
    bincode::serialize(&owned).unwrap_or_default()
}

fn decode_applied(data: &[u8]) -> Vec<(u64, Bytes)> {
    let owned: Vec<(u64, Vec<u8>)> = bincode::deserialize(data).unwrap_or_default();
    owned
        .into_iter()
        .map(|(i, c)| (i, Bytes::from(c)))
        .collect()
}

fn assert_converges_to_one_stable_leader(node_count: usize, seed: u64) {
    let mut sim = Sim::new(node_count, seed);

    sim.run_until(5_000);
    let leaders = sim.leaders();
    assert_eq!(
        leaders.len(),
        1,
        "seed={seed}: expected exactly one leader at t=5000, got indices {leaders:?}",
    );
    let leader = leaders[0];
    let term = sim.nodes[leader].current_term().get();

    // The leader keeps the job and its term does not move for another second.
    sim.run_until(6_000);
    assert!(
        sim.nodes[leader].is_leader(),
        "seed={seed}: node {leader} lost leadership between t=5000 and t=6000",
    );
    assert_eq!(
        sim.nodes[leader].current_term().get(),
        term,
        "seed={seed}: leader term moved between t=5000 and t=6000",
    );

    // No follower has run ahead of the leader's term.
    for (i, node) in sim.nodes.iter().enumerate() {
        assert!(
            node.current_term().get() <= term,
            "seed={seed}: node {i} term {} exceeds leader term {term}",
            node.current_term().get(),
        );
    }
}

/// The seeds a test runs: just `$SEED` when it is set (for replaying a
/// failure), otherwise the given battery.
fn seeds(battery: &[u64]) -> Vec<u64> {
    std::env::var("SEED")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .map_or_else(|| battery.to_vec(), |seed| vec![seed])
}

#[test]
fn three_node_cluster_elects_one_stable_leader() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED]) {
        assert_converges_to_one_stable_leader(3, seed);
    }
}

#[test]
fn five_node_cluster_elects_one_stable_leader() {
    for seed in seeds(&[1, 7, 99, 0x00C0_FFEE]) {
        assert_converges_to_one_stable_leader(5, seed);
    }
}

/// On a reliable network, commands proposed to the leader replicate to every
/// node, commit, and get applied in the same order everywhere.
fn assert_proposals_replicate_and_commit(node_count: usize, seed: u64) {
    let commands: [&[u8]; 4] = [b"set x=1", b"set y=2", b"del x", b"set y=3"];

    let mut sim = Sim::new(node_count, seed);
    sim.run_until(1_000);
    assert!(
        sim.sole_leader().is_some(),
        "seed={seed}: no leader by t=1000",
    );

    for command in commands {
        sim.propose_to_leader(command);
        sim.run_until(sim.now + 250); // let one round of replication settle
    }
    sim.run_until(sim.now + 1_000); // and a few heartbeats to carry commit

    let want: Vec<Bytes> = commands.iter().map(|c| Bytes::copy_from_slice(c)).collect();
    let n = commands.len() as u64;

    let leader = sim.sole_leader();
    assert!(
        leader.is_some(),
        "seed={seed}: no sole leader after proposals",
    );
    let leader = leader.unwrap_or(0);
    let leader_log = sim.nodes[leader].log().clone();

    for i in 0..node_count {
        let node = &sim.nodes[i];
        assert_eq!(
            node.log(),
            &leader_log,
            "seed={seed}: node {i} log diverges from leader {leader}",
        );
        assert_eq!(
            node.commit_index().get(),
            n,
            "seed={seed}: node {i} commit_index {} != {n}",
            node.commit_index().get(),
        );
        assert_eq!(
            sim.applied_commands(i),
            want,
            "seed={seed}: node {i} applied the wrong commands or order",
        );
    }
}

#[test]
fn three_node_cluster_replicates_and_commits_proposals() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED]) {
        assert_proposals_replicate_and_commit(3, seed);
    }
}

#[test]
fn five_node_cluster_replicates_and_commits_proposals() {
    for seed in seeds(&[1, 7, 99, 0x00C0_FFEE]) {
        assert_proposals_replicate_and_commit(5, seed);
    }
}

/// A lossy, jittery, duplicating network (no partitions) still settles on one
/// leader that then holds the job across a quiet stretch.
fn assert_elects_and_holds_under_loss(node_count: usize, seed: u64) {
    let net = Net {
        drop_permille: 60,
        dup_permille: 100,
        jitter: 25,
        repartition_every: 0,
    };
    let mut sim = Sim::new(node_count, seed).with_net(net);

    let leader = sim.run_until_leader(20_000);
    sim.run_until(sim.now + 3_000);
    assert!(
        sim.nodes[leader].is_leader(),
        "seed={seed}: node {leader} did not hold leadership through the quiet stretch",
    );
    assert_eq!(
        sim.sole_leader(),
        Some(leader),
        "seed={seed}: leadership was not stable under loss",
    );
}

#[test]
fn five_node_cluster_elects_a_stable_leader_under_message_loss() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED, 0x00C0_FFEE]) {
        assert_elects_and_holds_under_loss(5, seed);
    }
}

/// Under loss, duplication, and reordering (no partitions), a stream of
/// proposals still commits, and every node converges to the full applied
/// sequence once the network is given time to settle.
fn assert_commits_under_chaotic_delivery(node_count: usize, seed: u64) {
    let net = Net {
        drop_permille: 80,
        dup_permille: 150,
        jitter: 30,
        repartition_every: 0,
    };
    let mut sim = Sim::new(node_count, seed).with_net(net);

    let commands: Vec<Vec<u8>> = (0..12).map(|k| format!("op-{k}").into_bytes()).collect();
    for command in &commands {
        sim.run_until_leader(10_000);
        sim.propose_to_leader(command);
        sim.run_until(sim.now + 500);
    }
    sim.run_until(sim.now + 30_000); // long settle

    let want: Vec<Bytes> = commands.iter().map(|c| Bytes::copy_from_slice(c)).collect();
    let leader = sim.run_until_leader(10_000);
    assert_eq!(
        sim.applied_commands(leader),
        want,
        "seed={seed}: leader {leader} did not commit every proposal",
    );
    for i in 0..node_count {
        assert_eq!(
            sim.applied_commands(i),
            want,
            "seed={seed}: node {i} did not converge to the full applied sequence",
        );
    }
}

#[test]
fn five_node_cluster_commits_under_chaotic_delivery() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED]) {
        assert_commits_under_chaotic_delivery(5, seed);
    }
}

/// A minority partition (the old leader plus one) cannot commit; the majority
/// elects a fresh leader and makes progress. On heal, the minority discards
/// its uncommitted tail and catches up to the majority's log.
fn assert_partition_isolates_then_heals(seed: u64) {
    let node_count = 5;
    let net = Net {
        drop_permille: 0,
        dup_permille: 0,
        jitter: 5,
        repartition_every: 0,
    };
    let mut sim = Sim::new(node_count, seed).with_net(net);

    let old_leader = sim.run_until_leader(5_000);
    sim.propose_to_leader(b"a");
    sim.run_until(sim.now + 400);
    sim.propose_to_leader(b"b");
    sim.run_until(sim.now + 600);
    assert_eq!(
        sim.nodes[old_leader].commit_index().get(),
        2,
        "seed={seed}: cluster did not commit the pre-partition writes",
    );

    // Cut the old leader and its next-door node off from the other three.
    let mut groups = vec![0usize; node_count];
    groups[old_leader] = 1;
    groups[(old_leader + 1) % node_count] = 1;
    sim.set_groups(groups);
    sim.run_until(sim.now + 2_000);

    // The isolated old leader still thinks it leads; its write must not commit.
    if sim.nodes[old_leader].is_leader() {
        sim.propose_at(old_leader, b"orphan");
    }
    sim.run_until(sim.now + 2_000);
    assert_eq!(
        sim.nodes[old_leader].commit_index().get(),
        2,
        "seed={seed}: an isolated minority leader committed a write",
    );

    // The majority side elects someone and commits two more entries.
    let mut maj_leader = sim.leader_in_group(0);
    let deadline = sim.now + 5_000;
    while maj_leader.is_none() && sim.now < deadline {
        sim.run_until(sim.now + HEARTBEAT_PERIOD);
        maj_leader = sim.leader_in_group(0);
    }
    assert!(
        maj_leader.is_some(),
        "seed={seed}: majority side elected no leader",
    );
    let maj_leader = maj_leader.unwrap_or(0);
    sim.propose_at(maj_leader, b"c");
    sim.run_until(sim.now + 400);
    sim.propose_at(maj_leader, b"d");
    sim.run_until(sim.now + 1_000);
    assert!(
        sim.nodes[maj_leader].commit_index().get() >= 4,
        "seed={seed}: majority side failed to commit during the partition",
    );

    // Heal and let everything reconverge.
    sim.set_groups(vec![0; node_count]);
    sim.run_until(sim.now + 15_000);

    let want: Vec<Bytes> = ["a", "b", "c", "d"]
        .iter()
        .map(|s| Bytes::copy_from_slice(s.as_bytes()))
        .collect();
    let leader = sim.run_until_leader(10_000);
    let leader_log = sim.nodes[leader].log().clone();
    for i in 0..node_count {
        assert_eq!(
            sim.nodes[i].log(),
            &leader_log,
            "seed={seed}: node {i} log did not reconverge after heal",
        );
        assert_eq!(
            sim.applied_commands(i),
            want,
            "seed={seed}: node {i} applied sequence wrong after heal",
        );
    }
}

#[test]
fn five_node_cluster_survives_a_partition_and_heal() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED, 7, 99]) {
        assert_partition_isolates_then_heals(seed);
    }
}

/// The kitchen sink: loss, duplication, reordering, and a partition redrawn
/// every ~700ms, with a client proposing throughout. Correctness is enforced
/// continuously by `check_invariants`; this asserts the run stayed live (a
/// leader existed and commits happened) and, once the churn stops and the
/// network heals, every node converges to one identical applied sequence.
fn assert_survives_continuous_chaos(seed: u64) {
    let node_count = 5;
    let net = Net {
        drop_permille: 50,
        dup_permille: 120,
        jitter: 25,
        repartition_every: 700,
    };
    let mut sim = Sim::new(node_count, seed).with_net(net);

    let mut proposed = 0u32;
    while sim.now < 45_000 {
        if let Some(leader) = sim.sole_leader() {
            let command = format!("c{proposed}");
            sim.propose_at(leader, command.as_bytes());
            proposed += 1;
        }
        sim.run_until(sim.now + 137); // an odd slice, to sample many phases
    }

    assert!(
        sim.saw_leader,
        "seed={seed}: no leader ever emerged under chaos",
    );
    assert!(
        !sim.committed.is_empty(),
        "seed={seed}: nothing ever committed under chaos",
    );

    // Stop repartitioning, force a heal, and let the cluster settle.
    sim.net.repartition_every = 0;
    sim.next_net_change = NEVER;
    sim.set_groups(vec![0; node_count]);
    sim.run_until(sim.now + 10_000);

    // A fresh leader cannot advance commitIndex over prior-term entries until
    // it commits one of its own (§5.4.2), so nudge it with a few writes; then
    // every node's applied sequence must be identical and non-empty.
    for round in 0..4 {
        let leader = sim.run_until_leader(10_000);
        sim.propose_at(leader, format!("flush-{round}").as_bytes());
        sim.run_until(sim.now + 3_000);
    }

    let reference = sim.applied_commands(0);
    assert!(
        reference.len() > proposed as usize / 4,
        "seed={seed}: only {} of {proposed} proposals ever committed",
        reference.len(),
    );
    for i in 0..node_count {
        assert_eq!(
            sim.applied_commands(i),
            reference,
            "seed={seed}: node {i} did not converge to node 0's applied sequence",
        );
    }
}

#[test]
fn five_node_cluster_survives_continuous_chaos() {
    for seed in seeds(&[1, 42, 1_000, 0x5EED]) {
        assert_survives_continuous_chaos(seed);
    }
}

/// The cluster-recovery path: a follower that lost its persistent state
/// discards everything and rejoins fresh; the leader repopulates it via
/// `AppendEntries` (backing `nextIndex` down to the start) and it re-applies
/// the whole committed sequence, never a conflicting entry.
fn assert_wiped_follower_recovers(seed: u64) {
    let node_count = 5;
    let net = Net {
        drop_permille: 0,
        dup_permille: 0,
        jitter: 5,
        repartition_every: 0,
    };
    let mut sim = Sim::new(node_count, seed).with_net(net);

    let leader = sim.run_until_leader(5_000);
    let early: Vec<Vec<u8>> = (0..6).map(|k| format!("early-{k}").into_bytes()).collect();
    for command in &early {
        sim.propose_at(leader, command);
        sim.run_until(sim.now + 200);
    }
    sim.run_until(sim.now + 1_000);

    // Pick a follower and wipe it.
    let victim = (leader + 2) % node_count;
    assert!(!sim.nodes[victim].is_leader());
    sim.wipe_node(victim);
    assert!(sim.applied_commands(victim).is_empty());

    // Keep the cluster working; the fresh node must catch back up.
    let late: Vec<Vec<u8>> = (0..6).map(|k| format!("late-{k}").into_bytes()).collect();
    for command in &late {
        let current = sim.run_until_leader(5_000);
        sim.propose_at(current, command);
        sim.run_until(sim.now + 200);
    }
    sim.run_until(sim.now + 5_000);

    let want: Vec<Bytes> = early
        .iter()
        .chain(&late)
        .map(|c| Bytes::copy_from_slice(c))
        .collect();
    let reference = sim.run_until_leader(5_000);
    let leader_log = sim.nodes[reference].log().clone();
    for i in 0..node_count {
        assert_eq!(
            sim.nodes[i].log(),
            &leader_log,
            "seed={seed}: node {i} log did not reconverge after the wipe",
        );
        assert_eq!(
            sim.applied_commands(i),
            want,
            "seed={seed}: node {i} did not re-apply the full committed sequence",
        );
    }
}

#[test]
fn a_wiped_follower_rejoins_and_catches_up() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED, 7, 99]) {
        assert_wiped_follower_recovers(seed);
    }
}

// --- 8: snapshotting -----------------------------------------------------------

/// With a low compaction threshold, a healthy cluster keeps committing while
/// every node repeatedly snapshots and drops the covered log prefix; the
/// applied sequences stay identical throughout (`check_invariants` runs after
/// every step).
fn assert_cluster_compacts_and_stays_consistent(node_count: usize, seed: u64) {
    let mut sim = Sim::new(node_count, seed).with_snapshot_threshold(4);

    sim.run_until_leader(5_000);
    let commands: Vec<Vec<u8>> = (0..20).map(|k| format!("op-{k}").into_bytes()).collect();
    for command in &commands {
        let leader = sim.run_until_leader(5_000);
        sim.propose_at(leader, command);
        sim.run_until(sim.now + 250);
    }
    sim.run_until(sim.now + 2_000);

    let want: Vec<Bytes> = commands.iter().map(|c| Bytes::copy_from_slice(c)).collect();
    for i in 0..node_count {
        assert!(
            sim.nodes[i].snapshot_last_index().get() > 0,
            "seed={seed}: node {i} never compacted its log",
        );
        assert_eq!(
            sim.applied_commands(i),
            want,
            "seed={seed}: node {i} applied the wrong sequence after compaction",
        );
    }
}

#[test]
fn three_and_five_node_clusters_compact_and_stay_consistent() {
    for seed in seeds(&[1, 2, 42, 1_000, 0x5EED]) {
        assert_cluster_compacts_and_stays_consistent(3, seed);
        assert_cluster_compacts_and_stays_consistent(5, seed);
    }
}

/// A follower partitioned away while the leader compacts past the end of that
/// follower's log can only be caught up by `InstallSnapshot`: its `nextIndex`
/// falls below the leader's compaction base. After the heal it converges.
fn assert_lagging_follower_caught_up_by_snapshot(seed: u64) {
    let node_count = 5;
    let net = Net {
        drop_permille: 0,
        dup_permille: 0,
        jitter: 5,
        repartition_every: 0,
    };
    let mut sim = Sim::new(node_count, seed)
        .with_net(net)
        .with_snapshot_threshold(3);

    let leader = sim.run_until_leader(5_000);
    sim.propose_at(leader, b"pre");
    sim.run_until(sim.now + 400);

    // Cut one follower off from the other four.
    let victim = (leader + 2) % node_count;
    assert!(!sim.nodes[victim].is_leader());
    let mut groups = vec![0usize; node_count];
    groups[victim] = 1;
    sim.set_groups(groups);

    // The majority keeps committing; the leader compacts well past where the
    // isolated follower's log stops.
    for k in 0..12 {
        let current = sim.run_until_leader(5_000);
        sim.propose_at(current, format!("x-{k}").as_bytes());
        sim.run_until(sim.now + 250);
    }
    let base = sim
        .leader_in_group(0)
        .map_or(0, |l| sim.nodes[l].snapshot_last_index().get());
    assert!(
        base > sim.nodes[victim].log().last_index().get(),
        "seed={seed}: leader base {base} did not overtake the isolated follower",
    );

    // Heal and settle: the follower must be repaired by a snapshot.
    sim.set_groups(vec![0; node_count]);
    sim.run_until(sim.now + 10_000);

    assert!(
        sim.nodes[victim].snapshot_last_index().get() > 0,
        "seed={seed}: the lagging follower was not caught up by a snapshot",
    );
    let reference = sim.applied_commands(0);
    assert!(reference.len() >= 13, "seed={seed}: too little committed");
    for i in 0..node_count {
        assert_eq!(
            sim.applied_commands(i),
            reference,
            "seed={seed}: node {i} did not converge after the snapshot catch-up",
        );
    }
}

#[test]
fn a_lagging_follower_is_caught_up_by_a_snapshot() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED, 7]) {
        assert_lagging_follower_caught_up_by_snapshot(seed);
    }
}
