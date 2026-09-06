//! Deterministic simulation tests.
//!
//! Every test here is reproducible from a single `u64` seed: a simulated
//! network and clock, with no wall-clock time, real threads, or `HashMap`
//! iteration order leaking into behaviour. A failing assertion carries its seed
//! so it can be replayed with `SEED=<n> just sim-seed`.
//!
//! See AGENTS.md "Testing standards".

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use raft::core::{Effect, Input, LogicalInstant, Message, NodeId, RaftNode};

// Timer and network constants, in logical milliseconds. The heartbeat period
// sits comfortably below the election-timeout floor, so a healthy leader is
// never unseated on a reliable network.
const ELECTION_MIN: u64 = 150;
const ELECTION_MAX: u64 = 300;
const HEARTBEAT_PERIOD: u64 = 50;
const NET_DELAY: u64 = 10;
/// Sentinel deadline meaning "not armed": a leader runs no election timer.
const NEVER: u64 = u64::MAX;

/// A message in flight on the simulated network.
struct Envelope {
    deliver_at: u64,
    from: NodeId,
    to: NodeId,
    message: Message,
}

/// A whole cluster driven on one thread against a logical clock.
///
/// The network here is reliable and in-order with a fixed delay; loss,
/// reordering, and partitions arrive with the log-replication tests.
struct Sim {
    now: u64,
    ids: Vec<NodeId>,
    nodes: Vec<RaftNode>,
    election_deadline: Vec<u64>,
    heartbeat_deadline: Vec<u64>,
    inflight: Vec<Envelope>,
    /// Per node, the `(index, command)` pairs the core told it to apply, in
    /// the order it was told. The event loop asserts index order as it fills
    /// this; tests then check the nodes agree.
    applied: Vec<Vec<(u64, Bytes)>>,
    rng: StdRng,
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
            now: 0,
            ids,
            nodes,
            election_deadline,
            heartbeat_deadline,
            inflight: Vec::new(),
            applied: vec![Vec::new(); node_count],
            rng,
        }
    }

    fn index_of(&self, id: NodeId) -> usize {
        // Every routed id was built in `new`, so the fallback is unreachable.
        self.ids.iter().position(|&known| known == id).unwrap_or(0)
    }

    fn next_event_time(&self) -> u64 {
        let timers = self
            .election_deadline
            .iter()
            .copied()
            .chain(self.heartbeat_deadline.iter().copied());
        let messages = self.inflight.iter().map(|envelope| envelope.deliver_at);
        timers.chain(messages).min().unwrap_or(NEVER)
    }

    /// Runs the event loop until logical time `end` (inclusive), advancing the
    /// clock only to times where something actually happens.
    fn run_until(&mut self, end: u64) {
        loop {
            let t = self.next_event_time();
            if t > end {
                self.now = end;
                return;
            }
            self.now = t;

            for i in 0..self.nodes.len() {
                if self.election_deadline[i] == t {
                    self.step(i, Input::ElectionTimeout);
                }
                if self.heartbeat_deadline[i] == t {
                    self.heartbeat_deadline[i] = t + HEARTBEAT_PERIOD;
                    self.step(i, Input::HeartbeatTick);
                }
            }

            let (due, pending): (Vec<Envelope>, Vec<Envelope>) = self
                .inflight
                .drain(..)
                .partition(|envelope| envelope.deliver_at == t);
            self.inflight = pending;
            for envelope in due {
                let i = self.index_of(envelope.to);
                self.step(
                    i,
                    Input::Deliver {
                        from: envelope.from,
                        message: envelope.message,
                    },
                );
            }
        }
    }

    /// Steps node `i` with `input` and routes the effects it returns.
    fn step(&mut self, i: usize, input: Input) {
        let effects = self.nodes[i].step(input, LogicalInstant::from_millis(self.now));
        for effect in effects {
            match effect {
                Effect::SendRpc { to, message } => self.inflight.push(Envelope {
                    deliver_at: self.now + NET_DELAY,
                    from: self.nodes[i].id(),
                    to,
                    message,
                }),
                Effect::ResetElectionTimer => {
                    self.election_deadline[i] = self.arm_election();
                }
                Effect::ApplyToStateMachine { index, command } => {
                    let expected = self.applied[i].len() as u64 + 1;
                    assert_eq!(
                        index.get(),
                        expected,
                        "node {i} applied index {} out of order (expected {expected})",
                        index.get(),
                    );
                    self.applied[i].push((index.get(), command));
                }
                // No durability model yet (that is step 6).
                Effect::Persist { .. } | Effect::PersistLog { .. } => {}
            }
        }

        // A leader runs no election timer; every other role has one armed. The
        // core emits no explicit "stop timer", so reconcile against the role.
        if self.nodes[i].is_leader() {
            self.election_deadline[i] = NEVER;
        } else if self.election_deadline[i] == NEVER {
            self.election_deadline[i] = self.arm_election();
        }
    }

    fn arm_election(&mut self) -> u64 {
        self.now + self.rng.gen_range(ELECTION_MIN..ELECTION_MAX)
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

    /// Feeds `command` to the current leader as a client proposal and routes
    /// the resulting effects.
    fn propose(&mut self, seed: u64, command: &[u8]) {
        let leader = self.sole_leader();
        assert!(
            leader.is_some(),
            "seed={seed}: no sole leader to accept a proposal",
        );
        self.step(
            leader.unwrap_or(0),
            Input::Propose {
                command: Bytes::copy_from_slice(command),
            },
        );
    }
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
        sim.propose(seed, command);
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
        assert!(
            node.last_applied().get() <= node.commit_index().get(),
            "seed={seed}: node {i} applied past its commit index",
        );
        let applied: Vec<Bytes> = sim.applied[i].iter().map(|(_, cmd)| cmd.clone()).collect();
        assert_eq!(
            applied, want,
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
