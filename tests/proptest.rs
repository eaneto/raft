//! Property-based tests: random cluster sizes and event schedules over the
//! shared [`harness::Sim`].
//!
//! The harness re-checks the Raft safety properties after every core step, so a
//! schedule that runs to completion without panicking has held Election
//! Safety, Log Matching, Leader Completeness, State Machine Safety, and
//! `commitIndex` / `lastApplied` monotonicity throughout. On top of that these
//! tests assert liveness: once the network heals the cluster converges to one
//! identical applied sequence. Each case also draws its own `AppendEntries`
//! streaming limits — down to one batch in flight and a byte cap smaller than
//! a single command — and an optional compaction threshold, so pipelining,
//! batch splitting and snapshot catch-up run under the same churn.
//!
//! The schedules stay within Raft's fault budget — partitions and message loss
//! (which Raft tolerates), plus *learner additions* (a passive node) and
//! leadership transfers. Node
//! wipes and server removals can legitimately exceed the budget when combined
//! with a partition, so their recovery is covered by the curated
//! `tests/simulation.rs` batteries instead.
//!
//! `just proptest` runs these with `PROPTEST_CASES=2048`; a plain `cargo test`
//! runs the smaller in-file default. A failing case prints the seed and the
//! shrunk `Vec<Op>`.

#[path = "harness/mod.rs"]
mod harness;

use harness::{NEVER, Net, Sim};
use proptest::prelude::*;
use raft::core::ReplicationConfig;

/// One step in a generated schedule.
#[derive(Clone, Debug)]
enum Op {
    /// Propose a command to the current leader, if there is one.
    Propose,
    /// Propose this many commands to the current leader as one batch.
    ProposeBurst(usize),
    /// Advance the logical clock by this many milliseconds.
    Tick(u64),
    /// Split the cluster: each node goes to group 0 or 1.
    Partition(Vec<bool>),
    /// Heal every partition.
    Heal,
    /// Ask the leader to add a fresh passive learner.
    AddServer,
    /// Ask the leader to hand leadership to the node at this position (modulo
    /// the node count). The core refuses a target that is itself, a learner,
    /// or mid-change, so any position is a valid op.
    Transfer(usize),
    /// Ask the node at this position (modulo the node count) for a
    /// linearizable read; the harness checks any answer against what was
    /// committed before it was asked.
    Read(usize),
}

const CHURN_NET: Net = Net {
    drop_permille: 40,
    dup_permille: 60,
    jitter: 15,
    repartition_every: 0,
};

fn op_strategy(allow_add: bool) -> impl Strategy<Value = Op> {
    let add_weight = u32::from(allow_add);
    prop_oneof![
        5 => Just(Op::Propose),
        2 => (2usize..8).prop_map(Op::ProposeBurst),
        4 => (40u64..500).prop_map(Op::Tick),
        3 => prop::collection::vec(any::<bool>(), 5).prop_map(Op::Partition),
        3 => Just(Op::Heal),
        add_weight => Just(Op::AddServer),
        2 => (0usize..8).prop_map(Op::Transfer),
        3 => (0usize..8).prop_map(Op::Read),
    ]
}

/// Streaming limits from one batch in flight with a 0-byte cap (one entry per
/// batch) up to roomy ones, and a snapshot retry budget from one heartbeat up.
fn replication_strategy() -> impl Strategy<Value = ReplicationConfig> {
    (1usize..=4, 0usize..=24, 1u32..=10).prop_map(
        |(max_inflight, max_append_bytes, snapshot_retry_ticks)| ReplicationConfig {
            max_inflight,
            max_append_bytes,
            snapshot_retry_ticks,
        },
    )
}

/// No compaction, or compaction every few entries, so lagging peers are
/// caught up by snapshot under the same churn.
fn snapshot_threshold_strategy() -> impl Strategy<Value = Option<u64>> {
    prop::option::of(3u64..=12)
}

/// Runs `ops` against a fresh cluster and returns the final [`Sim`].
fn run_schedule(
    node_count: usize,
    seed: u64,
    replication: ReplicationConfig,
    snapshot_threshold: Option<u64>,
    ops: &[Op],
) -> Sim {
    let mut sim = Sim::new(node_count, seed)
        .with_net(CHURN_NET)
        .with_replication(replication);
    sim.snapshot_threshold = snapshot_threshold;
    sim.run_until_leader(15_000);

    let mut next_id = node_count as u64 + 1;
    let mut proposed = 0u32;
    for op in ops {
        match op {
            Op::Propose => {
                if let Some(leader) = sim.sole_leader() {
                    sim.propose_at(leader, format!("p{proposed}").as_bytes());
                    proposed += 1;
                }
            }
            Op::ProposeBurst(count) => {
                if let Some(leader) = sim.sole_leader() {
                    let burst: Vec<Vec<u8>> = (0..*count)
                        .map(|k| format!("p{proposed}.{k}").into_bytes())
                        .collect();
                    let commands: Vec<&[u8]> = burst.iter().map(Vec::as_slice).collect();
                    sim.propose_batch_at(leader, &commands);
                    proposed += 1;
                }
            }
            Op::Tick(ms) => {
                let until = sim.now + ms;
                sim.run_until(until);
            }
            Op::Partition(split) => {
                let groups: Vec<usize> = (0..sim.nodes.len())
                    .map(|i| usize::from(*split.get(i).unwrap_or(&false)))
                    .collect();
                sim.set_groups(groups);
            }
            Op::Heal => sim.set_groups(vec![0; sim.nodes.len()]),
            Op::AddServer => {
                if sim.sole_leader().is_some() {
                    sim.add_server(next_id);
                    next_id += 1;
                }
            }
            Op::Transfer(position) => {
                if sim.sole_leader().is_some() {
                    let target = sim.ids[position % sim.ids.len()];
                    sim.transfer_leadership(target.get());
                }
            }
            Op::Read(position) => sim.issue_read(position % sim.nodes.len()),
        }
    }
    sim
}

/// Heals the network and settles the cluster, nudging a fresh leader into
/// committing a current-term entry so prior-term entries can commit too.
fn heal_and_settle(sim: &mut Sim) {
    sim.next_net_change = NEVER;
    sim.set_groups(vec![0; sim.nodes.len()]);
    sim.run_until(sim.now + 25_000);
    for round in 0..5 {
        let leader = sim.run_until_leader(12_000);
        sim.propose_at(leader, format!("flush{round}").as_bytes());
        sim.run_until(sim.now + 3_000);
    }
}

fn assert_converged(sim: &Sim) {
    let reference = sim.applied_commands(0);
    for i in 0..sim.nodes.len() {
        let common = sim.applied_commands(i).len().min(reference.len());
        assert!(
            sim.applied_commands(i)[..common] == reference[..common],
            "node {i} diverged: {:?} vs {:?}",
            sim.applied_commands(i),
            reference,
        );
    }
}

fn config() -> ProptestConfig {
    // `PROPTEST_CASES` overrides this, so `just proptest` still runs 2048.
    ProptestConfig {
        cases: 48,
        ..ProptestConfig::default()
    }
}

proptest! {
    #![proptest_config(config())]

    /// No schedule of proposals, ticks and partitions drives the core into a
    /// safety violation or a panic.
    #[test]
    fn proptest_safety_holds_under_random_schedules(
        node_count in 3usize..=5,
        seed in any::<u64>(),
        replication in replication_strategy(),
        snapshot_threshold in snapshot_threshold_strategy(),
        ops in prop::collection::vec(op_strategy(false), 0..40),
    ) {
        let _ = run_schedule(node_count, seed, replication, snapshot_threshold, &ops);
    }

    /// After any partition churn, a healed cluster converges to one identical
    /// applied sequence.
    #[test]
    fn proptest_cluster_converges_after_churn(
        node_count in 3usize..=5,
        seed in any::<u64>(),
        replication in replication_strategy(),
        snapshot_threshold in snapshot_threshold_strategy(),
        ops in prop::collection::vec(op_strategy(false), 0..32),
    ) {
        let mut sim = run_schedule(node_count, seed, replication, snapshot_threshold, &ops);
        heal_and_settle(&mut sim);
        assert_converged(&sim);
    }

    /// Learners added at random points still let the cluster converge, with
    /// every safety property held throughout.
    #[test]
    fn proptest_membership_changes_preserve_safety(
        seed in any::<u64>(),
        replication in replication_strategy(),
        snapshot_threshold in snapshot_threshold_strategy(),
        ops in prop::collection::vec(op_strategy(true), 0..32),
    ) {
        let mut sim = run_schedule(3, seed, replication, snapshot_threshold, &ops);
        heal_and_settle(&mut sim);
        assert_converged(&sim);
    }
}
