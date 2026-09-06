//! Property-based tests: random cluster sizes and event schedules over the
//! shared [`harness::Sim`].
//!
//! The harness re-checks the Raft safety properties after every core step, so a
//! schedule that runs to completion without panicking has held Election
//! Safety, Log Matching, Leader Completeness, State Machine Safety, and
//! `commitIndex` / `lastApplied` monotonicity throughout. On top of that these
//! tests assert liveness: once the network heals the cluster converges to one
//! identical applied sequence.
//!
//! The schedules stay within Raft's fault budget — partitions and message loss
//! (which Raft tolerates), plus *learner additions* (a passive node). Node
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

/// One step in a generated schedule.
#[derive(Clone, Debug)]
enum Op {
    /// Propose a command to the current leader, if there is one.
    Propose,
    /// Advance the logical clock by this many milliseconds.
    Tick(u64),
    /// Split the cluster: each node goes to group 0 or 1.
    Partition(Vec<bool>),
    /// Heal every partition.
    Heal,
    /// Ask the leader to add a fresh passive learner.
    AddServer,
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
        4 => (40u64..500).prop_map(Op::Tick),
        3 => prop::collection::vec(any::<bool>(), 5).prop_map(Op::Partition),
        3 => Just(Op::Heal),
        add_weight => Just(Op::AddServer),
    ]
}

/// Runs `ops` against a fresh cluster and returns the final [`Sim`].
fn run_schedule(node_count: usize, seed: u64, ops: &[Op]) -> Sim {
    let mut sim = Sim::new(node_count, seed).with_net(CHURN_NET);
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
        ops in prop::collection::vec(op_strategy(false), 0..40),
    ) {
        let _ = run_schedule(node_count, seed, &ops);
    }

    /// After any partition churn, a healed cluster converges to one identical
    /// applied sequence.
    #[test]
    fn proptest_cluster_converges_after_churn(
        node_count in 3usize..=5,
        seed in any::<u64>(),
        ops in prop::collection::vec(op_strategy(false), 0..32),
    ) {
        let mut sim = run_schedule(node_count, seed, &ops);
        heal_and_settle(&mut sim);
        assert_converged(&sim);
    }

    /// Learners added at random points still let the cluster converge, with
    /// every safety property held throughout.
    #[test]
    fn proptest_membership_changes_preserve_safety(
        seed in any::<u64>(),
        ops in prop::collection::vec(op_strategy(true), 0..32),
    ) {
        let mut sim = run_schedule(3, seed, &ops);
        heal_and_settle(&mut sim);
        assert_converged(&sim);
    }
}
