//! Deterministic simulation tests: curated seed batteries over the shared
//! [`harness::Sim`].
//!
//! Every test is reproducible from a single `u64` seed; a failing assertion
//! carries its seed so it can be replayed with `SEED=<n> just sim-seed`. The
//! harness re-checks the Raft safety properties after every core step, so any
//! test that returns has held Election Safety, Log Matching, Leader
//! Completeness, State Machine Safety, and `commitIndex` / `lastApplied`
//! monotonicity for its whole run.

#[path = "harness/mod.rs"]
mod harness;

use bytes::Bytes;
use harness::{HEARTBEAT_PERIOD, JITTER5, NEVER, Net, Sim, seeds};
use raft::core::{ReplicationConfig, TRANSFER_TICKS};

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

    let leader = sim.sole_leader();
    assert!(
        leader.is_some(),
        "seed={seed}: no sole leader after proposals",
    );
    let leader = leader.unwrap_or(0);
    let leader_log = sim.nodes[leader].log().clone();
    // Every command plus each leader's opening no-op: the whole log.
    let n = leader_log.last_index().get();

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
    let pre_partition: Vec<Bytes> = vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")];
    assert_eq!(
        sim.applied_commands(old_leader),
        pre_partition,
        "seed={seed}: cluster did not commit the pre-partition writes",
    );
    let committed_before = sim.nodes[old_leader].commit_index().get();

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
        committed_before,
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
    assert_eq!(
        sim.applied_commands(maj_leader).len(),
        4,
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

/// A follower cut off behind the leader's compaction base is sent the
/// snapshot once per retry budget, not on every heartbeat and proposal; after
/// the heal one more send repairs it.
fn assert_snapshot_is_resent_only_per_retry_budget(seed: u64) {
    const RETRY_TICKS: u32 = 10;
    let node_count = 3;
    let mut sim = Sim::new(node_count, seed)
        .with_net(JITTER5)
        .with_snapshot_threshold(3)
        .with_replication(ReplicationConfig {
            snapshot_retry_ticks: RETRY_TICKS,
            ..ReplicationConfig::default()
        });
    let leader = sim.run_until_leader(5_000);
    sim.propose_at(leader, b"pre");
    sim.run_until(sim.now + 300);

    let victim = (leader + 1) % node_count;
    let mut groups = vec![0usize; node_count];
    groups[victim] = 1;
    sim.set_groups(groups);
    let cut_at = sim.now;
    for k in 0..20 {
        let current = sim.run_until_leader(5_000);
        sim.propose_at(current, format!("x-{k}").as_bytes());
        sim.run_until(sim.now + 150);
    }
    let cut_ticks = (sim.now - cut_at) / HEARTBEAT_PERIOD;
    sim.set_groups(vec![0; node_count]);
    sim.run_until(sim.now + 3_000);

    // At most one send per budget while cut off, plus slack for the first
    // send and the one that lands after the heal.
    let bound = cut_ticks / u64::from(RETRY_TICKS) + 3;
    assert!(
        sim.snapshots_sent as u64 <= bound,
        "seed={seed}: {} snapshots sent over {cut_ticks} cut-off heartbeats (bound {bound})",
        sim.snapshots_sent,
    );
    assert!(sim.nodes[victim].snapshot_last_index().get() > 0);
    let reference = sim.applied_commands(leader);
    for i in 0..node_count {
        assert_eq!(
            sim.applied_commands(i),
            reference,
            "seed={seed}: node {i} did not converge after the snapshot repair",
        );
    }
}

#[test]
fn a_snapshot_is_resent_only_once_per_retry_budget() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED]) {
        assert_snapshot_is_resent_only_per_retry_budget(seed);
    }
}

#[test]
fn a_lagging_follower_is_caught_up_by_a_snapshot() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED, 7]) {
        assert_lagging_follower_caught_up_by_snapshot(seed);
    }
}

// --- 9: single-server membership changes -------------------------------------

/// A server added by the leader catches up as a passive learner, joins the
/// configuration on every node, and then the four-node cluster keeps
/// committing with everyone's applied sequence in agreement.
fn assert_added_server_catches_up_and_votes(seed: u64) {
    let mut sim = Sim::new(3, seed).with_net(JITTER5);
    let leader = sim.run_until_leader(5_000);
    for k in 0..4 {
        sim.propose_at(leader, format!("pre-{k}").as_bytes());
        sim.run_until(sim.now + 200);
    }

    sim.add_server(4);
    let deadline = sim.now + 15_000;
    while sim.now < deadline && !sim.all_agree_member(4, true) {
        sim.run_until(sim.now + HEARTBEAT_PERIOD);
    }
    assert!(
        sim.all_agree_member(4, true),
        "seed={seed}: node 4 was not added to the configuration",
    );

    // The four-node cluster keeps making progress.
    for k in 0..4 {
        let current = sim.run_until_leader(8_000);
        sim.propose_at(current, format!("post-{k}").as_bytes());
        sim.run_until(sim.now + 300);
    }
    let reference = sim.applied_commands(0);
    assert!(
        reference.len() >= 8,
        "seed={seed}: progress stalled after the add"
    );
    for i in 0..sim.nodes.len() {
        let common = sim.applied_commands(i).len().min(reference.len());
        assert_eq!(
            sim.applied_commands(i)[..common],
            reference[..common],
            "seed={seed}: node {i} diverged after the add",
        );
    }
}

/// A server added to a compacting cluster soon has the `Config` entry that
/// admitted it folded into a snapshot, on every node. Everyone must keep the
/// four-node configuration — not fall back to the bootstrap set (or, on the
/// new server, to its empty learner set) — and the new server must be able to
/// lead.
fn assert_added_server_keeps_its_config_after_compaction(seed: u64) {
    let mut sim = Sim::new(3, seed)
        .with_net(JITTER5)
        .with_snapshot_threshold(3);
    let leader = sim.run_until_leader(5_000);
    sim.propose_at(leader, b"pre");
    sim.run_until(sim.now + 300);

    sim.add_server(4);
    let deadline = sim.now + 15_000;
    while sim.now < deadline && !sim.all_agree_member(4, true) {
        sim.run_until(sim.now + HEARTBEAT_PERIOD);
    }
    assert!(
        sim.all_agree_member(4, true),
        "seed={seed}: node 4 not added"
    );

    // Enough entries that every node compacts past the configuration entry.
    for k in 0..8 {
        let current = sim.run_until_leader(8_000);
        sim.propose_at(current, format!("post-{k}").as_bytes());
        sim.run_until(sim.now + 200);
    }
    for i in 0..sim.nodes.len() {
        assert!(
            sim.nodes[i].log().snapshot_last_index().get() > 4,
            "seed={seed}: node {i} never compacted past the configuration entry",
        );
        assert_eq!(
            sim.nodes[i].config().len(),
            4,
            "seed={seed}: node {i} lost the four-node configuration",
        );
    }

    sim.transfer_leadership(4);
    sim.run_until(sim.now + 2_000);
    let leader = sim.run_until_leader(8_000);
    sim.propose_at(leader, b"after");
    sim.run_until(sim.now + 1_000);
    let reference = sim.applied_commands(leader);
    assert_eq!(reference.last().map(|c| &c[..]), Some(&b"after"[..]));
    for i in 0..sim.nodes.len() {
        assert_eq!(
            sim.applied_commands(i),
            reference,
            "seed={seed}: node {i} diverged after compaction",
        );
    }
}

#[test]
fn an_added_server_keeps_its_config_after_compaction() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED]) {
        assert_added_server_keeps_its_config_after_compaction(seed);
    }
}

#[test]
fn a_server_added_by_the_leader_catches_up_and_votes() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED]) {
        assert_added_server_catches_up_and_votes(seed);
    }
}

/// A server that is already partitioned away is removed: the connected
/// majority recomputes its configuration and keeps committing as a smaller
/// cluster. (The isolated node keeps its stale view for good; pre-vote keeps
/// that harmless -- see `a_reconnected_removed_server_does_not_disrupt`.)
fn assert_removed_server_stops_counting(seed: u64) {
    let node_count = 5;
    let mut sim = Sim::new(node_count, seed).with_net(JITTER5);
    let leader = sim.run_until_leader(5_000);
    sim.propose_at(leader, b"c0");
    sim.run_until(sim.now + 300);

    // Isolate a follower, then remove it.
    let victim = (0..node_count).find(|&i| i != leader).unwrap_or(4);
    let mut groups = vec![0usize; node_count];
    groups[victim] = 1;
    sim.set_groups(groups);
    sim.run_until(sim.now + 400);

    let _ = sim.run_until_leader(5_000);
    sim.remove_server(sim.ids[victim].get());
    let deadline = sim.now + 12_000;
    while sim.now < deadline && !sim.all_agree_member(sim.ids[victim].get(), false) {
        sim.run_until(sim.now + HEARTBEAT_PERIOD);
    }
    assert!(
        sim.all_agree_member(sim.ids[victim].get(), false),
        "seed={seed}: the connected majority did not drop the removed node",
    );

    // The four-node cluster keeps committing.
    let l = sim.run_until_leader(5_000);
    let before = sim.nodes[l].commit_index().get();
    for k in 0..3 {
        let current = sim.run_until_leader(5_000);
        sim.propose_at(current, format!("c{k}").as_bytes());
        sim.run_until(sim.now + 400);
    }
    let l = sim.run_until_leader(5_000);
    let after = sim.nodes[l].commit_index().get();
    assert!(
        after > before,
        "seed={seed}: the shrunk cluster stopped committing ({before} -> {after})",
    );
}

#[test]
fn a_removed_server_stops_counting_toward_quorum() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED]) {
        assert_removed_server_stops_counting(seed);
    }
}

/// A leader that removes itself steps down once the change commits, and a new
/// leader emerges among the rest with the applied sequences still consistent.
fn assert_leader_removing_itself_steps_down(seed: u64) {
    let mut sim = Sim::new(3, seed).with_net(JITTER5);
    let old = sim.run_until_leader(5_000);
    sim.propose_at(old, b"x");
    sim.run_until(sim.now + 300);

    sim.remove_server(sim.ids[old].get());
    sim.run_until(sim.now + 6_000);
    assert!(
        !sim.nodes[old].is_leader(),
        "seed={seed}: the self-removed leader did not step down",
    );

    let fresh = sim.run_until_leader(8_000);
    assert_ne!(fresh, old, "seed={seed}: the removed node is leading again");
    sim.propose_at(fresh, b"y");
    sim.run_until(sim.now + 2_000);

    let reference = sim.applied_commands(fresh);
    assert!(reference.len() >= 2, "seed={seed}: progress stalled");
    for i in 0..sim.nodes.len() {
        let common = sim.applied_commands(i).len().min(reference.len());
        assert_eq!(
            sim.applied_commands(i)[..common],
            reference[..common],
            "seed={seed}: node {i} diverged after the self-removal",
        );
    }
}

#[test]
fn a_leader_that_removes_itself_steps_down() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED]) {
        assert_leader_removing_itself_steps_down(seed);
    }
}

/// Only one membership change runs at a time: a second request while the first
/// is uncommitted is ignored.
fn assert_one_change_at_a_time(seed: u64) {
    let mut sim = Sim::new(3, seed).with_net(JITTER5);
    let leader = sim.run_until_leader(5_000);
    sim.propose_at(leader, b"x");
    sim.run_until(sim.now + 200);

    sim.add_server(4);
    // Immediately try to remove node 3 while the add is still in flight.
    sim.remove_server(3);
    sim.run_until(sim.now + 12_000);

    assert!(
        sim.all_agree_member(3, true),
        "seed={seed}: node 3 was removed despite a pending add",
    );
    assert!(
        sim.all_agree_member(4, true),
        "seed={seed}: the add did not complete",
    );
}

#[test]
fn only_one_membership_change_runs_at_a_time() {
    for seed in seeds(&[1, 2, 3, 42, 1_000]) {
        assert_one_change_at_a_time(seed);
    }
}

/// An `AddServer` whose new server is unreachable is abandoned after the
/// catch-up budget, leaving the cluster untouched.
fn assert_catch_up_aborts_for_an_unreachable_server(seed: u64) {
    let mut sim = Sim::new(3, seed).with_net(JITTER5);
    let leader = sim.run_until_leader(5_000);
    sim.propose_at(leader, b"x");
    sim.run_until(sim.now + 200);

    // Bring node 4 up but cut it off from everyone before the add.
    sim.add_server(4);
    let mut groups = vec![0usize; sim.nodes.len()];
    groups[3] = 1;
    sim.set_groups(groups);

    sim.run_until(sim.now + 20_000);

    assert!(
        sim.all_agree_member(4, false),
        "seed={seed}: an unreachable server was still added",
    );
    let current = sim.run_until_leader(5_000);
    sim.propose_at(current, b"y");
    sim.run_until(sim.now + 1_000);
    assert!(
        sim.nodes[current].commit_index().get() >= 2,
        "seed={seed}: the cluster stalled after an aborted add",
    );
}

#[test]
fn a_catch_up_against_an_unreachable_server_aborts() {
    for seed in seeds(&[1, 2, 3, 42, 1_000]) {
        assert_catch_up_aborts_for_an_unreachable_server(seed);
    }
}

/// Pre-vote (thesis §9.6), term-inflation half. A follower cut off from the
/// cluster keeps timing out, but every pre-vote round it runs goes unanswered,
/// so it never promotes to a real candidate and its term never moves. When it
/// reconnects the sitting leader is untouched -- same node, same term -- and
/// the wanderer simply catches up. Without pre-vote its term would have climbed
/// once per timeout and the reconnection would have forced a fresh election.
fn assert_isolated_follower_does_not_inflate_its_term(seed: u64) {
    let node_count = 5;
    let mut sim = Sim::new(node_count, seed).with_net(JITTER5);
    let leader = sim.run_until_leader(5_000);
    sim.propose_at(leader, b"a");
    sim.run_until(sim.now + 500);
    let leader_term = sim.nodes[leader].current_term().get();

    // Cut one follower off from the other four.
    let victim = (0..node_count).find(|&i| i != leader).unwrap_or(1);
    let mut groups = vec![0usize; node_count];
    groups[victim] = 1;
    sim.set_groups(groups);
    let frozen_term = sim.nodes[victim].current_term().get();

    // Let it stew through many election timeouts.
    let deadline = sim.now + 8_000;
    while sim.now < deadline {
        sim.run_until(sim.now + HEARTBEAT_PERIOD);
        let vt = sim.nodes[victim].current_term().get();
        assert!(
            vt <= frozen_term,
            "seed={seed}: isolated follower inflated its term {frozen_term} -> {vt}",
        );
        assert!(
            !sim.nodes[victim].is_leader() && !sim.nodes[victim].is_candidate(),
            "seed={seed}: isolated follower reached a real candidacy",
        );
    }
    assert!(
        sim.nodes[leader].is_leader() && sim.nodes[leader].current_term().get() == leader_term,
        "seed={seed}: leader was disturbed while a lone follower was isolated",
    );

    // Heal: the wanderer rejoins without forcing an election, then catches up.
    sim.set_groups(vec![0; node_count]);
    sim.run_until(sim.now + 5_000);
    assert!(
        sim.nodes[leader].is_leader() && sim.nodes[leader].current_term().get() == leader_term,
        "seed={seed}: the reconnecting follower unseated the leader / forced a new term",
    );

    sim.propose_at(leader, b"b");
    sim.run_until(sim.now + 3_000);
    assert_eq!(
        sim.log_entries(victim),
        sim.log_entries(leader),
        "seed={seed}: the reconnected follower did not catch up",
    );
}

#[test]
fn an_isolated_follower_does_not_inflate_its_term() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED, 7, 99]) {
        assert_isolated_follower_does_not_inflate_its_term(seed);
    }
}

/// Pre-vote (thesis §9.6) + thesis §4.2.3, disruption half. A server removed
/// while it was partitioned keeps a stale view of the configuration for good.
/// When it reconnects it still believes it is a voter and keeps starting
/// pre-vote rounds, but every remaining member refuses -- the leader because it
/// leads, the followers because they hear the leader -- so its term never moves
/// and the shrunk cluster carries on committing, its leader and term intact.
fn assert_reconnected_removed_server_does_not_disrupt(seed: u64) {
    let node_count = 5;
    let mut sim = Sim::new(node_count, seed).with_net(JITTER5);
    let leader = sim.run_until_leader(5_000);
    sim.propose_at(leader, b"c0");
    sim.run_until(sim.now + 300);

    // Isolate a follower, then remove it in absentia.
    let victim = (0..node_count).find(|&i| i != leader).unwrap_or(4);
    let mut groups = vec![0usize; node_count];
    groups[victim] = 1;
    sim.set_groups(groups);
    sim.run_until(sim.now + 400);

    let _ = sim.run_until_leader(5_000);
    sim.remove_server(sim.ids[victim].get());
    let deadline = sim.now + 12_000;
    while sim.now < deadline && !sim.all_agree_member(sim.ids[victim].get(), false) {
        sim.run_until(sim.now + HEARTBEAT_PERIOD);
    }
    assert!(
        sim.all_agree_member(sim.ids[victim].get(), false),
        "seed={seed}: the connected majority did not drop the removed node",
    );

    // Reconnect the removed node and let it flail. It still thinks it is a
    // member (its config was never updated), so it will keep pre-voting.
    let survivor = sim.run_until_leader(5_000);
    assert_ne!(survivor, victim, "seed={seed}: the removed node is leading");
    let term = sim.nodes[survivor].current_term().get();
    let commit_before = sim.nodes[survivor].commit_index().get();
    sim.set_groups(vec![0; node_count]);

    let deadline = sim.now + 8_000;
    while sim.now < deadline {
        sim.run_until(sim.now + HEARTBEAT_PERIOD);
        assert!(
            sim.nodes[survivor].is_leader() && sim.nodes[survivor].current_term().get() == term,
            "seed={seed}: the reconnected removed node disturbed the leader",
        );
        assert!(
            !sim.nodes[victim].is_leader() && !sim.nodes[victim].is_candidate(),
            "seed={seed}: the removed node forced its way into a real candidacy",
        );
    }

    // The four-node cluster is still making progress.
    let l = sim.run_until_leader(5_000);
    sim.propose_at(l, b"c1");
    sim.run_until(sim.now + 2_000);
    assert!(
        sim.nodes[l].commit_index().get() > commit_before,
        "seed={seed}: the shrunk cluster stopped committing after the reconnect",
    );
}

#[test]
fn a_reconnected_removed_server_does_not_disrupt() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED]) {
        assert_reconnected_removed_server_does_not_disrupt(seed);
    }
}

/// Logical ms between checks while waiting for a handoff: finer than a
/// heartbeat, so the deadline check is tight.
const NET_STEP: u64 = 10;

/// Leadership transfer (thesis §3.10), happy path. The leader is asked to hand
/// over to a follower that has not yet acked its latest entries. It stops
/// taking proposals, catches the target up, and sends `TimeoutNow`; the target
/// wins at exactly the next term -- no pre-vote, no rival election -- even
/// though every voter was still hearing from the old leader. Nothing committed
/// before the handoff is lost, and the new leader commits.
fn assert_leader_hands_off_to_a_chosen_voter(seed: u64) {
    let node_count = 5;
    let mut sim = Sim::new(node_count, seed).with_net(JITTER5);
    let leader = sim.run_until_leader(5_000);
    sim.propose_at(leader, b"a");
    sim.run_until(sim.now + 300);
    let term = sim.nodes[leader].current_term().get();

    // Transfer right after a proposal, so the target is behind at first.
    let target = (0..node_count).find(|&i| i != leader).unwrap_or(1);
    sim.propose_at(leader, b"b");
    sim.transfer_leadership(sim.ids[target].get());
    assert!(
        sim.nodes[leader].is_transferring(),
        "seed={seed}: the transfer did not start",
    );

    let deadline = sim.now + u64::from(TRANSFER_TICKS) * HEARTBEAT_PERIOD;
    while sim.now < deadline && !sim.nodes[target].is_leader() {
        sim.run_until(sim.now + NET_STEP);
    }
    assert!(
        sim.nodes[target].is_leader(),
        "seed={seed}: the target did not take over within the transfer budget",
    );
    assert_eq!(
        sim.nodes[target].current_term().get(),
        term + 1,
        "seed={seed}: the handoff took more than one election",
    );

    sim.run_until(sim.now + 200);
    assert!(
        !sim.nodes[leader].is_leader(),
        "seed={seed}: the old leader kept office",
    );
    sim.propose_at(target, b"c");
    sim.run_until(sim.now + 1_000);
    let commands = sim.applied_commands(target);
    assert_eq!(
        commands,
        vec![
            Bytes::from_static(b"a"),
            Bytes::from_static(b"b"),
            Bytes::from_static(b"c"),
        ],
        "seed={seed}: the new leader lost or reordered a command",
    );
}

#[test]
fn a_leader_hands_off_to_a_chosen_voter() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED, 7, 99]) {
        assert_leader_hands_off_to_a_chosen_voter(seed);
    }
}

/// Leadership transfer (thesis §3.10), abort path. The target is cut off, so
/// it never catches up and never receives `TimeoutNow`. After `TRANSFER_TICKS`
/// heartbeats the leader abandons the transfer and takes proposals again --
/// same node, same term -- and the four connected servers keep committing.
fn assert_transfer_to_an_unreachable_voter_is_abandoned(seed: u64) {
    let node_count = 5;
    let mut sim = Sim::new(node_count, seed).with_net(JITTER5);
    let leader = sim.run_until_leader(5_000);
    sim.propose_at(leader, b"a");
    sim.run_until(sim.now + 300);
    let term = sim.nodes[leader].current_term().get();

    let target = (0..node_count).find(|&i| i != leader).unwrap_or(1);
    let mut groups = vec![0usize; node_count];
    groups[target] = 1;
    sim.set_groups(groups);
    sim.transfer_leadership(sim.ids[target].get());
    assert!(
        sim.nodes[leader].is_transferring(),
        "seed={seed}: the transfer did not start",
    );

    sim.run_until(sim.now + u64::from(TRANSFER_TICKS + 1) * HEARTBEAT_PERIOD);
    assert!(
        sim.nodes[leader].is_leader() && sim.nodes[leader].current_term().get() == term,
        "seed={seed}: an abandoned transfer disturbed the leader",
    );
    assert!(
        !sim.nodes[leader].is_transferring(),
        "seed={seed}: the transfer outlived its budget",
    );

    let commit_before = sim.nodes[leader].commit_index().get();
    sim.propose_at(leader, b"b");
    sim.run_until(sim.now + 1_000);
    assert!(
        sim.nodes[leader].commit_index().get() > commit_before,
        "seed={seed}: the leader did not commit after the transfer was abandoned",
    );
}

#[test]
fn a_transfer_to_an_unreachable_voter_is_abandoned() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED]) {
        assert_transfer_to_an_unreachable_voter_is_abandoned(seed);
    }
}

// --- Phase 2: AppendEntries batching / pipelining ------------------------------

/// Tight streaming limits: two batches in flight, each at most eight bytes of
/// payload (two of the `b-<k>-<j>` commands at most), so every catch-up is
/// split into many batches and the window is full most of the time.
const TIGHT: ReplicationConfig = ReplicationConfig {
    max_inflight: 2,
    max_append_bytes: 8,
    snapshot_retry_ticks: 20,
};

/// Under loss, duplication and reordering, bursts of batched proposals stream
/// through a tight pipelining window and all commit. One follower is cut off
/// for part of the run, so it falls far behind and must be walked back and
/// refilled in capped batches after the heal. The harness checks the byte cap
/// on every `AppendEntries` sent.
fn assert_bursts_commit_through_a_tight_window(node_count: usize, seed: u64) {
    let net = Net {
        drop_permille: 60,
        dup_permille: 100,
        jitter: 25,
        repartition_every: 0,
    };
    let mut sim = Sim::new(node_count, seed)
        .with_net(net)
        .with_replication(TIGHT);

    let bursts: Vec<Vec<Vec<u8>>> = (0..10)
        .map(|k| (0..4).map(|j| format!("b-{k}-{j}").into_bytes()).collect())
        .collect();
    let mut lagger = None;
    for (k, burst) in bursts.iter().enumerate() {
        let leader = sim.run_until_leader(10_000);
        if k == 2 {
            // Isolate a follower for the next four bursts.
            let victim = (leader + 1) % node_count;
            let mut groups = vec![0usize; node_count];
            groups[victim] = 1;
            sim.set_groups(groups);
            lagger = Some(victim);
        }
        if k == 6 {
            sim.set_groups(vec![0; node_count]);
        }
        let leader = sim.run_until_leader(10_000);
        let commands: Vec<&[u8]> = burst.iter().map(Vec::as_slice).collect();
        sim.propose_batch_at(leader, &commands);
        sim.run_until(sim.now + 400);
    }
    sim.run_until(sim.now + 30_000); // long settle

    let want: Vec<Bytes> = bursts
        .iter()
        .flatten()
        .map(|c| Bytes::copy_from_slice(c))
        .collect();
    for i in 0..node_count {
        assert_eq!(
            sim.applied_commands(i),
            want,
            "seed={seed}: node {i} (lagger {lagger:?}) did not converge to every burst",
        );
    }
}

#[test]
fn bursts_commit_through_a_tight_pipelining_window() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED, 7, 99]) {
        assert_bursts_commit_through_a_tight_window(3, seed);
        assert_bursts_commit_through_a_tight_window(5, seed);
    }
}

/// Followers compact every few entries while duplicated and reordered
/// `AppendEntries` from before each compaction keep arriving, anchored below
/// the follower's snapshot. They must be recognised as already covered —
/// never spliced over the snapshot — and the cluster converges.
fn assert_compaction_survives_stale_append_entries(node_count: usize, seed: u64) {
    let net = Net {
        drop_permille: 20,
        dup_permille: 300,
        jitter: 40,
        repartition_every: 0,
    };
    let mut sim = Sim::new(node_count, seed)
        .with_net(net)
        .with_replication(TIGHT)
        .with_snapshot_threshold(3);

    let mut want = Vec::new();
    for k in 0..8 {
        let burst: Vec<Vec<u8>> = (0..4).map(|j| format!("s-{k}-{j}").into_bytes()).collect();
        let leader = sim.run_until_leader(10_000);
        let commands: Vec<&[u8]> = burst.iter().map(Vec::as_slice).collect();
        sim.propose_batch_at(leader, &commands);
        want.extend(burst.iter().map(|c| Bytes::copy_from_slice(c)));
        sim.run_until(sim.now + 300);
    }
    sim.run_until(sim.now + 20_000);

    for i in 0..node_count {
        assert_eq!(
            sim.applied_commands(i),
            want,
            "seed={seed}: node {i} did not converge while compacting",
        );
    }
}

#[test]
fn compaction_survives_stale_append_entries() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED, 7, 99]) {
        assert_compaction_survives_stale_append_entries(3, seed);
        assert_compaction_survives_stale_append_entries(5, seed);
    }
}

/// A wiped follower (empty log, but the leader still remembers a high
/// `matchIndex` for it) is walked back past that stale `matchIndex` and
/// refilled under tight limits.
#[test]
fn a_wiped_follower_recovers_through_a_tight_window() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED]) {
        let node_count = 3;
        let mut sim = Sim::new(node_count, seed)
            .with_net(JITTER5)
            .with_replication(TIGHT);
        let leader = sim.run_until_leader(5_000);
        sim.propose_batch_at(leader, &[b"w-0", b"w-1", b"w-2", b"w-3", b"w-4"]);
        sim.run_until(sim.now + 1_000);

        let victim = (leader + 1) % node_count;
        sim.wipe_node(victim);
        let leader = sim.run_until_leader(5_000);
        sim.propose_batch_at(leader, &[b"w-5", b"w-6"]);
        sim.run_until(sim.now + 5_000);

        let reference = sim.applied_commands(leader);
        assert_eq!(reference.len(), 7, "seed={seed}: not everything committed");
        assert_eq!(
            sim.applied_commands(victim),
            reference,
            "seed={seed}: the wiped follower did not catch up",
        );
    }
}

// --- Phase 2: fast log backtracking ---------------------------------------------

/// Runs until some node in partition `group` other than `exclude` leads, and
/// returns it; asserts one appears.
fn run_until_leader_in_group(sim: &mut Sim, group: usize, exclude: &[usize]) -> usize {
    let deadline = sim.now + 5_000;
    let mut found = None;
    while found.is_none() && sim.now < deadline {
        sim.run_until(sim.now + HEARTBEAT_PERIOD);
        found = sim
            .leaders()
            .into_iter()
            .find(|&i| sim.groups[i] == group && !exclude.contains(&i));
    }
    assert!(
        found.is_some(),
        "seed={}: no new leader in group {group}",
        sim.seed,
    );
    found.unwrap_or(0)
}

/// Fast log backtracking (paper §5.3, Figure 7). A leader cut off from the
/// rest of the cluster keeps accepting proposals, piling up a long tail from
/// its own term that can never commit. The majority elects a successor, which
/// commits as long a tail of its own over the same indices; then that
/// successor is cut off too and a third leader is elected — one that starts
/// probing the deposed first leader from past both tails. Stepping back one
/// entry per rejection would take a round trip per stale entry; the hint
/// steps over the stale term in one.
fn assert_divergent_tail_is_skipped_per_term(seed: u64) {
    const STALE: usize = 60;
    let node_count = 5;
    let mut sim = Sim::new(node_count, seed).with_net(JITTER5);
    let first = sim.run_until_leader(5_000);
    sim.propose_at(first, b"committed");
    sim.run_until(sim.now + 500);

    // Isolate the leader; it keeps leading its term alone and appends a tail
    // that no quorum will ever acknowledge.
    let mut groups = vec![0usize; node_count];
    groups[first] = 1;
    sim.set_groups(groups.clone());
    for k in 0..STALE {
        sim.propose_at(first, format!("stale-{k}").as_bytes());
    }

    // The majority elects a successor, which commits a tail of its own.
    let second = run_until_leader_in_group(&mut sim, 0, &[]);
    for k in 0..STALE {
        sim.propose_at(second, format!("fresh-{k}").as_bytes());
    }
    sim.run_until(sim.now + 1_000);

    // Cut the successor off too: the remaining three elect a leader whose
    // `nextIndex` for everyone starts past the fresh tail.
    groups[second] = 2;
    sim.set_groups(groups.clone());
    let third = run_until_leader_in_group(&mut sim, 0, &[second]);

    // Reconnect the first leader alone and count its rejections while it
    // catches up. The successor stays cut off: its own `nextIndex` for the
    // first leader dates from before the fresh tail, so it would repair it
    // without any backtracking.
    sim.rejections_sent[first] = 0;
    groups[first] = 0;
    sim.set_groups(groups);
    sim.run_until(sim.now + 3_000);

    assert_eq!(
        sim.log_entries(first),
        sim.log_entries(third),
        "seed={seed}: the deposed leader did not converge on the new log",
    );
    // One hinted round trip covers the stale term; the rest are heartbeats
    // that crossed the first rejection on the wire.
    let rejections = sim.rejections_sent[first];
    assert!(
        rejections <= 3,
        "seed={seed}: {rejections} rejections to discard a {STALE}-entry stale tail",
    );
}

#[test]
fn a_divergent_tail_is_skipped_a_term_at_a_time() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED, 7, 99]) {
        assert_divergent_tail_is_skipped_per_term(seed);
    }
}

// --- Phase 2: ReadIndex (thesis §6.4) ------------------------------------------

/// Linearizable reads under the kitchen-sink network: loss, duplication,
/// reordering and a partition redrawn every ~700ms. Every slice proposes a
/// write to the leader (if any), asks a rotating node for a read, and asks
/// every node that believes it leads -- a deposed leader in a minority among
/// them. The harness checks each served read against everything committed
/// before it was issued. Once the network heals and settles, every read has
/// been answered one way or the other.
fn assert_reads_stay_linearizable_under_chaos(seed: u64) {
    let node_count = 5;
    let net = Net {
        drop_permille: 50,
        dup_permille: 120,
        jitter: 25,
        repartition_every: 700,
    };
    let mut sim = Sim::new(node_count, seed).with_net(net);

    let mut slice = 0usize;
    while sim.now < 30_000 {
        if let Some(leader) = sim.sole_leader() {
            sim.propose_at(leader, format!("c{slice}").as_bytes());
        }
        sim.issue_read(slice % node_count);
        for leader in sim.leaders() {
            sim.issue_read(leader);
        }
        slice += 1;
        sim.run_until(sim.now + 137);
    }

    sim.net.repartition_every = 0;
    sim.next_net_change = NEVER;
    sim.set_groups(vec![0; node_count]);
    sim.run_until(sim.now + 10_000);
    let leader = sim.run_until_leader(10_000);
    sim.issue_read(leader);
    sim.run_until(sim.now + 1_000);

    assert!(
        sim.reads_served > 0,
        "seed={seed}: no read was ever served under chaos",
    );
    assert!(
        sim.pending_reads.is_empty(),
        "seed={seed}: reads still unanswered after the heal: {:?}",
        sim.pending_reads,
    );
}

#[test]
fn reads_stay_linearizable_under_chaos() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED, 7, 99]) {
        assert_reads_stay_linearizable_under_chaos(seed);
    }
}

/// The stale read `ReadIndex` exists to prevent. A leader cut off from the rest
/// still believes it leads; the majority elects a successor and commits a
/// write the old leader never sees. A read asked of the old leader must not be
/// served from its stale state: no quorum echoes its round, so the read waits,
/// and fails once the heal makes it step down. The new leader serves a read
/// that reflects the write.
fn assert_deposed_leader_never_serves_a_stale_read(seed: u64) {
    let node_count = 5;
    let mut sim = Sim::new(node_count, seed).with_net(JITTER5);
    let old_leader = sim.run_until_leader(5_000);
    sim.propose_at(old_leader, b"before");
    sim.run_until(sim.now + 500);

    let mut groups = vec![0usize; node_count];
    groups[old_leader] = 1;
    sim.set_groups(groups);
    let mut new_leader = None;
    let deadline = sim.now + 5_000;
    while new_leader.is_none() && sim.now < deadline {
        sim.run_until(sim.now + HEARTBEAT_PERIOD);
        new_leader = sim.leader_in_group(0);
    }
    assert!(
        new_leader.is_some(),
        "seed={seed}: the majority elected no leader"
    );
    let new_leader = new_leader.unwrap_or(0);
    sim.propose_at(new_leader, b"after");
    sim.run_until(sim.now + 500);

    assert!(
        sim.nodes[old_leader].is_leader(),
        "seed={seed}: the isolated leader stepped down on its own",
    );
    sim.issue_read(old_leader);
    sim.run_until(sim.now + 2_000);
    assert_eq!(
        (sim.reads_served, sim.reads_failed),
        (0, 0),
        "seed={seed}: the isolated leader answered a read it cannot confirm",
    );

    sim.set_groups(vec![0; node_count]);
    sim.run_until(sim.now + 1_000);
    assert_eq!(
        (sim.reads_served, sim.reads_failed),
        (0, 1),
        "seed={seed}: the deposed leader's read did not fail on the heal",
    );

    let leader = sim.run_until_leader(5_000);
    sim.issue_read(leader);
    sim.run_until(sim.now + 500);
    assert_eq!(
        sim.reads_served, 1,
        "seed={seed}: the current leader did not serve a read",
    );
}

#[test]
fn a_deposed_leader_never_serves_a_stale_read() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED, 7, 99]) {
        assert_deposed_leader_never_serves_a_stale_read(seed);
    }
}
