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

#[test]
fn a_server_added_by_the_leader_catches_up_and_votes() {
    for seed in seeds(&[1, 2, 3, 42, 1_000, 0x5EED]) {
        assert_added_server_catches_up_and_votes(seed);
    }
}

/// A server that is already partitioned away is removed: the connected
/// majority recomputes its configuration and keeps committing as a smaller
/// cluster. (The isolated node keeps its stale view until pre-vote, Phase 2.)
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
