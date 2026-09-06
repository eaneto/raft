//! Multi-node integration tests over the real TCP transport.
//!
//! These exercise the wiring the simulation tests deliberately bypass: real
//! sockets, real OS threads, real timers, real `FileStorage`. Keep them small
//! and few — deep behavioural coverage belongs in `simulation.rs`.

use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tempfile::TempDir;

use raft::core::{LogIndex, NodeId};
use raft::node::{Config, Node};
use raft::statemachine::StateMachine;
use raft::storage::{FileStorage, Storage};

/// A `StateMachine` whose applied commands are visible to the test thread.
#[derive(Clone)]
struct SharedSm(Arc<Mutex<Vec<Bytes>>>);

impl SharedSm {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn applied(&self) -> Vec<Bytes> {
        match self.0.lock() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}

impl StateMachine for SharedSm {
    fn apply(&mut self, _index: LogIndex, command: &Bytes) {
        if let Ok(mut guard) = self.0.lock() {
            guard.push(command.clone());
        }
    }

    fn snapshot(&self) -> Bytes {
        let commands: Vec<Vec<u8>> = match self.0.lock() {
            Ok(guard) => guard.iter().map(|c| c.to_vec()).collect(),
            Err(poisoned) => poisoned.into_inner().iter().map(|c| c.to_vec()).collect(),
        };
        Bytes::from(bincode::serialize(&commands).unwrap_or_default())
    }

    fn restore(&mut self, snapshot: &Bytes) {
        let commands: Vec<Vec<u8>> = bincode::deserialize(snapshot).unwrap_or_default();
        let restored: Vec<Bytes> = commands.into_iter().map(Bytes::from).collect();
        match self.0.lock() {
            Ok(mut guard) => *guard = restored,
            Err(poisoned) => *poisoned.into_inner() = restored,
        }
    }
}

#[track_caller]
fn unwrap<T, E: std::fmt::Display>(result: Result<T, E>) -> T {
    match result {
        Ok(value) => value,
        Err(err) => unreachable!("expected Ok: {err}"),
    }
}

/// A localhost address that was free a moment ago.
fn free_addr() -> SocketAddr {
    let listener = unwrap(TcpListener::bind("127.0.0.1:0"));
    unwrap(listener.local_addr())
}

fn wait_until(timeout: Duration, mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    done()
}

#[test]
fn three_node_cluster_replicates_proposals_over_tcp() {
    const N: usize = 3;
    let ids: Vec<NodeId> = (1..=N as u64).map(NodeId::new).collect();
    let addrs: Vec<SocketAddr> = (0..N).map(|_| free_addr()).collect();
    let dirs: Vec<TempDir> = (0..N).map(|_| unwrap(tempfile::tempdir())).collect();
    let sms: Vec<SharedSm> = (0..N).map(|_| SharedSm::new()).collect();

    let mut nodes: Vec<Node> = Vec::new();
    for i in 0..N {
        let peers = (0..N)
            .filter(|&j| j != i)
            .map(|j| (ids[j], addrs[j]))
            .collect();
        let config = Config::new(ids[i], addrs[i], dirs[i].path())
            .with_peers(peers)
            .with_seed(i as u64 + 1);
        let storage = unwrap(FileStorage::open(dirs[i].path()));
        nodes.push(unwrap(Node::start(config, storage, sms[i].clone())));
    }

    // Let an election settle.
    std::thread::sleep(Duration::from_millis(1_500));

    // Propose each command to every node; only the leader acts on it.
    let commands: Vec<Bytes> = (0..5)
        .map(|k| Bytes::from(format!("cmd-{k}").into_bytes()))
        .collect();
    for command in &commands {
        for node in &nodes {
            let _ = node.propose(command.clone());
        }
        std::thread::sleep(Duration::from_millis(150));
    }

    let converged = wait_until(Duration::from_secs(10), || {
        sms.iter().all(|sm| sm.applied() == commands)
    });
    let applied: Vec<Vec<Bytes>> = sms.iter().map(SharedSm::applied).collect();
    assert!(
        converged,
        "nodes did not converge to {commands:?}; got {applied:?}",
    );

    for node in nodes {
        let _ = node.shutdown();
    }
}

#[test]
fn a_restarted_node_reloads_its_log_from_disk() {
    let id = NodeId::new(1);
    let addr = free_addr();
    let dir = unwrap(tempfile::tempdir());

    // A single-node cluster is its own majority, so proposals commit at once.
    let sm = SharedSm::new();
    let config = Config::new(id, addr, dir.path()).with_seed(1);
    let node = unwrap(Node::start(
        config,
        unwrap(FileStorage::open(dir.path())),
        sm.clone(),
    ));

    // Give the lone node time to elect itself, then propose once each.
    std::thread::sleep(Duration::from_millis(700));
    assert!(node.propose(Bytes::from_static(b"one")).is_ok());
    assert!(node.propose(Bytes::from_static(b"two")).is_ok());
    assert!(
        wait_until(Duration::from_secs(5), || sm.applied().len() >= 2),
        "single node did not apply its own proposals: {:?}",
        sm.applied(),
    );
    let _ = node.shutdown();

    // Reopen the same data dir: the log must come back.
    let mut storage = unwrap(FileStorage::open(dir.path()));
    let reloaded = unwrap(storage.load());
    let commands: Vec<&[u8]> = reloaded
        .entries
        .iter()
        .filter_map(|entry| entry.command_bytes().map(AsRef::as_ref))
        .collect();
    assert_eq!(commands, vec![b"one".as_slice(), b"two".as_slice()]);
}

#[test]
fn a_single_node_compacts_its_log_and_reloads_from_the_snapshot() {
    let id = NodeId::new(1);
    let addr = free_addr();
    let dir = unwrap(tempfile::tempdir());

    let total: usize = 12;
    let sm = SharedSm::new();
    // Compact every 3 applied entries, so by the end most of the log is gone.
    let config = Config::new(id, addr, dir.path())
        .with_seed(1)
        .with_snapshot_threshold(3);
    let node = unwrap(Node::start(
        config,
        unwrap(FileStorage::open(dir.path())),
        sm.clone(),
    ));

    std::thread::sleep(Duration::from_millis(700));
    let commands: Vec<Bytes> = (0..total)
        .map(|k| Bytes::from(format!("op-{k}").into_bytes()))
        .collect();
    for command in &commands {
        assert!(node.propose(command.clone()).is_ok());
    }
    assert!(
        wait_until(Duration::from_secs(5), || sm.applied().len() >= total),
        "single node did not apply all proposals: {:?}",
        sm.applied(),
    );
    let _ = node.shutdown();

    // The on-disk state is now a snapshot plus a short log tail.
    let mut storage = unwrap(FileStorage::open(dir.path()));
    let reloaded = unwrap(storage.load());
    let Some(snapshot) = reloaded.snapshot else {
        unreachable!("a snapshot was taken");
    };
    assert!(
        snapshot.meta.last_included_index.get() >= (total as u64) - 3,
        "snapshot only covers through {}",
        snapshot.meta.last_included_index,
    );
    assert!(
        reloaded.entries.len() < total,
        "log was not compacted: {} entries",
        reloaded.entries.len(),
    );

    // Restarting on that dir restores the state machine from the snapshot and
    // replays only the tail, ending up with the full applied sequence.
    let sm2 = SharedSm::new();
    let node2 = unwrap(Node::start(
        Config::new(id, free_addr(), dir.path())
            .with_seed(2)
            .with_snapshot_threshold(3),
        unwrap(FileStorage::open(dir.path())),
        sm2.clone(),
    ));
    assert!(
        wait_until(Duration::from_secs(5), || sm2.applied() == commands),
        "restarted node did not reproduce the applied sequence: {:?}",
        sm2.applied(),
    );
    let _ = node2.shutdown();
}

/// Starts an `n`-node cluster over TCP with a shared address book; every node
/// bootstraps with all `n` as voters. Returns the nodes and their state
/// machines.
fn start_cluster(n: usize) -> (Vec<Node>, Vec<SharedSm>, Vec<SocketAddr>, Vec<TempDir>) {
    let ids: Vec<NodeId> = (1..=n as u64).map(NodeId::new).collect();
    let addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();
    let dirs: Vec<TempDir> = (0..n).map(|_| unwrap(tempfile::tempdir())).collect();
    let sms: Vec<SharedSm> = (0..n).map(|_| SharedSm::new()).collect();

    let mut nodes = Vec::new();
    for i in 0..n {
        let peers = (0..n)
            .filter(|&j| j != i)
            .map(|j| (ids[j], addrs[j]))
            .collect();
        let config = Config::new(ids[i], addrs[i], dirs[i].path())
            .with_peers(peers)
            .with_seed(i as u64 + 1);
        nodes.push(unwrap(Node::start(
            config,
            unwrap(FileStorage::open(dirs[i].path())),
            sms[i].clone(),
        )));
    }
    (nodes, sms, addrs, dirs)
}

fn leader_index(nodes: &[Node]) -> Option<usize> {
    nodes
        .iter()
        .position(|node| node.status().is_ok_and(|s| s.is_leader))
}

#[test]
fn the_leader_removes_a_follower_from_the_cluster() {
    let (nodes, _sms, _addrs, _dirs) = start_cluster(4);

    assert!(
        wait_until(Duration::from_secs(3), || leader_index(&nodes).is_some()),
        "no leader elected",
    );
    let leader = unwrap(leader_index(&nodes).ok_or("no leader"));
    let victim = (0..4).find(|&i| i != leader).unwrap_or(0);
    let victim_id = NodeId::new(victim as u64 + 1);

    assert!(nodes[leader].remove_server(victim_id).is_ok());

    assert!(
        wait_until(Duration::from_secs(5), || {
            nodes[leader]
                .status()
                .is_ok_and(|s| s.voters.len() == 3 && !s.voters.contains(&victim_id))
        }),
        "leader configuration did not shrink: {:?}",
        nodes[leader].status().map(|s| s.voters),
    );

    // The smaller cluster still commits.
    let before = unwrap(nodes[leader].status()).commit_index.get();
    assert!(
        nodes[leader]
            .propose(Bytes::from_static(b"after-remove"))
            .is_ok()
    );
    assert!(
        wait_until(Duration::from_secs(5), || {
            unwrap(nodes[leader].status()).commit_index.get() > before
        }),
        "cluster stopped committing after the removal",
    );

    for node in nodes {
        let _ = node.shutdown();
    }
}

#[test]
fn the_leader_adds_a_learner_that_catches_up_and_starts_voting() {
    let (nodes, _sms, addrs, dirs) = start_cluster(3);

    assert!(
        wait_until(Duration::from_secs(3), || leader_index(&nodes).is_some()),
        "no leader elected",
    );
    let leader = unwrap(leader_index(&nodes).ok_or("no leader"));

    // A few committed entries for the newcomer to catch up on.
    for k in 0..5 {
        let _ = nodes[leader].propose(Bytes::from(format!("e{k}").into_bytes()));
    }

    // Stand up node 4 as a passive learner that knows the other three.
    let new_id = NodeId::new(4);
    let new_addr = free_addr();
    let new_dir = unwrap(tempfile::tempdir());
    let new_peers = (0..3)
        .map(|j| (NodeId::new(j as u64 + 1), addrs[j]))
        .collect();
    let new_node = unwrap(Node::start(
        Config::new(new_id, new_addr, new_dir.path())
            .with_peers(new_peers)
            .with_seed(99)
            .joining_as_learner(),
        unwrap(FileStorage::open(new_dir.path())),
        SharedSm::new(),
    ));

    assert!(nodes[leader].add_server(new_id, new_addr).is_ok());

    assert!(
        wait_until(Duration::from_secs(10), || {
            let leader_ok = nodes[leader]
                .status()
                .is_ok_and(|s| s.voters.len() == 4 && s.voters.contains(&new_id));
            let learner_ok = new_node.status().is_ok_and(|s| s.voters.len() == 4);
            leader_ok && learner_ok
        }),
        "the learner was not added to the configuration: leader={:?} learner={:?}",
        nodes[leader].status().map(|s| s.voters),
        new_node.status().map(|s| s.voters),
    );

    let _ = new_node.shutdown();
    for node in nodes {
        let _ = node.shutdown();
    }
    drop(dirs);
}
