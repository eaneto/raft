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
        .map(|entry| entry.command.as_ref())
        .collect();
    assert_eq!(commands, vec![b"one".as_slice(), b"two".as_slice()]);
}
