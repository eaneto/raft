//! The driver: owns the real clock, the seeded RNG, the timers, the transport,
//! and the storage; runs the single event loop; and performs the core's
//! effects.
//!
//! **Concurrency model: threads with blocking IO, no async runtime.** One
//! "raft thread" owns the [`RaftNode`] and is the only place [`RaftNode::step`]
//! is called. Transport threads decode inbound frames and push them onto an
//! [`mpsc`] channel; client proposals and the shutdown signal arrive on the
//! same channel. Timers are `recv_timeout` deadlines, re-armed from a seeded
//! [`StdRng`], so a given seed drives the same election jitter every run.
//!
//! A storage write that fails is fatal: a failed `fsync` can leave the OS
//! reporting success on the next one while the data never reached disk, so
//! there is nothing safe to do but stop. The loop returns
//! [`Stopped::FatalStorage`] and the embedding binary must treat that as
//! process-fatal — log and exit, never continue, never read the data back and
//! trust it.

use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::clock::{Clock, MonotonicClock};
use crate::core::{
    ClusterConfig, Effect, Input, InstallSnapshotArgs, LogIndex, MembershipChange, Message, NodeId,
    RaftNode,
};
use crate::statemachine::StateMachine;
use crate::storage::{self, PersistentState, SnapshotMeta, Storage};
use crate::transport::{self, TcpTransport, Transport};

/// Static configuration for one server.
#[derive(Clone, Debug)]
pub struct Config {
    /// This server's id.
    pub id: NodeId,
    /// The other servers: `(id, address)`.
    pub peers: Vec<(NodeId, SocketAddr)>,
    /// The address to listen on for peer connections.
    pub listen: SocketAddr,
    /// Directory for the durable log and metadata.
    pub data_dir: PathBuf,
    /// Seed for the election-timeout RNG. Same seed ⇒ same jitter sequence.
    pub seed: u64,
    /// Election timeout is drawn uniformly from `[min, max)`.
    pub election_timeout: (Duration, Duration),
    /// How often a leader sends heartbeats. Keep it well below
    /// `election_timeout.0`.
    pub heartbeat_interval: Duration,
    /// Compact the log once `lastApplied` has run `snapshot_threshold` entries
    /// past the last snapshot. `None` disables compaction, so the log grows
    /// without bound.
    pub snapshot_threshold: Option<u64>,
    /// Largest `InstallSnapshot` chunk, in bytes.
    pub snapshot_chunk_size: usize,
    /// Start as a passive non-voting learner (a brand-new node joining an
    /// existing cluster). Only takes effect when there is no recovered state.
    pub join_as_learner: bool,
}

impl Config {
    /// A config with the paper's rough timing defaults (150–300 ms election,
    /// 50 ms heartbeat), no peers, and seed 0.
    #[must_use]
    pub fn new(id: NodeId, listen: SocketAddr, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            id,
            peers: Vec::new(),
            listen,
            data_dir: data_dir.into(),
            seed: 0,
            election_timeout: (Duration::from_millis(150), Duration::from_millis(300)),
            heartbeat_interval: Duration::from_millis(50),
            snapshot_threshold: None,
            snapshot_chunk_size: 64 * 1024,
            join_as_learner: false,
        }
    }

    /// Marks this node as a brand-new learner joining an existing cluster.
    #[must_use]
    pub const fn joining_as_learner(mut self) -> Self {
        self.join_as_learner = true;
        self
    }

    /// Sets the peer list.
    #[must_use]
    pub fn with_peers(mut self, peers: Vec<(NodeId, SocketAddr)>) -> Self {
        self.peers = peers;
        self
    }

    /// Sets the election-timeout RNG seed.
    #[must_use]
    pub const fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Sets the log-compaction threshold (see [`Config::snapshot_threshold`]).
    #[must_use]
    pub const fn with_snapshot_threshold(mut self, entries: u64) -> Self {
        self.snapshot_threshold = Some(entries);
        self
    }
}

/// Why the driver's event loop stopped.
#[derive(Debug)]
pub enum Stopped {
    /// [`Node::shutdown`] was called, or the [`Node`] handle was dropped.
    ShutDown,
    /// A storage write failed. The process must not continue — a failed
    /// `fsync` is not safely recoverable — so the embedding binary should log
    /// this and exit.
    FatalStorage(storage::Error),
}

/// Failure to start a [`Node`].
#[derive(Debug)]
#[non_exhaustive]
pub enum StartError {
    /// The transport could not bind its listening socket.
    Transport(transport::Error),
    /// The persistent state could not even be read (an IO error, as opposed
    /// to recoverable corruption).
    Storage(storage::Error),
}

impl fmt::Display for StartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(source) => write!(f, "transport: {source}"),
            Self::Storage(source) => write!(f, "storage: {source}"),
        }
    }
}

impl std::error::Error for StartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(source) => Some(source),
            Self::Storage(source) => Some(source),
        }
    }
}

/// The [`Node`] handle has shut down and no longer accepts proposals.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeGone;

impl fmt::Display for NodeGone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the raft node has shut down")
    }
}

impl std::error::Error for NodeGone {}

enum Event {
    Deliver {
        from: NodeId,
        message: Message,
    },
    Propose(Bytes),
    /// A membership change; `SocketAddr` is `Some` only for `AddServer`, so the
    /// driver can reach the new server.
    Membership(MembershipChange, Option<SocketAddr>),
    /// A request for a status snapshot, answered on the given channel.
    Describe(Sender<NodeStatus>),
    Shutdown,
}

/// A point-in-time view of a running [`Node`], from [`Node::status`].
#[derive(Clone, Debug)]
pub struct NodeStatus {
    /// Whether this node currently believes it is the leader.
    pub is_leader: bool,
    /// The voters in the active configuration, sorted.
    pub voters: Vec<NodeId>,
    /// The highest committed log index.
    pub commit_index: LogIndex,
}

/// A running Raft server.
///
/// Dropping the handle shuts the server down; use [`Node::shutdown`] to wait
/// for the loop to stop and learn why.
pub struct Node {
    id: NodeId,
    events: Sender<Event>,
    raft_thread: Option<JoinHandle<Stopped>>,
}

impl Node {
    /// Starts the server: opens `storage`, binds the transport, restores the
    /// core from disk, and spawns the event loop.
    ///
    /// # Errors
    ///
    /// [`StartError::Transport`] if the listen socket cannot be bound, or
    /// [`StartError::Storage`] if the persistent state cannot be read at all.
    /// Recoverable corruption is *not* an error: it is logged and the node
    /// discards its Raft state and starts as a fresh follower, to be
    /// repopulated by the leader.
    pub fn start<S, M>(config: Config, storage: S, state_machine: M) -> Result<Self, StartError>
    where
        S: Storage + Send + 'static,
        M: StateMachine + 'static,
    {
        let (net_tx, net_rx) = mpsc::channel::<(NodeId, Message)>();
        let transport = TcpTransport::start(config.id, &config.peers, config.listen, net_tx)
            .map_err(StartError::Transport)?;

        let (events_tx, events_rx) = mpsc::channel::<Event>();

        // Forward decoded network messages into the single event stream.
        {
            let events_tx = events_tx.clone();
            thread::spawn(move || {
                while let Ok((from, message)) = net_rx.recv() {
                    if events_tx.send(Event::Deliver { from, message }).is_err() {
                        break;
                    }
                }
            });
        }

        let id = config.id;
        let raft_thread = thread::spawn(move || {
            match Driver::restore(config, transport, storage, state_machine) {
                Ok(driver) => driver.run(&events_rx),
                Err(stopped) => stopped,
            }
        });

        Ok(Self {
            id,
            events: events_tx,
            raft_thread: Some(raft_thread),
        })
    }

    /// This server's id.
    #[must_use]
    pub const fn id(&self) -> NodeId {
        self.id
    }

    /// Submits a client command. It is accepted only while this node leads;
    /// otherwise the core drops it (client redirect is a later feature).
    ///
    /// # Errors
    ///
    /// [`NodeGone`] if the event loop has already stopped.
    pub fn propose(&self, command: Bytes) -> Result<(), NodeGone> {
        self.events
            .send(Event::Propose(command))
            .map_err(|_| NodeGone)
    }

    /// Asks the leader to add `id` (reachable at `addr`) to the cluster. The
    /// core only acts while this node leads; the new server catches up as a
    /// non-voting learner before its configuration entry is appended.
    ///
    /// # Errors
    ///
    /// [`NodeGone`] if the event loop has already stopped.
    pub fn add_server(&self, id: NodeId, addr: SocketAddr) -> Result<(), NodeGone> {
        self.events
            .send(Event::Membership(
                MembershipChange::AddServer(id),
                Some(addr),
            ))
            .map_err(|_| NodeGone)
    }

    /// Asks the leader to remove `id` from the cluster.
    ///
    /// # Errors
    ///
    /// [`NodeGone`] if the event loop has already stopped.
    pub fn remove_server(&self, id: NodeId) -> Result<(), NodeGone> {
        self.events
            .send(Event::Membership(MembershipChange::RemoveServer(id), None))
            .map_err(|_| NodeGone)
    }

    /// Returns a point-in-time [`NodeStatus`].
    ///
    /// # Errors
    ///
    /// [`NodeGone`] if the event loop has already stopped.
    pub fn status(&self) -> Result<NodeStatus, NodeGone> {
        let (tx, rx) = mpsc::channel();
        self.events
            .send(Event::Describe(tx))
            .map_err(|_| NodeGone)?;
        rx.recv().map_err(|_| NodeGone)
    }

    /// Signals shutdown and waits for the event loop to stop, returning why.
    #[must_use]
    pub fn shutdown(mut self) -> Stopped {
        let _ = self.events.send(Event::Shutdown);
        Self::join(self.raft_thread.take())
    }

    fn join(handle: Option<JoinHandle<Stopped>>) -> Stopped {
        handle.map_or(Stopped::ShutDown, |handle| {
            handle.join().unwrap_or(Stopped::ShutDown)
        })
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if self.raft_thread.is_some() {
            let _ = self.events.send(Event::Shutdown);
            let _ = Self::join(self.raft_thread.take());
        }
    }
}

/// A snapshot being reassembled from a leader's `InstallSnapshot` chunk stream.
struct IncomingSnapshot {
    meta: SnapshotMeta,
    buf: Vec<u8>,
}

/// Everything the event loop owns.
struct Driver<S, M> {
    config: Config,
    transport: TcpTransport,
    storage: S,
    state_machine: M,
    clock: MonotonicClock,
    rng: StdRng,
    node: RaftNode,
    election_deadline: Option<Instant>,
    heartbeat_deadline: Option<Instant>,
    /// The newest snapshot this node holds, for serving
    /// [`Effect::SendSnapshot`]. `None` until it takes or installs one.
    snapshot: Option<(SnapshotMeta, Bytes)>,
    /// A snapshot currently arriving from the leader, if any.
    incoming_snapshot: Option<IncomingSnapshot>,
    /// Known addresses for peers, seeded from [`Config::peers`] and extended by
    /// [`Node::add_server`]. Used to reconcile the transport with a committed
    /// configuration.
    peer_addrs: BTreeMap<NodeId, SocketAddr>,
}

impl<S: Storage, M: StateMachine> Driver<S, M> {
    fn restore(
        config: Config,
        transport: TcpTransport,
        mut storage: S,
        mut state_machine: M,
    ) -> Result<Self, Stopped> {
        let state = match storage.load() {
            Ok(state) => state,
            Err(storage::Error::Corrupt { detail }) => {
                log::error!(
                    "node {}: persistent state unusable ({detail}); discarding all Raft state \
                     and rejoining as a fresh follower to be repopulated by the leader",
                    config.id,
                );
                PersistentState::fresh()
            }
            Err(other) => return Err(Stopped::FatalStorage(other)),
        };

        let PersistentState {
            current_term,
            voted_for,
            snapshot,
            entries,
        } = state;

        // A recovered snapshot seeds the state machine, and the core comes up
        // with commitIndex / lastApplied already at the snapshot point.
        let snapshot = snapshot.map(|snap| {
            state_machine.restore(&snap.data);
            (snap.meta, snap.data)
        });
        let snapshot_base = snapshot.as_ref().map(|(meta, _)| {
            (
                meta.last_included_index,
                meta.last_included_term,
                meta.config.clone(),
            )
        });

        let peer_addrs: BTreeMap<NodeId, SocketAddr> = config.peers.iter().copied().collect();
        let peer_ids = config.peers.iter().map(|(id, _)| *id);
        // A brand-new joining node (no prior state) comes up as a passive
        // learner until the leader's configuration entry names it; a node with
        // recovered state derives its configuration from that state instead.
        let node = if config.join_as_learner && snapshot_base.is_none() && entries.is_empty() {
            RaftNode::new_learner(config.id)
        } else {
            RaftNode::from_state(
                config.id,
                peer_ids,
                current_term,
                voted_for,
                snapshot_base,
                entries,
            )
        };

        let mut rng = StdRng::seed_from_u64(config.seed);
        let first_election = Instant::now() + random_timeout(&mut rng, &config);
        Ok(Self {
            config,
            transport,
            storage,
            state_machine,
            clock: MonotonicClock::new(),
            rng,
            node,
            election_deadline: Some(first_election),
            heartbeat_deadline: None,
            snapshot,
            incoming_snapshot: None,
            peer_addrs,
        })
    }

    fn run(mut self, events: &Receiver<Event>) -> Stopped {
        loop {
            let wait = soonest(self.election_deadline, self.heartbeat_deadline)
                .map_or(Duration::from_millis(50), |deadline| {
                    deadline.saturating_duration_since(Instant::now())
                });

            match events.recv_timeout(wait) {
                Ok(Event::Shutdown) | Err(RecvTimeoutError::Disconnected) => {
                    return Stopped::ShutDown;
                }
                Ok(Event::Deliver { from, message }) => {
                    if let Err(fatal) = self.step(Input::Deliver { from, message }) {
                        return Stopped::FatalStorage(fatal);
                    }
                }
                Ok(Event::Propose(command)) => {
                    if let Err(fatal) = self.step(Input::Propose { command }) {
                        return Stopped::FatalStorage(fatal);
                    }
                }
                Ok(Event::Membership(change, addr)) => {
                    if let (MembershipChange::AddServer(id), Some(addr)) = (change, addr) {
                        self.peer_addrs.insert(id, addr);
                        // Start talking to the new server now, so it can catch
                        // up as a learner before its configuration entry is
                        // even appended.
                        let mut wanted: Vec<(NodeId, SocketAddr)> = self
                            .transport
                            .peers()
                            .filter_map(|peer| self.peer_addrs.get(&peer).map(|a| (peer, *a)))
                            .collect();
                        wanted.push((id, addr));
                        self.transport.set_peers(&wanted);
                    }
                    if let Err(fatal) = self.step(Input::ChangeMembership { change }) {
                        return Stopped::FatalStorage(fatal);
                    }
                }
                Ok(Event::Describe(reply)) => {
                    let _ = reply.send(NodeStatus {
                        is_leader: self.node.is_leader(),
                        voters: self.node.config().voters().iter().copied().collect(),
                        commit_index: self.node.commit_index(),
                    });
                }
                Err(RecvTimeoutError::Timeout) => {}
            }

            if let Err(fatal) = self.fire_due_timers() {
                return Stopped::FatalStorage(fatal);
            }
            self.reconcile_timers();
        }
    }

    fn step(&mut self, input: Input) -> Result<(), storage::Error> {
        let effects = self.node.step(input, self.clock.now());
        self.perform(effects)?;
        self.maybe_compact()
    }

    fn perform(&mut self, effects: Vec<Effect>) -> Result<(), storage::Error> {
        for effect in effects {
            match effect {
                Effect::SendRpc { to, message } => self.transport.send(to, &message),
                Effect::Persist {
                    current_term,
                    voted_for,
                } => self.storage.persist_metadata(current_term, voted_for)?,
                Effect::PersistLog {
                    from_index,
                    entries,
                } => self.storage.persist_log(from_index, &entries)?,
                Effect::ApplyToStateMachine { index, command } => {
                    self.state_machine.apply(index, &command);
                }
                Effect::ResetElectionTimer => {
                    self.election_deadline =
                        Some(Instant::now() + random_timeout(&mut self.rng, &self.config));
                }
                Effect::SendSnapshot { to } => self.send_snapshot(to),
                Effect::StoreSnapshotChunk {
                    last_included_index,
                    last_included_term,
                    config,
                    offset,
                    data,
                    done,
                } => {
                    let meta = SnapshotMeta {
                        last_included_index,
                        last_included_term,
                        config,
                    };
                    self.receive_snapshot_chunk(meta, offset, &data, done)?;
                }
                Effect::MembershipChanged { config } => self.reconcile_transport(&config),
            }
        }
        Ok(())
    }

    /// Points the transport at exactly the other voters in `config`, using
    /// known addresses. A voter with no known address (e.g. one a peer learned
    /// of only through the committed configuration entry) is skipped until an
    /// address is supplied.
    fn reconcile_transport(&mut self, config: &ClusterConfig) {
        let peers: Vec<(NodeId, SocketAddr)> = config
            .voters()
            .iter()
            .filter(|&&voter| voter != self.config.id)
            .filter_map(|voter| self.peer_addrs.get(voter).map(|addr| (*voter, *addr)))
            .collect();
        self.transport.set_peers(&peers);
    }

    /// Streams the node's current snapshot to `to` as `InstallSnapshot` chunks.
    fn send_snapshot(&self, to: NodeId) {
        let Some((meta, data)) = &self.snapshot else {
            log::warn!(
                "node {}: asked to send a snapshot it does not hold",
                self.config.id,
            );
            return;
        };
        let chunk = self.config.snapshot_chunk_size.max(1);
        let total = data.len();
        let mut offset = 0;
        loop {
            let end = (offset + chunk).min(total);
            let done = end == total;
            self.transport.send(
                to,
                &Message::InstallSnapshot(InstallSnapshotArgs {
                    term: self.node.current_term(),
                    leader_id: self.config.id,
                    last_included_index: meta.last_included_index,
                    last_included_term: meta.last_included_term,
                    config: meta.config.clone(),
                    offset: offset as u64,
                    data: data[offset..end].to_vec(),
                    done,
                }),
            );
            if done {
                break;
            }
            offset = end;
        }
    }

    /// Accumulates one received `InstallSnapshot` chunk. On the terminal chunk,
    /// persists the whole snapshot, restores the state machine from it, and
    /// feeds the core [`Input::SnapshotInstalled`] so it adopts the snapshot
    /// and acknowledges the leader.
    fn receive_snapshot_chunk(
        &mut self,
        meta: SnapshotMeta,
        offset: u64,
        data: &[u8],
        done: bool,
    ) -> Result<(), storage::Error> {
        let expected = self
            .incoming_snapshot
            .as_ref()
            .filter(|inc| inc.meta == meta)
            .map_or(0, |inc| inc.buf.len() as u64);
        if offset == 0 || offset != expected {
            // A fresh transfer, or a gap: (re)start reassembly.
            self.incoming_snapshot = if offset == 0 {
                Some(IncomingSnapshot {
                    meta,
                    buf: data.to_vec(),
                })
            } else {
                // Out-of-order chunk with nothing to attach it to; wait for the
                // leader's next stream, which restarts from offset 0.
                None
            };
        } else if let Some(inc) = self.incoming_snapshot.as_mut() {
            inc.buf.extend_from_slice(data);
        }

        if done && let Some(inc) = self.incoming_snapshot.take() {
            let IncomingSnapshot { meta, buf } = inc;
            self.storage.persist_snapshot(meta.clone(), &buf)?;
            let bytes = Bytes::from(buf);
            self.state_machine.restore(&bytes);
            let SnapshotMeta {
                last_included_index,
                last_included_term,
                config,
            } = meta.clone();
            self.snapshot = Some((meta, bytes));
            return self.step(Input::SnapshotInstalled {
                last_included_index,
                last_included_term,
                config,
            });
        }
        Ok(())
    }

    /// If the log has grown `snapshot_threshold` entries past the last
    /// snapshot, ask the state machine to snapshot itself through `lastApplied`,
    /// persist that, and have the core drop the covered log prefix.
    fn maybe_compact(&mut self) -> Result<(), storage::Error> {
        let Some(threshold) = self.config.snapshot_threshold else {
            return Ok(());
        };
        let last_applied = self.node.last_applied();
        let base = self.node.snapshot_last_index().get();
        if last_applied.get().saturating_sub(base) < threshold.max(1) {
            return Ok(());
        }
        let Some(term) = self.node.log().term_at(last_applied) else {
            return Ok(());
        };
        let meta = SnapshotMeta {
            last_included_index: last_applied,
            last_included_term: term,
            config: self.node.config().clone(),
        };
        let data = self.state_machine.snapshot();
        self.storage.persist_snapshot(meta.clone(), &data)?;
        self.snapshot = Some((meta, data));
        self.step(Input::CompactLog {
            up_to_index: last_applied,
        })
    }

    fn fire_due_timers(&mut self) -> Result<(), storage::Error> {
        let now = Instant::now();
        if !self.node.is_leader() && self.election_deadline.is_some_and(|d| now >= d) {
            self.step(Input::ElectionTimeout)?;
        }
        if self.node.is_leader() && self.heartbeat_deadline.is_some_and(|d| now >= d) {
            self.heartbeat_deadline = Some(Instant::now() + self.config.heartbeat_interval);
            self.step(Input::HeartbeatTick)?;
        }
        Ok(())
    }

    /// The core emits `ResetElectionTimer` but no "stop timer"; reconcile the
    /// armed deadlines against the current role, as the simulator does.
    fn reconcile_timers(&mut self) {
        if self.node.is_leader() {
            self.election_deadline = None;
            if self.heartbeat_deadline.is_none() {
                self.heartbeat_deadline = Some(Instant::now());
            }
        } else {
            self.heartbeat_deadline = None;
            if self.election_deadline.is_none() {
                self.election_deadline =
                    Some(Instant::now() + random_timeout(&mut self.rng, &self.config));
            }
        }
    }
}

fn soonest(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
    [a, b].into_iter().flatten().min()
}

fn random_timeout(rng: &mut StdRng, config: &Config) -> Duration {
    let lo = u64::try_from(config.election_timeout.0.as_millis()).unwrap_or(150);
    let hi = u64::try_from(config.election_timeout.1.as_millis()).unwrap_or(300);
    Duration::from_millis(rng.gen_range(lo..hi.max(lo + 1)))
}
