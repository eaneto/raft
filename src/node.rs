//! The driver: owns the real clock, the seeded RNG, the timers, the transport,
//! and the storage; runs the single event loop; and performs the core's
//! effects.
//!
//! **Concurrency model: threads with blocking IO, no async runtime**
//! (`AGENTS.md` §7). One "raft thread" owns the [`RaftNode`] and is the only
//! place [`RaftNode::step`] is called. Transport threads decode inbound
//! frames and push them onto an [`mpsc`] channel; client proposals and the
//! shutdown signal arrive on the same channel. Timers are `recv_timeout`
//! deadlines, re-armed from a seeded [`StdRng`], so a given seed drives the
//! same election jitter every run.
//!
//! A storage write that fails is fatal (`AGENTS.md` §8 rules 1 and 6): the
//! loop stops and returns [`Stopped::FatalStorage`]. The embedding binary must
//! treat that as process-fatal — log and exit, never continue.

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
use crate::core::{Effect, Input, Message, NodeId, RaftNode};
use crate::statemachine::StateMachine;
use crate::storage::{self, PersistentState, Storage};
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
        }
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
}

/// Why the driver's event loop stopped.
#[derive(Debug)]
pub enum Stopped {
    /// [`Node::shutdown`] was called, or the [`Node`] handle was dropped.
    ShutDown,
    /// A storage write failed. Per `AGENTS.md` §8 the process must not
    /// continue: the embedding binary should log this and exit.
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
    Deliver { from: NodeId, message: Message },
    Propose(Bytes),
    Shutdown,
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
    /// starts fresh (`AGENTS.md` §8 rule 7).
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
}

impl<S: Storage, M: StateMachine> Driver<S, M> {
    fn restore(
        config: Config,
        transport: TcpTransport,
        mut storage: S,
        state_machine: M,
    ) -> Result<Self, Stopped> {
        let state = match storage.load() {
            Ok(state) => state,
            Err(storage::Error::Corrupt { detail }) => {
                log::error!(
                    "node {}: persistent state unusable ({detail}); discarding all Raft state \
                     and rejoining fresh (AGENTS.md §8 rule 7)",
                    config.id,
                );
                PersistentState::fresh()
            }
            Err(other) => return Err(Stopped::FatalStorage(other)),
        };

        let peer_ids = config.peers.iter().map(|(id, _)| *id);
        let node = RaftNode::from_state(
            config.id,
            peer_ids,
            state.current_term,
            state.voted_for,
            state.entries,
        );

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
        self.perform(effects)
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
            }
        }
        Ok(())
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
