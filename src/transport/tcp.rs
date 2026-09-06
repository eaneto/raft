//! [`TcpTransport`]: blocking-socket, thread-per-connection transport.
//!
//! Threads, per node:
//! - one **listener** accepting inbound connections;
//! - one **reader** per accepted connection, decoding frames into the driver's
//!   inbound channel;
//! - one **sender** per peer, owning a lazily (re)dialed [`TcpStream`] and
//!   draining a bounded queue that [`TcpTransport::send`] feeds.
//!
//! Everything shuts down through an [`AtomicBool`] plus, for the readers that
//! block in `read`, a `shutdown(Both)` on a cloned handle.

use std::collections::BTreeMap;
use std::io::Write;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use super::{Error, Transport, encode_frame, read_frame};
use crate::core::{Message, NodeId};

/// Bounded outbound queue depth per peer. A full queue drops the message
/// (Raft retries on the next heartbeat).
const SEND_QUEUE: usize = 1024;
/// How often the listener wakes to re-check the shutdown flag.
const ACCEPT_POLL: Duration = Duration::from_millis(5);
/// How often a sender thread wakes to re-check shutdown / retry a dial.
const SEND_POLL: Duration = Duration::from_millis(200);
/// Timeout for dialing a peer.
const DIAL_TIMEOUT: Duration = Duration::from_millis(500);

/// A running TCP transport. Dropping it stops every thread.
pub struct TcpTransport {
    senders: BTreeMap<NodeId, SyncSender<Message>>,
    shutdown: Arc<AtomicBool>,
    reader_streams: Arc<Mutex<Vec<TcpStream>>>,
    reader_handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    listener_handle: Option<JoinHandle<()>>,
    sender_handles: Vec<JoinHandle<()>>,
}

impl TcpTransport {
    /// Binds `listen`, starts the listener and one sender thread per peer, and
    /// begins forwarding decoded messages to `inbound`.
    ///
    /// # Errors
    ///
    /// [`Error::Bind`] if `listen` cannot be bound.
    pub fn start(
        id: NodeId,
        peers: &[(NodeId, SocketAddr)],
        listen: SocketAddr,
        inbound: Sender<(NodeId, Message)>,
    ) -> Result<Self, Error> {
        let listener = TcpListener::bind(listen).map_err(|source| Error::Bind {
            addr: listen,
            source,
        })?;
        listener
            .set_nonblocking(true)
            .map_err(|source| Error::Bind {
                addr: listen,
                source,
            })?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let reader_streams: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));
        let reader_handles: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));

        let listener_handle = {
            let shutdown = Arc::clone(&shutdown);
            let reader_streams = Arc::clone(&reader_streams);
            let reader_handles = Arc::clone(&reader_handles);
            thread::spawn(move || {
                accept_loop(
                    &listener,
                    &inbound,
                    &shutdown,
                    &reader_streams,
                    &reader_handles,
                );
            })
        };

        let mut senders = BTreeMap::new();
        let mut sender_handles = Vec::new();
        for &(peer, addr) in peers {
            let (tx, rx) = sync_channel::<Message>(SEND_QUEUE);
            senders.insert(peer, tx);
            let shutdown = Arc::clone(&shutdown);
            sender_handles.push(thread::spawn(move || send_loop(id, addr, &rx, &shutdown)));
        }

        Ok(Self {
            senders,
            shutdown,
            reader_streams,
            reader_handles,
            listener_handle: Some(listener_handle),
            sender_handles,
        })
    }
}

impl Transport for TcpTransport {
    fn send(&self, to: NodeId, message: &Message) {
        if let Some(tx) = self.senders.get(&to) {
            match tx.try_send(message.clone()) {
                Ok(()) | Err(TrySendError::Full(_)) => {}
                Err(TrySendError::Disconnected(_)) => {
                    log::debug!("transport: sender thread for {to} is gone");
                }
            }
        }
    }
}

impl Drop for TcpTransport {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        // Unblock readers parked in `read`.
        if let Ok(streams) = self.reader_streams.lock() {
            for stream in streams.iter() {
                let _ = stream.shutdown(Shutdown::Both);
            }
        }
        if let Some(handle) = self.listener_handle.take() {
            let _ = handle.join();
        }
        for handle in self.sender_handles.drain(..) {
            let _ = handle.join();
        }
        if let Ok(mut handles) = self.reader_handles.lock() {
            for handle in handles.drain(..) {
                let _ = handle.join();
            }
        }
    }
}

fn accept_loop(
    listener: &TcpListener,
    inbound: &Sender<(NodeId, Message)>,
    shutdown: &Arc<AtomicBool>,
    reader_streams: &Arc<Mutex<Vec<TcpStream>>>,
    reader_handles: &Arc<Mutex<Vec<JoinHandle<()>>>>,
) {
    while !shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let _ = stream.set_nodelay(true);
                if let Ok(clone) = stream.try_clone()
                    && let Ok(mut streams) = reader_streams.lock()
                {
                    streams.push(clone);
                }
                let inbound = inbound.clone();
                let shutdown = Arc::clone(shutdown);
                let handle = thread::spawn(move || reader_loop(stream, &inbound, &shutdown));
                if let Ok(mut handles) = reader_handles.lock() {
                    handles.push(handle);
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => thread::sleep(ACCEPT_POLL),
            Err(_) => thread::sleep(ACCEPT_POLL),
        }
    }
}

fn reader_loop(
    mut stream: TcpStream,
    inbound: &Sender<(NodeId, Message)>,
    shutdown: &Arc<AtomicBool>,
) {
    loop {
        if shutdown.load(Ordering::SeqCst) {
            return;
        }
        match read_frame(&mut stream) {
            Ok(Some(message)) => {
                if inbound.send(message).is_err() {
                    return; // driver is gone
                }
            }
            // Clean EOF, a connection reset, a decode error, or our own
            // shutdown(Both): in every case this connection is finished.
            Ok(None) | Err(_) => return,
        }
    }
}

fn send_loop(id: NodeId, addr: SocketAddr, queue: &Receiver<Message>, shutdown: &Arc<AtomicBool>) {
    let mut stream: Option<TcpStream> = None;
    while !shutdown.load(Ordering::SeqCst) {
        let message = match queue.recv_timeout(SEND_POLL) {
            Ok(message) => message,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
        };

        if stream.is_none() {
            match TcpStream::connect_timeout(&addr, DIAL_TIMEOUT) {
                Ok(fresh) => {
                    let _ = fresh.set_nodelay(true);
                    stream = Some(fresh);
                }
                Err(_) => continue, // peer down; the message is dropped
            }
        }

        let Some(frame) = encode_frame(id, &message) else {
            continue; // unencodable message: drop it
        };
        if let Some(active) = stream.as_mut()
            && active.write_all(&frame).is_err()
        {
            stream = None; // reconnect next time
        }
    }
}

impl TcpTransport {
    /// The set of peer ids this transport can send to.
    pub fn peers(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.senders.keys().copied()
    }
}
