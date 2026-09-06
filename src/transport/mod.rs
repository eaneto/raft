//! RPC transport between Raft servers.
//!
//! The core produces [`Message`] values inside
//! [`Effect::SendRpc`](crate::core::Effect::SendRpc) and consumes them inside
//! [`Input::Deliver`](crate::core::Input::Deliver); it fixes no wire format.
//! This module does: a length-prefixed frame carrying the sender's
//! [`NodeId`] and a `bincode`-encoded `Message`.
//!
//! [`Transport`] is the send side — best effort, like a datagram: a lost or
//! duplicated message is fine, Raft recovers. Receiving is wired separately:
//! an implementation pushes `(from, message)` pairs into an
//! [`std::sync::mpsc::Sender`] the driver owns.
//!
//! [`TcpTransport`] is the real implementation (blocking sockets, one thread
//! per peer for sending and one per accepted connection for receiving). The
//! deterministic simulator does not use this module at all — it routes the
//! core's effects directly.

mod tcp;

pub use tcp::TcpTransport;

use std::fmt;
use std::io::{self, Read};
use std::net::SocketAddr;

use crate::core::{Message, NodeId};

/// The send side of a Raft transport: best-effort, non-blocking, fire and
/// forget.
pub trait Transport: Send + Sync {
    /// Hands `message` to `to`. Never blocks for long and never fails loudly;
    /// a message that cannot be sent is simply dropped (Raft will retry).
    fn send(&self, to: NodeId, message: &Message);
}

/// A transport setup failure.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The listening socket could not be bound.
    Bind {
        /// The address we tried to listen on.
        addr: SocketAddr,
        /// The underlying OS error.
        source: io::Error,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bind { addr, source } => write!(f, "cannot bind {addr}: {source}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bind { source, .. } => Some(source),
        }
    }
}

/// Frame: `[u32 LE payload length][u64 LE sender id][bincode(Message)]`.
fn encode_frame(from: NodeId, message: &Message) -> Option<Vec<u8>> {
    let body = bincode::serialize(message).ok()?;
    let payload_len = u32::try_from(8 + body.len()).ok()?;
    let mut frame = Vec::with_capacity(4 + 8 + body.len());
    frame.extend_from_slice(&payload_len.to_le_bytes());
    frame.extend_from_slice(&from.get().to_le_bytes());
    frame.extend_from_slice(&body);
    Some(frame)
}

/// Reads one frame. `Ok(None)` is a clean end of stream; a timeout / partial
/// read surfaces as an [`io::Error`].
fn read_frame(reader: &mut impl Read) -> io::Result<Option<(NodeId, Message)>> {
    let mut len_bytes = [0u8; 4];
    match read_full(reader, &mut len_bytes)? {
        FillOutcome::Eof => return Ok(None),
        FillOutcome::Short => {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "frame length truncated",
            ));
        }
        FillOutcome::Full => {}
    }
    let payload_len = u32::from_le_bytes(len_bytes) as usize;
    if payload_len < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too short",
        ));
    }
    let mut payload = vec![0u8; payload_len];
    match read_full(reader, &mut payload)? {
        FillOutcome::Full => {}
        FillOutcome::Eof | FillOutcome::Short => {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "frame payload truncated",
            ));
        }
    }
    let from = NodeId::new(u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]));
    let message = bincode::deserialize::<Message>(&payload[8..])
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    Ok(Some((from, message)))
}

enum FillOutcome {
    Full,
    Short,
    Eof,
}

fn read_full(reader: &mut impl Read, buf: &mut [u8]) -> io::Result<FillOutcome> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) if filled == 0 => return Ok(FillOutcome::Eof),
            Ok(0) => return Ok(FillOutcome::Short),
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(FillOutcome::Full)
}
