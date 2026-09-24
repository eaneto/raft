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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::error::Error as _;
    use std::io::{self, Read};
    use std::net::SocketAddr;

    use super::{Error, encode_frame, read_frame};
    use crate::core::{Message, NodeId, RequestVoteReply, Term};

    /// A reader that plays back a script of reads, then reports end of stream.
    struct Script(VecDeque<io::Result<Vec<u8>>>);

    impl Script {
        fn new(steps: impl IntoIterator<Item = io::Result<Vec<u8>>>) -> Self {
            Self(steps.into_iter().collect())
        }
    }

    impl Read for Script {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            match self.0.pop_front() {
                None => Ok(0),
                Some(Err(err)) => Err(err),
                Some(Ok(mut bytes)) => {
                    let n = bytes.len().min(buf.len());
                    buf[..n].copy_from_slice(&bytes[..n]);
                    // Whatever did not fit is read next time.
                    if n < bytes.len() {
                        self.0.push_front(Ok(bytes.split_off(n)));
                    }
                    Ok(n)
                }
            }
        }
    }

    fn message() -> Message {
        Message::RequestVoteReply(RequestVoteReply {
            term: Term::new(3),
            vote_granted: true,
        })
    }

    fn frame() -> Vec<u8> {
        let Some(frame) = encode_frame(NodeId::new(7), &message()) else {
            unreachable!("a small message always encodes");
        };
        frame
    }

    #[track_caller]
    fn error_kind(result: io::Result<Option<(NodeId, Message)>>) -> io::ErrorKind {
        match result {
            Err(err) => err.kind(),
            Ok(frame) => unreachable!("expected an error, got {frame:?}"),
        }
    }

    #[test]
    fn a_frame_round_trips_and_then_the_stream_ends_cleanly() {
        let mut reader = Script::new([Ok(frame())]);

        assert_eq!(
            read_frame(&mut reader).ok(),
            Some(Some((NodeId::new(7), message())))
        );
        assert_eq!(read_frame(&mut reader).ok(), Some(None));
    }

    #[test]
    fn a_stream_ending_inside_a_frame_is_an_error() {
        let frame = frame();

        let mut reader = Script::new([Ok(frame[..2].to_vec())]);
        assert_eq!(
            error_kind(read_frame(&mut reader)),
            io::ErrorKind::UnexpectedEof
        );

        let mut reader = Script::new([Ok(frame[..frame.len() - 1].to_vec())]);
        assert_eq!(
            error_kind(read_frame(&mut reader)),
            io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn a_length_too_short_for_the_sender_id_is_rejected() {
        // Three payload bytes cannot hold the 8-byte sender id.
        let mut bytes = 3u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(&[1, 2, 3]);
        let mut reader = Script::new([Ok(bytes)]);

        assert_eq!(
            error_kind(read_frame(&mut reader)),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn a_frame_with_no_message_body_is_rejected() {
        let mut bytes = 8u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(&7u64.to_le_bytes());
        let mut reader = Script::new([Ok(bytes)]);

        assert_eq!(
            error_kind(read_frame(&mut reader)),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn an_interrupted_read_is_retried() {
        let mut reader = Script::new([
            Err(io::Error::from(io::ErrorKind::Interrupted)),
            Ok(frame()),
        ]);

        assert_eq!(
            read_frame(&mut reader).ok(),
            Some(Some((NodeId::new(7), message())))
        );
    }

    #[test]
    fn any_other_read_error_ends_the_frame() {
        let mut reader = Script::new([
            Err(io::Error::from(io::ErrorKind::ConnectionReset)),
            Ok(frame()),
        ]);

        assert_eq!(
            error_kind(read_frame(&mut reader)),
            io::ErrorKind::ConnectionReset
        );
    }

    #[test]
    fn a_bind_error_names_the_address_and_exposes_the_cause() {
        let err = Error::Bind {
            addr: SocketAddr::from(([127, 0, 0, 1], 9000)),
            source: io::Error::other("in use"),
        };

        assert_eq!(err.to_string(), "cannot bind 127.0.0.1:9000: in use");
        assert_eq!(
            err.source().map(ToString::to_string),
            Some("in use".to_string())
        );
    }
}
