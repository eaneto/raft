//! A Raft consensus implementation focused on correctness and understandability.
//!
//! The design is *sans-IO*: [`core`] is a pure, deterministic state machine
//! that performs no IO, reads no clock, spawns no thread, and draws no
//! randomness — every side effect is returned as an [`core::Effect`] value for
//! a driver to run. [`node`] is that driver (threads + blocking IO, no async
//! runtime); [`storage`], [`transport`], and [`clock`] are the traits it
//! depends on, each with a real implementation and a simulated one so the whole
//! cluster can be replayed deterministically from a single seed.
//!
//! Persistence is designed to be correct on Linux regardless of filesystem: the
//! storage layer assumes the worst-case `fsync` semantics (a failed `fsync` may
//! drop the dirty pages while a later one still reports success), treats any
//! `fsync` failure as fatal, and makes `currentTerm` / `votedFor` / log
//! appends durable before the RPC that relied on them is answered.

pub mod clock;
pub mod core;
pub mod node;
pub mod statemachine;
pub mod storage;
pub mod transport;
