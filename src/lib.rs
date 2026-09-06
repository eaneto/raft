//! A Raft consensus implementation focused on correctness and understandability.
//!
//! Project standards, scope, architecture, and the durability model live in
//! [`AGENTS.md`](https://github.com/) at the repo root. Read it before changing code.

// --------------------------------------------------------------------------
// TECH DEBT: the modules below are the original prototype and predate the
// standards in AGENTS.md ("Migration plan"). They are exempted from the lint
// gate so it stays meaningful for new code. Bring one module up to standard
// at a time, then delete its `allow`.
// --------------------------------------------------------------------------
#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo
)]
pub mod command;

#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo
)]
pub mod raft;

// --------------------------------------------------------------------------
// New code below this line meets the AGENTS.md standards and carries no lint
// debt. Keep it that way.
// --------------------------------------------------------------------------

pub mod core;
pub mod storage;
