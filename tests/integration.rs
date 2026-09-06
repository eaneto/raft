//! Multi-node integration tests over a real (in-process) transport.
//!
//! These exercise wiring the simulation tests deliberately bypass: real socket
//! or channel transport, real task scheduling, real timers. Keep them small and
//! few — deep behavioural coverage belongs in `simulation.rs`.
//!
//! See AGENTS.md "Testing standards".

// Scaffolding only.
#[test]
fn integration_harness_present() {}
