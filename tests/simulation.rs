//! Deterministic simulation tests.
//!
//! Every test here MUST be reproducible from a single `u64` seed: a simulated
//! network, clock, and storage layer, no wall-clock time, no real threads, no
//! `HashMap` iteration order leaking into behaviour. A failing run must print
//! its seed so it can be replayed with `SEED=<n> just sim`.
//!
//! See AGENTS.md "Testing standards" and "Durability model".

// Scaffolding only — the simulator lands with the sans-IO core refactor.
#[test]
fn simulation_harness_present() {}
