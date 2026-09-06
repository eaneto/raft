# CLAUDE.md

Read [`AGENTS.md`](./AGENTS.md) and follow it. It is the canonical spec for this
project's scope, architecture, coding standards, durability model, testing
requirements, Raft invariants, and Definition of Done.

Quick reminders (all detailed in `AGENTS.md`):

- Run `just check` before considering any change done; use `/definition-of-done` for the
  full checklist.
- The `src/core/` state machine is pure: no IO, no clock, no threads, no unseeded
  randomness — side effects are returned as values (§5).
- `fsync` failure is fatal; persist `currentTerm`/`votedFor`/log entries before replying
  to the RPC that depends on them (§8).
- `src/raft.rs` and `src/command.rs` are the pre-standards prototype and are lint-exempt
  in `src/lib.rs`; bring modules up to standard one at a time (§3).
- Use paper terminology for new code (`nextIndex`, `matchIndex`, `commitIndex`, …).
