# Build roadmap

Incremental build of the sans-IO Raft core. `AGENTS.md` is the canonical spec;
this file just tracks the sequence of small steps and what's done.

## How we're working

- One small increment per commit, on branch `sans-io-core`.
- Propose a plan and ask the open design questions **before** writing (`AGENTS.md` §0).
- `just check` green before every commit.
- Commit subject: `area: imperative summary` (`AGENTS.md` §12).
- Replay a simulation seed: `SEED=<n> just sim-seed`.

## Decisions locked in so far

| Topic | Choice |
| --- | --- |
| Core surface | `RaftNode::step(Input, LogicalInstant) -> Vec<Effect>`, pure |
| Command payload | `bytes::Bytes` — opaque, core stays non-generic |
| `Role` | enum carrying per-role state: `Candidate { votes_granted }`, `Leader { next_index, match_index }` |
| Timers | driver owns them; core emits `Effect::ResetElectionTimer`, receives `Input::ElectionTimeout` / `HeartbeatTick` |
| `Effect::Persist` | `{ current_term, voted_for }` for now — log persistence shape TBD in step 5 |
| Message sender | `Input::Deliver { from, .. }` carries it, so Figure 2 reply structs stay sender-free |
| Sim harness | lean, inline in `tests/simulation.rs`; grow in place |

## Done

- [x] **1. Domain newtypes** (`Term`, `LogIndex`, `NodeId`) — `fd23f8c`
- [x] **2. `LogEntry` + 1-based `Log`** — `db78ef5`
- [x] **3. Boundary skeleton** (`Input`, `Effect`, `RaftNode::step` no-op) — `7fdf505`
- [x] **4a. Election: `RequestVote` + candidacy** (term rules, §5.4.1 up-to-date, one vote/term, persist-before-reply, win → heartbeats) — `a32f718`
- [x] **4b-i. `AppendEntries` receiver + `HeartbeatTick`** (term/role rules, §5.3 log-match check, timer reset on contact; no append yet) — `f996472`
- [x] **4b-ii. Simulation: one stable leader** (lean harness; 3- and 5-node, seed battery) — `59f38c3`

## Next

- [ ] **5a. Leader append & replicate.** `Propose` on a leader appends to its log,
      emits a log-durability `Persist`, broadcasts per-peer `AppendEntries` built
      from `next_index`.
      - Design Q: what `Effect::Persist` carries once the log is durable
        (whole-log snapshot vs. append/truncate deltas).
- [ ] **5b. Follower splice + leader ack handling.** Receiver rules 3–5 (truncate
      conflicts, append, advance `commitIndex` from `leaderCommit`).
      `AppendEntriesReply` updates `match_index` / `next_index`, backs off on
      failure, advances the leader's `commitIndex` only on a **current-term**
      majority (§5.4.2). Emit `ApplyToStateMachine` as `commitIndex` moves.
- [ ] **5c. Simulation: unreliable network.** Extend the harness with loss /
      reorder / duplicate / partition / heal. Assert Log Matching, Leader
      Completeness, State Machine Safety, and `commitIndex` / `lastApplied`
      monotonicity across seeds.
- [ ] **6. Storage layer** (`src/storage/`). Trait + real impl + simulated impl.
      Checksummed, recoverable log (length-prefixed + CRC32C, torn tail expected);
      redundant `currentTerm` / `votedFor` in two fsync'd files; `fsync` failure is
      fatal; fsync the parent dir after create/rename. Model the failure modes in
      the simulated impl (`AGENTS.md` §8). Cluster-recovery path has a test.
- [ ] **7. Driver** (`src/node.rs`). Owns real clock, seeded RNG, timers, real
      transport (TCP), storage. Runs the event loop, performs effects. Decide
      async vs. threads and record it in `AGENTS.md` §7.
- [ ] **8. Snapshotting / log compaction** + `InstallSnapshot` (paper §7, thesis §5).
- [ ] **9. Cluster membership changes** — prefer single-server (thesis §4); record
      the decision when starting.
- [ ] **Property-based tests** (`proptest`). Random cluster sizes + event
      schedules; assert invariants §9.1–§9.5 after every step; shrink to a minimal
      failing schedule.
- [ ] **Retire the prototype.** Bring `src/raft.rs` / `src/command.rs` up to
      standard one module at a time (dropping its `#[allow(...)]` in `src/lib.rs`
      in the same change), or delete each once the new code supersedes it.

## Phase 2 (only once the above is solid and simulation-tested)

- Pre-vote (thesis §9.6) + CandidateId / disruption fixes
- Leadership transfer (`TimeoutNow`, thesis §3.10)
- Batching / pipelining `AppendEntries`
- Read-index / lease reads (thesis §6.4)

## Target module layout (`AGENTS.md` §5)

```
src/core/           done through leader election
src/storage/        step 6
src/transport/      step 7
src/clock.rs        step 7
src/statemachine.rs step 5b (trait the committed log applies to)
src/node.rs         step 7
```
