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
| `Role` | enum carrying per-role state: `PreCandidate { pre_votes_granted }`, `Candidate { votes_granted }`, `Leader { next_index, match_index }` |
| Timers | driver owns them; core emits `Effect::ResetElectionTimer`, receives `Input::ElectionTimeout` / `HeartbeatTick` |
| `Effect::Persist` | `{ current_term, voted_for }` — metadata only |
| `Effect::PersistLog` | `{ from_index, entries }` — a delta: the log from `from_index` (1-based) onward, in full. Append sets `from_index` past the old tail; a follower splice can step it back over conflicts |
| Message sender | `Input::Deliver { from, .. }` carries it, so Figure 2 reply structs stay sender-free |
| `AppendEntriesReply` | carries `match_index` (follower's `prev_log_index + entries.len()` on success) so the leader can set `matchIndex` — matches `mmatchIndex` in the TLA+ spec |
| Commit-time apply | core emits `Effect::ApplyToStateMachine` and owns `last_applied`; failed `AppendEntries` triggers an immediate single-decrement retry |
| Sim harness | lean, inline in `tests/simulation.rs`; grow in place |
| Snapshot payload | metadata (`last_included_index` / term) lives in the pure core; the bytes are the `StateMachine`'s, held and persisted by the driver. `Log` carries a compaction base and rebases all index arithmetic |
| `InstallSnapshot` | chunked wire format (`offset` + `done`); the reply echoes `last_included_index` so the core can set `matchIndex` without having sent the chunks. Effects `SendSnapshot { to }` / `StoreSnapshotChunk`; inputs `SnapshotInstalled` / `CompactLog` |
| Compaction trigger | driver-owned: `Config::snapshot_threshold` entries past the last snapshot; core just trims on `CompactLog` |
| Log-file layout | 8-byte start-index header so recovery can place records after compaction; snapshot written (tmp → rename → dir fsync) **before** the log prefix is dropped, so a crash between leaves a trimmable head, never a gap |
| Membership changes | **single-server** (thesis §4), not joint consensus. Configuration is a `LogEntryKind::Config` entry; a server uses the latest config in its log, committed or not (§4.1). `RaftNode` derives `config` from the log, falling back to a snapshot's config or the bootstrap set |
| Catch-up | a new server joins as a passive non-voting learner (`RaftNode::new_learner`); a leader replicates to it until its `matchIndex` reaches the log end, then appends the `Config` entry. Bounded by a heartbeat-tick budget (`CATCH_UP_TICKS`) — **DEVIATION** from thesis §4.2.1's election-timeout-long rounds, since the core has no clock |
| Change lifecycle | `Input::ChangeMembership` → (catch-up) → `Config` entry → commit. A new leader with an uncommitted `Config` entry adopts the pending change. Leader steps down after committing a config that removes it (§4.2.2). A server absent from its own config stays passive. `Effect::MembershipChanged` drives transport reconciliation |
| Pre-vote (§9.6) | election timeout → `Role::PreCandidate` runs a `Message::PreVote` straw poll at `currentTerm + 1` with no term bump / self-vote / persist; a quorum of grants promotes to a real `Candidate`. "A leader is still alive" is `RaftNode::heard_from_leader` — set on accepted `AppendEntries` / `InstallSnapshot`, cleared on `ElectionTimeout` — so the core reads no clock. A (pre-)vote is withheld while `heard_from_leader`, and a leader grants no pre-votes (§4.2.3). A removed server keeps its stale config for good but can no longer disrupt |
| `PreVoteRound` | pre-vote rounds are numbered (`PreVoteArgs.round`, echoed by the reply) and a grant counts only for the round it answers. Rounds at one term are otherwise indistinguishable — `currentTerm` deliberately does not move — so a late "yes" from an earlier round would be spent in a later one, promoting on consent the peer no longer gives; the term bump that follows unseats a healthy leader via `AppendEntriesReply`. This is why `PreVote` gets its own structs instead of reusing `RequestVoteArgs` / `RequestVoteReply` |

## Done

- [x] **1. Domain newtypes** (`Term`, `LogIndex`, `NodeId`) — `fd23f8c`
- [x] **2. `LogEntry` + 1-based `Log`** — `db78ef5`
- [x] **3. Boundary skeleton** (`Input`, `Effect`, `RaftNode::step` no-op) — `7fdf505`
- [x] **4a. Election: `RequestVote` + candidacy** (term rules, §5.4.1 up-to-date, one vote/term, persist-before-reply, win → heartbeats) — `a32f718`
- [x] **4b-i. `AppendEntries` receiver + `HeartbeatTick`** (term/role rules, §5.3 log-match check, timer reset on contact; no append yet) — `f996472`
- [x] **4b-ii. Simulation: one stable leader** (lean harness; 3- and 5-node, seed battery) — `59f38c3`
- [x] **5a. Leader append & replicate** (`Propose` on a leader appends at the
      current term, emits `Effect::PersistLog` for the new tail, replicates via a
      unified `append_entries_to(peer)` built from `next_index`; no ack handling
      or commit yet — those are 5b) — `039b6f1`
- [x] **5b. Follower splice + leader ack handling** (receiver rules 2–5:
      conflict-only truncate + append + `commitIndex` from `leaderCommit`;
      `AppendEntriesReply` gains `match_index`, drives `next_index`/`match_index`,
      backs off + retries on failure; leader commits on a **current-term**
      majority (§5.4.2); `ApplyToStateMachine` emitted in index order. Sim:
      reliable-network replication/commit test over the seed batteries) —
      `99eba96`
- [x] **5c. Simulation: unreliable network** (`Net` fault config: drop / dup /
      jitter-reorder / periodic random partition; `check_invariants` runs after
      every step enforcing §9.1/§9.3/§9.4/§9.5 + commitIndex/lastApplied
      monotonicity; tests for loss, chaotic delivery, partition+heal, and
      continuous chaos over seed batteries) — `e0bec45`
- [x] **6. Storage layer** (`src/storage/`). `Storage` trait + `FileStorage`
      (CRC32C length-prefixed log with torn-tail recovery; redundant fsync'd
      `currentTerm`/`votedFor` with checksum fallback + disagree rule;
      fsync-fatal `Error::Sync`; parent-dir fsync on create; `/proc/self/mountinfo`
      tier warning) + `MemStorage` (staged-vs-durable, `DropWritesThenLie` /
      `RetainWrites` fault injection, `restart()`). Dependency-free CRC-32C.
      Cluster-recovery sim test (`a_wiped_follower_rejoins_and_catches_up`).
      New dev-dep `tempfile`. — `b6c559c`
- [x] **7. Driver** (`src/node.rs` + `src/transport/`, `src/clock.rs`,
      `src/statemachine.rs`). **Threads + blocking IO, no async runtime**
      (recorded in `AGENTS.md` §7). One raft thread runs a `recv_timeout` event
      loop, performs every `Effect`, fires seeded-RNG timers; storage write
      error → `Stopped::FatalStorage`, `Corrupt` load → start fresh. `TcpTransport`:
      length-prefixed bincode frames, thread-per-connection, lazy redial.
      `RaftNode::from_state` / `Log::from_entries` + `serde` derives on the wire
      types. Integration tests over real TCP (3-node replication; restart reloads
      the log). — `8d37968`
- [x] **8. Snapshotting / log compaction + `InstallSnapshot`** (paper §7,
      thesis §5). `Log` gains a compaction base and rebased index arithmetic
      (`compact` / `from_snapshot`). Core: `InstallSnapshot` receiver, effects
      `SendSnapshot` / `StoreSnapshotChunk`, inputs `SnapshotInstalled` /
      `CompactLog`, leader falls back to a snapshot when a peer drops below the
      base, `leader_id` tracking. Storage: `persist_snapshot` with a
      tmp→rename→dir-fsync snapshot file and a log-prefix rewrite behind an
      8-byte start-index header; `MemStorage` mirrors it under fsync-fault
      injection. `StateMachine::snapshot` / `restore`. Driver: `Config`
      snapshot knobs, chunk streaming + reassembly, threshold-driven
      `maybe_compact`, restore-from-snapshot on startup. Sim: whole-snapshot
      round-trip, offset-aware invariants, compaction + lagging-follower
      catch-up seed batteries. Also a docs pass making every module rustdoc
      self-contained (no `AGENTS.md` cross-references). — `2fc0644..2bfe542`

- [x] **9. Cluster membership changes** — **single-server** (thesis §4), not
      joint consensus. `LogEntry` becomes `{ term, kind }` with
      `LogEntryKind::Command | Config`; `ClusterConfig` wraps a sorted voter
      set. `RaftNode.config` is derived from the log (committed or not),
      falling back to a snapshot's config or the bootstrap set; `quorum` /
      `peers` follow it. `Input::ChangeMembership` with `AddServer` catch-up
      (passive `new_learner`, heartbeat-tick budget — DEVIATION vs §4.2.1) and
      immediate `RemoveServer`; a new leader adopts an in-flight change; a
      self-removing leader steps down on commit (§4.2.2); a server absent from
      its config stays passive. Storage: entry kind tag + config encoding,
      `SnapshotMeta.config` threaded end to end. `TcpTransport::set_peers`;
      `Node::add_server` / `remove_server` / `status`; `Effect::MembershipChanged`
      → transport reconciliation. Sim + TCP integration coverage. Removed-node
      disruption pre-vote is Phase 2. — `8f632cc..30c0225`

- [x] **Property-based tests** (`proptest`). `tests/harness/mod.rs` extracts the
      `Sim` harness so `tests/simulation.rs` (curated seed batteries) and
      `tests/proptest.rs` (random schedules) share it. `proptest.rs` generates
      cluster sizes 3–5 and `Op` schedules (propose / tick / partition / heal /
      add-learner), runs them through `Sim` — which asserts every safety
      property after each step — then heals and asserts convergence. `just
      proptest` runs 2048 cases. Wipes / voter removals stay in the curated
      batteries (they can exceed the fault budget under a partition). —
      `bf3f021..863105a`

- [x] **Retire the prototype.** `src/raft.rs` / `src/command.rs` deleted
      (superseded by the sans-IO modules); their `#[allow(...)]` markers, the
      `src/lib.rs` banner comments, the unused `clap` dependency, and the
      `AGENTS.md` §3 migration plan removed. — `738b778`

## Phase 1 complete

Leader election, log replication, snapshotting / `InstallSnapshot`, and
single-server membership changes are implemented, unit-tested, driven over a
deterministic simulator (curated seed batteries + `proptest`) and over a real
TCP transport. `just check` is green. Every module rustdoc is self-contained.

## Phase 2

- [x] **Pre-vote (thesis §9.6) + disruption fixes (§4.2.3).** `Role::PreCandidate`
      + `Message::PreVote` / `PreVoteReply` (own `PreVoteArgs` / `PreVoteReply`
      structs, because of the round — see the `PreVoteRound` row);
      `handle_election_timeout` runs a straw poll that touches no persistent
      state, `promote_pre_candidate_if_quorum` starts the real election.
      `RaftNode::heard_from_leader` (timer-derived, no clock in the core) gates
      both the pre-vote grant and a new "disregard `RequestVote` while a leader
      is active" rule; a leader grants no pre-votes. Rounds are numbered so a
      grant cannot be spent in a later round. Unit tests for every transition +
      two simulation batteries
      (`an_isolated_follower_does_not_inflate_its_term`,
      `a_reconnected_removed_server_does_not_disrupt`). — `d349688..f31cff4`
- [ ] Leadership transfer (`TimeoutNow`, thesis §3.10)
- [ ] Batching / pipelining `AppendEntries`
- [ ] Read-index / lease reads (thesis §6.4)

## Module layout (`AGENTS.md` §5)

```
src/core/           pure state machine: election, replication, snapshots, membership
src/storage/        Storage trait + FileStorage + fault-injecting MemStorage
src/transport/      Transport trait + TcpTransport (dynamic peer set)
src/clock.rs        Clock trait + MonotonicClock
src/statemachine.rs StateMachine trait (apply / snapshot / restore)
src/node.rs         the driver: event loop, timers, effects, membership API
tests/harness/      shared Sim; tests/simulation.rs + tests/proptest.rs use it
```
