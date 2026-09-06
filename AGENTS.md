# AGENTS.md

Read this file before making changes. If a change needs to deviate from it, say so
explicitly in the commit/PR and explain why.

---

## 0. Basic general definitions

Don't infer, always ask. If something isn't perfectly clear it'll
always be simpler and more precise to ask than do the wrong thing.

Always propose a plan of the changes before actually writing them
down.

---

## 1. Primary goal

A **correct and understandable** Raft implementation, built as a learning exercise.

Priorities, in order:

1. **Correctness** — the safety properties in §9 hold under crashes, partitions, message
   loss/duplication/reordering, and `fsync` failures (§8).
2. **Understandability** — code maps cleanly onto the papers (§4). A reader who knows the
   paper should recognise the code.
3. **Testability** — determinism and dependency injection so behaviour can be simulated
   and replayed (§7).
4. Performance is explicitly **not** a near-term goal. Do not trade away 1–3 for it.

Special emphasis: **persistence must be correct on Linux independent of the filesystem.**
See §8.

---

## 2. Scope

**In scope (build this):**

| Area | Notes |
| --- | --- |
| Leader election | §5 of the extended paper, Figure 2 |
| Log replication | §5, Figure 2 |
| Safety rules | §5.4 (election restriction, commit rule for current term only) |
| Persistence | `currentTerm`, `votedFor`, log, snapshots — durable per §8 |
| Snapshotting / log compaction | §7 of the paper; thesis §5. Includes `InstallSnapshot` |
| Cluster membership changes | Paper §6. Prefer **single-server** changes (thesis §4) over joint consensus; decide and record the choice when starting that work |

**Phase 2 (only after the above is solid and simulation-tested):**

- Pre-vote (thesis §9.6) and the CandidateId/disruption fixes
- Leadership transfer (`TimeoutNow`, thesis §3.10) — a *deliberate* handoff, distinct from
  failover via election
- Batching / pipelining of `AppendEntries`
- Read-index / lease reads (thesis §6.4)

**Out of scope:** multi-Raft / sharding, a production network stack, Byzantine fault
tolerance, alternative consensus algorithms.

---

## 3. Current state & migration plan

There is an existing prototype: `src/raft.rs`, `src/command.rs`. It is a single-file,
IO-coupled first pass that follows Kleppmann-style pseudocode (`prefix_len` / `suffix`
naming). It works but does not meet these standards, so both modules carry a
`#[allow(...)]` lint-debt marker in `src/lib.rs`.

**Target module layout** (the sans-IO design, see §5):

```
src/core/          pure Raft state machine — no IO, no clock, no threads, no RNG
src/storage/       log + metadata + snapshots: trait + real impl + simulated impl
src/transport/     RPC send/recv: trait + real (TCP) + simulated
src/clock.rs       time source trait
src/statemachine.rs application state machine trait the committed log applies to
src/node.rs        the driver: owns the clock/RNG, runs the event loop, performs effects
```

Migrate one module at a time. When a module meets these standards, delete its
`#[allow(...)]` marker in `src/lib.rs` in the same change.

**Known deviations in the prototype to fix during migration:**

- `start_election` increments `current_term` then *decrements* it if the election fails —
  term must be monotonic (§9, invariant 7). Stay a candidate at the higher term until the
  next timeout instead.
- RPCs are sent synchronously (blocking `TcpStream`) from inside state-transition methods.
  The core must not do IO (§5).
- `unwrap()` on wire parsing (`command.rs`, `send_*_request`).
- `HashMap` iteration in `broadcast_current_log` / `commit_log_entries` — iteration order
  must not affect behaviour (§7).
- Ad-hoc `&str` error types — replace with real error enums (§6).

Adopt **paper terminology** for all new identifiers: `nextIndex`, `matchIndex`,
`commitIndex`, `lastApplied`, `AppendEntries`, `RequestVote`, `InstallSnapshot`.

---

## 4. Source of truth

Consult these, in this order, for any behavioural question:

1. **Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm (Extended
   Version)", 2014.** Figure 2 (state + RPCs — treat as a spec), Figure 3 (safety
   properties), §5.4 (safety argument), §6 (membership), §7 (log compaction).
2. **Ongaro, "Consensus: Bridging Theory and Practice" (PhD thesis), 2014.** §3.10
   leadership transfer, §4 single-server membership changes, §5 log compaction, §6 client
   interaction, §9 formal proof.
3. **Ongaro's TLA+ spec (`raft.tla`)** — the precise reference when prose is ambiguous.
4. **Rebello et al., "Can Applications Recover from `fsync` Failures?", USENIX ATC 2020**
   — the basis for the durability model in §8.

When code intentionally differs from the papers, add a comment
`// DEVIATION: <what> because <why>` and note it in the PR.

---

## 5. Architecture: the sans-IO core

The single most important design rule.

**`src/core/` is a pure, deterministic state machine.** Its public surface is roughly:

```rust
fn step(&mut self, input: Input, now: LogicalInstant) -> Vec<Effect>;
```

Inside the core there is **no** `std::time`, **no** file/socket/`println!` IO, **no**
thread spawning, **no** ambient randomness. Every side effect is *returned* as an `Effect`
value (`SendRpc`, `Persist`, `ApplyToStateMachine`, `SetTimer`, …). The **driver**
(`src/node.rs`) owns the real clock, the seeded RNG, storage, and transport, and is the
only place effects are actually performed.

Consequences:

- All time is **logical**: the driver passes `now`; the core compares timeouts against it.
- All randomness (election-timeout jitter) comes from a **seeded** generator the driver
  injects. Same seed ⇒ same run.
- The core can be exhaustively driven by the simulator (§7) with no real IO.
- `storage`, `transport`, and `clock` are **traits** with a real impl and a simulated impl.

If you find yourself wanting to do IO in the core, return an `Effect` instead.

---

## 6. Coding standards (Rust)

- **Edition** 2024, toolchain pinned in `rust-toolchain.toml` (currently 1.98.1). Bump the
  edition and the pin deliberately, each in its own commit.
- **Formatting**: `rustfmt` with `rustfmt.toml`. `just fmt-check` must pass.
- **Lints**: configured in `Cargo.toml [lints]`; `just clippy` runs
  `clippy --all-targets --all-features -- -D warnings`. Warnings are errors in CI.
  - Do not silence a lint repo-wide to dodge a fix. A local `#[allow(...)]` needs a
    one-line reason comment.
- **`unsafe`**: `deny` at the crate level. If genuinely needed (e.g. a raw `fsync`-related
  syscall not exposed by `std`), isolate it in the smallest possible function with a
  `// SAFETY:` comment, and call it out in the PR. Prefer `std` (`File::sync_all`,
  `File::sync_data`, opening a directory and `sync_all`ing it) and, if required, `rustix`
  or `nix` over hand-written `unsafe`.
- **Panics in non-test code**: no bare `unwrap()` / `expect()` / `panic!()` /
  `todo!()` / `unimplemented!()` on a reachable path. If a panic is truly correct (an
  invariant that cannot fail), write `expect("<invariant that guarantees this>")` and add
  a `// PANIC:` comment. Everything else returns `Result`.
- **Docs**: every public item has a `///` doc comment (`missing_docs` warns; rustdoc runs
  with `-D warnings` so broken intra-doc links fail CI). Document *why*, and for `Result`
  functions document the error conditions.
- **Naming**: paper terminology (§3). Types over primitives for domain concepts
  (`Term(u64)`, `LogIndex(u64)`, `NodeId(u64)`) once the core is written.
- **Dependencies**: keep the set small and justify additions in the PR. `Cargo.lock` is
  committed (see §12).

---

## 7. Error handling & concurrency

- **Errors**: concrete `enum` error types per layer (`storage::Error`, `transport::Error`,
  …), `#[non_exhaustive]`, implementing `std::error::Error`. `thiserror` is acceptable.
  No `Box<dyn Error>` in library APIs; no `&str` errors.
- **Fatal vs recoverable**: a lost network message is recoverable (Raft handles it). A
  failed `fsync` is **fatal** (§8) — log and exit, do not paper over it.
- **Concurrency**: the core is single-threaded and synchronous. The driver may be async
  (`tokio`) or thread-based — decide when writing `node.rs` and record it here. The
  simulator drives the core directly on one thread with a logical clock; it must not
  depend on a real async runtime.
- **Determinism**: nothing that affects an `Effect` may depend on `HashMap`/`HashSet`
  iteration order, address-of, wall-clock time, or thread scheduling. Use `BTreeMap` or
  iterate a sorted `Vec<NodeId>`.

---

## 8. Durability model & crash-consistency assumptions

Persistence must be correct on Linux **regardless of filesystem**. We do not restrict the
implementation to a filesystem allowlist — instead the storage layer assumes the
worst-case `fsync` semantics documented by Rebello et al., and the cluster provides a
recovery path when a node's local state is unusable.

### What we assume can happen after `fsync` returns an error

- The dirty pages may be **dropped** from the page cache (ext4 `data=ordered`, the
  default), so a *subsequent* `fsync` on the same file returns `0` (success) although the
  data never reached disk.
- The error is delivered to a file descriptor **at most once** (pre-4.13 Linux could lose
  it entirely; 4.13+ `errseq_t` gives each fd exactly one report). An fd opened after the
  failure never sees it.
- The page cache and the disk **diverge**: re-reading the file returns cached bytes that
  were never persisted, until eviction/remount, after which the old on-disk bytes appear.
- On some filesystems (xfs, btrfs) the write error instead shuts the filesystem down or
  flips it read-only.

### Rules (MUST)

1. **`fsync` failure is fatal.** On an error from `sync_all` / `sync_data` (or a directory
   fsync), log it at `error` and terminate the process. **Never** retry-and-continue, and
   **never** treat a later `fsync` returning `0` as evidence of durability.
2. **Persist before reply.** `currentTerm`, `votedFor`, and any log append or truncation
   MUST be durably `fsync`ed before the RPC handler whose correctness depends on them
   returns a response (Raft Figure 2: "Updated on stable storage before responding to
   RPCs"). This is what stops a node forgetting a promise after a crash.
3. **Checksummed, recoverable log.** The on-disk log is a sequence of length-prefixed
   records, each with a CRC32C over its bytes. Recovery replays records until the first one
   that is short or fails its checksum; a torn tail is **expected**, not corruption.
4. **`fsync` the parent directory** after creating or renaming any segment or snapshot
   file, before treating that file as durable.
5. **Redundant critical metadata.** `currentTerm` / `votedFor` are written to **two**
   independent files, each `fsync`ed. On load: if one fails its checksum, use the other; if
   both parse but disagree, take the more conservative value (higher term; "no vote").
6. **Never trust the page cache after a write-path error.** On any storage write error the
   node goes to a terminal broken state and exits. It does not read data back and rely on
   it.
7. **Cluster-level recovery path.** A node whose persistent state fails validation (or that
   crashed on an `fsync` error) MAY discard all Raft state and rejoin as a fresh follower;
   the leader repopulates it via `InstallSnapshot` + `AppendEntries`. This path MUST have
   a test.
8. `O_DIRECT` / `O_SYNC` are not required. Default is buffered writes plus explicit
   `fsync`. If you use them, add a `// DEVIATION:` comment explaining why.

### Supported filesystems

| Tier | Filesystems | Meaning |
| --- | --- | --- |
| 1 | ext4 (`data=ordered` and `data=journal`), xfs | Exercised by the `filesystems` CI job; recommended for real use |
| 2 | btrfs, zfs | Expected to work; not CI-tested |
| dev only | tmpfs | Not durable — tests and local dev only |

At startup the node reads the data directory's filesystem type and mount options (from
`/proc/self/mountinfo`), logs them, and prints a **warning** for anything outside Tier 1.
It does **not** refuse to start.

### Testing this

The simulated storage layer (§7) models each failure mode above — drop-dirty-pages-then-
return-`0`, retain-dirty-pages, error-once-per-fd, cache/disk divergence — as selectable
behaviours driven by the run seed. The `filesystems` CI job additionally runs the
`persistence` test binary against real loopback ext4/xfs mounts.

---

## 9. Raft correctness invariants

Every change must preserve all of these. If a change touches one, name it in the PR and
give a one-line argument for why it still holds.

**Safety properties (paper Figure 3):**

1. **Election Safety** — at most one leader per term.
2. **Leader Append-Only** — a leader never overwrites or deletes entries in its own log;
   it only appends.
3. **Log Matching** — if two logs contain an entry with the same index and term, the logs
   are identical in every entry up through that index.
4. **Leader Completeness** — if an entry is committed in a term, it is present in the logs
   of all leaders of higher terms.
5. **State Machine Safety** — if a server has applied an entry at an index, no other server
   ever applies a different entry at that index.

**Additional invariants we enforce:**

6. **Persist-before-reply** — §8 rule 2.
7. **Monotonic `currentTerm`** — a node's term never decreases.
8. **One vote per term** — `votedFor` is durable before a vote is granted; at most one
   candidate per term is granted a vote.
9. **`commitIndex` monotonic** — never decreases; a leader advances it only when an entry
   from its **current** term is on a majority (paper §5.4.2).
10. **Applied-in-order** — `lastApplied` advances one entry at a time, in index order, and
    never past `commitIndex`.

---

## 10. Testing standards

Three layers, all required as the codebase grows:

1. **Unit + integration** (`cargo test`, `tests/integration.rs`) — pure helpers and
   small multi-node wiring over a real in-process transport. Keep integration tests few.
2. **Deterministic simulation** (`tests/simulation.rs`) — the primary correctness
   coverage. Simulated network (drop / duplicate / reorder / delay / partition / heal),
   simulated clock, simulated storage (§8). Everything is driven by **one `u64` seed**. A
   failing test **prints its seed**; `SEED=<n> just sim` replays it bit-for-bit.
3. **Property-based** (`proptest`) — generate random cluster sizes and event schedules;
   after every step assert invariants §9.1–§9.5; shrink to a minimal failing schedule.

Rules:

- New logic in `src/core/` is not "done" until it has simulation coverage.
- Every bug fix adds a regression test — a pinned failing seed where applicable.
- Simulation tests must not use wall-clock time, real threads, real sockets, or unseeded
  randomness.
- Long property runs: `just proptest` (`PROPTEST_CASES=2048`).

---

## 11. Definition of Done

A change is done when **all applicable** items pass. `/definition-of-done` runs this list.

- [ ] `just check` is green (`fmt-check`, `clippy -D warnings`, `test`, `doc`).
- [ ] New/changed core logic has simulation coverage; pure helpers have unit tests.
- [ ] Bug fixes include a regression test (pinned seed where applicable).
- [ ] Invariants from §9 that the change touches are named in the PR with a one-line
      preservation argument.
- [ ] Public items have `///` docs; `Result`-returning fns document their errors.
- [ ] No new `unwrap`/`expect`/`panic!`/`todo!`/`unimplemented!` on a reachable non-test
      path without a `// PANIC:` justification.
- [ ] Durability-path changes re-checked against §8; fsync-fault simulation still passes.
- [ ] New identifiers use paper terminology (§3).
- [ ] If a module now meets the standards, its `#[allow(...)]` marker in `src/lib.rs` is
      removed.
- [ ] Module docs / README updated if behaviour or public API changed.

---

## 12. Commit & PR conventions

- **Commit subject**: `area: imperative summary` where `area` ∈
  `core`, `storage`, `transport`, `sim`, `node`, `docs`, `ci`, `deps`, `harness`.
  The body explains **why**, not what.
- **One logical change per commit.** Keep the tree green at every commit so history is
  bisectable.
- **PR description** covers: what & why, invariants touched (§9) with a preservation note,
  test evidence (seeds for simulation/proptest failures now covered), and any
  `// DEVIATION:` from the papers with rationale.
- `Cargo.lock` **is committed** (reproducible builds matter more here than the
  library-crate convention of omitting it). The repo `.gitignore` reflects this.

---

## 13. Tooling & commands

`just` is the single source of truth for checks (`justfile`). Install it with
`cargo install just` or your package manager.

| Command | Does |
| --- | --- |
| `just` | list recipes |
| `just check` | `fmt-check` + `clippy` + `test` + `doc` — the local gate |
| `just fmt` | format in place |
| `just clippy` | lint, warnings-as-errors |
| `just test` | full test suite |
| `just sim [args]` | simulation suite only |
| `just sim-seed <n>` | replay one simulation seed with output |
| `just proptest` | property tests with more cases |
| `just doc` | build docs as CI does |
| `just ci` | exactly what CI runs |
| `just fix` | apply clippy's safe suggestions |

CI (`.github/workflows/ci.yml`) runs `just ci`, plus a `filesystems` job that runs the
persistence tests against loopback ext4/xfs. CI is the enforcement backstop — it runs
regardless of editor or agent.

Optional local pre-commit hooks: `pipx install pre-commit && pre-commit install`
(config in `.pre-commit-config.yaml`; calls the same `just` recipes).

---

## 14. Keeping this tool-agnostic

- **`AGENTS.md` (this file) is canonical.** It is plain Markdown, readable by any agent or
  human.
- `CLAUDE.md` is a one-line pointer to this file. Other tools should point their rules file
  here too (`.cursor/rules/*.mdc`, `.github/copilot-instructions.md`; Codex reads
  `AGENTS.md` natively).
- All enforcement logic lives in tool-neutral files: `justfile`, `Cargo.toml [lints]`,
  `rustfmt.toml`, `rust-toolchain.toml`, `.pre-commit-config.yaml`,
  `.github/workflows/ci.yml`.
- `.claude/` holds Claude Code conveniences only, and nothing in it is authoritative:
  `settings.json` (a format-on-save hook, pre-approved `cargo`/`just` commands) and
  `commands/definition-of-done.md` (runs §11). Deleting `.claude/` changes nothing about
  the project's standards.
