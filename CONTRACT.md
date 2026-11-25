# CONTRACT — cortex9-raft-log-store

Ref: CX9-6198

## Problem

Replicated state machines need a log that every replica agrees on even when
messages are lost, duplicated, reordered or delayed and when nodes crash and
restart. This crate implements the Raft consensus core (leader election, log
replication, commitment, snapshot installation and single-server membership
change) as a **pure, deterministic state machine**, plus durable storage
adapters for its log and snapshots.

## Architecture (hexagonal)

| Layer | Path | May depend on |
|-------|------|---------------|
| Domain | `src/domain/` | `core`/`alloc` collections only. No `std::fs`, `std::net`, `std::io`, `std::time`, threads, async runtimes. |
| Ports | `src/ports.rs` | Domain types. Traits only. |
| Application | `src/application/` | Domain + ports. Executes the effects emitted by the domain in the order required by Raft. |
| Adapters | `src/adapters/` | Ports + std (files, OS clock). |

`scripts/check-architecture.sh` fails CI if the domain imports I/O, time,
threads or adapters.

## Ports

- `LogStore`: load persisted `HardState` + entries; `append(entries)` with
  *truncate-then-append* semantics (an entry at index *i* replaces every stored
  entry with index ≥ *i*); `save_hard_state`; `compact(index)`.
- `SnapshotStore`: `save(&Snapshot)` atomically, `load() -> Option<Snapshot>`.
- `StateMachine`: `apply(index, &[u8])`, `snapshot() -> Vec<u8>`, `restore(&[u8])`.
- `Clock`: monotonic milliseconds; the application turns elapsed time into ticks.
- `Rand`: `next_u64()`; used only for randomized election timeouts.

## Domain behaviour

The domain (`RaftCore`) consumes inputs (`tick`, `step(Message)`,
`propose`, `propose_conf_change`, `compact`) and accumulates **effects**
(Command pattern) that the application drains with `take_effects()`:

1. `PersistHardState`, `PersistSnapshot`, `PersistEntries` — always emitted
   before any `Send` produced by the same input, so a vote or an
   acknowledgement is never sent before it is durable.
2. `Send(Message)`.
3. `Apply(entries)` / `RestoreSnapshot(meta)` — only committed data.

Roles are a State Machine (`Follower`, `Candidate`, `Leader`).

## Invariants (tested)

- **Election Safety**: at most one leader per term (simulation, property tests).
- **Log Matching**: if two logs contain an entry with the same index and term,
  the logs are identical up to that index (simulation, property tests on
  `RaftLog::maybe_append`).
- **Leader Append-Only**: a leader never truncates its own log.
- **State Machine Safety**: every node applies the same command at a given
  index; applied sequences are prefixes of one another (simulation).
- **Commit monotonicity**: `commit` never decreases and never truncates a
  committed entry (a conflicting append below `commit` is rejected as
  `RaftError::CommittedConflict`).
- **Current-term commit rule**: a leader only advances `commit` by counting
  replicas for an entry of its current term (Raft §5.4.2).
- **Single pending membership change**: a new configuration change is refused
  while a previous one is uncommitted; a configuration takes effect as soon as
  it is appended (Raft thesis §4.1) and is rolled back if truncated.
- **Durability**: after restart from `LogStore` + `SnapshotStore` a node
  recovers term, vote, commit, snapshot and log suffix. A torn tail record in
  the file log is discarded (CRC32) instead of being trusted.

## Acceptance criteria

- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`,
  `cargo test` green in CI.
- Deterministic cluster simulation (5 nodes, random drops, duplication,
  reordering, partitions, crashes/restarts, compaction, membership change)
  holds every invariant above for every proptest seed.
- File adapters survive restart and torn-write recovery tests.
- Architecture check passes.

## Out of scope

- Networking/RPC transport (the caller moves `Message`s).
- Pre-vote, CheckQuorum, leader leases, leadership transfer, read index.
- Joint consensus (only single-server changes) and learners.
- Chunked/streamed snapshot transfer (snapshots travel in one message).
- Log file garbage collection beyond rewrite-on-compact.
- Byzantine faults.
