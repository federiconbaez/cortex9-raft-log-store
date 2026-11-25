# cortex9-raft-log-store

Raft consensus core written as a **pure, deterministic state machine** in Rust,
with durable log/snapshot storage behind ports. No async runtime, no external
runtime dependencies (only `proptest` for tests).

> Ref: CX9-6198 · License: Apache-2.0 · Contract: [`CONTRACT.md`](CONTRACT.md)

## What it implements

- Leader election with randomized timeouts and the §5.4.1 up-to-date rule.
- Log replication (`AppendEntries`) with conflict truncation, fast back-off
  hints that skip a whole conflicting term, and the §5.4.2 current-term commit rule.
- Log compaction and `InstallSnapshot` (retains a matching suffix, discards a
  conflicting log).
- Single-server membership change (Raft thesis §4.1): config effective on
  append, rolled back on truncation, one change in flight, removed leader steps
  down after commit.
- Durable `HardState` (term, vote, commit) persisted **before** any message
  that depends on it leaves the node.
- File adapter: CRC32-framed WAL with `fsync`, atomic rewrite on
  truncate/compact (`tmp` + `fsync` + `rename` + dir `fsync`), torn-tail repair,
  corrupted vote file treated as an error rather than "not voted".

## Architecture

```
src/
├── domain/          # pure: RaftCore (State + Command patterns), RaftLog, messages, types
├── ports.rs         # LogStore, SnapshotStore (Repository), StateMachine, Clock, Rand
├── application/     # RaftNode executes Effects in safe order; Ticker maps Clock -> ticks
└── adapters/        # memory, file (WAL + snapshots), codec+CRC32, SeededRand/OsRand, clocks
```

The domain never performs I/O. Every input (`tick`, `step`, `propose`,
`propose_conf_change`, `compact`) produces `Effect`s that the application
executes: persistence first, then sends, then snapshot restore and apply.
`scripts/check-architecture.sh` enforces that `src/domain` imports no
`std::fs/net/io/time/thread`, no async runtime and no outer layer.

| Pattern | Where |
|---------|-------|
| State Machine | `Role` transitions in `RaftCore` (`become_follower/campaign/become_leader`) |
| Command | `Effect` values emitted by the core, executed by `RaftNode::drive` |
| Repository | `LogStore`, `SnapshotStore` ports with memory and file adapters |
| Strategy | `Rand`/`Clock` ports (seeded vs OS) |

## Usage

```rust
use cortex9_raft_log_store::{adapters::{file::*, rand::OsRand}, application::RaftNode, domain::*};

let mut node = RaftNode::open(
    RaftConfig::new(1), OsRand::default(), Configuration::new([1, 2, 3]),
    FileLogStore::open("data/1")?, FileSnapshotStore::open("data/1")?,
    my_state_machine, /* snapshot every */ 1_000,
)?;
let outbound: Vec<Message> = node.tick()?;          // drive from your timer
let outbound = node.step(incoming_message)?;        // feed RPCs from your transport
let (index, outbound) = node.propose(b"cmd".to_vec())?;
```

You provide the transport: deliver each returned `Message` to `message.to`.

## Verification

```
./scripts/check-architecture.sh
cargo fmt --check && cargo clippy --all-targets -- -D warnings
cargo test                                         # 37 tests
PROPTEST_CASES=1000 cargo test --release --test simulation
```

`tests/simulation.rs` runs a deterministic 5-node cluster with message loss,
duplication, reordering and delay, network partitions, crashes and restarts
from durable stores, compaction/snapshot transfer and membership changes, and
checks on **every step**: Election Safety, Log Matching, Leader Completeness,
committed-entry immutability and State Machine Safety, plus liveness after the
network heals. Locally it has been run with 3 000 seeds without a violation.
While building it, the simulator caught a false positive in my own checker
(stale partitioned leaders legitimately lack later commits) and a harness bug
(proposals to a stale leader are lost and must be retried) — both fixed in the
tests, not by weakening the core.

## Honest limits (out of scope)

- No network transport or RPC framing; you move `Message`s.
- No Pre-Vote / CheckQuorum / leases / leadership transfer / ReadIndex. A
  partitioned node can bump terms and disrupt a healthy leader on rejoin
  (safe, but costs an election).
- Single-server changes only; no joint consensus, no learners.
- Snapshots travel in one message (no chunking/streaming).
- Configuration lookup scans the in-memory log suffix (O(n) per mutation);
  fine for compacted logs, not tuned for huge suffixes.
- File WAL rewrites the whole file on truncate/compact; no segmenting.
- Not audited or benchmarked for production use.
