//! File adapter durability tests, including a full node restart.

use cortex9_raft_log_store::adapters::clock::ManualClock;
use cortex9_raft_log_store::adapters::file::{FileLogStore, FileSnapshotStore};
use cortex9_raft_log_store::adapters::rand::SeededRand;
use cortex9_raft_log_store::application::{RaftNode, Ticker};
use cortex9_raft_log_store::domain::*;
use cortex9_raft_log_store::ports::{LogStore, SnapshotStore, StateMachine};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

static N: AtomicU32 = AtomicU32::new(0);

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "cx9-raft-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Self(p)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn e(term: u64, index: u64) -> Entry {
    Entry {
        term,
        index,
        kind: EntryKind::Command(format!("{term}/{index}").into_bytes()),
    }
}

#[test]
fn append_truncate_compact_and_reload() {
    let d = TempDir::new();
    let mut s = FileLogStore::open(&d.0).unwrap();
    assert_eq!(s.load().unwrap().entries, vec![]);
    s.append(&[e(1, 1), e(1, 2), e(1, 3)]).unwrap();
    s.append(&[e(2, 3), e(2, 4)]).unwrap(); // truncate-then-append
    s.save_hard_state(&HardState {
        term: 2,
        voted_for: Some(7),
        commit: 2,
    })
    .unwrap();
    drop(s);
    let mut s = FileLogStore::open(&d.0).unwrap();
    let p = s.load().unwrap();
    assert_eq!(p.entries, vec![e(1, 1), e(1, 2), e(2, 3), e(2, 4)]);
    assert_eq!(
        p.hard_state,
        HardState {
            term: 2,
            voted_for: Some(7),
            commit: 2
        }
    );
    s.compact(2).unwrap();
    s.truncate_from(4).unwrap();
    drop(s);
    let mut s = FileLogStore::open(&d.0).unwrap();
    assert_eq!(s.load().unwrap().entries, vec![e(2, 3)]);
}

#[test]
fn torn_tail_is_discarded_on_load() {
    let d = TempDir::new();
    let mut s = FileLogStore::open(&d.0).unwrap();
    s.append(&[e(1, 1), e(1, 2)]).unwrap();
    drop(s);
    // Simulate a crash mid-write: half a record at the end.
    let mut f = OpenOptions::new()
        .append(true)
        .open(d.0.join("log.wal"))
        .unwrap();
    f.write_all(&[42, 0, 0, 0, 1, 2, 3]).unwrap();
    drop(f);
    let mut s = FileLogStore::open(&d.0).unwrap();
    assert_eq!(s.load().unwrap().entries, vec![e(1, 1), e(1, 2)]);
    // And the file was repaired, so appending continues cleanly.
    s.append(&[e(1, 3)]).unwrap();
    drop(s);
    let mut s = FileLogStore::open(&d.0).unwrap();
    assert_eq!(s.load().unwrap().entries.len(), 3);
}

#[test]
fn corrupt_hard_state_is_an_error_not_a_default() {
    let d = TempDir::new();
    let mut s = FileLogStore::open(&d.0).unwrap();
    s.save_hard_state(&HardState {
        term: 3,
        voted_for: None,
        commit: 0,
    })
    .unwrap();
    let path = d.0.join("hardstate.bin");
    let mut bytes = std::fs::read(&path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::write(&path, bytes).unwrap();
    assert!(
        s.load().is_err(),
        "a corrupted vote must never silently reset to 'not voted'"
    );
}

#[test]
fn snapshot_store_roundtrip() {
    let d = TempDir::new();
    let mut s = FileSnapshotStore::open(&d.0).unwrap();
    assert_eq!(s.load().unwrap(), None);
    let snap = Snapshot {
        meta: SnapshotMeta {
            index: 9,
            term: 4,
            config: Configuration::new([1, 2, 3]),
        },
        data: b"kv-state".to_vec(),
    };
    s.save(&snap).unwrap();
    assert_eq!(
        FileSnapshotStore::open(&d.0).unwrap().load().unwrap(),
        Some(snap)
    );
}

#[derive(Default)]
struct Counter {
    sum: u64,
    applied: Vec<Index>,
}
impl StateMachine for Counter {
    fn apply(&mut self, index: Index, c: &[u8]) {
        self.sum += c[0] as u64;
        self.applied.push(index);
    }
    fn snapshot(&self) -> Vec<u8> {
        self.sum.to_le_bytes().to_vec()
    }
    fn restore(&mut self, d: &[u8]) {
        self.sum = u64::from_le_bytes(d.try_into().unwrap());
    }
}

#[test]
fn single_node_restart_recovers_state_with_snapshots() {
    let d = TempDir::new();
    let open = || {
        RaftNode::open(
            RaftConfig::new(1),
            SeededRand::new(3),
            Configuration::new([1]),
            FileLogStore::open(&d.0).unwrap(),
            FileSnapshotStore::open(&d.0).unwrap(),
            Counter::default(),
            4, // snapshot every 4 applied entries
        )
        .unwrap()
    };
    let mut n = open();
    let clock = ManualClock::default();
    let mut ticker = Ticker::new(&clock, 10);
    while n.core().role() != Role::Leader {
        clock.advance(10);
        for _ in 0..ticker.due() {
            n.tick().unwrap();
        }
    }
    for i in 1..=10u8 {
        n.propose(vec![i]).unwrap();
    }
    assert_eq!(n.state_machine().sum, 55);
    assert!(n.core().log().snapshot_index() >= 8);
    let term = n.core().term();
    drop(n);

    let n = open();
    assert_eq!(n.core().term(), term);
    assert_eq!(
        n.state_machine().sum,
        55,
        "snapshot restore + replay of committed suffix"
    );
    assert_eq!(n.core().log().last_index(), 11); // noop + 10 commands
}

#[test]
fn ticker_converts_time_to_ticks() {
    let clock = ManualClock::default();
    let mut t = Ticker::new(&clock, 50);
    clock.advance(49);
    assert_eq!(t.due(), 0);
    clock.advance(1);
    assert_eq!(t.due(), 1);
    clock.advance(175);
    assert_eq!(t.due(), 3);
    clock.advance(25);
    assert_eq!(t.due(), 1);
}
