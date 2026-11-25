//! Property tests for `RaftLog` (pure domain).

use cortex9_raft_log_store::domain::*;
use proptest::prelude::*;

fn build(terms: &[u64]) -> Vec<Entry> {
    terms
        .iter()
        .enumerate()
        .map(|(i, &t)| Entry {
            term: t,
            index: i as u64 + 1,
            kind: EntryKind::Command(vec![i as u8, t as u8]),
        })
        .collect()
}

/// Non-decreasing term sequences, as every valid Raft log has.
fn terms_strategy() -> impl Strategy<Value = Vec<u64>> {
    prop::collection::vec(0u64..3, 0..30).prop_map(|deltas| {
        let mut t = 1;
        deltas
            .into_iter()
            .map(|d| {
                t += d;
                t
            })
            .collect()
    })
}

fn log_with(entries: &[Entry]) -> RaftLog {
    let mut log = RaftLog::new();
    log.maybe_append(0, 0, entries).unwrap();
    log
}

proptest! {
    /// After a successful append the follower's log equals the leader's log
    /// up to the last appended index (Log Matching).
    #[test]
    fn append_makes_prefix_identical(
        f_terms in terms_strategy(),
        l_terms in terms_strategy(),
        cut in 0usize..30,
        len in 0usize..30,
    ) {
        let leader = build(&l_terms);
        let mut follower = log_with(&build(&f_terms));
        let prev = cut.min(leader.len());
        let prev_term = if prev == 0 { 0 } else { leader[prev - 1].term };
        let batch: Vec<Entry> = leader[prev..(prev + len).min(leader.len())].to_vec();
        match follower.maybe_append(prev as u64, prev_term, &batch).unwrap() {
            Some(last_new) => {
                prop_assert_eq!(last_new, (prev + batch.len()) as u64);
                for e in &leader[..last_new as usize] {
                    // Every index up to last_new matches the leader exactly
                    // when the prefix matched (the leader's prefix is a
                    // prefix of the follower's only if terms agreed).
                    if follower.term_at(e.index) == Some(e.term) {
                        prop_assert_eq!(&follower.entries()[(e.index - 1) as usize], e);
                    }
                }
                for e in &batch {
                    prop_assert_eq!(&follower.entries()[(e.index - 1) as usize], e);
                }
            }
            None => {
                prop_assert!(!follower.matches(prev as u64, prev_term));
            }
        }
    }

    /// Replaying the same AppendEntries is idempotent (duplicates are harmless).
    #[test]
    fn append_is_idempotent(terms in terms_strategy(), cut in 0usize..30) {
        let leader = build(&terms);
        let mut a = RaftLog::new();
        let prev = cut.min(leader.len());
        a.maybe_append(0, 0, &leader[..prev]).unwrap();
        let before = a.entries().to_vec();
        a.maybe_append(0, 0, &leader[..prev]).unwrap();
        prop_assert_eq!(a.entries(), &before[..]);
    }

    /// A shorter (stale, reordered) AppendEntries never truncates entries
    /// that already match.
    #[test]
    fn stale_append_does_not_truncate(terms in terms_strategy(), k in 0usize..30) {
        prop_assume!(!terms.is_empty());
        let leader = build(&terms);
        let mut log = log_with(&leader);
        let k = k.min(leader.len());
        log.maybe_append(0, 0, &leader[..k]).unwrap();
        prop_assert_eq!(log.last_index(), leader.len() as u64);
    }

    /// Committed entries can never be overwritten.
    #[test]
    fn committed_entries_are_immutable(terms in terms_strategy(), c in 1usize..30) {
        prop_assume!(!terms.is_empty());
        let entries = build(&terms);
        let mut log = log_with(&entries);
        let c = c.min(entries.len()) as u64;
        log.commit_to(c);
        let mut forged = entries[(c - 1) as usize].clone();
        forged.term += 100;
        let prev_term = log.term_at(c - 1).unwrap();
        let r = log.maybe_append(c - 1, prev_term, &[forged]);
        prop_assert!(matches!(r, Err(RaftError::CommittedConflict { .. })), "{:?}", r);
        prop_assert_eq!(log.entries(), &entries[..]);
    }

    /// Compaction preserves term_at for every retained index and the boundary.
    #[test]
    fn compaction_preserves_terms(terms in terms_strategy(), at in 0usize..30) {
        let entries = build(&terms);
        let mut log = log_with(&entries);
        log.commit_to(entries.len() as u64);
        log.take_committed();
        let at = at.min(entries.len()) as u64;
        log.compact(at).unwrap();
        prop_assert_eq!(log.snapshot_index(), at);
        for e in &entries {
            if e.index >= at {
                prop_assert_eq!(log.term_at(e.index), Some(e.term));
            } else {
                prop_assert_eq!(log.term_at(e.index), None);
            }
        }
    }

    /// The reject hint is strictly below prev_index (so the leader backs off)
    /// and never below commit.
    #[test]
    fn reject_hint_bounds(terms in terms_strategy(), prev in 1u64..40, c in 0u64..30) {
        let entries = build(&terms);
        let mut log = log_with(&entries);
        log.commit_to(c);
        prop_assume!(prev > log.commit());
        let h = log.reject_hint(prev);
        prop_assert!(h < prev);
        prop_assert!(h >= log.commit());
    }
}

#[test]
fn up_to_date_rule() {
    let log = log_with(&build(&[1, 1, 2]));
    assert!(log.is_up_to_date(3, 2));
    assert!(
        log.is_up_to_date(1, 3),
        "higher last term wins regardless of length"
    );
    assert!(
        !log.is_up_to_date(10, 1),
        "lower last term loses regardless of length"
    );
    assert!(!log.is_up_to_date(2, 2));
}

#[test]
fn unstable_tracks_truncation() {
    let mut log = log_with(&build(&[1, 1, 1, 1]));
    assert_eq!(log.take_unstable().len(), 4);
    assert!(log.take_unstable().is_empty());
    let replacement = Entry {
        term: 2,
        index: 3,
        kind: EntryKind::Noop,
    };
    log.maybe_append(2, 1, std::slice::from_ref(&replacement))
        .unwrap();
    assert_eq!(log.take_unstable(), vec![replacement]);
    assert_eq!(log.last_index(), 3);
}

#[test]
fn install_snapshot_retains_matching_suffix() {
    let mut log = log_with(&build(&[1, 1, 2, 2, 3]));
    let meta = SnapshotMeta {
        index: 3,
        term: 2,
        config: Configuration::new([1]),
    };
    assert!(log.install_snapshot(&meta));
    assert_eq!(log.first_index(), 4);
    assert_eq!(log.last_index(), 5);
    assert_eq!(log.commit(), 3);

    let mut other = log_with(&build(&[1, 1, 1]));
    let meta = SnapshotMeta {
        index: 3,
        term: 2,
        config: Configuration::new([1]),
    };
    assert!(!other.install_snapshot(&meta));
    assert_eq!(other.last_index(), 3);
    assert!(other.entries().is_empty());
}

#[test]
fn restore_rejects_gaps_and_drops_compacted_prefix() {
    let mut es = build(&[1, 1, 1, 1]);
    let hs = HardState {
        term: 1,
        voted_for: None,
        commit: 4,
    };
    let meta = SnapshotMeta {
        index: 2,
        term: 1,
        config: Configuration::new([1]),
    };
    let log = RaftLog::restore(Some(&meta), es.clone(), &hs).unwrap();
    assert_eq!(log.first_index(), 3);
    assert_eq!(log.commit(), 4);
    es.remove(2);
    assert!(RaftLog::restore(None, es, &hs).is_err());
}
