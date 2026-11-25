//! Randomized deterministic simulation of a 5-node cluster with message loss,
//! duplication, reordering, delays, partitions, crashes/restarts, log
//! compaction + snapshot installation and membership changes. Every step
//! checks Election Safety, Log Matching, Leader Completeness and State
//! Machine Safety (see `common::Sim::check_invariants`).

mod common;

use common::{Faults, Sim};
use cortex9_raft_log_store::domain::{ConfChange, Role};
use proptest::prelude::*;

fn run(seed: u64, steps: u64) -> Sim {
    let mut sim = Sim::new(5, seed, Faults::default());
    for t in 0..steps {
        sim.step(true);
        if t % 3 == 0 {
            sim.propose();
        }
    }
    // Liveness after healing: a leader emerges and a new command reaches
    // every live voter's state machine.
    sim.heal();
    let mut proposed: Vec<Vec<u8>> = Vec::new();
    for t in 0..2_000 {
        sim.step(false);
        if t % 40 == 0 {
            if let Some((_, cmd)) = sim.propose_cmd() {
                proposed.push(cmd);
            }
        }
        if proposed.iter().any(|c| sim.applied_everywhere(c)) {
            return sim;
        }
    }
    panic!(
        "cluster did not converge after healing (seed {seed}): {:?}",
        sim.stats
    );
}

proptest! {
    // 256 cases by default; raise with PROPTEST_CASES for soak runs.
    #[test]
    fn invariants_hold_under_faults(seed in any::<u64>()) {
        let sim = run(seed, 400);
        prop_assert!(sim.stats.max_commit > 0);
    }
}

#[test]
fn many_fixed_seeds_make_progress_and_exercise_snapshots() {
    let mut installed = 0;
    let mut commits = 0;
    for seed in 1..=24u64 {
        let sim = run(seed * 7919, 600);
        installed += sim.stats.snapshots_installed;
        commits += sim.stats.max_commit;
    }
    assert!(commits > 24 * 20, "too little progress: {commits}");
    assert!(installed > 0, "fault mix never exercised InstallSnapshot");
}

#[test]
fn membership_remove_and_readd_keeps_invariants() {
    for seed in 1..=12u64 {
        let mut sim = Sim::new(
            5,
            seed,
            Faults {
                crash_pct: 0,
                partition_pct: 0,
                ..Faults::default()
            },
        );
        // Wait for a leader.
        while sim.leader().is_none() {
            sim.step(false);
        }
        let leader = sim.leader().unwrap();
        // Remove a follower (or the leader itself on odd seeds).
        let victim = if seed % 2 == 1 {
            leader
        } else {
            *sim.ids.iter().find(|&&i| i != leader).unwrap()
        };
        let mut idx = None;
        for _ in 0..500 {
            sim.step(false);
            if idx.is_none() {
                idx = sim.conf_change(ConfChange::RemoveVoter(victim));
            }
            if let Some(i) = idx {
                let committed_everywhere = sim
                    .live()
                    .filter(|(id, _)| **id != victim)
                    .all(|(_, n)| n.core().log().commit() >= i);
                if committed_everywhere && sim.leader().is_some_and(|l| l != victim) {
                    break;
                }
            }
            if t_propose(seed) {
                sim.propose();
            }
        }
        let i = idx.expect("conf change accepted");
        // Decommission the removed node.
        sim.isolated.insert(victim);
        for _ in 0..300 {
            sim.step(false);
            sim.propose();
        }
        let l = sim.leader().expect("4-node cluster has a leader");
        assert_ne!(l, victim);
        assert!(!sim.nodes[&l]
            .as_ref()
            .unwrap()
            .core()
            .config()
            .contains(victim));
        assert!(sim.nodes[&l].as_ref().unwrap().core().log().commit() > i);

        // Re-add it.
        let mut idx2 = None;
        for _ in 0..500 {
            sim.step(false);
            if idx2.is_none() {
                idx2 = sim.conf_change(ConfChange::AddVoter(victim));
            }
            if idx2.is_some() {
                break;
            }
        }
        sim.isolated.clear();
        let mut ok = false;
        for _ in 0..3000 {
            sim.step(false);
            let target = sim
                .leader()
                .and_then(|l| sim.nodes[&l].as_ref().map(|n| n.core().log().commit()));
            if let (Some(t), Some(Some(n))) = (target, sim.nodes.get(&victim)) {
                if n.core().config().contains(victim)
                    && n.core().log().commit() >= t
                    && t > idx2.unwrap()
                {
                    ok = true;
                    break;
                }
            }
            sim.propose();
        }
        assert!(ok, "re-added node never caught up (seed {seed})");
        assert!(
            sim.live()
                .filter(|(_, n)| n.core().role() == Role::Leader)
                .count()
                >= 1
        );
    }
}

fn t_propose(seed: u64) -> bool {
    seed % 3 != 0
}
