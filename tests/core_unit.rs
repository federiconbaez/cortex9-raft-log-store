//! Scenario tests for `RaftCore` driven message by message.

use cortex9_raft_log_store::adapters::rand::SeededRand;
use cortex9_raft_log_store::domain::*;

fn node(id: NodeId, voters: &[NodeId]) -> RaftCore<SeededRand> {
    RaftCore::new(
        RaftConfig::new(id),
        SeededRand::new(id),
        Configuration::new(voters.iter().copied()),
        HardState::default(),
        None,
        Vec::new(),
    )
    .unwrap()
}

fn sends(effects: &[Effect]) -> Vec<Message> {
    effects
        .iter()
        .filter_map(|e| match e {
            Effect::Send(m) => Some(m.clone()),
            _ => None,
        })
        .collect()
}

fn tick_until_campaign(n: &mut RaftCore<SeededRand>) {
    for _ in 0..100 {
        n.tick();
        if n.role() != Role::Follower {
            return;
        }
    }
    panic!("never campaigned");
}

#[test]
fn invalid_config_is_rejected() {
    let mut cfg = RaftConfig::new(1);
    cfg.heartbeat_tick = 10;
    let r = RaftCore::new(
        cfg,
        SeededRand::new(1),
        Configuration::new([1]),
        HardState::default(),
        None,
        vec![],
    );
    assert!(matches!(r, Err(RaftError::InvalidConfig(_))));
}

#[test]
fn single_node_elects_itself_and_commits() {
    let mut n = node(1, &[1]);
    tick_until_campaign(&mut n);
    assert_eq!(n.role(), Role::Leader);
    let idx = n.propose(b"x".to_vec()).unwrap();
    assert_eq!(n.log().commit(), idx);
    let eff = n.take_effects();
    // Persist before apply.
    let persist_pos = eff
        .iter()
        .position(|e| matches!(e, Effect::PersistEntries(_)))
        .unwrap();
    let apply_pos = eff
        .iter()
        .position(|e| matches!(e, Effect::Apply(_)))
        .unwrap();
    assert!(persist_pos < apply_pos);
}

#[test]
fn followers_reject_proposals_with_leader_hint() {
    let mut n = node(2, &[1, 2, 3]);
    assert_eq!(
        n.propose(vec![1]),
        Err(RaftError::NotLeader { leader: None })
    );
}

#[test]
fn three_node_election_and_replication() {
    let mut a = node(1, &[1, 2, 3]);
    let mut b = node(2, &[1, 2, 3]);
    let mut c = node(3, &[1, 2, 3]);
    tick_until_campaign(&mut a);
    assert_eq!(a.role(), Role::Candidate);
    let eff = a.take_effects();
    // Vote must be durable before RequestVote leaves.
    let hs = eff
        .iter()
        .position(|e| matches!(e, Effect::PersistHardState(_)))
        .unwrap();
    let first_send = eff
        .iter()
        .position(|e| matches!(e, Effect::Send(_)))
        .unwrap();
    assert!(hs < first_send);
    for m in sends(&eff) {
        let to = if m.to == 2 { &mut b } else { &mut c };
        to.step(m).unwrap();
        for r in sends(&to.take_effects()) {
            a.step(r).unwrap();
        }
    }
    assert_eq!(a.role(), Role::Leader);
    let idx = a.propose(b"hello".to_vec()).unwrap();
    // Replicate until quiescent.
    let mut pending = sends(&a.take_effects());
    while let Some(m) = pending.pop() {
        let target = match m.to {
            1 => &mut a,
            2 => &mut b,
            _ => &mut c,
        };
        target.step(m).unwrap();
        pending.extend(sends(&target.take_effects()));
    }
    assert!(a.log().commit() >= idx);
    // Heartbeat propagates the commit index.
    for _ in 0..3 {
        a.tick();
    }
    let mut pending = sends(&a.take_effects());
    while let Some(m) = pending.pop() {
        let target = match m.to {
            1 => &mut a,
            2 => &mut b,
            _ => &mut c,
        };
        target.step(m).unwrap();
        pending.extend(sends(&target.take_effects()));
    }
    assert_eq!(b.log().commit(), a.log().commit());
    assert_eq!(c.log().commit(), a.log().commit());
}

#[test]
fn vote_denied_to_stale_log_and_granted_once_per_term() {
    let mut f = node(1, &[1, 2, 3]);
    // Give the follower an entry of term 2.
    f.step(Message {
        from: 2,
        to: 1,
        term: 2,
        body: Body::AppendEntries {
            prev_index: 0,
            prev_term: 0,
            entries: vec![Entry {
                term: 2,
                index: 1,
                kind: EntryKind::Noop,
            }],
            leader_commit: 0,
        },
    })
    .unwrap();
    f.take_effects();
    // Candidate 3 with an empty log in term 3: denied.
    f.step(Message {
        from: 3,
        to: 1,
        term: 3,
        body: Body::RequestVote {
            last_index: 0,
            last_term: 0,
        },
    })
    .unwrap();
    let r = sends(&f.take_effects());
    assert_eq!(r[0].body, Body::RequestVoteResp { granted: false });
    // Candidate 2 up to date in term 3: granted.
    f.step(Message {
        from: 2,
        to: 1,
        term: 3,
        body: Body::RequestVote {
            last_index: 1,
            last_term: 2,
        },
    })
    .unwrap();
    assert_eq!(
        sends(&f.take_effects())[0].body,
        Body::RequestVoteResp { granted: true }
    );
    // Another candidate in the same term: denied (one vote per term).
    f.step(Message {
        from: 3,
        to: 1,
        term: 3,
        body: Body::RequestVote {
            last_index: 5,
            last_term: 2,
        },
    })
    .unwrap();
    assert_eq!(
        sends(&f.take_effects())[0].body,
        Body::RequestVoteResp { granted: false }
    );
    assert_eq!(f.hard_state().voted_for, Some(2));
}

#[test]
fn stale_term_messages_are_answered_with_current_term() {
    let mut f = node(1, &[1, 2, 3]);
    f.step(Message {
        from: 2,
        to: 1,
        term: 5,
        body: Body::RequestVote {
            last_index: 0,
            last_term: 0,
        },
    })
    .unwrap();
    f.take_effects();
    f.step(Message {
        from: 3,
        to: 1,
        term: 4,
        body: Body::AppendEntries {
            prev_index: 0,
            prev_term: 0,
            entries: vec![],
            leader_commit: 0,
        },
    })
    .unwrap();
    let r = sends(&f.take_effects());
    assert_eq!(r[0].term, 5);
    assert_eq!(
        r[0].body,
        Body::AppendEntriesResp {
            success: false,
            index: 0
        }
    );
}

#[test]
fn leader_steps_down_on_higher_term() {
    let mut a = node(1, &[1]);
    tick_until_campaign(&mut a);
    assert_eq!(a.role(), Role::Leader);
    a.step(Message {
        from: 2,
        to: 1,
        term: 99,
        body: Body::AppendEntriesResp {
            success: false,
            index: 0,
        },
    })
    .unwrap();
    assert_eq!(a.role(), Role::Follower);
    assert_eq!(a.term(), 99);
}

#[test]
fn only_one_conf_change_in_flight() {
    let mut a = node(1, &[1, 2, 3]);
    // Win the election directly.
    tick_until_campaign(&mut a);
    let t = a.term();
    a.step(Message {
        from: 2,
        to: 1,
        term: t,
        body: Body::RequestVoteResp { granted: true },
    })
    .unwrap();
    assert_eq!(a.role(), Role::Leader);
    a.propose_conf_change(ConfChange::AddVoter(4)).unwrap();
    assert!(a.config().contains(4), "config takes effect on append");
    assert!(matches!(
        a.propose_conf_change(ConfChange::RemoveVoter(3)),
        Err(RaftError::ConfChangePending { .. })
    ));
    assert_eq!(
        a.propose_conf_change(ConfChange::AddVoter(4)).unwrap_err(),
        RaftError::ConfChangePending { index: 2 }
    );
}

#[test]
fn invalid_conf_changes_are_rejected() {
    let mut a = node(1, &[1]);
    tick_until_campaign(&mut a);
    assert_eq!(
        a.propose_conf_change(ConfChange::AddVoter(1)),
        Err(RaftError::InvalidConfChange)
    );
    assert_eq!(
        a.propose_conf_change(ConfChange::RemoveVoter(1)),
        Err(RaftError::InvalidConfChange)
    );
    assert_eq!(
        a.propose_conf_change(ConfChange::AddVoter(0)),
        Err(RaftError::InvalidConfChange)
    );
}

#[test]
fn truncated_config_entry_is_rolled_back() {
    let mut f = node(1, &[1, 2, 3]);
    let cfg4 = Configuration::new([1, 2, 3, 4]);
    f.step(Message {
        from: 2,
        to: 1,
        term: 2,
        body: Body::AppendEntries {
            prev_index: 0,
            prev_term: 0,
            entries: vec![Entry {
                term: 2,
                index: 1,
                kind: EntryKind::Config(cfg4.clone()),
            }],
            leader_commit: 0,
        },
    })
    .unwrap();
    assert_eq!(f.config(), &cfg4);
    // New leader of term 3 overwrites index 1.
    f.step(Message {
        from: 3,
        to: 1,
        term: 3,
        body: Body::AppendEntries {
            prev_index: 0,
            prev_term: 0,
            entries: vec![Entry {
                term: 3,
                index: 1,
                kind: EntryKind::Noop,
            }],
            leader_commit: 0,
        },
    })
    .unwrap();
    assert_eq!(f.config(), &Configuration::new([1, 2, 3]));
}

#[test]
fn leader_does_not_commit_previous_term_entries_by_counting() {
    // Raft §5.4.2 / Figure 8: entry from term 1 replicated on a majority
    // must not be committed by a term-3 leader until a term-3 entry is.
    let entries = vec![Entry {
        term: 1,
        index: 1,
        kind: EntryKind::Noop,
    }];
    let mut a = RaftCore::new(
        RaftConfig::new(1),
        SeededRand::new(1),
        Configuration::new([1, 2, 3]),
        HardState {
            term: 2,
            voted_for: None,
            commit: 0,
        },
        None,
        entries,
    )
    .unwrap();
    tick_until_campaign(&mut a);
    let t = a.term();
    a.step(Message {
        from: 2,
        to: 1,
        term: t,
        body: Body::RequestVoteResp { granted: true },
    })
    .unwrap();
    assert_eq!(a.role(), Role::Leader);
    // Peer 2 acknowledges only index 1 (the old-term entry).
    a.step(Message {
        from: 2,
        to: 1,
        term: t,
        body: Body::AppendEntriesResp {
            success: true,
            index: 1,
        },
    })
    .unwrap();
    assert_eq!(
        a.log().commit(),
        0,
        "must not commit a previous-term entry by counting"
    );
    a.step(Message {
        from: 2,
        to: 1,
        term: t,
        body: Body::AppendEntriesResp {
            success: true,
            index: 2,
        },
    })
    .unwrap();
    assert_eq!(a.log().commit(), 2);
}

#[test]
fn removed_leader_steps_down_after_commit() {
    let mut a = node(1, &[1, 2]);
    tick_until_campaign(&mut a);
    let t = a.term();
    a.step(Message {
        from: 2,
        to: 1,
        term: t,
        body: Body::RequestVoteResp { granted: true },
    })
    .unwrap();
    assert_eq!(a.role(), Role::Leader);
    let idx = a.propose_conf_change(ConfChange::RemoveVoter(1)).unwrap();
    assert_eq!(
        a.role(),
        Role::Leader,
        "still leads until the change commits"
    );
    a.step(Message {
        from: 2,
        to: 1,
        term: t,
        body: Body::AppendEntriesResp {
            success: true,
            index: idx,
        },
    })
    .unwrap();
    assert!(a.log().commit() >= idx);
    assert_eq!(a.role(), Role::Follower);
    // A removed node never campaigns.
    for _ in 0..200 {
        a.tick();
    }
    assert_eq!(a.role(), Role::Follower);
}

#[test]
fn compaction_requires_applied_and_triggers_snapshot_sends() {
    let mut a = node(1, &[1, 2, 3]);
    tick_until_campaign(&mut a);
    let t = a.term();
    a.step(Message {
        from: 2,
        to: 1,
        term: t,
        body: Body::RequestVoteResp { granted: true },
    })
    .unwrap();
    for i in 0..5u8 {
        a.propose(vec![i]).unwrap();
    }
    a.take_effects();
    assert!(matches!(
        a.compact(3, vec![]),
        Err(RaftError::CompactBeyondApplied { .. })
    ));
    // Commit everything via peer 2's ack, then apply.
    let last = a.log().last_index();
    a.step(Message {
        from: 2,
        to: 1,
        term: t,
        body: Body::AppendEntriesResp {
            success: true,
            index: last,
        },
    })
    .unwrap();
    a.take_effects();
    a.compact(last, b"state".to_vec()).unwrap();
    let eff = a.take_effects();
    assert!(matches!(
        eff[0],
        Effect::PersistSnapshot {
            retain_log_suffix: true,
            ..
        }
    ));
    // Peer 3 never acknowledged anything: it now needs a snapshot.
    a.step(Message {
        from: 3,
        to: 1,
        term: t,
        body: Body::AppendEntriesResp {
            success: false,
            index: 0,
        },
    })
    .unwrap();
    let s = sends(&a.take_effects());
    assert!(
        matches!(&s[0].body, Body::InstallSnapshot { snapshot } if snapshot.meta.index == last && snapshot.data == b"state")
    );
}
