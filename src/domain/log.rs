//! In-memory view of the replicated log (snapshot prefix + entry suffix).
//!
//! `RaftLog` is pure: persistence happens through effects emitted by
//! `RaftCore`, which reads `take_unstable()` to know what must be written.

use super::error::RaftError;
use super::types::{Configuration, Entry, EntryKind, HardState, Index, SnapshotMeta, Term};

#[derive(Clone, Debug, Default)]
pub struct RaftLog {
    snapshot_index: Index,
    snapshot_term: Term,
    /// `entries[k].index == snapshot_index + 1 + k`.
    entries: Vec<Entry>,
    commit: Index,
    applied: Index,
    /// First index that has not yet been handed out for persistence.
    unstable_from: Index,
}

impl RaftLog {
    pub fn new() -> Self {
        Self {
            unstable_from: 1,
            ..Self::default()
        }
    }

    /// Rebuilds the log from durable state. Entries at or below the snapshot
    /// index are ignored (crash between snapshot save and log compaction).
    pub fn restore(
        snapshot: Option<&SnapshotMeta>,
        entries: Vec<Entry>,
        hs: &HardState,
    ) -> Result<Self, RaftError> {
        let (si, st) = snapshot.map(|m| (m.index, m.term)).unwrap_or((0, 0));
        let entries: Vec<Entry> = entries.into_iter().filter(|e| e.index > si).collect();
        for (k, e) in entries.iter().enumerate() {
            if e.index != si + 1 + k as u64 {
                return Err(RaftError::InvalidConfig("persisted log is not contiguous"));
            }
            if k > 0 && e.term < entries[k - 1].term {
                return Err(RaftError::InvalidConfig("persisted log terms decrease"));
            }
        }
        let last = si + entries.len() as u64;
        let commit = hs.commit.max(si).min(last);
        Ok(Self {
            snapshot_index: si,
            snapshot_term: st,
            entries,
            commit,
            applied: si,
            unstable_from: last + 1,
        })
    }

    pub fn snapshot_index(&self) -> Index {
        self.snapshot_index
    }
    pub fn snapshot_term(&self) -> Term {
        self.snapshot_term
    }
    pub fn first_index(&self) -> Index {
        self.snapshot_index + 1
    }
    pub fn last_index(&self) -> Index {
        self.snapshot_index + self.entries.len() as u64
    }
    pub fn commit(&self) -> Index {
        self.commit
    }
    pub fn applied(&self) -> Index {
        self.applied
    }
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub fn last_term(&self) -> Term {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.snapshot_term)
    }

    /// Term of the entry at `index`, if it is known (snapshot boundary or suffix).
    pub fn term_at(&self, index: Index) -> Option<Term> {
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        if index < self.snapshot_index || index > self.last_index() {
            return None;
        }
        Some(self.entries[(index - self.first_index()) as usize].term)
    }

    pub fn matches(&self, index: Index, term: Term) -> bool {
        self.term_at(index) == Some(term)
    }

    /// Raft §5.4.1 election restriction.
    pub fn is_up_to_date(&self, last_index: Index, last_term: Term) -> bool {
        last_term > self.last_term()
            || (last_term == self.last_term() && last_index >= self.last_index())
    }

    /// Leader-side append. Leaders never truncate (Leader Append-Only).
    pub fn append_local(&mut self, term: Term, kind: EntryKind) -> Index {
        let index = self.last_index() + 1;
        debug_assert!(term >= self.last_term());
        self.entries.push(Entry { term, index, kind });
        index
    }

    /// Follower-side append (AppendEntries receiver, Raft §5.3).
    ///
    /// Returns `Ok(None)` when `(prev_index, prev_term)` does not match,
    /// `Ok(Some(last_new_index))` on success. Never truncates committed
    /// entries: such a request is reported as `CommittedConflict`.
    pub fn maybe_append(
        &mut self,
        prev_index: Index,
        prev_term: Term,
        entries: &[Entry],
    ) -> Result<Option<Index>, RaftError> {
        if !self.matches(prev_index, prev_term) {
            return Ok(None);
        }
        for (k, e) in entries.iter().enumerate() {
            if e.index != prev_index + 1 + k as u64 {
                return Err(RaftError::InvalidConfig("non-contiguous AppendEntries"));
            }
        }
        let mut first_new = None;
        for (k, e) in entries.iter().enumerate() {
            if e.index <= self.snapshot_index {
                continue; // compacted => committed => identical by Log Matching
            }
            match self.term_at(e.index) {
                Some(t) if t == e.term => continue,
                Some(_) => {
                    if e.index <= self.commit {
                        return Err(RaftError::CommittedConflict {
                            index: e.index,
                            commit: self.commit,
                        });
                    }
                    self.entries
                        .truncate((e.index - self.first_index()) as usize);
                    first_new = Some(k);
                    break;
                }
                None => {
                    first_new = Some(k);
                    break;
                }
            }
        }
        if let Some(k) = first_new {
            let from = entries[k].index;
            self.entries.extend_from_slice(&entries[k..]);
            self.unstable_from = self.unstable_from.min(from);
        }
        Ok(Some(prev_index + entries.len() as u64))
    }

    /// Entries in `[lo, lo+max)` clipped to the log. Errors if `lo` is compacted.
    pub fn entries_from(&self, lo: Index, max: usize) -> Result<Vec<Entry>, RaftError> {
        if lo <= self.snapshot_index {
            return Err(RaftError::Unavailable { index: lo });
        }
        if lo > self.last_index() {
            return Ok(Vec::new());
        }
        let start = (lo - self.first_index()) as usize;
        let end = (start + max).min(self.entries.len());
        Ok(self.entries[start..end].to_vec())
    }

    /// Advances commit; never moves backwards and never past the last entry.
    pub fn commit_to(&mut self, index: Index) -> bool {
        let index = index.min(self.last_index());
        if index > self.commit {
            self.commit = index;
            true
        } else {
            false
        }
    }

    /// Committed entries not yet applied; marks them applied.
    pub fn take_committed(&mut self) -> Vec<Entry> {
        if self.applied >= self.commit {
            return Vec::new();
        }
        let lo = (self.applied + 1 - self.first_index()) as usize;
        let hi = (self.commit + 1 - self.first_index()) as usize;
        self.applied = self.commit;
        self.entries[lo..hi].to_vec()
    }

    /// Entries appended since the last call; they must be persisted with
    /// truncate-then-append semantics.
    pub fn take_unstable(&mut self) -> Vec<Entry> {
        let last = self.last_index();
        if self.unstable_from > last {
            self.unstable_from = last + 1;
            return Vec::new();
        }
        let lo = self.unstable_from.max(self.first_index());
        let out = self.entries[(lo - self.first_index()) as usize..].to_vec();
        self.unstable_from = last + 1;
        out
    }

    /// Discards entries up to and including `index` (already applied).
    pub fn compact(&mut self, index: Index) -> Result<Term, RaftError> {
        if index > self.applied {
            return Err(RaftError::CompactBeyondApplied {
                requested: index,
                applied: self.applied,
            });
        }
        if index <= self.snapshot_index {
            return Ok(self.snapshot_term);
        }
        let term = self
            .term_at(index)
            .ok_or(RaftError::Unavailable { index })?;
        let drop = (index - self.snapshot_index) as usize;
        self.entries.drain(..drop);
        self.snapshot_index = index;
        self.snapshot_term = term;
        Ok(term)
    }

    /// Installs a snapshot received from the leader. Returns `true` if the
    /// suffix after the snapshot was retained (it matched), `false` if the
    /// whole log was discarded.
    pub fn install_snapshot(&mut self, meta: &SnapshotMeta) -> bool {
        let retained = self.matches(meta.index, meta.term) && meta.index <= self.last_index();
        if retained {
            let drop = (meta.index - self.snapshot_index) as usize;
            self.entries.drain(..drop);
        } else {
            self.entries.clear();
        }
        self.snapshot_index = meta.index;
        self.snapshot_term = meta.term;
        self.commit = self.commit.max(meta.index);
        self.applied = self.applied.max(meta.index);
        self.unstable_from = self.unstable_from.max(meta.index + 1);
        if !retained {
            self.unstable_from = meta.index + 1;
        }
        retained
    }

    /// Latest configuration in the suffix at or below `index`, with its index.
    pub fn config_at(&self, index: Index) -> Option<(Index, &Configuration)> {
        self.entries.iter().rev().find_map(|e| match &e.kind {
            EntryKind::Config(c) if e.index <= index => Some((e.index, c)),
            _ => None,
        })
    }

    /// Latest configuration in the suffix, with its index.
    pub fn last_config(&self) -> Option<(Index, &Configuration)> {
        self.config_at(Index::MAX)
    }

    /// For a rejected AppendEntries: a hint that skips the whole conflicting
    /// term at `prev_index`, never below `commit`.
    pub fn reject_hint(&self, prev_index: Index) -> Index {
        if prev_index > self.last_index() {
            return self.last_index();
        }
        let Some(t) = self.term_at(prev_index) else {
            return self.commit;
        };
        let mut i = prev_index;
        while i > self.commit && i > self.first_index() && self.term_at(i - 1) == Some(t) {
            i -= 1;
        }
        i.saturating_sub(1).max(self.commit)
    }
}
