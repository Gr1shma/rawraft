use crate::{Entry, Index, Term};

/// The replicated log, held in memory.
///
/// Entries are numbered from `1`. Index `0` is a sentinel for "before the
/// first entry": it always exists and has term `0`.
#[derive(Clone, Debug, Default)]
pub struct RaftLog {
    entries: Vec<Entry>,
}

impl RaftLog {
    /// Creates an empty log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the index of the last entry, or `0` if the log is empty.
    #[must_use]
    pub fn last_index(&self) -> Index {
        self.entries.len() as Index
    }

    /// Returns the term of the last entry, or `0` if the log is empty.
    #[must_use]
    pub fn last_term(&self) -> Term {
        self.entries.last().map_or(0, |entry| entry.term)
    }

    /// Returns the term of the entry at `index`.
    ///
    /// Index `0` returns `Some(0)`. An index past the end returns `None`.
    #[must_use]
    pub fn term_at(&self, index: Index) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        self.entry(index).map(|entry| entry.term)
    }

    /// Returns the entry at `index`, or `None` if there is none.
    ///
    /// Index `0` has no entry.
    #[must_use]
    pub fn entry(&self, index: Index) -> Option<&Entry> {
        let position = usize::try_from(index.checked_sub(1)?).ok()?;
        self.entries.get(position)
    }

    /// Returns all entries with an index of at least `index`.
    ///
    /// Used by a leader to build `AppendEntries` for a follower.
    #[must_use]
    pub fn entries_from(&self, index: Index) -> &[Entry] {
        let start = usize::try_from(index.saturating_sub(1)).unwrap_or(usize::MAX);
        self.entries.get(start..).unwrap_or(&[])
    }

    /// Returns `true` if the log has an entry at `index` with term `term`.
    ///
    /// This is the `AppendEntries` consistency check. Index `0` matches
    /// term `0`.
    #[must_use]
    pub fn matches(&self, index: Index, term: Term) -> bool {
        self.term_at(index) == Some(term)
    }

    /// Appends a new entry created by this node as leader.
    ///
    /// Returns the index of the new entry.
    pub fn push(&mut self, term: Term, data: Vec<u8>) -> Index {
        let index = self.last_index() + 1;
        self.entries.push(Entry { term, index, data });
        index
    }

    /// Applies entries received from a leader, starting after `prev_index`.
    ///
    /// Entries already present with the same term are kept. At the first
    /// entry whose term differs from ours, that entry and everything after
    /// it is deleted, and the leader's entries replace them.
    ///
    /// The caller must first check that [`RaftLog::matches`] holds for the
    /// leader's `prev_index` and `prev_term`, and `entries` must be
    /// contiguous, starting at `prev_index + 1`, with correct `index` fields.
    pub fn append_entries(&mut self, prev_index: Index, entries: &[Entry]) {
        for (offset, entry) in entries.iter().enumerate() {
            let index = prev_index + 1 + offset as Index;
            match self.term_at(index) {
                // We already have this entry. Do not truncate.
                Some(term) if term == entry.term => {}
                // Conflict: drop this entry and everything after it.
                Some(_) => {
                    self.entries.truncate((index - 1) as usize);
                    self.entries.push(entry.clone());
                }
                // Past the end: this is a new entry.
                None => self.entries.push(entry.clone()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(term: Term, index: Index) -> Entry {
        Entry {
            term,
            index,
            data: vec![index as u8],
        }
    }

    fn log_with_terms(terms: &[Term]) -> RaftLog {
        let mut log = RaftLog::new();
        for &term in terms {
            log.push(term, Vec::new());
        }
        log
    }

    #[test]
    fn empty_log_has_sentinel_at_index_zero() {
        let log = RaftLog::new();
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert_eq!(log.term_at(0), Some(0));
        assert_eq!(log.term_at(1), None);
        assert!(log.matches(0, 0));
        assert!(!log.matches(1, 1));
    }

    #[test]
    fn push_assigns_consecutive_indexes() {
        let mut log = RaftLog::new();
        assert_eq!(log.push(1, b"a".to_vec()), 1);
        assert_eq!(log.push(1, b"b".to_vec()), 2);
        assert_eq!(log.push(2, b"c".to_vec()), 3);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
        assert_eq!(log.entry(2).unwrap().data, b"b".to_vec());
    }

    #[test]
    fn matches_checks_both_index_and_term() {
        let log = log_with_terms(&[1, 1, 2]);
        assert!(log.matches(3, 2));
        assert!(!log.matches(3, 1));
        assert!(!log.matches(4, 2));
    }

    #[test]
    fn entries_from_returns_the_tail() {
        let log = log_with_terms(&[1, 1, 2]);
        assert_eq!(log.entries_from(1).len(), 3);
        assert_eq!(log.entries_from(3).len(), 1);
        assert_eq!(log.entries_from(3)[0].index, 3);
        assert!(log.entries_from(4).is_empty());
    }

    #[test]
    fn append_entries_extends_the_log() {
        let mut log = RaftLog::new();
        log.append_entries(0, &[entry(1, 1), entry(1, 2)]);
        assert_eq!(log.last_index(), 2);
        log.append_entries(2, &[entry(2, 3)]);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.last_term(), 2);
    }

    #[test]
    fn resending_old_entries_does_not_truncate() {
        let mut log = log_with_terms(&[1, 1, 1]);
        // A delayed message carries only the first two entries.
        log.append_entries(0, &[entry(1, 1), entry(1, 2)]);
        assert_eq!(log.last_index(), 3);
    }

    #[test]
    fn conflicting_entry_truncates_it_and_everything_after() {
        let mut log = log_with_terms(&[1, 1, 1]);
        // The leader's entry at index 2 has term 2, so ours is wrong.
        log.append_entries(1, &[entry(2, 2)]);
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.term_at(1), Some(1));
        assert_eq!(log.term_at(2), Some(2));
    }

    #[test]
    fn entries_after_a_conflict_are_all_replaced() {
        let mut log = log_with_terms(&[1, 1, 1, 1]);
        log.append_entries(1, &[entry(2, 2), entry(2, 3)]);
        assert_eq!(log.last_index(), 3);
        assert_eq!(log.term_at(3), Some(2));
    }
}
