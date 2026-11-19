/// Identifier of a node in the cluster.
pub type NodeId = u64;

/// A Raft term: a logical clock that increases with each election.
///
/// Term `0` means "no term yet" and only exists before the first election.
pub type Term = u64;

/// A position in the Raft log.
///
/// Real entries are numbered from `1`. Index `0` is a sentinel meaning
/// "before the first entry". It is the last index of an empty log, and the
/// `prev_index` of a replication request that starts at the beginning.
pub type Index = u64;

/// One entry in the replicated log.
///
/// `data` is opaque to Raft. In rawraft it holds an encoded client request,
/// but this crate never looks inside it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Term in which the leader created this entry.
    pub term: Term,
    /// Position of this entry in the log.
    pub index: Index,
    /// The replicated payload.
    pub data: Vec<u8>,
}

/// State that must be durable before a node replies to any RPC.
///
/// Figure 2 of the Raft paper lists `currentTerm`, `votedFor` and the log as
/// persistent state. The log is stored separately, so this holds the other two.
/// The commit index is deliberately not here: it is volatile and can be
/// rebuilt after a restart.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    /// Latest term this node has seen.
    pub current_term: Term,
    /// The candidate this node voted for in `current_term`, if any.
    pub voted_for: Option<NodeId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_hard_state_is_term_zero_with_no_vote() {
        let state = HardState::default();
        assert_eq!(state.current_term, 0);
        assert_eq!(state.voted_for, None);
    }

    #[test]
    fn entries_with_same_term_index_and_data_are_equal() {
        let a = Entry {
            term: 1,
            index: 1,
            data: b"x".to_vec(),
        };
        let b = Entry {
            term: 1,
            index: 1,
            data: b"x".to_vec(),
        };
        assert_eq!(a, b);
    }
}
