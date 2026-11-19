use crate::{Index, NodeId, Term};

/// A message exchanged between Raft nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// A candidate asks for a vote.
    RequestVote {
        /// The candidate's term.
        term: Term,
        /// The candidate asking for the vote.
        candidate: NodeId,
        /// Index of the candidate's last log entry.
        last_log_index: Index,
        /// Term of the candidate's last log entry.
        last_log_term: Term,
    },
    /// The answer to a `RequestVote`.
    VoteResponse {
        /// The voter's current term, so a stale candidate can step down.
        term: Term,
        /// Whether the vote was granted.
        granted: bool,
    },
}
