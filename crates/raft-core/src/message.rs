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
    /// A leader asserts its authority.
    ///
    /// For now this carries no log entries, so it works as a heartbeat.
    /// The replication fields are added with log replication.
    AppendEntries {
        /// The leader's term.
        term: Term,
        /// The leader, so followers know where to redirect clients.
        leader: NodeId,
    },
    /// The answer to an `AppendEntries`.
    AppendResponse {
        /// The receiver's current term, so a stale leader can step down.
        term: Term,
        /// Whether the receiver accepted the sender as leader.
        success: bool,
    },
}

impl Message {
    /// Returns the term carried by this message.
    #[must_use]
    pub fn term(&self) -> Term {
        match self {
            Self::RequestVote { term, .. }
            | Self::VoteResponse { term, .. }
            | Self::AppendEntries { term, .. }
            | Self::AppendResponse { term, .. } => *term,
        }
    }
}
