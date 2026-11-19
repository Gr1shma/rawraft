use crate::{HardState, Message, NodeId};

/// Work the runtime must carry out on behalf of a [`Raft`](crate::Raft) node.
///
/// Order matters: persist `hard_state` first, and only then send `messages`.
/// A node must never tell a peer something (such as a vote) that it could
/// forget after a crash.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Ready {
    /// New durable state, if it changed since the last `Ready`.
    pub hard_state: Option<HardState>,
    /// Messages to send, each paired with its destination.
    pub messages: Vec<(NodeId, Message)>,
}
