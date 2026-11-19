use crate::NodeId;

/// Static settings for one Raft node.
#[derive(Clone, Debug)]
pub struct Config {
    /// This node's identifier.
    pub id: NodeId,
    /// The other nodes in the cluster. Must not include `id`.
    pub peers: Vec<NodeId>,
    /// Shortest election timeout, in ticks.
    pub election_timeout_min: u64,
    /// Longest election timeout, in ticks. Values below the minimum are
    /// treated as equal to it.
    pub election_timeout_max: u64,
    /// Ticks between heartbeats sent by a leader. Must be well below
    /// `election_timeout_min`, or followers will time out between heartbeats.
    pub heartbeat_interval: u64,
    /// Seed for this node's random number generator. The same seed always
    /// produces the same timeouts, which keeps simulations reproducible.
    pub seed: u64,
}
