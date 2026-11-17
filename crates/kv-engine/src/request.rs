use serde::{Deserialize, Serialize};

use crate::Command;

/// Unique identifier of a client.
pub type ClientId = u128;

/// A command tagged with the identity needed for deduplication.
///
/// A retried request must reuse the same `client_id` and `sequence`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientRequest {
    /// The client that issued the request.
    pub client_id: ClientId,
    /// Per-client request number, increasing with each new request.
    pub sequence: u64,
    /// The operation to perform.
    pub command: Command,
}
