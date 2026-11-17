use serde::{Deserialize, Serialize};

use crate::{Command, Result};

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

impl ClientRequest {
    /// Serializes this request into bytes for a Raft log entry.
    ///
    /// # Errors
    /// Returns [`Error::Encode`](crate::Error::Encode) if serialization fails.
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::serde::encode_to_vec(
            self,
            bincode::config::standard(),
        )?)
    }

    /// Deserializes a request from Raft log entry bytes.
    ///
    /// # Errors
    /// Returns [`Error::Decode`](crate::Error::Decode) if `bytes` is not a
    /// valid encoded request.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (request, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trip() {
        let request = ClientRequest {
            client_id: 7,
            sequence: 3,
            command: Command::Put {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
        };
        let bytes = request.encode().unwrap();
        assert_eq!(ClientRequest::decode(&bytes).unwrap(), request);
    }

    #[test]
    fn decode_garbage_fails() {
        assert!(ClientRequest::decode(b"garbage").is_err());
    }
}
