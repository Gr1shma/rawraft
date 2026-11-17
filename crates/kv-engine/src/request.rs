use serde::{Deserialize, Serialize};

use crate::{Command, Error, Result};

/// Unique identifier of a client.
pub type ClientId = u128;

/// A command tagged with the identity needed for deduplication.
///
/// A `(client_id, sequence)` pair identifies one specific command:
///
/// - a retry must reuse both the same pair *and* an identical [`Command`];
/// - a new request from the same client must use a strictly greater
///   `sequence`.
///
/// Reusing a pair with a different command is rejected with
/// [`Error::ConflictingRequest`], and going backwards is rejected with
/// [`Error::StaleRequest`]; see [`StateMachine`](crate::StateMachine).
///
/// # Encoding
///
/// The serialized form of this type is part of the replicated log format: enum
/// variants inside [`Command`] encode by declaration index, so new variants
/// must only be appended at the end.
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
    /// Returns [`Error::Encode`] if serialization fails.
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::serde::encode_to_vec(
            self,
            bincode::config::standard(),
        )?)
    }

    /// Deserializes a request from Raft log entry bytes.
    ///
    /// Decoding is strict: `bytes` must hold exactly one encoded request and
    /// nothing else.
    ///
    /// # Errors
    /// Returns [`Error::Decode`] if `bytes` is not a valid encoded request, or
    /// [`Error::TrailingBytes`] if a valid request is followed by extra bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (request, bytes_read) =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard())?;
        if bytes_read != bytes.len() {
            return Err(Error::TrailingBytes {
                extra: bytes.len() - bytes_read,
            });
        }
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

    /// Changing these bytes changes the replicated log format. Only append new
    /// variants at the end of the enum.
    #[test]
    fn encoded_bytes_are_stable() {
        // Frame layout: client_id (1 as a varint), sequence (1 as a varint),
        // then the variant tag, then the payload.
        let cases: [(Command, u8, &[u8]); 4] = [
            (
                Command::Get { key: b"a".to_vec() },
                0,
                &[0x01, 0x01, 0x00, 0x01, b'a'],
            ),
            (
                Command::Put {
                    key: b"a".to_vec(),
                    value: b"b".to_vec(),
                },
                1,
                &[0x01, 0x01, 0x01, 0x01, b'a', 0x01, b'b'],
            ),
            (
                Command::Delete { key: b"a".to_vec() },
                2,
                &[0x01, 0x01, 0x02, 0x01, b'a'],
            ),
            (
                Command::Cas {
                    key: b"a".to_vec(),
                    expected: None,
                    value: None,
                },
                3,
                &[0x01, 0x01, 0x03, 0x01, b'a', 0x00, 0x00],
            ),
        ];

        for (command, tag, expected) in cases {
            let request = ClientRequest {
                client_id: 1,
                sequence: 1,
                command: command.clone(),
            };
            let encoded = request.encode().unwrap();
            assert_eq!(encoded, expected, "encoding changed for {command:?}");
            // The third byte is the variant tag: Get=0, Put=1, Delete=2, Cas=3.
            assert_eq!(expected[2], tag, "variant tag changed for {command:?}");
            assert_eq!(ClientRequest::decode(&encoded).unwrap(), request);
        }
    }

    #[test]
    fn decode_rejects_trailing_bytes() {
        let request = ClientRequest {
            client_id: 1,
            sequence: 1,
            command: Command::Get { key: b"a".to_vec() },
        };
        let mut bytes = request.encode().unwrap();
        bytes.extend_from_slice(b"\xff\xff\xff");

        assert!(matches!(
            ClientRequest::decode(&bytes),
            Err(Error::TrailingBytes { extra: 3 })
        ));
    }

    #[test]
    fn decode_rejects_truncated_input() {
        let request = ClientRequest {
            client_id: 1,
            sequence: 1,
            command: Command::Get { key: b"a".to_vec() },
        };
        let bytes = request.encode().unwrap();

        assert!(matches!(
            ClientRequest::decode(&bytes[..bytes.len() - 1]),
            Err(Error::Decode(_))
        ));
    }
}
