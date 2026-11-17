use thiserror::Error;

/// Result alias used throughout this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors produced when snapshotting or restoring an engine, or when applying
/// a request whose sequence number has already been used.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The engine state could not be serialized.
    #[error("snapshot encode failed: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    /// Snapshot bytes were corrupt or incompatible.
    #[error("snapshot decode failed: {0}")]
    Decode(#[from] bincode::error::DecodeError),

    /// A request carried a sequence number older than the client's latest,
    /// so its original response is no longer known.
    #[error("stale request: client {client_id} sequence {sequence}")]
    StaleRequest {
        /// The client that sent the request.
        client_id: u128,
        /// The rejected sequence number.
        sequence: u64,
    },

    /// A decoded value was followed by bytes the decoder did not consume, so
    /// the input was not a complete encoding of the expected value.
    #[error("decoded value was followed by {extra} unexpected bytes")]
    TrailingBytes {
        /// How many bytes followed the decoded value.
        extra: usize,
    },

    /// A request reused a sequence number that was already applied for this
    /// client, but carried a different command. The original command stays in
    /// effect; the new one is not applied.
    #[error("conflicting request: client {client_id} sequence {sequence}")]
    ConflictingRequest {
        /// The client that sent the request.
        client_id: u128,
        /// The reused sequence number.
        sequence: u64,
    },
}
