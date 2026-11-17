use thiserror::Error;

/// Result alias used throughout this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors produced when snapshotting or restoring an engine.
#[derive(Debug, Error)]
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
}
