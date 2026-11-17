use crate::{Command, Response, Result};

/// A deterministic key-value storage engine.
///
/// Applying the same commands in the same order to an empty engine must
/// always produce the same state and the same responses, on every node.
/// The engine never decides what is committed; the caller applies only
/// entries that Raft has already committed.
pub trait Engine {
    /// Applies `command` and returns its response.
    ///
    /// Logical outcomes such as "key not found" are part of the
    /// [`Response`], not errors.
    fn apply(&mut self, command: &Command) -> Response;

    /// Serializes the entire engine state into bytes.
    ///
    /// Two engines with the same contents must produce identical bytes.
    ///
    /// # Errors
    /// Returns [`Error::Encode`](crate::Error::Encode) if serialization fails.
    fn snapshot(&self) -> Result<Vec<u8>>;

    /// Replaces the engine state with one produced by [`Engine::snapshot`].
    ///
    /// # Errors
    /// Returns [`Error::Decode`](crate::Error::Decode) if `data` is corrupt.
    /// On error the existing state is left unchanged.
    fn restore(&mut self, data: &[u8]) -> Result<()>;
}
