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
    /// Called only with committed entries, in log order, at most once per
    /// entry. The result must depend on nothing that differs between nodes.
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

    /// Replaces the entire engine state with one produced by
    /// [`Engine::snapshot`].
    ///
    /// This is a replacement, not a merge: state the snapshot does not contain
    /// is discarded, including keys that exist only locally.
    ///
    /// Restoring must be atomic. If it fails, the existing state must be left
    /// exactly as it was, because
    /// [`StateMachine::restore`](crate::StateMachine::restore) restores the
    /// engine before it touches the session table and relies on this.
    ///
    /// # Errors
    /// Returns [`Error::Decode`](crate::Error::Decode) if `data` is corrupt, or
    /// [`Error::TrailingBytes`](crate::Error::TrailingBytes) if `data` holds a
    /// valid snapshot followed by extra bytes.
    fn restore(&mut self, data: &[u8]) -> Result<()>;
}
