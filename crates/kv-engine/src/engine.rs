use crate::{Command, Response};

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
}
