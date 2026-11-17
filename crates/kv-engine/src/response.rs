use serde::{Deserialize, Serialize};

/// The result of applying a [`Command`](crate::Command).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Response {
    /// Result of a `Get`: the value, or `None` if the key does not exist.
    Value(Option<Vec<u8>>),
    /// A `Put` was applied.
    Stored,
    /// Result of a `Delete`.
    Deleted {
        /// Whether the key existed before deletion.
        existed: bool,
    },
    /// Result of a `Cas`.
    Cas {
        /// Whether the swap was performed.
        success: bool,
        /// The value of the key after the operation.
        current: Option<Vec<u8>>,
    },
}
