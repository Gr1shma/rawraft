use serde::{Deserialize, Serialize};

/// The result of applying a [`Command`](crate::Command).
///
/// # Encoding
///
/// The serialized form is part of the replicated log format, and variants
/// encode by declaration index: only append new variants at the end. See the
/// [crate docs](crate) for details.
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
        /// On success, the value after the swap (`None` if the swap deleted the
        /// key). On failure, the value that blocked the swap.
        current: Option<Vec<u8>>,
    },
}
