use serde::{Deserialize, Serialize};

/// An operation on the key-value store.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// Read the value stored under `key`.
    Get {
        /// Key to read.
        key: Vec<u8>,
    },
    /// Store `value` under `key`, replacing any existing value.
    Put {
        /// Key to write.
        key: Vec<u8>,
        /// Value to store.
        value: Vec<u8>,
    },
    /// Remove `key` if present.
    Delete {
        /// Key to remove.
        key: Vec<u8>,
    },
}
