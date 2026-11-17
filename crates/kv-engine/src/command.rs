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
    /// Compare-and-swap: update `key` only if its current value equals
    /// `expected`.
    Cas {
        /// Key to update.
        key: Vec<u8>,
        /// Required current value; `None` means the key must be absent.
        expected: Option<Vec<u8>>,
        /// New value on success; `None` deletes the key.
        value: Option<Vec<u8>>,
    },
}
