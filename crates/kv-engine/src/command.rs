use serde::{Deserialize, Serialize};

/// An operation on the key-value store.
///
/// # Encoding
///
/// The serialized form is part of the replicated log format, and variants
/// encode by declaration index: only append new variants at the end. See the
/// [crate docs](crate) for details.
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
    ///
    /// # Examples
    ///
    /// ```
    /// use kv_engine::{Command, Engine, MemEngine, Response};
    ///
    /// let mut engine = MemEngine::new();
    ///
    /// // `expected: None` means "only if the key is absent".
    /// let create = Command::Cas {
    ///     key: b"a".to_vec(),
    ///     expected: None,
    ///     value: Some(b"1".to_vec()),
    /// };
    /// assert_eq!(
    ///     engine.apply(&create),
    ///     Response::Cas { success: true, current: Some(b"1".to_vec()) }
    /// );
    ///
    /// // The key exists now, so the same command fails and changes nothing.
    /// assert_eq!(
    ///     engine.apply(&create),
    ///     Response::Cas { success: false, current: Some(b"1".to_vec()) }
    /// );
    ///
    /// // `value: None` deletes the key, once `expected` matches.
    /// let remove = Command::Cas {
    ///     key: b"a".to_vec(),
    ///     expected: Some(b"1".to_vec()),
    ///     value: None,
    /// };
    /// assert_eq!(
    ///     engine.apply(&remove),
    ///     Response::Cas { success: true, current: None }
    /// );
    /// ```
    Cas {
        /// Key to update.
        key: Vec<u8>,
        /// Required current value; `None` means the key must be absent.
        expected: Option<Vec<u8>>,
        /// New value on success; `None` deletes the key.
        value: Option<Vec<u8>>,
    },
}
