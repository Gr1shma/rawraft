use std::collections::BTreeMap;

use crate::{Command, Engine, Error, Response, Result};

/// In-memory [`Engine`] backed by a [`BTreeMap`].
///
/// A `BTreeMap` keeps keys sorted, so iteration order is always the same.
/// That matters later, when snapshots must be identical on every node.
///
/// # Examples
///
/// ```
/// use kv_engine::{Command, Engine, MemEngine, Response};
///
/// let mut engine = MemEngine::new();
/// engine.apply(&Command::Put {
///     key: b"a".to_vec(),
///     value: b"1".to_vec(),
/// });
/// assert_eq!(
///     engine.apply(&Command::Get { key: b"a".to_vec() }),
///     Response::Value(Some(b"1".to_vec()))
/// );
///
/// // A snapshot restores into a second, independent engine.
/// let bytes = engine.snapshot().unwrap();
/// let mut restored = MemEngine::new();
/// restored.restore(&bytes).unwrap();
/// assert_eq!(
///     restored.apply(&Command::Get { key: b"a".to_vec() }),
///     Response::Value(Some(b"1".to_vec()))
/// );
/// ```
#[derive(Debug, Default)]
pub struct MemEngine {
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemEngine {
    /// Creates an empty engine.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Engine for MemEngine {
    fn apply(&mut self, command: &Command) -> Response {
        match command {
            Command::Get { key } => Response::Value(self.data.get(key).cloned()),
            Command::Put { key, value } => {
                self.data.insert(key.clone(), value.clone());
                Response::Stored
            }
            Command::Delete { key } => Response::Deleted {
                existed: self.data.remove(key).is_some(),
            },
            Command::Cas {
                key,
                expected,
                value,
            } => {
                if self.data.get(key) == expected.as_ref() {
                    match value {
                        Some(v) => {
                            self.data.insert(key.clone(), v.clone());
                        }
                        None => {
                            self.data.remove(key);
                        }
                    }
                    Response::Cas {
                        success: true,
                        current: value.clone(),
                    }
                } else {
                    Response::Cas {
                        success: false,
                        current: self.data.get(key).cloned(),
                    }
                }
            }
        }
    }

    fn snapshot(&self) -> Result<Vec<u8>> {
        Ok(bincode::serde::encode_to_vec(
            &self.data,
            bincode::config::standard(),
        )?)
    }

    fn restore(&mut self, data: &[u8]) -> Result<()> {
        // Decode and validate first, assign after, so any failure leaves
        // `self` untouched.
        let (map, bytes_read): (BTreeMap<Vec<u8>, Vec<u8>>, usize) =
            bincode::serde::decode_from_slice(data, bincode::config::standard())?;
        if bytes_read != data.len() {
            return Err(Error::TrailingBytes {
                extra: data.len() - bytes_read,
            });
        }
        self.data = map;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(key: &str, value: &str) -> Command {
        Command::Put {
            key: key.into(),
            value: value.into(),
        }
    }

    fn get(key: &str) -> Command {
        Command::Get { key: key.into() }
    }

    fn cas(key: &str, expected: Option<&str>, value: Option<&str>) -> Command {
        Command::Cas {
            key: key.into(),
            expected: expected.map(Into::into),
            value: value.map(Into::into),
        }
    }

    #[test]
    fn put_then_get_returns_value() {
        let mut engine = MemEngine::new();
        assert_eq!(engine.apply(&put("a", "1")), Response::Stored);
        assert_eq!(
            engine.apply(&get("a")),
            Response::Value(Some(b"1".to_vec()))
        );
    }

    #[test]
    fn get_missing_key_returns_none() {
        let mut engine = MemEngine::new();
        assert_eq!(engine.apply(&get("nope")), Response::Value(None));
    }

    #[test]
    fn delete_reports_whether_key_existed() {
        let mut engine = MemEngine::new();
        engine.apply(&put("a", "1"));

        let del = Command::Delete { key: b"a".to_vec() };
        assert_eq!(engine.apply(&del), Response::Deleted { existed: true });
        assert_eq!(engine.apply(&del), Response::Deleted { existed: false });
        assert_eq!(engine.apply(&get("a")), Response::Value(None));
    }

    #[test]
    fn cas_succeeds_when_expected_matches() {
        let mut engine = MemEngine::new();
        engine.apply(&put("a", "1"));

        assert_eq!(
            engine.apply(&cas("a", Some("1"), Some("2"))),
            Response::Cas {
                success: true,
                current: Some(b"2".to_vec())
            }
        );
        assert_eq!(
            engine.apply(&get("a")),
            Response::Value(Some(b"2".to_vec()))
        );
    }

    #[test]
    fn cas_fails_when_expected_differs() {
        let mut engine = MemEngine::new();
        engine.apply(&put("a", "1"));

        assert_eq!(
            engine.apply(&cas("a", Some("wrong"), Some("2"))),
            Response::Cas {
                success: false,
                current: Some(b"1".to_vec())
            }
        );
        assert_eq!(
            engine.apply(&get("a")),
            Response::Value(Some(b"1".to_vec()))
        );
    }

    #[test]
    fn cas_with_none_expected_requires_absent_key() {
        let mut engine = MemEngine::new();

        // Key is absent, so this succeeds.
        assert_eq!(
            engine.apply(&cas("a", None, Some("1"))),
            Response::Cas {
                success: true,
                current: Some(b"1".to_vec())
            }
        );
        // Key now exists, so the same command fails.
        assert_eq!(
            engine.apply(&cas("a", None, Some("2"))),
            Response::Cas {
                success: false,
                current: Some(b"1".to_vec())
            }
        );
    }

    #[test]
    fn cas_with_expected_fails_on_absent_key() {
        let mut engine = MemEngine::new();

        assert_eq!(
            engine.apply(&cas("a", Some("1"), Some("2"))),
            Response::Cas {
                success: false,
                current: None
            }
        );
        // Nothing was created.
        assert_eq!(engine.apply(&get("a")), Response::Value(None));
    }

    #[test]
    fn cas_with_none_value_deletes_key() {
        let mut engine = MemEngine::new();
        engine.apply(&put("a", "1"));

        assert_eq!(
            engine.apply(&cas("a", Some("1"), None)),
            Response::Cas {
                success: true,
                current: None
            }
        );
        assert_eq!(engine.apply(&get("a")), Response::Value(None));
    }

    #[test]
    fn snapshot_restore_round_trip() {
        let mut original = MemEngine::new();
        original.apply(&put("a", "1"));
        original.apply(&put("b", "2"));
        let bytes = original.snapshot().unwrap();

        let mut restored = MemEngine::new();
        restored.restore(&bytes).unwrap();

        assert_eq!(
            restored.apply(&get("a")),
            Response::Value(Some(b"1".to_vec()))
        );
        assert_eq!(
            restored.apply(&get("b")),
            Response::Value(Some(b"2".to_vec()))
        );
    }

    #[test]
    fn snapshot_is_independent_of_insert_order() {
        let mut first = MemEngine::new();
        first.apply(&put("a", "1"));
        first.apply(&put("b", "2"));

        let mut second = MemEngine::new();
        second.apply(&put("b", "2"));
        second.apply(&put("a", "1"));

        assert_eq!(first.snapshot().unwrap(), second.snapshot().unwrap());
    }

    #[test]
    fn snapshot_restore_round_trip_is_byte_identical() {
        let mut original = MemEngine::new();
        original.apply(&put("b", "2"));
        original.apply(&put("a", "1"));
        let first = original.snapshot().unwrap();

        let mut restored = MemEngine::new();
        restored.restore(&first).unwrap();
        let second = restored.snapshot().unwrap();

        // Raft compares and transfers snapshots as bytes, so a restored engine
        // must re-snapshot to exactly what it was restored from.
        assert_eq!(first, second);
    }

    #[test]
    fn empty_snapshot_round_trips() {
        let empty = MemEngine::new();
        let bytes = empty.snapshot().unwrap();

        let mut restored = MemEngine::new();
        restored.apply(&put("stale", "1"));
        restored.restore(&bytes).unwrap();

        assert_eq!(restored.snapshot().unwrap(), bytes);
        assert_eq!(restored.apply(&get("stale")), Response::Value(None));
    }

    #[test]
    fn restore_replaces_existing_data() {
        let mut source = MemEngine::new();
        source.apply(&put("keep", "1"));
        let bytes = source.snapshot().unwrap();

        let mut target = MemEngine::new();
        target.apply(&put("old", "2"));
        target.restore(&bytes).unwrap();

        // Replacement, not merge: keys absent from the snapshot are discarded.
        assert_eq!(target.apply(&get("old")), Response::Value(None));
        assert_eq!(
            target.apply(&get("keep")),
            Response::Value(Some(b"1".to_vec()))
        );
    }

    #[test]
    fn empty_keys_and_values_are_stored() {
        let mut engine = MemEngine::new();
        let empty = Command::Put {
            key: Vec::new(),
            value: Vec::new(),
        };

        assert_eq!(engine.apply(&empty), Response::Stored);
        // An empty value is still a present key, not a missing one.
        assert_eq!(engine.apply(&get("")), Response::Value(Some(Vec::new())));
        assert_eq!(
            engine.apply(&Command::Delete { key: Vec::new() }),
            Response::Deleted { existed: true }
        );
        assert_eq!(engine.apply(&get("")), Response::Value(None));
    }

    #[test]
    fn restore_with_corrupt_data_leaves_state_unchanged() {
        let mut engine = MemEngine::new();
        engine.apply(&put("a", "1"));

        assert!(engine.restore(b"garbage").is_err());
        assert_eq!(
            engine.apply(&get("a")),
            Response::Value(Some(b"1".to_vec()))
        );
    }

    #[test]
    fn restore_with_truncated_data_leaves_state_unchanged() {
        let mut engine = MemEngine::new();
        engine.apply(&put("a", "1"));
        let bytes = engine.snapshot().unwrap();

        assert!(matches!(
            engine.restore(&bytes[..bytes.len() - 1]),
            Err(Error::Decode(_))
        ));
        assert_eq!(
            engine.apply(&get("a")),
            Response::Value(Some(b"1".to_vec()))
        );
    }

    #[test]
    fn restore_with_trailing_bytes_leaves_state_unchanged() {
        let mut engine = MemEngine::new();
        engine.apply(&put("a", "1"));
        let mut bytes = engine.snapshot().unwrap();
        bytes.extend_from_slice(b"\xde\xad");

        assert!(matches!(
            engine.restore(&bytes),
            Err(Error::TrailingBytes { extra: 2 })
        ));
        assert_eq!(
            engine.apply(&get("a")),
            Response::Value(Some(b"1".to_vec()))
        );
    }
}
