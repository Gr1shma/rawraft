use std::collections::BTreeMap;

use crate::{Command, Engine, Response};

/// In-memory [`Engine`] backed by a [`BTreeMap`].
///
/// A `BTreeMap` keeps keys sorted, so iteration order is always the same.
/// That matters later, when snapshots must be identical on every node.
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
    fn apply(&mut self, command: &crate::Command) -> crate::Response {
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

    fn snapshot(&self) -> crate::Result<Vec<u8>> {
        Ok(bincode::serde::encode_to_vec(
            &self.data,
            bincode::config::standard(),
        )?)
    }

    fn restore(&mut self, data: &[u8]) -> crate::Result<()> {
        // Decode first and assign after, so a failure leaves `self` untouched.
        let (map, _): (BTreeMap<Vec<u8>, Vec<u8>>, usize) =
            bincode::serde::decode_from_slice(data, bincode::config::standard())?;
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
    fn restore_with_corrupt_data_leaves_state_unchanged() {
        let mut engine = MemEngine::new();
        engine.apply(&put("a", "1"));

        assert!(engine.restore(b"garbage").is_err());
        assert_eq!(
            engine.apply(&get("a")),
            Response::Value(Some(b"1".to_vec()))
        );
    }
}
