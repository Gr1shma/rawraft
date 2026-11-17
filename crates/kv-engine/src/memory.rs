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
        }
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
}
