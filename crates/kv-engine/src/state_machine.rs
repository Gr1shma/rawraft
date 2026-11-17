use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{ClientId, ClientRequest, Engine, Error, Response, Result};

/// What we remember about a client's latest request.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Session {
    last_sequence: u64,
    last_response: Response,
}

/// The bytes we write for a snapshot: engine state plus the session table.
#[derive(Debug, Serialize, Deserialize)]
struct Snapshot {
    engine: Vec<u8>,
    sessions: BTreeMap<ClientId, Session>,
}

/// Wraps an [`Engine`] so that each client request is applied exactly once.
#[derive(Debug)]
pub struct StateMachine<E: Engine> {
    engine: E,
    sessions: BTreeMap<ClientId, Session>,
}

impl<E: Engine> StateMachine<E> {
    /// Creates a state machine around `engine` with no sessions.
    #[must_use]
    pub fn new(engine: E) -> Self {
        Self {
            engine,
            sessions: BTreeMap::new(),
        }
    }

    /// Applies a committed request, skipping duplicates.
    ///
    /// If this client's `sequence` was already applied, returns the saved
    /// response without touching the engine.
    ///
    /// # Errors
    /// Returns [`Error::StaleRequest`] if `sequence` is older than the
    /// client's latest applied request.
    pub fn apply(&mut self, request: &ClientRequest) -> Result<Response> {
        if let Some(session) = self.sessions.get(&request.client_id) {
            if request.sequence == session.last_sequence {
                return Ok(session.last_response.clone());
            }
            if request.sequence < session.last_sequence {
                return Err(Error::StaleRequest {
                    client_id: request.client_id,
                    sequence: request.sequence,
                });
            }
        }

        let response = self.engine.apply(&request.command);
        self.sessions.insert(
            request.client_id,
            Session {
                last_sequence: request.sequence,
                last_response: response.clone(),
            },
        );
        Ok(response)
    }

    /// Serializes the engine state and the session table together.
    ///
    /// # Errors
    /// Returns an error if the engine or the encoder fails.
    pub fn snapshot(&self) -> Result<Vec<u8>> {
        let snapshot = Snapshot {
            engine: self.engine.snapshot()?,
            sessions: self.sessions.clone(),
        };
        Ok(bincode::serde::encode_to_vec(
            &snapshot,
            bincode::config::standard(),
        )?)
    }

    /// Restores engine state and sessions from [`StateMachine::snapshot`].
    ///
    /// # Errors
    /// Returns an error if `data` is corrupt. State is unchanged on error.
    pub fn restore(&mut self, data: &[u8]) -> Result<()> {
        let (snapshot, _): (Snapshot, usize) =
            bincode::serde::decode_from_slice(data, bincode::config::standard())?;
        // The engine restore can still fail; do it before touching sessions
        // so an error leaves everything as it was.
        self.engine.restore(&snapshot.engine)?;
        self.sessions = snapshot.sessions;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Command, MemEngine};

    fn request(client_id: ClientId, sequence: u64, command: Command) -> ClientRequest {
        ClientRequest {
            client_id,
            sequence,
            command,
        }
    }

    fn cas_create(key: &str, value: &str) -> Command {
        Command::Cas {
            key: key.into(),
            expected: None,
            value: Some(value.into()),
        }
    }

    #[test]
    fn first_request_is_applied() {
        let mut sm = StateMachine::new(MemEngine::new());
        let response = sm.apply(&request(1, 1, cas_create("a", "1"))).unwrap();
        assert_eq!(
            response,
            Response::Cas {
                success: true,
                current: Some(b"1".to_vec())
            }
        );
    }

    #[test]
    fn duplicate_returns_saved_response_without_reapplying() {
        let mut sm = StateMachine::new(MemEngine::new());
        let req = request(1, 1, cas_create("a", "1"));

        let first = sm.apply(&req).unwrap();
        // If this were applied again, the key already exists and CAS would fail.
        let retry = sm.apply(&req).unwrap();

        assert_eq!(first, retry);
        assert_eq!(
            retry,
            Response::Cas {
                success: true,
                current: Some(b"1".to_vec())
            }
        );
    }

    #[test]
    fn different_clients_do_not_interfere() {
        let mut sm = StateMachine::new(MemEngine::new());
        let a = sm.apply(&request(1, 1, cas_create("a", "1"))).unwrap();
        let b = sm.apply(&request(2, 1, cas_create("b", "2"))).unwrap();

        assert_eq!(
            a,
            Response::Cas {
                success: true,
                current: Some(b"1".to_vec())
            }
        );
        assert_eq!(
            b,
            Response::Cas {
                success: true,
                current: Some(b"2".to_vec())
            }
        );
    }

    #[test]
    fn stale_sequence_is_rejected() {
        let mut sm = StateMachine::new(MemEngine::new());
        sm.apply(&request(1, 2, cas_create("a", "1"))).unwrap();

        let result = sm.apply(&request(1, 1, cas_create("b", "2")));
        assert!(matches!(
            result,
            Err(Error::StaleRequest {
                client_id: 1,
                sequence: 1
            })
        ));
    }

    #[test]
    fn snapshot_restore_preserves_data_and_sessions() {
        let mut original = StateMachine::new(MemEngine::new());
        let req = request(1, 1, cas_create("a", "1"));
        original.apply(&req).unwrap();
        let bytes = original.snapshot().unwrap();

        let mut restored = StateMachine::new(MemEngine::new());
        restored.restore(&bytes).unwrap();

        // The data came across.
        let get = Command::Get { key: b"a".to_vec() };
        assert_eq!(
            restored.apply(&request(2, 1, get)).unwrap(),
            Response::Value(Some(b"1".to_vec()))
        );
        // The session came across: the retry returns the saved response
        // instead of failing the CAS a second time.
        assert_eq!(
            restored.apply(&req).unwrap(),
            Response::Cas {
                success: true,
                current: Some(b"1".to_vec())
            }
        );
    }

    #[test]
    fn restore_with_corrupt_data_leaves_state_unchanged() {
        let mut sm = StateMachine::new(MemEngine::new());
        let req = request(1, 1, cas_create("a", "1"));
        sm.apply(&req).unwrap();

        assert!(sm.restore(b"garbage").is_err());

        // Data and session are both still there.
        assert_eq!(
            sm.apply(&req).unwrap(),
            Response::Cas {
                success: true,
                current: Some(b"1".to_vec())
            }
        );
    }
}
