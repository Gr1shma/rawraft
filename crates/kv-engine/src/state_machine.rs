use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{ClientId, ClientRequest, Command, Engine, Error, Response, Result};

/// What we remember about a client's latest request.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Session {
    last_sequence: u64,
    last_command: Command,
    last_response: Response,
}

/// A snapshot before it is encoded: the engine's own snapshot bytes plus the
/// session table.
#[derive(Debug, Serialize, Deserialize)]
struct Snapshot {
    engine: Vec<u8>,
    sessions: BTreeMap<ClientId, Session>,
}

/// Wraps an [`Engine`] so that each client request is applied at most once.
///
/// A `(client_id, sequence)` pair identifies one specific command. A retry must
/// repeat the same pair *and* an identical [`Command`]; a new request from the
/// same client must use a strictly greater `sequence`. This crate assumes
/// **one outstanding request per client**, which is what makes the sequence
/// number an effective deduplication key:
///
/// - a request that repeats the latest pair and command returns the saved
///   response without touching the engine;
/// - the latest pair with a different command is rejected with
///   [`Error::ConflictingRequest`];
/// - an older sequence is rejected with [`Error::StaleRequest`].
///
/// Snapshots cover the engine state *and* the session table, so a retry that
/// arrives after a restore is still answered from the saved response.
///
/// # Examples
///
/// ```
/// use kv_engine::{ClientRequest, Command, MemEngine, Response, StateMachine};
///
/// let mut machine = StateMachine::new(MemEngine::new());
/// let create = ClientRequest {
///     client_id: 1,
///     sequence: 1,
///     command: Command::Cas {
///         key: b"a".to_vec(),
///         expected: None, // the key must be absent
///         value: Some(b"1".to_vec()),
///     },
/// };
///
/// let first = machine.apply(&create).unwrap();
/// assert_eq!(
///     first,
///     Response::Cas { success: true, current: Some(b"1".to_vec()) }
/// );
///
/// // Retrying the same request returns the saved response. Running the CAS a
/// // second time would fail, because the key now exists.
/// assert_eq!(machine.apply(&create).unwrap(), first);
/// ```
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
    /// If this client's `sequence` was already applied with the same command,
    /// returns the saved response without touching the engine.
    ///
    /// # Errors
    /// Returns [`Error::StaleRequest`] if `sequence` is older than the client's
    /// latest applied request, or [`Error::ConflictingRequest`] if `sequence`
    /// matches the latest applied request but the command differs. In both
    /// cases the engine is left untouched.
    ///
    /// Both are deterministic outcomes of the replicated log: every node that
    /// applies the same entry rejects it in the same way, so state stays
    /// identical. A caller must therefore still treat the entry as *applied* —
    /// advance `last_applied` past it — and must not retry it. The error should
    /// be returned to the client, not treated as a fatal local failure.
    pub fn apply(&mut self, request: &ClientRequest) -> Result<Response> {
        if let Some(session) = self.sessions.get(&request.client_id) {
            if request.sequence == session.last_sequence {
                if request.command == session.last_command {
                    return Ok(session.last_response.clone());
                }
                return Err(Error::ConflictingRequest {
                    client_id: request.client_id,
                    sequence: request.sequence,
                });
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
                last_command: request.command.clone(),
                last_response: response.clone(),
            },
        );
        Ok(response)
    }

    /// Serializes the engine state and the session table together.
    ///
    /// # Errors
    /// Returns [`Error::Encode`] if the encoder fails, or any error the engine
    /// returns from its own [`Engine::snapshot`].
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
    /// The whole state is replaced: sessions recorded locally but absent from
    /// the snapshot are discarded, as is any engine state the snapshot does not
    /// contain.
    ///
    /// On error nothing is changed. The engine is restored first and the
    /// session table only afterwards, so a failing [`Engine::restore`] cannot
    /// leave new engine data paired with an old session table.
    ///
    /// # Errors
    /// Returns [`Error::Decode`] if `data` is corrupt, [`Error::TrailingBytes`]
    /// if `data` is a valid snapshot followed by extra bytes, or any error the
    /// engine returns from its own [`Engine::restore`].
    pub fn restore(&mut self, data: &[u8]) -> Result<()> {
        let (snapshot, bytes_read): (Snapshot, usize) =
            bincode::serde::decode_from_slice(data, bincode::config::standard())?;
        // Validate before touching any state.
        if bytes_read != data.len() {
            return Err(Error::TrailingBytes {
                extra: data.len() - bytes_read,
            });
        }
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
    use crate::MemEngine;

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
    fn replayed_read_returns_the_original_value() {
        let mut sm = StateMachine::new(MemEngine::new());
        sm.apply(&request(1, 1, cas_create("a", "1"))).unwrap();

        let read = request(1, 2, get("a"));
        assert_eq!(
            sm.apply(&read).unwrap(),
            Response::Value(Some(b"1".to_vec()))
        );

        // Another client moves the value on.
        sm.apply(&request(2, 1, put("a", "2"))).unwrap();

        // The replay is answered from the session, not by re-reading: if dedup
        // only stored writes, or were skipped for reads, this would be
        // `Some("2")`.
        assert_eq!(
            sm.apply(&read).unwrap(),
            Response::Value(Some(b"1".to_vec()))
        );
        // A new request from the same client does see the new value.
        assert_eq!(
            sm.apply(&request(1, 3, get("a"))).unwrap(),
            Response::Value(Some(b"2".to_vec()))
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
    fn sequence_gaps_are_applied_and_lower_sequences_are_stale() {
        let mut sm = StateMachine::new(MemEngine::new());
        assert!(sm.apply(&request(1, 1, cas_create("a", "1"))).is_ok());
        // A gap is fine: only the client's latest sequence is tracked.
        assert!(sm.apply(&request(1, 5, cas_create("b", "2"))).is_ok());

        assert!(matches!(
            sm.apply(&request(1, 3, cas_create("c", "3"))),
            Err(Error::StaleRequest {
                client_id: 1,
                sequence: 3
            })
        ));
        // The rejected request never reached the engine.
        assert_eq!(
            sm.apply(&request(2, 1, get("c"))).unwrap(),
            Response::Value(None)
        );
    }

    #[test]
    fn first_request_from_an_unknown_client_accepts_any_sequence() {
        // Intentional: a sequence is only compared against a session that has
        // already been recorded, so a client's first request may carry any
        // sequence, including 0. Sequence 0 is then tracked like any other.
        let mut sm = StateMachine::new(MemEngine::new());
        assert!(sm.apply(&request(1, 0, cas_create("a", "1"))).is_ok());
        assert!(sm.apply(&request(2, 9, cas_create("b", "2"))).is_ok());

        assert_eq!(
            sm.apply(&request(1, 0, cas_create("a", "1"))).unwrap(),
            Response::Cas {
                success: true,
                current: Some(b"1".to_vec())
            }
        );
    }

    #[test]
    fn reusing_a_sequence_with_a_different_command_is_rejected() {
        let mut sm = StateMachine::new(MemEngine::new());
        assert_eq!(
            sm.apply(&request(1, 1, cas_create("a", "1"))).unwrap(),
            Response::Cas {
                success: true,
                current: Some(b"1".to_vec())
            }
        );

        let conflicting = request(1, 1, put("a", "DIFFERENT"));
        assert!(matches!(
            sm.apply(&conflicting),
            Err(Error::ConflictingRequest {
                client_id: 1,
                sequence: 1
            })
        ));
        // The conflicting command was not applied.
        assert_eq!(
            sm.apply(&request(2, 1, get("a"))).unwrap(),
            Response::Value(Some(b"1".to_vec()))
        );
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
        assert_eq!(
            restored.apply(&request(2, 1, get("a"))).unwrap(),
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
    fn snapshot_round_trip_is_byte_identical() {
        let mut original = StateMachine::new(MemEngine::new());
        original
            .apply(&request(1, 1, cas_create("b", "2")))
            .unwrap();
        original.apply(&request(2, 1, put("a", "1"))).unwrap();
        original.apply(&request(1, 2, get("a"))).unwrap();
        let first = original.snapshot().unwrap();

        let mut restored = StateMachine::new(MemEngine::new());
        restored.restore(&first).unwrap();

        // Sessions and engine must both re-snapshot byte for byte.
        assert_eq!(restored.snapshot().unwrap(), first);
    }

    #[test]
    fn empty_snapshot_round_trips() {
        let empty = StateMachine::new(MemEngine::new());
        let bytes = empty.snapshot().unwrap();

        let mut restored = StateMachine::new(MemEngine::new());
        restored.restore(&bytes).unwrap();

        assert_eq!(restored.snapshot().unwrap(), bytes);
        assert_eq!(
            restored.apply(&request(1, 1, get("a"))).unwrap(),
            Response::Value(None)
        );
    }

    #[test]
    fn restore_replaces_the_whole_state() {
        let mut target = StateMachine::new(MemEngine::new());
        target.apply(&request(1, 1, put("old", "x"))).unwrap();

        let mut source = StateMachine::new(MemEngine::new());
        source.apply(&request(1, 1, put("keep", "y"))).unwrap();
        let bytes = source.snapshot().unwrap();

        target.restore(&bytes).unwrap();

        assert_eq!(
            target.apply(&request(2, 1, get("old"))).unwrap(),
            Response::Value(None)
        );
        assert_eq!(
            target.apply(&request(2, 2, get("keep"))).unwrap(),
            Response::Value(Some(b"y".to_vec()))
        );
    }

    #[test]
    fn restore_keeps_engine_and_sessions_when_inner_bytes_are_corrupt() {
        let mut sm = StateMachine::new(MemEngine::new());
        let create = request(1, 4, cas_create("live", "v"));
        sm.apply(&create).unwrap();

        // The outer struct decodes, but the nested engine bytes are not a valid
        // `MemEngine` snapshot, so the failure happens inside `engine.restore`
        // *after* the outer decode succeeded.
        let broken = Snapshot {
            engine: b"garbage".to_vec(),
            sessions: BTreeMap::new(),
        };
        let bytes = bincode::serde::encode_to_vec(&broken, bincode::config::standard()).unwrap();

        assert!(sm.restore(&bytes).is_err());

        // Engine half: read through a different client id, which no session can
        // answer, so this really goes to the engine.
        assert_eq!(
            sm.apply(&request(2, 1, get("live"))).unwrap(),
            Response::Value(Some(b"v".to_vec()))
        );
        // Session half: the retry returns the saved response rather than
        // failing the CAS a second time.
        assert_eq!(
            sm.apply(&create).unwrap(),
            Response::Cas {
                success: true,
                current: Some(b"v".to_vec())
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
        // The engine half, read through a client id with no session.
        assert_eq!(
            sm.apply(&request(2, 1, get("a"))).unwrap(),
            Response::Value(Some(b"1".to_vec()))
        );
    }

    #[test]
    fn restore_rejects_truncated_data_and_keeps_state() {
        let mut source = StateMachine::new(MemEngine::new());
        source.apply(&request(1, 1, cas_create("a", "1"))).unwrap();
        let bytes = source.snapshot().unwrap();

        let mut target = StateMachine::new(MemEngine::new());
        assert!(matches!(
            target.restore(&bytes[..bytes.len() - 1]),
            Err(Error::Decode(_))
        ));
        // Nothing was applied: a fresh machine has the same snapshot bytes.
        assert_eq!(
            target.snapshot().unwrap(),
            StateMachine::new(MemEngine::new()).snapshot().unwrap()
        );
    }

    #[test]
    fn restore_rejects_trailing_bytes_and_keeps_state() {
        let mut source = StateMachine::new(MemEngine::new());
        source.apply(&request(1, 1, cas_create("a", "1"))).unwrap();
        let mut bytes = source.snapshot().unwrap();
        bytes.extend_from_slice(b"\x00\x00");

        let mut target = StateMachine::new(MemEngine::new());
        assert!(matches!(
            target.restore(&bytes),
            Err(Error::TrailingBytes { extra: 2 })
        ));
        // Neither the engine nor the session table was touched.
        assert_eq!(
            target.snapshot().unwrap(),
            StateMachine::new(MemEngine::new()).snapshot().unwrap()
        );
    }
}
