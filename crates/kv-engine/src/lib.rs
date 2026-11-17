//! Deterministic key-value state machine for Jatayu.
//!
//! Every node in a Jatayu cluster runs this state machine over the same
//! committed log, so applying the same commands in the same order to an empty
//! state machine must produce the same state **and the same snapshot bytes** on
//! every node. Snapshots are compared and transferred between nodes, so
//! byte-for-byte equality is part of the contract, not a nicety: nothing here
//! may depend on hash iteration order, the wall clock, or randomness.
//!
//! # How the pieces fit
//!
//! - [`Command`] and [`Response`] are the replicated payload: a [`Command`]
//!   goes into the log, and the [`Response`] it produces goes back to the
//!   client.
//! - [`Engine`] is the pluggable store that performs commands. It never decides
//!   what is committed; the caller applies only committed entries.
//! - [`MemEngine`] is the reference implementation: a
//!   [`BTreeMap`](std::collections::BTreeMap) behind the [`Engine`] trait.
//! - [`StateMachine`] wraps an [`Engine`] and adds client request
//!   deduplication plus snapshotting of the session table, so a request that is
//!   retried through the log is not applied twice.
//! - [`ClientRequest`] is what goes into a Raft log entry: a [`Command`] tagged
//!   with the client id and sequence number used for deduplication.
//!
//! # Encoding and compatibility
//!
//! [`Command`], [`Response`] and [`ClientRequest`] are serialized with
//! `bincode`, and those bytes are the replicated log format. Enum variants are
//! encoded by their declaration index, so **only append new variants at the end
//! of [`Command`] and [`Response`]**: reordering or inserting a variant changes
//! the bytes of every existing log entry. There is no version or magic prefix
//! in a log entry or a snapshot yet, so a decoder cannot recognise data written
//! by an incompatible build. Decoding is strict — [`ClientRequest::decode`],
//! [`Engine::restore`] and [`StateMachine::restore`] reject input that has
//! unconsumed trailing bytes.
//!
//! # Known limitations
//!
//! - A session keeps only the *latest* response for a client, so a duplicate
//!   older than that is rejected with [`Error::StaleRequest`] instead of being
//!   answered.
//! - The session table never expires and is snapshotted in full, so it grows
//!   with the number of clients ever seen, not with the data stored.
//! - A client whose sequence counter restarts — a reconnect that reuses its id,
//!   for example — is stuck, because every request looks stale until session
//!   expiry exists.
//! - [`Engine::restore`] replaces the whole state; keys absent from the
//!   snapshot are discarded rather than merged.
//! - [`StateMachine::restore`] is atomic only because [`Engine::restore`] is:
//!   it restores the engine before it touches the session table.

mod command;
mod engine;
mod error;
mod memory;
mod request;
mod response;
mod state_machine;

pub use command::Command;
pub use engine::Engine;
pub use error::{Error, Result};
pub use memory::MemEngine;
pub use request::{ClientId, ClientRequest};
pub use response::Response;
pub use state_machine::StateMachine;
