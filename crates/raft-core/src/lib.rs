//! Pure Raft consensus state machine. Performs no I/O: no sockets, files,
//! threads or wall-clock time.

mod config;
mod log;
mod message;
mod raft;
mod ready;
mod types;

pub use config::Config;
pub use log::RaftLog;
pub use message::Message;
pub use raft::{Raft, Role};
pub use ready::Ready;
pub use types::{Entry, HardState, Index, NodeId, Term};
