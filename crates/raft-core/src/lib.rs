//! Pure Raft consensus state machine. Performs no I/O: no sockets, files,
//! threads or wall-clock time.

mod log;
mod types;

pub use log::RaftLog;
pub use types::{Entry, HardState, Index, NodeId, Term};
