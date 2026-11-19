//! Pure Raft consensus state machine. Performs no I/O: no sockets, files,
//! threads or wall-clock time.

mod types;

pub use types::{Entry, HardState, Index, NodeId, Term};
