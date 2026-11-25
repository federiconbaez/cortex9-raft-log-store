//! Application layer: drives `RaftCore` and executes its effects.

pub mod node;
pub mod ticker;

pub use node::{NodeError, RaftNode};
pub use ticker::Ticker;
