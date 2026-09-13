//! Network layer for Raft-KV consensus communication.

pub mod client;
pub mod codec;
pub mod server;
pub mod transport;

// Re-export main types
pub use client::RpcClient;
pub use server::RpcServer;
pub use transport::{Transport, TransportPair};

// Re-export message types from core
pub use raft_kv_core::message::*;

// Define NetworkError locally or re-export from core if available
// Since we moved messages to core, let's define NetworkError here for network-specific issues
use thiserror::Error;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Timeout error")]
    Timeout,
}

// Re-export RaftError for consistency if needed, but NetworkError is preferred here
pub use raft_kv_core::error::RaftError;
