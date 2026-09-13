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

// Re-export error types
pub use raft_kv_core::error::RaftError;

// Define local NetworkError if needed, or rely on RaftError::Network
#[derive(Debug, thiserror::Error)]
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
