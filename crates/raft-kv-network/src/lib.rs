//! Network layer for Raft-KV consensus communication.

pub mod client;
pub mod codec;
pub mod server;
pub mod transport;

// Re-export core types needed across the network module
use bytes::Bytes;
pub use raft_kv_core::error::RaftError;
pub use raft_kv_core::message::{
    AppendEntriesRequest, AppendEntriesResponse, Command, InstallSnapshotRequest,
    InstallSnapshotResponse, LogEntry, RequestVoteRequest, RequestVoteResponse, Role,
};
use serde::{Deserialize, Serialize};

// Define RpcMessage locally or re-export if defined in core
// For this architecture, let's define it here as the network envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcMessage {
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    InstallSnapshotRequest(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
}

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

pub use client::RpcClient;
pub use server::RpcServer;
pub use transport::{Transport, TransportPair};
