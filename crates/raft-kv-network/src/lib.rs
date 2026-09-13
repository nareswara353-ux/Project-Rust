//! Network layer for Raft-KV consensus communication.

pub mod client;
pub mod codec;
pub mod server;
pub mod transport;

use raft_kv_core::{
    InstallSnapshotRequest, InstallSnapshotResponse, LogEntry, RequestVoteRequest,
    RequestVoteResponse,
};
use serde::{Deserialize, Serialize};

// Re-export main types
pub use client::RpcClient;
pub use server::RpcServer;
pub use transport::{Transport, TransportPair};

// Re-export error types
pub use raft_kv_core::error::RaftError;

/// Network-specific error type
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

/// Main RPC message envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcMessage {
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    InstallSnapshotRequest(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    pub conflict_index: Option<u64>,
    pub conflict_term: Option<u64>,
}

// Re-export other message types if needed or define locally
pub use raft_kv_core::message::{Command, LogEntry, Snapshot};
