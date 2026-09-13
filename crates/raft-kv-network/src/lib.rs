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
pub use raft_kv_core::message::{
    AppendEntriesRequest, AppendEntriesResponse, Command, InstallSnapshotRequest,
    InstallSnapshotResponse, LogEntry, RequestVoteRequest, RequestVoteResponse, Snapshot,
};

// Define RpcMessage locally or re-export if defined in core
// For now, let's assume we define the envelope here or use a type alias if core has it.
// If core doesn't have RpcMessage enum, we define it here.
use raft_kv_core::message::*; // Ensure all message types are available

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RpcMessage {
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    InstallSnapshotRequest(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
}

// Define NetworkError locally
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

pub use raft_kv_core::error::RaftError;
