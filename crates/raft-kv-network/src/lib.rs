//! Network layer for Raft-KV consensus communication.

use serde::{Deserialize, Serialize};
use thiserror::Error;

// Re-export all message types from core to avoid duplication
pub use raft_kv_core::message::{
    AppendEntriesRequest, AppendEntriesResponse, Command, InstallSnapshotRequest,
    InstallSnapshotResponse, LogEntry, RequestVoteRequest, RequestVoteResponse, Snapshot,
};

// Define the unified RPC Message envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcMessage {
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    InstallSnapshotRequest(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
}

// Network-specific error type
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

pub mod client;
pub mod codec;
pub mod server;
pub mod transport;

pub use client::RpcClient;
pub use server::RpcServer;
pub use transport::{Transport, TransportPair};
