use bytes::Bytes;
use raft_kv_core::message::{
    AppendEntriesRequest, AppendEntriesResponse, Command, InstallSnapshotRequest,
    InstallSnapshotResponse, LogEntry, RequestVoteRequest, RequestVoteResponse, Snapshot,
};
use raft_kv_core::NodeId;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Connection failed: {0}")]
    Connection(String),
    #[error("Timeout")]
    Timeout,
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcMessage {
    RequestVote(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
    InstallSnapshot(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
}

pub mod client;
pub mod codec;
pub mod server;
pub mod transport;

pub use client::RpcClient;
pub use server::RpcServer;
pub use transport::{Transport, TransportPair};

pub use raft_kv_core::error::RaftError;
pub use raft_kv_core::message::{Command, LogEntry, Snapshot};
