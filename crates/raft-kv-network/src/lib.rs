use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("IO error: {0}")]
    Io(String),
    #[error("Timeout")]
    Timeout,
}

impl From<std::io::Error> for NetworkError {
    fn from(err: std::io::Error) -> Self {
        NetworkError::Io(err.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcMessage {
    RequestVote(raft_kv_core::message::RequestVoteRequest),
    RequestVoteResponse(raft_kv_core::message::RequestVoteResponse),
    AppendEntries(raft_kv_core::message::AppendEntriesRequest),
    AppendEntriesResponse(raft_kv_core::message::AppendEntriesResponse),
    InstallSnapshot(raft_kv_core::message::InstallSnapshotRequest),
    InstallSnapshotResponse(raft_kv_core::message::InstallSnapshotResponse),
}

pub use raft_kv_core::message::*;
