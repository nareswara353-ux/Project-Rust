use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Timeout")]
    Timeout,
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

pub mod client;
pub mod codec;
pub mod server;
pub mod transport;

pub use client::RpcClient;
pub use server::RpcServer;
pub use transport::{Transport, TransportPair};
