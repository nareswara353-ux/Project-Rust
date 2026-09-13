//! Network layer for Raft-KV consensus communication.
//!
//! This crate handles all inter-node communication including:
//! - RPC message definitions (AppendEntries, RequestVote, InstallSnapshot)
//! - TCP Server for receiving requests
//! - TCP Client for sending requests with timeouts
//! - In-memory transport for testing

pub mod client;
pub mod codec;
pub mod server;
pub mod transport;

// Re-export main types
pub use client::RpcClient;
pub use server::RpcServer;
pub use transport::{Transport, TransportPair};

// Re-export message types
pub use raft_kv_core::error::RaftError;
pub use raft_kv_core::message::*;

// Re-export network-specific error if distinct (or use core's)
// For now, we rely on RaftError::Network variant
