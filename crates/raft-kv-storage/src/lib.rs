//! Persistent storage layer for Raft-KV.
//!
//! This crate provides the implementation of Write-Ahead Log (WAL),
//! Snapshot management, and the in-memory Key-Value Store with persistence capabilities.

pub mod snapshot;
pub mod store;
pub mod wal;

// Re-export main types for convenience
pub use raft_kv_core::message::Snapshot;
pub use store::KeyValueStore;
pub use wal::WriteAheadLog;

// Re-export error types if needed by users
pub use raft_kv_core::RaftError;
