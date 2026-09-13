//! Persistent storage layer for Raft-KV.
//!
//! This crate provides the durable storage components required by a Raft node:
//! - **Write-Ahead Log (WAL)**: Ensures log entries are persisted before being applied.
//! - **Snapshot Store**: Manages state snapshots for log compaction and fast recovery.
//! - **Key-Value Store**: The in-memory state machine that holds the application data.

pub mod snapshot;
pub mod store;
pub mod wal;

pub use snapshot::Snapshot;
pub use store::KeyValueStore;
pub use wal::WriteAheadLog;
