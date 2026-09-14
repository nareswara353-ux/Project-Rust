//! CLI library for Raft-KV distributed store.
//!
//! This crate provides the configuration parsing, error handling,
//! and runtime orchestration for running a Raft-KV node.

pub mod config;
pub mod error;
pub mod runtime;

// Re-export main types for convenience
pub use config::{CliConfig, ConfigError};
pub use error::CliError;
pub use runtime::Runtime;
