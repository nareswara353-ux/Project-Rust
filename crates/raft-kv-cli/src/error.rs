use raft_kv_core::RaftError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CliError {
    #[error("Configuration error: {0}")]
    Config(#[from] crate::config::ConfigError),

    #[error("Storage initialization failed: {0}")]
    StorageInit(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("Consensus error: {0}")]
    Consensus(#[from] RaftError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Runtime error: {0}")]
    Runtime(String),
}

pub type Result<T> = std::result::Result<T, CliError>;
