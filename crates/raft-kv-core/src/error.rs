use thiserror::Error;

#[derive(Error, Debug)]
pub enum RaftError {
    #[error("Node is not the leader")]
    NotLeader,
    #[error("Term mismatch: expected {0}, got {1}")]
    TermMismatch(u64, u64),
    #[error("Log mismatch at index {0}")]
    LogMismatch(u64),
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Snapshot error: {0}")]
    SnapshotError(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
}

pub type Result<T> = std::result::Result<T, RaftError>;
