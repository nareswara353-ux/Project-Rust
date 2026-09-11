use thiserror::Error;

#[derive(Error, Debug)]
pub enum RaftError {
    #[error("Node not leader")]
    NotLeader,
    #[error("Term mismatch")]
    TermMismatch,
    #[error("Log mismatch")]
    LogMismatch,
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Network error: {0}")]
    Network(String),
}

pub type Result<T> = std::result::Result<T, RaftError>;
