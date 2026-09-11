use thiserror::Error;

#[derive(Error, Debug)]
pub enum RaftError {
    #[error("node is not the leader")]
    NotLeader,

    #[error("term mismatch: local {local}, remote {remote}")]
    TermMismatch { local: u64, remote: u64 },

    #[error("log inconsistency at index {index}")]
    LogMismatch { index: u64 },

    #[error("storage failure: {0}")]
    Storage(String),

    #[error("network communication failed: {0}")]
    Network(String),

    #[error("invalid configuration: {0}")]
    Config(String),

    #[error("snapshot application failed: {0}")]
    Snapshot(String),

    #[error("operation timed out")]
    Timeout,

    #[error("node shut down")]
    ShutDown,
}

pub type Result<T> = std::result::Result<T, RaftError>;
