use thiserror::Error;

#[derive(Error, Debug)]
pub enum RaftError {
    #[error("Node is not the leader")]
    NotLeader,

    #[error("Request term {request_term} is stale, current term is {current_term}")]
    StaleTerm {
        request_term: u64,
        current_term: u64,
    },

    #[error(
        "Log mismatch at index {index}: local term {local_term:?}, remote term {remote_term:?}"
    )]
    LogMismatch {
        index: u64,
        local_term: Option<u64>,
        remote_term: Option<u64>,
    },

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Network error: {0}")]
    Network(String),

    #[error("State machine error: {0}")]
    StateMachine(String),

    #[error("Snapshot error: {0}")]
    Snapshot(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Operation timed out")]
    Timeout,

    #[error("Shutdown in progress")]
    Shutdown,
}

pub type Result<T> = std::result::Result<T, RaftError>;
