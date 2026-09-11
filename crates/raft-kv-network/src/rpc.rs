use raft_kv_core::{LogEntry, NodeId, Term};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    AppendEntries {
        term: Term,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    Vote {
        term: Term,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: Term,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    AppendEntries { success: bool, term: Term },
    Vote { vote_granted: bool, term: Term },
}
