use serde::{Deserialize, Serialize};

pub type NodeId = u64;
pub type Term = u64;
pub type Index = u64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    Noop,
    Put { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: Term,
    pub index: Index,
    pub command: Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum State {
    #[default]
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerState {
    Probe,
    Replicate,
    Snapshot,
}

impl State {
    pub fn is_leader(self) -> bool {
        matches!(self, State::Leader)
    }

    pub fn is_candidate(self) -> bool {
        matches!(self, State::Candidate)
    }

    pub fn is_follower(self) -> bool {
        matches!(self, State::Follower)
    }
}
