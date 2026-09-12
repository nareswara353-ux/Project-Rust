use crate::message::{LogEntry, Snapshot};
use std::cmp::max;

pub type NodeId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

impl Role {
    pub fn is_leader(self) -> bool {
        matches!(self, Role::Leader)
    }
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: NodeId,
    pub addr: String,
}

impl Node {
    pub fn new(id: NodeId, addr: String) -> Self {
        Self { id, addr }
    }
}

#[derive(Debug)]
pub struct NodeState {
    pub id: NodeId,
    pub role: Role,
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub election_timeout_elapsed: bool,
}

impl NodeState {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            election_timeout_elapsed: false,
        }
    }

    pub fn become_leader(&mut self) {
        self.role = Role::Leader;
    }

    pub fn reset_election_timeout(&mut self) {
        self.election_timeout_elapsed = false;
    }

    pub fn get_entry(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return Some(&LogEntry {
                term: 0,
                index: 0,
                command: crate::message::Command::Noop,
            });
        }
        self.log.iter().find(|e| e.index == index)
    }

    pub fn match_log_entry(&self, index: u64, term: u64) -> bool {
        if let Some(entry) = self.get_entry(index) {
            entry.term == term
        } else {
            index == 0
        }
    }

    pub fn last_log_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }
}
