use crate::message::{LogEntry, Snapshot};
use crate::error::{RaftError, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type NodeId = u64;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower
    }
}

#[derive(Debug)]
pub struct NodeState {
    pub role: Role,
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub snapshot: Option<Snapshot>,
}

impl Default for NodeState {
    fn default() -> Self {
        Self {
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            snapshot: None,
        }
    }
}

impl NodeState {
    pub fn last_log_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    pub fn get_entry(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return Some(&LogEntry {
                index: 0,
                term: 0,
                command: crate::message::Command::Noop,
            });
        }
        self.log.iter().find(|e| e.index == index)
    }
}
