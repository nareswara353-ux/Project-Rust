use crate::types::{NodeId, State, LogEntry, Term, Index};
use crate::error::{Result, RaftError};

pub struct RaftNode {
    pub id: NodeId,
    pub state: State,
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,
    pub commit_index: Index,
    pub last_applied: Index,
    pub next_index: Vec<Index>,
    pub match_index: Vec<Index>,
    pub cluster_nodes: usize,
}

impl RaftNode {
    pub fn new(id: NodeId, cluster_size: usize) -> Self {
        Self {
            id,
            state: State::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: vec![1; cluster_size],
            match_index: vec![0; cluster_size],
            cluster_nodes: cluster_size,
        }
    }

    pub fn become_follower(&mut self, term: Term) {
        self.state = State::Follower;
        self.current_term = term;
        self.voted_for = None;
    }

    pub fn become_candidate(&mut self) -> Term {
        self.state = State::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.current_term
    }

    pub fn become_leader(&mut self) {
        assert!(self.state.is_candidate());
        self.state = State::Leader;
        let cluster_size = self.cluster_nodes;
        self.next_index = vec![self.last_log_index() + 1; cluster_size];
        self.match_index = vec![0; cluster_size];
    }

    pub fn last_log_index(&self) -> Index {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    pub fn last_log_term(&self) -> Term {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    pub fn append_entry(&mut self, entry: LogEntry) -> Index {
        self.log.push(entry);
        self.log.last().unwrap().index
    }

    pub fn get_entry(&self, index: Index) -> Option<&LogEntry> {
        if index == 0 {
            return Some(&LogEntry { term: 0, index: 0, command: Command::Noop });
        }
        self.log.get((index - 1) as usize)
    }

    pub fn quorum_size(&self) -> usize {
        (self.cluster_nodes / 2) + 1
    }
}

use crate::types::Command;
