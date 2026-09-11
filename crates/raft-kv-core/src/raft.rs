use crate::types::{Index, LogEntry, NodeId, State, Term, Command};
use std::time::{Duration, Instant};

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
    pub election_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub last_heartbeat: Instant,
    pub cluster_size: usize,
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
            election_timeout: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(100),
            last_heartbeat: Instant::now(),
            cluster_size,
        }
    }

    pub fn become_follower(&mut self, term: Term) {
        self.state = State::Follower;
        self.current_term = term;
        self.voted_for = None;
    }

    pub fn become_candidate(&mut self) {
        self.state = State::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
    }

    pub fn become_leader(&mut self) {
        self.state = State::Leader;
        let next_idx = self.last_log_index() + 1;
        self.next_index.fill(next_idx);
        self.match_index.fill(0);
    }

    pub fn last_log_index(&self) -> Index {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    pub fn last_log_term(&self) -> Term {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    pub fn get_entry(&self, index: Index) -> Option<&LogEntry> {
        if index == 0 {
            return Some(&LogEntry {
                term: 0,
                index: 0,
                command: Command::Noop,
            });
        }
        self.log.get((index - 1) as usize)
    }

    pub fn append_entry(&mut self, entry: LogEntry) {
        self.log.push(entry);
    }

    pub fn tick(&mut self) -> bool {
        self.last_heartbeat.elapsed() >= self.election_timeout
    }

    pub fn reset_election_timer(&mut self) {
        self.last_heartbeat = Instant::now();
    }
}
