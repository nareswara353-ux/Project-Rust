use crate::error::Result;
use crate::types::{LogEntry, NodeId, State};

pub struct RaftNode {
    pub id: NodeId,
    pub state: State,
    pub current_term: u64,
    pub log: Vec<LogEntry>,
}

impl RaftNode {
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            state: State::Follower,
            current_term: 0,
            log: Vec::new(),
        }
    }

    pub fn tick(&mut self) -> Result<()> {
        Ok(())
    }
}
