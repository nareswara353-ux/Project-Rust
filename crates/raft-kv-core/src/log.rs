use crate::message::LogEntry;
use crate::error::{RaftError, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicatedLog {
    entries: Vec<LogEntry>,
    commit_index: u64,
}

impl Default for ReplicatedLog {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicatedLog {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            commit_index: 0,
        }
    }

    pub fn append(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return Some(&LogEntry { term: 0, command: crate::message::Command::Noop });
        }
        let offset = (index - 1) as usize;
        self.entries.get(offset)
    }

    pub fn last_index(&self) -> u64 {
        if self.entries.is_empty() {
            return 0;
        }
        self.entries.last().unwrap().index
    }

    pub fn last_term(&self) -> u64 {
        if self.entries.is_empty() {
            return 0;
        }
        self.entries.last().unwrap().term
    }

    pub fn truncate_after(&mut self, index: u64) {
        self.entries.retain(|e| e.index <= index);
    }

    pub fn entries_from(&self, start_index: u64) -> Vec<LogEntry> {
        if start_index == 0 || self.entries.is_empty() {
            return self.entries.clone();
        }
        let start_offset = (start_index - 1) as usize;
        if start_offset >= self.entries.len() {
            return Vec::new();
        }
        self.entries[start_offset..].to_vec()
    }

    pub fn match_log(&self, prev_log_index: u64, prev_log_term: u64) -> bool {
        if prev_log_index == 0 {
            return true;
        }
        if let Some(entry) = self.get(prev_log_index) {
            return entry.term == prev_log_term;
        }
        false
    }

    pub fn set_commit_index(&mut self, index: u64) {
        if index > self.last_index() {
            self.commit_index = self.last_index();
        } else {
            self.commit_index = index;
        }
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
