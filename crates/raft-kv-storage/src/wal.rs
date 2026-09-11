use raft_kv_core::{LogEntry, Index, Result, RaftError};

pub struct WriteAheadLog {
    path: String,
    entries: Vec<LogEntry>,
}

impl WriteAheadLog {
    pub fn new(path: &str) -> Result<Self> {
        Ok(Self {
            path: path.to_string(),
            entries: Vec::new(),
        })
    }

    pub fn append(&mut self, entry: LogEntry) -> Result<Index> {
        self.entries.push(entry);
        Ok(self.entries.len() as Index)
    }

    pub fn get(&self, index: Index) -> Option<&LogEntry> {
        self.entries.get((index - 1) as usize)
    }
}
