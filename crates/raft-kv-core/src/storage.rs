use crate::message::{LogEntry, Snapshot};
use crate::error::{RaftError, Result};
use async_trait::async_trait;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn get_log_entry(&self, index: u64) -> Result<Option<LogEntry>>;
    
    async fn append_log_entry(&self, entry: &LogEntry) -> Result<()>;
    
    async fn append_log_entries(&self, entries: &[LogEntry]) -> Result<()>;
    
    async fn truncate_log_after(&self, index: u64) -> Result<()>;
    
    async fn get_term_at(&self, index: u64) -> Result<Option<u64>>;
    
    async fn get_last_log_index(&self) -> Result<u64>;
    
    async fn get_last_log_term(&self) -> Result<u64>;
    
    async fn save_snapshot(&self, snapshot: &Snapshot) -> Result<()>;
    
    async fn load_snapshot(&self) -> Result<Option<Snapshot>>;
    
    async fn get_current_term(&self) -> Result<u64>;
    
    async fn set_current_term(&self, term: u64) -> Result<()>;
    
    async fn get_voted_for(&self) -> Result<Option<u64>>;
    
    async fn set_voted_for(&self, candidate_id: Option<u64>) -> Result<()>;
}
