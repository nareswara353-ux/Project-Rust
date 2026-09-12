use crate::message::{LogEntry, Snapshot};
use std::error::Error;

pub type StorageResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[async_trait::async_trait]
pub trait Storage: Send + Sync {
    async fn get_last_log_index(&self) -> StorageResult<u64>;
    async fn get_log_entry(&self, index: u64) -> StorageResult<Option<LogEntry>>;
    async fn append_entries(&self, entries: &[LogEntry]) -> StorageResult<()>;
    async fn delete_from(&self, from: u64) -> StorageResult<()>;
    async fn get_current_term(&self) -> StorageResult<u64>;
    async fn set_current_term(&self, term: u64) -> StorageResult<()>;
    async fn get_voted_for(&self) -> StorageResult<Option<u64>>;
    async fn set_voted_for(&self, voted_for: Option<u64>) -> StorageResult<()>;
    async fn save_snapshot(&self, snapshot: &Snapshot) -> StorageResult<()>;
    async fn get_snapshot(&self) -> StorageResult<Option<Snapshot>>;
    async fn get_earliest_log_index(&self) -> StorageResult<u64>;
}
