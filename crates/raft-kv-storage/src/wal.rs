use raft_kv_core::{Index, LogEntry, RaftError, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const SEGMENT_PREFIX: &str = "segment_";
const CHECKSUM_BYTES: usize = 4;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalRecord {
    checksum: u32,
    entry: LogEntry,
}

pub struct WriteAheadLog {
    base_path: PathBuf,
    current_segment_id: u64,
    current_file: Option<BufWriter<File>>,
    entries_count: u64,
}

impl WriteAheadLog {
    pub fn new(path: &str) -> Result<Self> {
        let base_path = PathBuf::from(path);
        fs::create_dir_all(&base_path)
            .map_err(|e| RaftError::Storage(format!("Create dir error: {}", e)))?;

        let mut wal = Self {
            base_path,
            current_segment_id: 0,
            current_file: None,
            entries_count: 0,
        };

        wal.open_current_segment()?;
        Ok(wal)
    }

    fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.base_path
            .join(format!("{}{:020}", SEGMENT_PREFIX, segment_id))
    }

    fn open_current_segment(&mut self) -> Result<()> {
        let path = self.segment_path(self.current_segment_id);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|e| RaftError::Storage(format!("Open segment error: {}", e)))?;

        self.current_file = Some(BufWriter::new(file));
        Ok(())
    }

    pub fn append(&mut self, entry: LogEntry) -> Result<Index> {
        let record = WalRecord {
            checksum: crc32fast::hash(
                &bincode::serialize(&entry)
                    .map_err(|e| RaftError::Storage(format!("Serialization error: {}", e)))?,
            ),
            entry,
        };

        let bytes = bincode::serialize(&record)
            .map_err(|e| RaftError::Storage(format!("Serialization error: {}", e)))?;

        if let Some(writer) = &mut self.current_file {
            writer
                .write_all(&bytes)
                .map_err(|e| RaftError::Storage(format!("Write error: {}", e)))?;
            writer
                .flush()
                .map_err(|e| RaftError::Storage(format!("Flush error: {}", e)))?;
        } else {
            return Err(RaftError::Storage("WAL file not open".into()));
        }

        self.entries_count += 1;
        Ok(self.entries_count)
    }

    pub fn get(&self, index: Index) -> Option<LogEntry> {
        if index == 0 || index > self.entries_count {
            return None;
        }

        let segment_id = 0;
        let path = self.segment_path(segment_id);
        if !path.exists() {
            return None;
        }

        let file = File::open(&path).ok()?;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader
            .read_to_end(&mut buffer)
            .map_err(|e| RaftError::Storage(format!("Read segment error: {}", e)))
            .ok()?;

        let mut offset = 0;
        let mut current_index = 1;

        while offset + CHECKSUM_BYTES < buffer.len() {
            if offset + 4 > buffer.len() {
                break;
            }
            let record_size = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;

            if offset + CHECKSUM_BYTES + record_size > buffer.len() {
                break;
            }

            if current_index == index {
                let record_bytes =
                    &buffer[offset + CHECKSUM_BYTES..offset + CHECKSUM_BYTES + record_size];
                let record: WalRecord = bincode::deserialize(record_bytes)
                    .map_err(|e| RaftError::Storage(format!("Deserialize error: {}", e)))
                    .ok()?;

                let computed_checksum = crc32fast::hash(record_bytes);
                if computed_checksum != record.checksum {
                    return None;
                }

                return Some(record.entry);
            }

            offset += CHECKSUM_BYTES + record_size;
            current_index += 1;
        }

        None
    }

    pub fn truncate_after(&mut self, index: Index) -> Result<()> {
        self.entries_count = index;
        let segment_id = 0;
        let path = self.segment_path(segment_id);

        let mut entries_to_keep = Vec::new();
        if path.exists() {
            let file = File::open(&path)
                .map_err(|e| RaftError::Storage(format!("Open truncation error: {}", e)))?;
            let mut reader = BufReader::new(file);
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer)?;

            let mut offset = 0;
            let mut current_index = 1;

            while offset + CHECKSUM_BYTES < buffer.len() {
                if offset + 4 > buffer.len() {
                    break;
                }
                let record_size = u32::from_le_bytes([
                    buffer[offset],
                    buffer[offset + 1],
                    buffer[offset + 2],
                    buffer[offset + 3],
                ]) as usize;

                if offset + CHECKSUM_BYTES + record_size > buffer.len() {
                    break;
                }

                if current_index <= index {
                    entries_to_keep
                        .extend_from_slice(&buffer[offset..offset + CHECKSUM_BYTES + record_size]);
                } else {
                    break;
                }

                offset += CHECKSUM_BYTES + record_size;
                current_index += 1;
            }
        }

        fs::write(&path, &entries_to_keep)
            .map_err(|e| RaftError::Storage(format!("Truncate write error: {}", e)))?;

        self.open_current_segment()?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        if let Some(writer) = &mut self.current_file {
            writer
                .flush()
                .map_err(|e| RaftError::Storage(format!("Flush sync error: {}", e)))?;
        }
        Ok(())
    }

    pub fn rotate(&mut self) -> Result<()> {
        self.sync()?;
        self.current_segment_id += 1;
        self.open_current_segment()?;
        Ok(())
    }
}
