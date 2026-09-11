use raft_kv_core::{Index, LogEntry, Result, RaftError};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const SEGMENT_PREFIX: &str = "segment_";
const CHECKSUM_BYTES: usize = 4;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalRecord {
    entry: LogEntry,
    checksum: u32,
}

pub struct WriteAheadLog {
    base_path: PathBuf,
    current_segment: u64,
    current_file: Option<BufWriter<File>>,
    entries_count: u64,
}

impl WriteAheadLog {
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&path).map_err(|e| RaftError::Storage(e.to_string()))?;

        let mut wal = Self {
            base_path: path,
            current_segment: 0,
            current_file: None,
            entries_count: 0,
        };

        wal.open_current_segment()?;
        Ok(wal)
    }

    fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.base_path.join(format!("{}{:020}", SEGMENT_PREFIX, segment_id))
    }

    fn open_current_segment(&mut self) -> Result<()> {
        let path = self.segment_path(self.current_segment);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| RaftError::Storage(e.to_string()))?;

        self.current_file = Some(BufWriter::new(file));
        Ok(())
    }

    fn compute_checksum(entry: &LogEntry) -> u32 {
        let bytes = bincode::serialize(entry).unwrap_or_default();
        crc32fast::hash(&bytes)
    }

    pub fn append(&mut self, entry: LogEntry) -> Result<Index> {
        let checksum = Self::compute_checksum(&entry);
        let record = WalRecord { entry, checksum };

        let bytes = bincode::serialize(&record)
            .map_err(|e| RaftError::Storage(format!("Serialization error: {}", e)))?;

        if let Some(writer) = &mut self.current_file {
            writer.write_all(&bytes)
                .map_err(|e| RaftError::Storage(format!("Write error: {}", e)))?;
            writer.flush()
                .map_err(|e| RaftError::Storage(format!("Flush error: {}", e)))?;
        } else {
            return Err(RaftError::Storage("WAL file not open".into()));
        }

        self.entries_count += 1;
        Ok(self.entries_count)
    }

    pub fn read_all(&self) -> Result<Vec<LogEntry>> {
        let mut entries = Vec::new();
        let mut segment_id = 0;

        loop {
            let path = self.segment_path(segment_id);
            if !path.exists() {
                break;
            }

            let file = File::open(&path)
                .map_err(|e| RaftError::Storage(format!("Open segment error: {}", e)))?;

            let mut reader = BufReader::new(file);
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer)
                .map_err(|e| RaftError::Storage(format!("Read segment error: {}", e)))?;

            let mut offset = 0;
            while offset < buffer.len() {
                if offset + CHECKSUM_BYTES > buffer.len() {
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

                let record_bytes = &buffer[offset + CHECKSUM_BYTES..offset + CHECKSUM_BYTES + record_size];
                let record: WalRecord = bincode::deserialize(record_bytes)
                    .map_err(|e| RaftError::Storage(format!("Deserialize error: {}", e)))?;

                let computed_checksum = Self::compute_checksum(&record.entry);
                if computed_checksum != record.checksum {
                    return Err(RaftError::Storage("Checksum mismatch detected".into()));
                }

                entries.push(record.entry);
                offset += CHECKSUM_BYTES + record_size;
            }

            segment_id += 1;
        }

        Ok(entries)
    }

    pub fn truncate_after(&mut self, index: Index) -> Result<()> {
        let mut all_entries = self.read_all()?;
        if index >= all_entries.len() as u64 {
            return Ok(());
        }

        all_entries.truncate(index as usize);

        self.current_file = None;
        for i in 0..=self.current_segment {
            let path = self.segment_path(i);
            if path.exists() {
                fs::remove_file(&path)
                    .map_err(|e| RaftError::Storage(format!("Remove segment error: {}", e)))?;
            }
        }

        self.current_segment = 0;
        self.entries_count = 0;
        self.open_current_segment()?;

        for entry in all_entries {
            self.append(entry)?;
        }

        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        if let Some(writer) = &mut self.current_file {
            writer.sync_all()
                .map_err(|e| RaftError::Storage(format!("Sync error: {}", e)))?;
        }
        Ok(())
    }
}

impl Drop for WriteAheadLog {
    fn drop(&mut self) {
        if let Some(writer) = &mut self.current_file {
            let _ = writer.flush();
        }
    }
}
