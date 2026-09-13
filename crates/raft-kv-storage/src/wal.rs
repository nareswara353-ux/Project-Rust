use raft_kv_core::{Index, LogEntry, RaftError, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

const SEGMENT_PREFIX: &str = "segment_";
const CHECKSUM_BYTES: usize = 4;
const LENGTH_BYTES: usize = 4;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalRecord {
    checksum: u32,
    entry: LogEntry,
}

pub struct WriteAheadLog {
    base_path: PathBuf,
    current_segment_id: u64,
    entries_count: u64,
    current_file: Option<BufWriter<File>>,
    current_file_size: u64,
}

impl WriteAheadLog {
    pub fn new(base_path: &str) -> Result<Self> {
        let path = PathBuf::from(base_path);
        fs::create_dir_all(&path)
            .map_err(|e| RaftError::Storage(format!("Create dir error: {}", e)))?;

        let mut wal = Self {
            base_path: path,
            current_segment_id: 0,
            entries_count: 0,
            current_file: None,
            current_file_size: 0,
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

        let metadata = file
            .metadata()
            .map_err(|e| RaftError::Storage(format!("Metadata error: {}", e)))?;

        self.current_file_size = metadata.len();
        self.current_file = Some(BufWriter::new(file));
        Ok(())
    }

    pub fn append(&mut self, entry: LogEntry) -> Result<Index> {
        let entry_bytes = bincode::serialize(&entry)
            .map_err(|e| RaftError::Storage(format!("Serialization error: {}", e)))?;

        let checksum = crc32fast::hash(&entry_bytes);
        let record = WalRecord { checksum, entry };

        let record_bytes = bincode::serialize(&record)
            .map_err(|e| RaftError::Storage(format!("Record serialization error: {}", e)))?;

        let len = record_bytes.len() as u32;
        let len_bytes = len.to_be_bytes();

        if let Some(writer) = &mut self.current_file {
            writer
                .write_all(&len_bytes)
                .map_err(|e| RaftError::Storage(format!("Write length error: {}", e)))?;
            writer
                .write_all(&record_bytes)
                .map_err(|e| RaftError::Storage(format!("Write record error: {}", e)))?;
            writer
                .flush()
                .map_err(|e| RaftError::Storage(format!("Flush error: {}", e)))?;
        } else {
            return Err(RaftError::Storage("WAL file not open".into()));
        }

        self.current_file_size += (LENGTH_BYTES + record_bytes.len()) as u64;
        self.entries_count += 1;
        Ok(self.entries_count)
    }

    pub fn get(&self, index: Index) -> Option<LogEntry> {
        if index == 0 || index > self.entries_count {
            return None;
        }

        let segment_path = self.segment_path(self.current_segment_id);
        if !segment_path.exists() {
            return None;
        }

        let file = File::open(&segment_path).ok()?;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();

        if reader.read_to_end(&mut buffer).is_err() {
            return None;
        }

        let mut offset = 0;
        let mut current_index = 0;

        while offset < buffer.len() {
            if offset + LENGTH_BYTES > buffer.len() {
                break;
            }

            let len_bytes: [u8; 4] = buffer[offset..offset + LENGTH_BYTES].try_into().ok()?;
            let record_len = u32::from_be_bytes(len_bytes) as usize;
            offset += LENGTH_BYTES;

            if offset + record_len > buffer.len() {
                break;
            }

            let record_bytes = &buffer[offset..offset + record_len];

            // Validasi checksum
            if record_len < CHECKSUM_BYTES {
                break;
            }

            let record: WalRecord = match bincode::deserialize(record_bytes) {
                Ok(r) => r,
                Err(_) => {
                    eprintln!("Corrupt entry at index {}", current_index + 1);
                    return None;
                }
            };

            let calculated_checksum = crc32fast::hash(&record_bytes[CHECKSUM_BYTES..]);
            if record.checksum != calculated_checksum {
                eprintln!("Checksum mismatch at index {}", current_index + 1);
                return None;
            }

            current_index += 1;

            if current_index == index {
                return Some(record.entry);
            }

            offset += record_len;
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
                .map_err(|e| RaftError::Storage(format!("Open truncate error: {}", e)))?;
            let mut reader = BufReader::new(file);
            let mut buffer = Vec::new();
            reader
                .read_to_end(&mut buffer)
                .map_err(|e| RaftError::Storage(format!("Read truncate error: {}", e)))?;

            let mut offset = 0;
            let mut current_index = 0;

            while offset < buffer.len() {
                if offset + LENGTH_BYTES > buffer.len() {
                    break;
                }

                let len_bytes: [u8; 4] = match buffer[offset..offset + LENGTH_BYTES].try_into() {
                    Ok(b) => b,
                    Err(_) => break,
                };
                let record_len = u32::from_be_bytes(len_bytes) as usize;

                if offset + LENGTH_BYTES + record_len > buffer.len() {
                    break;
                }

                current_index += 1;

                if current_index <= index {
                    let start = offset;
                    let end = offset + LENGTH_BYTES + record_len;
                    if end <= buffer.len() {
                        entries_to_keep.extend_from_slice(&buffer[start..end]);
                    }
                } else {
                    break;
                }

                offset += LENGTH_BYTES + record_len;
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
                .map_err(|e| RaftError::Storage(format!("Sync error: {}", e)))?;
        }
        Ok(())
    }

    pub fn len(&self) -> u64 {
        self.entries_count
    }

    pub fn is_empty(&self) -> bool {
        self.entries_count == 0
    }
}
