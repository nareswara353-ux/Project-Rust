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
        // Hitung ulang jumlah entri dari file yang ada
        wal.entries_count = wal.count_entries_from_disk()?;
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

    fn count_entries_from_disk(&self) -> Result<u64> {
        let path = self.segment_path(self.current_segment_id);
        if !path.exists() {
            return Ok(0);
        }

        let file = File::open(&path)
            .map_err(|e| RaftError::Storage(format!("Open count error: {}", e)))?;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader
            .read_to_end(&mut buffer)
            .map_err(|e| RaftError::Storage(format!("Read count error: {}", e)))?;

        let mut offset = 0;
        let mut count = 0;

        while offset < buffer.len() {
            if offset + LENGTH_BYTES > buffer.len() {
                break;
            }
            let len_bytes: [u8; LENGTH_BYTES] = buffer[offset..offset + LENGTH_BYTES]
                .try_into()
                .map_err(|_| RaftError::Storage("Invalid length bytes".into()))?;
            let record_len = u32::from_be_bytes(len_bytes) as usize;
            offset += LENGTH_BYTES;

            if offset + CHECKSUM_BYTES + record_len > buffer.len() {
                break;
            }
            offset += CHECKSUM_BYTES + record_len;
            count += 1;
        }

        Ok(count)
    }

    pub fn append(&mut self, entry: LogEntry) -> Result<Index> {
        let entry_bytes = bincode::serialize(&entry)
            .map_err(|e| RaftError::Storage(format!("Serialization error: {}", e)))?;

        let record = WalRecord {
            checksum: crc32fast::hash(&entry_bytes),
            entry,
        };

        let record_data = bincode::serialize(&record)
            .map_err(|e| RaftError::Storage(format!("Record serialization error: {}", e)))?;

        let record_len = record_data.len() as u32;
        let len_bytes = record_len.to_be_bytes();

        if let Some(writer) = &mut self.current_file {
            writer
                .write_all(&len_bytes)
                .map_err(|e| RaftError::Storage(format!("Write length error: {}", e)))?;
            writer
                .write_all(&record_data)
                .map_err(|e| RaftError::Storage(format!("Write data error: {}", e)))?;
            writer
                .flush()
                .map_err(|e| RaftError::Storage(format!("Flush error: {}", e)))?;

            self.current_file_size += (LENGTH_BYTES + CHECKSUM_BYTES + record_data.len()) as u64;
        } else {
            return Err(RaftError::Storage("WAL file not open".into()));
        }

        self.entries_count += 1;
        Ok(self.entries_count)
    }

    pub fn get(&self, index: Index) -> Result<Option<LogEntry>> {
        if index == 0 || index > self.entries_count {
            return Ok(None);
        }

        let segment_path = self.segment_path(self.current_segment_id);
        if !segment_path.exists() {
            return Ok(None);
        }

        let file = File::open(&segment_path)
            .map_err(|e| RaftError::Storage(format!("Open get error: {}", e)))?;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader
            .read_to_end(&mut buffer)
            .map_err(|e| RaftError::Storage(format!("Read get error: {}", e)))?;

        let mut offset = 0;
        let mut current_index = 0;

        while offset < buffer.len() {
            if offset + LENGTH_BYTES > buffer.len() {
                break;
            }
            let len_bytes: [u8; LENGTH_BYTES] = buffer[offset..offset + LENGTH_BYTES]
                .try_into()
                .map_err(|_| RaftError::Storage("Invalid length bytes in get".into()))?;
            let record_len = u32::from_be_bytes(len_bytes) as usize;
            offset += LENGTH_BYTES;

            if offset + CHECKSUM_BYTES + record_len > buffer.len() {
                break;
            }

            current_index += 1;

            if current_index == index {
                let checksum_bytes: [u8; CHECKSUM_BYTES] = buffer[offset..offset + CHECKSUM_BYTES]
                    .try_into()
                    .map_err(|_| RaftError::Storage("Invalid checksum bytes".into()))?;
                let stored_checksum = u32::from_be_bytes(checksum_bytes);

                let record_data =
                    &buffer[offset + CHECKSUM_BYTES..offset + CHECKSUM_BYTES + record_len];

                // Validasi Checksum
                let calculated_checksum = crc32fast::hash(record_data);
                if stored_checksum != calculated_checksum {
                    return Err(RaftError::Storage(format!(
                        "Checksum mismatch at index {}. Stored: {}, Calculated: {}",
                        index, stored_checksum, calculated_checksum
                    )));
                }

                let record: WalRecord = bincode::deserialize(record_data).map_err(|e| {
                    RaftError::Storage(format!("Deserialize error at index {}: {}", index, e))
                })?;

                return Ok(Some(record.entry));
            }

            offset += CHECKSUM_BYTES + record_len;
        }

        Ok(None)
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
                let len_bytes: [u8; LENGTH_BYTES] = buffer[offset..offset + LENGTH_BYTES]
                    .try_into()
                    .map_err(|_| RaftError::Storage("Invalid length in truncate".into()))?;
                let record_len = u32::from_be_bytes(len_bytes) as usize;
                offset += LENGTH_BYTES;

                if offset + CHECKSUM_BYTES + record_len > buffer.len() {
                    break;
                }

                current_index += 1;

                if current_index <= index {
                    // Simpan ulang length prefix + data
                    entries_to_keep.extend_from_slice(
                        &buffer[offset - LENGTH_BYTES..offset + CHECKSUM_BYTES + record_len],
                    );
                } else {
                    break;
                }

                offset += CHECKSUM_BYTES + record_len;
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
