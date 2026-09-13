use raft_kv_core::{Index, LogEntry, RaftError, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const SEGMENT_PREFIX: &str = "segment_";
const CHECKSUM_BYTES: usize = 4;
const LENGTH_BYTES: usize = 4;

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
            .map_err(|e| RaftError::StorageError(format!("Create dir error: {}", e)))?;

        let mut wal = Self {
            base_path: path,
            current_segment_id: 0,
            entries_count: 0,
            current_file: None,
            current_file_size: 0,
        };

        wal.recover()?;
        Ok(wal)
    }

    fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.base_path
            .join(format!("{}{:020}", SEGMENT_PREFIX, segment_id))
    }

    fn list_segment_ids(&self) -> Result<Vec<u64>> {
        let mut ids = Vec::new();
        if let Ok(read_dir) = fs::read_dir(&self.base_path) {
            for entry in read_dir.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if let Some(id_str) = name.strip_prefix(SEGMENT_PREFIX) {
                        if let Ok(id) = id_str.parse::<u64>() {
                            ids.push(id);
                        }
                    }
                }
            }
        }
        ids.sort();
        Ok(ids)
    }

    fn recover(&mut self) -> Result<()> {
        let ids = self.list_segment_ids()?;
        if ids.is_empty() {
            self.current_segment_id = 0;
            self.entries_count = 0;
            self.open_current_segment()?;
            return Ok(());
        }

        self.current_segment_id = *ids.last().unwrap();
        let mut total = 0;
        for id in ids {
            let path = self.segment_path(id);
            let entries = self.read_entries_from_path(&path)?;
            total += entries.len() as u64;
        }
        self.entries_count = total;
        self.open_current_segment()?;
        Ok(())
    }

    fn open_current_segment(&mut self) -> Result<()> {
        let path = self.segment_path(self.current_segment_id);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|e| RaftError::StorageError(format!("Open segment error: {}", e)))?;

        let metadata = file
            .metadata()
            .map_err(|e| RaftError::StorageError(format!("Metadata error: {}", e)))?;

        self.current_file_size = metadata.len();
        self.current_file = Some(BufWriter::new(file));
        Ok(())
    }

    fn read_entries_from_path(&self, path: &Path) -> Result<Vec<LogEntry>> {
        let file = File::open(path)
            .map_err(|e| RaftError::StorageError(format!("Open read error: {}", e)))?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut len_bytes = [0u8; LENGTH_BYTES];
            match reader.read_exact(&mut len_bytes) {
                Ok(()) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(RaftError::StorageError(format!("Read length error: {}", e)))
                }
            }

            let record_len = u32::from_be_bytes(len_bytes) as usize;
            if record_len == 0 {
                break;
            }

            let mut checksum_bytes = [0u8; CHECKSUM_BYTES];
            if reader.read_exact(&mut checksum_bytes).is_err() {
                break;
            }
            let checksum = u32::from_be_bytes(checksum_bytes);

            let mut entry_bytes = vec![0u8; record_len];
            if reader.read_exact(&mut entry_bytes).is_err() {
                break;
            }

            if crc32fast::hash(&entry_bytes) != checksum {
                break;
            }

            let entry: LogEntry = match bincode::deserialize(&entry_bytes) {
                Ok(e) => e,
                Err(_) => break,
            };
            entries.push(entry);
        }

        Ok(entries)
    }

    pub fn append(&mut self, entry: LogEntry) -> Result<Index> {
        let entry_bytes = bincode::serialize(&entry)
            .map_err(|e| RaftError::StorageError(format!("Serialization error: {}", e)))?;

        if entry_bytes.len() > u32::MAX as usize {
            return Err(RaftError::StorageError("Entry too large".into()));
        }

        let len = entry_bytes.len() as u32;
        let checksum = crc32fast::hash(&entry_bytes);

        if let Some(writer) = &mut self.current_file {
            writer
                .write_all(&len.to_be_bytes())
                .map_err(|e| RaftError::StorageError(format!("Write length error: {}", e)))?;
            writer
                .write_all(&checksum.to_be_bytes())
                .map_err(|e| RaftError::StorageError(format!("Write checksum error: {}", e)))?;
            writer
                .write_all(&entry_bytes)
                .map_err(|e| RaftError::StorageError(format!("Write entry error: {}", e)))?;
            writer
                .flush()
                .map_err(|e| RaftError::StorageError(format!("Flush error: {}", e)))?;
        } else {
            return Err(RaftError::StorageError("WAL file not open".into()));
        }

        self.current_file_size += (LENGTH_BYTES + CHECKSUM_BYTES + entry_bytes.len()) as u64;
        self.entries_count += 1;
        Ok(self.entries_count)
    }

    pub fn get(&self, index: Index) -> Option<LogEntry> {
        if index == 0 || index > self.entries_count {
            return None;
        }

        let ids = self.list_segment_ids().ok()?;
        let mut current_index = 0;

        for id in ids {
            let path = self.segment_path(id);
            let entries = self.read_entries_from_path(&path).ok()?;
            for entry in entries {
                current_index += 1;
                if current_index == index {
                    return Some(entry);
                }
            }
        }

        None
    }

    pub fn truncate_after(&mut self, index: Index) -> Result<()> {
        if index > self.entries_count {
            return Err(RaftError::StorageError("Index out of bounds".into()));
        }

        let ids = self.list_segment_ids()?;
        let mut entries_to_keep = Vec::new();
        let mut count = 0;

        for id in ids {
            let path = self.segment_path(id);
            let entries = self.read_entries_from_path(&path)?;
            for entry in entries {
                if count >= index {
                    break;
                }
                entries_to_keep.push(entry);
                count += 1;
            }
            if count >= index {
                break;
            }
        }

        for id in self.list_segment_ids()? {
            let path = self.segment_path(id);
            if path.exists() {
                fs::remove_file(&path)
                    .map_err(|e| RaftError::StorageError(format!("Remove segment error: {}", e)))?;
            }
        }

        self.current_segment_id = 0;
        self.entries_count = 0;
        self.current_file = None;
        self.current_file_size = 0;
        self.open_current_segment()?;

        for entry in entries_to_keep {
            self.append(entry)?;
        }

        self.sync()?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        if let Some(writer) = &mut self.current_file {
            writer
                .flush()
                .map_err(|e| RaftError::StorageError(format!("Sync flush error: {}", e)))?;
            writer
                .get_ref()
                .sync_all()
                .map_err(|e| RaftError::StorageError(format!("Sync error: {}", e)))?;
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