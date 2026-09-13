use raft_kv_core::message::LogEntry;
use raft_kv_core::RaftError;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

const MAGIC_NUMBER: u32 = 0x52414654; // "RAFT"
const VERSION: u32 = 1;
const HEADER_SIZE: usize = 16; // magic(4) + version(4) + length(4) + checksum(4)

pub struct WriteAheadLog {
    dir: PathBuf,
    current_file: Option<File>,
    current_index: u64,
}

impl WriteAheadLog {
    pub fn new<P: Into<PathBuf>>(dir: P) -> Result<Self, RaftError> {
        let dir = dir.into();
        fs::create_dir_all(&dir)
            .map_err(|e| RaftError::Storage(format!("Create dir error: {}", e)))?;

        let mut wal = Self {
            dir,
            current_file: None,
            current_index: 0,
        };

        wal.open_or_create_segment(0)?;
        Ok(wal)
    }

    fn segment_path(&self, index: u64) -> PathBuf {
        self.dir.join(format!("segment_{:020}", index))
    }

    fn open_or_create_segment(&mut self, index: u64) -> Result<(), RaftError> {
        let path = self.segment_path(index);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(&path)
            .map_err(|e| RaftError::Storage(format!("Open segment error: {}", e)))?;

        // Validate or write header if new file
        let metadata = file
            .metadata()
            .map_err(|e| RaftError::Storage(format!("Metadata error: {}", e)))?;

        if metadata.len() == 0 {
            let mut header = [0u8; HEADER_SIZE];
            header[0..4].copy_from_slice(&MAGIC_NUMBER.to_be_bytes());
            header[4..8].copy_from_slice(&VERSION.to_be_bytes());
            // length and checksum are 0 for header
            file.write_all(&header)
                .map_err(|e| RaftError::Storage(format!("Write header error: {}", e)))?;
            file.sync_all()
                .map_err(|e| RaftError::Storage(format!("Sync header error: {}", e)))?;
        } else {
            // Validate existing header
            let mut read_file = File::open(&path)
                .map_err(|e| RaftError::Storage(format!("Open read error: {}", e)))?;
            let mut header = [0u8; HEADER_SIZE];
            read_file
                .read_exact(&mut header)
                .map_err(|e| RaftError::Storage(format!("Read header error: {}", e)))?;

            let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
            let version = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);

            if magic != MAGIC_NUMBER {
                return Err(RaftError::Storage("Invalid magic number in segment".into()));
            }
            if version != VERSION {
                return Err(RaftError::Storage("Unsupported segment version".into()));
            }
        }

        self.current_file = Some(file);
        self.current_index = index;
        Ok(())
    }

    pub fn append(&mut self, entry: &LogEntry) -> Result<(), RaftError> {
        let file = self
            .current_file
            .as_mut()
            .ok_or_else(|| RaftError::Storage("WAL file not open".into()))?;

        let entry_bytes = bincode::serialize(entry)
            .map_err(|e| RaftError::Storage(format!("Serialization error: {}", e)))?;

        let len = entry_bytes.len() as u32;
        let checksum = crc32fast::hash(&entry_bytes);

        // Write record: [length (4)][checksum (4)][data (len)]
        let mut record = Vec::with_capacity(HEADER_SIZE + entry_bytes.len());
        record.extend_from_slice(&len.to_be_bytes());
        record.extend_from_slice(&checksum.to_be_bytes());
        record.extend_from_slice(&entry_bytes);

        file.write_all(&record)
            .map_err(|e| RaftError::Storage(format!("Write record error: {}", e)))?;

        file.sync_all()
            .map_err(|e| RaftError::Storage(format!("Sync error: {}", e)))?;

        Ok(())
    }

    pub fn get(&mut self, index: u64) -> Result<Option<LogEntry>, RaftError> {
        // Simplified: In a real impl, we'd seek to the specific offset for this index.
        // For now, we iterate from the beginning of the current segment (or a known start).
        // This is inefficient but works for small logs or as a placeholder.

        let path = self.segment_path(0); // Start from first segment
        if !path.exists() {
            return Ok(None);
        }

        let mut file =
            File::open(&path).map_err(|e| RaftError::Storage(format!("Open error: {}", e)))?;

        // Skip header
        file.seek(SeekFrom::Start(HEADER_SIZE as u64))
            .map_err(|e| RaftError::Storage(format!("Seek error: {}", e)))?;

        let mut current_idx = 0;
        loop {
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(RaftError::Storage(format!("Read len error: {}", e))),
            }
            let len = u32::from_be_bytes(len_buf) as usize;

            let mut checksum_buf = [0u8; 4];
            file.read_exact(&mut checksum_buf)
                .map_err(|e| RaftError::Storage(format!("Read checksum error: {}", e)))?;
            let stored_checksum = u32::from_be_bytes(checksum_buf);

            let mut data = vec![0u8; len];
            file.read_exact(&mut data)
                .map_err(|e| RaftError::Storage(format!("Read data error: {}", e)))?;

            let calculated_checksum = crc32fast::hash(&data);
            if calculated_checksum != stored_checksum {
                return Err(RaftError::Storage(
                    "Checksum mismatch: corrupted entry".into(),
                ));
            }

            if current_idx == index {
                let entry: LogEntry = bincode::deserialize(&data)
                    .map_err(|e| RaftError::Storage(format!("Deserialize error: {}", e)))?;
                return Ok(Some(entry));
            }

            current_idx += 1;
        }

        Ok(None)
    }

    pub fn truncate_after(&mut self, index: u64) -> Result<(), RaftError> {
        // Truncation logic: reopen file, read valid entries up to index, rewrite file.
        // This is a simplified implementation.
        let path = self.segment_path(0);
        if !path.exists() {
            return Ok(());
        }

        let mut entries_to_keep = Vec::new();
        {
            let mut file =
                File::open(&path).map_err(|e| RaftError::Storage(format!("Open error: {}", e)))?;
            file.seek(SeekFrom::Start(HEADER_SIZE as u64))
                .map_err(|e| RaftError::Storage(format!("Seek error: {}", e)))?;

            let mut current_idx = 0;
            loop {
                let mut len_buf = [0u8; 4];
                match file.read_exact(&mut len_buf) {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(RaftError::Storage(format!("Read len error: {}", e))),
                }
                let len = u32::from_be_bytes(len_buf) as usize;

                let mut checksum_buf = [0u8; 4];
                file.read_exact(&mut checksum_buf)
                    .map_err(|e| RaftError::Storage(format!("Read checksum error: {}", e)))?;
                let stored_checksum = u32::from_be_bytes(checksum_buf);

                let mut data = vec![0u8; len];
                file.read_exact(&mut data)
                    .map_err(|e| RaftError::Storage(format!("Read data error: {}", e)))?;

                let calculated_checksum = crc32fast::hash(&data);
                if calculated_checksum != stored_checksum {
                    // Stop at corruption
                    break;
                }

                if current_idx <= index {
                    entries_to_keep.push(data);
                } else {
                    break;
                }
                current_idx += 1;
            }
        }

        // Rewrite file
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| RaftError::Storage(format!("Open truncate error: {}", e)))?;

        let mut header = [0u8; HEADER_SIZE];
        header[0..4].copy_from_slice(&MAGIC_NUMBER.to_be_bytes());
        header[4..8].copy_from_slice(&VERSION.to_be_bytes());
        file.write_all(&header)
            .map_err(|e| RaftError::Storage(format!("Write header error: {}", e)))?;

        for data in entries_to_keep {
            let len = data.len() as u32;
            let checksum = crc32fast::hash(&data);

            let mut record = Vec::with_capacity(8 + data.len());
            record.extend_from_slice(&len.to_be_bytes());
            record.extend_from_slice(&checksum.to_be_bytes());
            record.extend_from_slice(&data);

            file.write_all(&record)
                .map_err(|e| RaftError::Storage(format!("Write record error: {}", e)))?;
        }

        file.sync_all()
            .map_err(|e| RaftError::Storage(format!("Sync error: {}", e)))?;

        Ok(())
    }

    pub fn last_index(&self) -> u64 {
        // Placeholder: should track actual last index written
        self.current_index
    }
}
