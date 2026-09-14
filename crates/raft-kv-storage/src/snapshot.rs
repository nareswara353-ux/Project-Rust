use raft_kv_core::message::Snapshot;
use raft_kv_core::RaftError;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::PathBuf;

pub struct SnapshotManager {
    dir: PathBuf,
}

impl SnapshotManager {
    pub fn new(dir: &str) -> Result<Self, RaftError> {
        let path = PathBuf::from(dir);
        fs::create_dir_all(&path)
            .map_err(|e| RaftError::Storage(format!("Create snapshot dir error: {}", e)))?;
        Ok(Self { dir: path })
    }

    pub fn save(&self, snapshot: &Snapshot) -> Result<(), RaftError> {
        let filename = format!(
            "snapshot_{}_{}.bin",
            snapshot.last_included_index, snapshot.last_included_term
        );
        let path = self.dir.join(filename);

        let data = bincode::serialize(snapshot)
            .map_err(|e| RaftError::Snapshot(format!("Serialize snapshot error: {}", e)))?;

        let mut file = File::create(&path)
            .map_err(|e| RaftError::Snapshot(format!("Create snapshot file error: {}", e)))?;

        file.write_all(&data)
            .map_err(|e| RaftError::Snapshot(format!("Write snapshot error: {}", e)))?;

        file.sync_all()
            .map_err(|e| RaftError::Snapshot(format!("Sync snapshot error: {}", e)))?;

        Ok(())
    }

    pub fn load_latest(&self) -> Result<Option<Snapshot>, RaftError> {
        let entries = fs::read_dir(&self.dir)
            .map_err(|e| RaftError::Snapshot(format!("Read snapshot dir error: {}", e)))?;

        let mut latest: Option<Snapshot> = None;

        for entry in entries {
            let entry = entry
                .map_err(|e| RaftError::Snapshot(format!("Read snapshot entry error: {}", e)))?;

            let filename = entry.file_name();
            let name_str = filename.to_string_lossy();

            if name_str.starts_with("snapshot_") && name_str.ends_with(".bin") {
                let path = entry.path();
                let mut file = File::open(&path)
                    .map_err(|e| RaftError::Snapshot(format!("Open snapshot file error: {}", e)))?;

                let mut data = Vec::new();
                file.read_to_end(&mut data)
                    .map_err(|e| RaftError::Snapshot(format!("Read snapshot data error: {}", e)))?;

                let snapshot: Snapshot = bincode::deserialize(&data).map_err(|e| {
                    RaftError::Snapshot(format!("Deserialize snapshot error: {}", e))
                })?;

                if latest.is_none()
                    || snapshot.last_included_index > latest.as_ref().unwrap().last_included_index
                {
                    latest = Some(snapshot);
                }
            }
        }

        Ok(latest)
    }

    pub fn delete_older_than(&self, index: u64) -> Result<(), RaftError> {
        let entries = fs::read_dir(&self.dir)
            .map_err(|e| RaftError::Snapshot(format!("Read snapshot dir error: {}", e)))?;

        for entry in entries {
            let entry = entry
                .map_err(|e| RaftError::Snapshot(format!("Read snapshot entry error: {}", e)))?;

            let filename = entry.file_name();
            let name_str = filename.to_string_lossy();

            if name_str.starts_with("snapshot_") && name_str.ends_with(".bin") {
                let parts: Vec<&str> = name_str.split('_').collect();
                if parts.len() >= 3 {
                    if let Ok(idx) = parts[1].parse::<u64>() {
                        if idx < index {
                            fs::remove_file(entry.path()).map_err(|e| {
                                RaftError::Snapshot(format!("Delete old snapshot error: {}", e))
                            })?;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
