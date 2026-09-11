use raft_kv_core::{Index, NodeId, Term};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

const SNAPSHOT_FILE: &str = "snapshot.bin";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub last_index: Index,
    pub last_term: Term,
    pub cluster_config: Vec<NodeId>,
    pub data: Vec<u8>,
}

impl Snapshot {
    pub fn new(last_index: Index, last_term: Term) -> Self {
        Self {
            last_index,
            last_term,
            cluster_config: Vec::new(),
            data: Vec::new(),
        }
    }

    pub fn with_data(mut self, data: Vec<u8>) -> Self {
        self.data = data;
        self
    }

    pub fn with_cluster_config(mut self, nodes: Vec<NodeId>) -> Self {
        self.cluster_config = nodes;
        self
    }

    pub fn save(&self, snapshot_dir: &str) -> Result<(), std::io::Error> {
        let path = PathBuf::from(snapshot_dir).join(SNAPSHOT_FILE);
        fs::create_dir_all(&path.parent().unwrap())?;

        let file = File::create(&path)?;
        let mut writer = BufWriter::new(file);
        let bytes = bincode::serialize(self).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("Serialize error: {}", e))
        })?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        Ok(())
    }

    pub fn load(snapshot_dir: &str) -> Result<Option<Self>, std::io::Error> {
        let path = PathBuf::from(snapshot_dir).join(SNAPSHOT_FILE);
        if !path.exists() {
            return Ok(None);
        }

        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer)?;

        let snapshot: Self = bincode::deserialize(&buffer).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Deserialize error: {}", e),
            )
        })?;
        Ok(Some(snapshot))
    }

    pub fn is_empty(&self) -> bool {
        self.last_index == 0
    }
}
