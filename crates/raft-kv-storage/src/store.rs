use crate::snapshot::Snapshot;
use raft_kv_core::{Command, Index};
use std::collections::HashMap;

pub struct KeyValueStore {
    data: HashMap<String, String>,
    last_applied_index: Index,
}

impl Default for KeyValueStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyValueStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
            last_applied_index: 0,
        }
    }

    pub fn apply(&mut self, index: Index, command: &Command) -> Option<String> {
        self.last_applied_index = index;
        match command {
            Command::Noop => None,
            Command::Put { key, value } => {
                self.data.insert(key.clone(), value.clone());
                Some(value.clone())
            }
            Command::Delete { key } => self.data.remove(key),
        }
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn last_applied_index(&self) -> Index {
        self.last_applied_index
    }

    pub fn create_snapshot(&self) -> Snapshot {
        let mut kv_data = Vec::new();
        for (key, value) in &self.data {
            let mut entry = Vec::new();
            entry.extend_from_slice(&(key.len() as u32).to_be_bytes());
            entry.extend_from_slice(key.as_bytes());
            entry.extend_from_slice(&(value.len() as u32).to_be_bytes());
            entry.extend_from_slice(value.as_bytes());
            kv_data.extend(entry);
        }

        Snapshot::new(0, 0)
            .with_data(kv_data)
            .with_cluster_config(Vec::new())
    }

    pub fn restore_from_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), String> {
        self.data.clear();
        let bytes = &snapshot.data;
        let mut offset = 0;

        while offset + 8 <= bytes.len() {
            let key_len = u32::from_be_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + key_len > bytes.len() {
                return Err("Invalid snapshot format: key length mismatch".into());
            }
            let key = String::from_utf8(bytes[offset..offset + key_len].to_vec())
                .map_err(|e| format!("Invalid UTF-8 in key: {}", e))?;
            offset += key_len;

            if offset + 4 > bytes.len() {
                return Err("Invalid snapshot format: value length missing".into());
            }
            let val_len = u32::from_be_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + val_len > bytes.len() {
                return Err("Invalid snapshot format: value length mismatch".into());
            }
            let value = String::from_utf8(bytes[offset..offset + val_len].to_vec())
                .map_err(|e| format!("Invalid UTF-8 in value: {}", e))?;
            offset += val_len;

            self.data.insert(key, value);
        }

        Ok(())
    }
}
