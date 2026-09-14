use raft_kv_core::message::{Command, Snapshot};
use raft_kv_core::RaftError;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct KeyValueStore {
    data: Arc<RwLock<HashMap<String, String>>>,
    last_applied_index: Arc<RwLock<u64>>,
    last_applied_term: Arc<RwLock<u64>>,
}

impl Default for KeyValueStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KeyValueStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            last_applied_index: Arc::new(RwLock::new(0)),
            last_applied_term: Arc::new(RwLock::new(0)),
        }
    }

    pub fn apply(
        &self,
        index: u64,
        term: u64,
        command: &Command,
    ) -> Result<Option<String>, RaftError> {
        let mut data = self
            .data
            .write()
            .map_err(|_| RaftError::StateMachine("Poisoned lock in state machine".to_string()))?;

        let result = match command {
            Command::Noop => None,
            Command::Put { key, value } => {
                data.insert(key.clone(), value.clone());
                Some(value.clone())
            }
            Command::Delete { key } => data.remove(key),
        };

        let mut last_index = self
            .last_applied_index
            .write()
            .map_err(|_| RaftError::StateMachine("Poisoned lock in state machine".to_string()))?;
        *last_index = index;

        let mut last_term = self
            .last_applied_term
            .write()
            .map_err(|_| RaftError::StateMachine("Poisoned lock in state machine".to_string()))?;
        *last_term = term;

        Ok(result)
    }

    pub fn get(&self, key: &str) -> Result<Option<String>, RaftError> {
        let data = self
            .data
            .read()
            .map_err(|_| RaftError::StateMachine("Poisoned lock in state machine".to_string()))?;
        Ok(data.get(key).cloned())
    }

    pub fn contains_key(&self, key: &str) -> Result<bool, RaftError> {
        let data = self
            .data
            .read()
            .map_err(|_| RaftError::StateMachine("Poisoned lock in state machine".to_string()))?;
        Ok(data.contains_key(key))
    }

    pub fn last_applied_index(&self) -> Result<u64, RaftError> {
        let index = self
            .last_applied_index
            .read()
            .map_err(|_| RaftError::StateMachine("Poisoned lock in state machine".to_string()))?;
        Ok(*index)
    }

    pub fn last_applied_term(&self) -> Result<u64, RaftError> {
        let term = self
            .last_applied_term
            .read()
            .map_err(|_| RaftError::StateMachine("Poisoned lock in state machine".to_string()))?;
        Ok(*term)
    }

    pub fn create_snapshot(&self) -> Result<Snapshot, RaftError> {
        let data = self
            .data
            .read()
            .map_err(|_| RaftError::StateMachine("Poisoned lock in state machine".to_string()))?;
        let last_index = self
            .last_applied_index
            .read()
            .map_err(|_| RaftError::StateMachine("Poisoned lock in state machine".to_string()))?;
        let last_term = self
            .last_applied_term
            .read()
            .map_err(|_| RaftError::StateMachine("Poisoned lock in state machine".to_string()))?;

        let mut snapshot_data = Vec::new();
        for (key, value) in data.iter() {
            let mut entry = Vec::new();
            entry.extend_from_slice(&(key.len() as u32).to_be_bytes());
            entry.extend_from_slice(key.as_bytes());
            entry.extend_from_slice(&(value.len() as u32).to_be_bytes());
            entry.extend_from_slice(value.as_bytes());
            snapshot_data.extend(entry);
        }

        Ok(Snapshot {
            last_included_index: *last_index,
            last_included_term: *last_term,
            data: snapshot_data.into(),
        })
    }

    pub fn restore_from_snapshot(&self, snapshot: &Snapshot) -> Result<(), RaftError> {
        let mut data = self
            .data
            .write()
            .map_err(|_| RaftError::StateMachine("Poisoned lock in state machine".to_string()))?;
        data.clear();

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
                return Err(RaftError::StateMachine(
                    "Invalid snapshot format: key length mismatch".to_string(),
                ));
            }
            let key = String::from_utf8(bytes[offset..offset + key_len].to_vec())
                .map_err(|e| RaftError::StateMachine(format!("Invalid UTF-8 in key: {}", e)))?;
            offset += key_len;

            if offset + 4 > bytes.len() {
                return Err(RaftError::StateMachine(
                    "Invalid snapshot format: value length missing".to_string(),
                ));
            }
            let val_len = u32::from_be_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + val_len > bytes.len() {
                return Err(RaftError::StateMachine(
                    "Invalid snapshot format: value length mismatch".to_string(),
                ));
            }
            let value = String::from_utf8(bytes[offset..offset + val_len].to_vec())
                .map_err(|e| RaftError::StateMachine(format!("Invalid UTF-8 in value: {}", e)))?;
            offset += val_len;

            data.insert(key, value);
        }

        let mut last_index = self
            .last_applied_index
            .write()
            .map_err(|_| RaftError::StateMachine("Poisoned lock in state machine".to_string()))?;
        *last_index = snapshot.last_included_index;

        let mut last_term = self
            .last_applied_term
            .write()
            .map_err(|_| RaftError::StateMachine("Poisoned lock in state machine".to_string()))?;
        *last_term = snapshot.last_included_term;

        Ok(())
    }
}
