use std::collections::HashMap;

pub struct KeyValueStore {
    data: HashMap<String, String>,
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
        }
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn put(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    pub fn delete(&mut self, key: &str) {
        self.data.remove(key);
    }
}
