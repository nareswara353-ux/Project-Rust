use std::time::Duration;

pub struct RaftConfig {
    pub node_id: u64,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    pub max_log_entries_per_append: usize,
    pub snapshot_threshold: u64,
    pub apply_queue_size: usize,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            max_log_entries_per_append: 1000,
            snapshot_threshold: 10000,
            apply_queue_size: 100,
        }
    }
}

impl RaftConfig {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            ..Default::default()
        }
    }

    pub fn with_timeouts(mut self, min_ms: u64, max_ms: u64) -> Self {
        self.election_timeout_min = Duration::from_millis(min_ms);
        self.election_timeout_max = Duration::from_millis(max_ms);
        self
    }

    pub fn with_heartbeat(mut self, interval_ms: u64) -> Self {
        self.heartbeat_interval = Duration::from_millis(interval_ms);
        self
    }

    pub fn random_election_timeout(&self) -> Duration {
        let range = self.election_timeout_max.as_millis() as u64
            - self.election_timeout_min.as_millis() as u64;
        let random_ms = if range > 0 {
            (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64)
                % range
        } else {
            0
        };
        self.election_timeout_min + Duration::from_millis(random_ms)
    }
}
