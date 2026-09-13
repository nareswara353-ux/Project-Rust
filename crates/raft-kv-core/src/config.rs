use std::time::Duration;

/// Configuration for a Raft node.
#[derive(Debug, Clone)]
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
    /// Create a new config with the given node ID and default timeouts.
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            ..Default::default()
        }
    }

    /// Set custom election timeout range (in milliseconds).
    pub fn with_timeouts(mut self, min_ms: u64, max_ms: u64) -> Self {
        self.election_timeout_min = Duration::from_millis(min_ms);
        self.election_timeout_max = Duration::from_millis(max_ms);
        self
    }

    /// Set custom heartbeat interval (in milliseconds).
    pub fn with_heartbeat(mut self, interval_ms: u64) -> Self {
        self.heartbeat_interval = Duration::from_millis(interval_ms);
        self
    }

    /// Validate the configuration constraints.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.election_timeout_min >= self.election_timeout_max {
            return Err("election_timeout_min must be less than election_timeout_max");
        }

        // Heartbeat should be significantly smaller than election timeout to avoid unnecessary elections
        if self.heartbeat_interval >= self.election_timeout_min {
            return Err("heartbeat_interval must be less than election_timeout_min");
        }

        if self.node_id == 0 {
            return Err("node_id cannot be zero");
        }

        Ok(())
    }

    /// Generate a random election timeout within the configured range.
    pub fn random_election_timeout(&self) -> Duration {
        let min = self.election_timeout_min.as_millis() as u64;
        let max = self.election_timeout_max.as_millis() as u64;
        let range = max - min;

        let random_offset = if range > 0 {
            // Simple pseudo-random using system time nanos
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            now % range
        } else {
            0
        };

        Duration::from_millis(min + random_offset)
    }
}
