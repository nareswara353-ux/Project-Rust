use crate::config::CliConfig;
use crate::error::Result;
use raft_kv_core::{Node, NodeId, RaftConfig};
use raft_kv_storage::WriteAheadLog;
use tracing::info;

pub struct Runtime {
    config: CliConfig,
}

impl Runtime {
    pub fn new(config: CliConfig) -> Self {
        Self { config }
    }

    pub async fn run(self) -> Result<()> {
        info!("Initializing runtime for node {}", self.config.id);

        let wal_path = format!("{}/wal", self.config.data_dir);
        let _snapshot_path = format!("{}/snapshots", self.config.data_dir);

        let _wal = WriteAheadLog::new(&wal_path)
            .map_err(|e| crate::error::CliError::StorageInit(e.to_string()))?;

        let _store = raft_kv_storage::KeyValueStore::new();

        let _peers: Vec<Node> = self
            .config
            .peers
            .iter()
            .enumerate()
            .map(|(i, addr)| Node::new(i as NodeId, addr.to_string()))
            .collect();

        let _raft_config = RaftConfig::new(self.config.id)
            .with_timeouts(
                self.config.election_timeout_min_ms,
                self.config.election_timeout_max_ms,
            )
            .with_heartbeat(self.config.heartbeat_interval_ms);

        info!("Starting Raft consensus engine...");

        let shutdown_signal = tokio::signal::ctrl_c();
        tokio::pin!(shutdown_signal);

        loop {
            tokio::select! {
                _ = &mut shutdown_signal => {
                    info!("Shutdown signal received");
                    break;
                }
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                }
            }
        }

        info!("Runtime shutting down gracefully");
        Ok(())
    }
}