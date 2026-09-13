use crate::config::CliConfig;
use crate::error::{CliError, Result};
use raft_kv_core::{NodeId, Node, RaftConfig};
use raft_kv_storage::{WriteAheadLog, KeyValueStore};
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

        // 1. Initialize Storage
        let wal_path = format!("{}/wal", self.config.data_dir);
        let _snapshot_path = format!("{}/snapshots", self.config.data_dir);
        
        let _wal = WriteAheadLog::new(&wal_path)
            .map_err(|e| CliError::StorageInit(e.to_string()))?;
        
        let _store = KeyValueStore::new();

        // 2. Prepare Peers
        let _peers: Vec<Node> = self.config.peers
            .iter()
            .enumerate()
            .map(|(i, addr)| Node::new(i as NodeId, addr.to_string()))
            .collect();

        // 3. Initialize Raft Config
        let _raft_config = RaftConfig::new(self.config.id)
            .with_timeouts(
                self.config.election_timeout_min_ms,
                self.config.election_timeout_max_ms,
            )
            .with_heartbeat(self.config.heartbeat_interval_ms);

        info!("Starting Raft consensus engine...");
        
        // TODO: Instantiate actual Raft node here once fully wired
        // let state = Arc::new(RwLock::new(NodeState::new(self.config.id)));
        // let storage = Arc::new(wal); // Needs to implement Storage trait wrapper
        // let raft = Raft::new(state, storage, peers);

        // 4. Start Background Tasks (Placeholder)
        let shutdown_signal = tokio::signal::ctrl_c();
        tokio::pin!(shutdown_signal);

        loop {
            tokio::select! {
                _ = &mut shutdown_signal => {
                    info!("Shutdown signal received");
                    break;
                }
                // TODO: Add channel receivers for applying commands or handling RPCs
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                    // Heartbeat or tick logic would go here in a real impl
                    // trace!("Node tick");
                }
            }
        }

        info!("Runtime shutting down gracefully");
        Ok(())
    }
}
