use clap::Parser;
use raft_kv_core::{NodeId, RaftConfig, Node};
use raft_kv_storage::{WriteAheadLog, KeyValueStore, Snapshot};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, error};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser, Debug)]
#[command(author, version, about = "Raft-KV Distributed Store")]
struct Args {
    /// Unique ID for this node
    #[arg(short, long)]
    id: NodeId,

    /// Address to bind the RPC server (e.g., 127.0.0.1:5001)
    #[arg(short, long, default_value = "127.0.0.1:5000")]
    addr: SocketAddr,

    /// List of peer addresses (e.g., --peers 127.0.0.1:5001 --peers 127.0.0.1:5002)
    #[arg(short, long)]
    peers: Vec<SocketAddr>,

    /// Directory for persistent storage (WAL, Snapshots)
    #[arg(short, long, default_value = "./data")]
    data_dir: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    info!("Starting Raft-KV node {} at {}", args.id, args.addr);

    // Validate configuration
    if args.peers.contains(&args.addr) {
        error!("Node address cannot be in the peer list");
        return Err("Invalid configuration: self in peer list".into());
    }

    // Initialize Storage Layer
    let wal_path = format!("{}/wal", args.data_dir);
    let snapshot_path = format!("{}/snapshots", args.data_dir);
    
    info!("Initializing WAL at {}", wal_path);
    let mut wal = WriteAheadLog::new(&wal_path)?;
    
    info!("Initializing KV Store");
    let store = KeyValueStore::new();

    // TODO: Load existing state from WAL/Snapshot here
    // For now, we start fresh or restore from snapshot if exists
    
    // Initialize Raft Core
    // Note: We need a concrete implementation of Storage trait and NodeState
    // This is a simplified bootstrap. In a full impl, we'd wire up the Raft struct here.
    
    info!("Node {} ready. Listening on {}", args.id, args.addr);
    
    // Placeholder for actual Raft loop and RPC server
    // let config = RaftConfig::new(args.id);
    // let raft = Raft::new(config, Arc::new(wal), Arc::new(store));
    
    // Simulate running
    tokio::signal::ctrl_c().await?;
    info!("Shutting down node {}...", args.id);

    Ok(())
}
