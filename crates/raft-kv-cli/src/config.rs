use clap::Parser;
use raft_kv_core::NodeId;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(author, version, about = "Raft-KV Distributed Store")]
pub struct CliArgs {
    /// Unique ID for this node
    #[arg(short, long)]
    pub id: NodeId,

    /// Address to bind the RPC server (e.g., 127.0.0.1:5001)
    #[arg(short, long, default_value = "127.0.0.1:5000")]
    pub addr: SocketAddr,

    /// List of peer addresses (e.g., --peers 127.0.0.1:5001 --peers 127.0.0.1:5002)
    #[arg(short, long)]
    pub peers: Vec<SocketAddr>,

    /// Directory for persistent storage (WAL, Snapshots)
    #[arg(short, long, default_value = "./data")]
    pub data_dir: String,

    /// Election timeout minimum in milliseconds
    #[arg(long, default_value = "150")]
    pub election_timeout_min_ms: u64,

    /// Election timeout maximum in milliseconds
    #[arg(long, default_value = "300")]
    pub election_timeout_max_ms: u64,

    /// Heartbeat interval in milliseconds
    #[arg(long, default_value = "50")]
    pub heartbeat_interval_ms: u64,
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub peers: Vec<SocketAddr>,
    pub data_dir: PathBuf,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
}

impl NodeConfig {
    pub fn from_args(args: CliArgs) -> Result<Self, String> {
        if args.peers.contains(&args.addr) {
            return Err("Node address cannot be in the peer list".to_string());
        }

        if args.id == 0 {
            return Err("Node ID must be greater than 0".to_string());
        }

        if args.election_timeout_min_ms >= args.election_timeout_max_ms {
            return Err("Election timeout min must be less than max".to_string());
        }

        Ok(Self {
            id: args.id,
            addr: args.addr,
            peers: args.peers,
            data_dir: PathBuf::from(args.data_dir),
            election_timeout_min: Duration::from_millis(args.election_timeout_min_ms),
            election_timeout_max: Duration::from_millis(args.election_timeout_max_ms),
            heartbeat_interval: Duration::from_millis(args.heartbeat_interval_ms),
        })
    }

    pub fn wal_path(&self) -> PathBuf {
        self.data_dir.join("wal")
    }

    pub fn snapshot_path(&self) -> PathBuf {
        self.data_dir.join("snapshots")
    }
}
