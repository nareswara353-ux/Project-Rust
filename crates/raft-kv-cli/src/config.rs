use clap::Parser;
use raft_kv_core::NodeId;
use std::net::SocketAddr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Node address cannot be in the peer list")]
    SelfInPeerList,
    #[error("Invalid node ID: must be non-zero")]
    InvalidNodeId,
}

#[derive(Parser, Debug)]
#[command(author, version, about = "Raft-KV Distributed Store")]
struct CliArgs {
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

    /// Election timeout minimum in milliseconds
    #[arg(long, default_value = "150")]
    election_timeout_min_ms: u64,

    /// Election timeout maximum in milliseconds
    #[arg(long, default_value = "300")]
    election_timeout_max_ms: u64,

    /// Heartbeat interval in milliseconds
    #[arg(long, default_value = "50")]
    heartbeat_interval_ms: u64,
}

#[derive(Debug, Clone)]
pub struct CliConfig {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub peers: Vec<SocketAddr>,
    pub data_dir: String,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
}

impl CliConfig {
    pub fn from_args() -> Result<Self, ConfigError> {
        let args = CliArgs::parse();

        if args.id == 0 {
            return Err(ConfigError::InvalidNodeId);
        }

        if args.peers.contains(&args.addr) {
            return Err(ConfigError::SelfInPeerList);
        }

        Ok(Self {
            id: args.id,
            addr: args.addr,
            peers: args.peers,
            data_dir: args.data_dir,
            election_timeout_min_ms: args.election_timeout_min_ms,
            election_timeout_max_ms: args.election_timeout_max_ms,
            heartbeat_interval_ms: args.heartbeat_interval_ms,
        })
    }
}
