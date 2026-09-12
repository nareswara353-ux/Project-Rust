use crate::codec::decode_message;
use crate::{NetworkError, RpcMessage};
use raft_kv_core::NodeId;
use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{info, warn};

pub struct RpcServer {
    addr: SocketAddr,
    node_id: NodeId,
    tx: mpsc::Sender<RpcMessage>,
}

impl RpcServer {
    pub fn new(addr: SocketAddr, node_id: NodeId, tx: mpsc::Sender<RpcMessage>) -> Self {
        Self { addr, node_id, tx }
    }

    pub async fn run(self) -> Result<(), NetworkError> {
        let listener = TcpListener::bind(self.addr).await?;
        info!("RPC Server listening on {}", self.addr);

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    let tx = self.tx.clone();
                    let node_id = self.node_id;
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, tx, node_id, peer_addr).await {
                            warn!("Connection error from {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    warn!("Accept error: {}", e);
                }
            }
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    tx: mpsc::Sender<RpcMessage>,
    _local_id: NodeId,
    peer_addr: SocketAddr,
) -> Result<(), NetworkError> {
    let mut buf = vec![0u8; 4096];

    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            info!("Connection closed by {}", peer_addr);
            break;
        }

        let msg = decode_message(&buf[..n])?;
        info!("Received message from {}: {:?}", peer_addr, msg);

        if let Err(e) = tx.send(msg).await {
            warn!("Failed to forward message to core: {}", e);
            break;
        }
    }

    Ok(())
}
