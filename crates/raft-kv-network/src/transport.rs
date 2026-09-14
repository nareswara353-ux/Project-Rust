use crate::{NetworkError, RpcMessage, NodeId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};

pub trait Transport: Send + Sync {
    fn send(&self, target: NodeId, msg: RpcMessage) -> impl futures::future::Future<Output = Result<RpcMessage, NetworkError>> + Send;
    fn broadcast(&self, exclude: NodeId, msg: RpcMessage) -> impl futures::future::Future<Output = Result<(), NetworkError>> + Send;
}

pub struct TcpTransport {
    addr: std::net::SocketAddr,
    peers: Arc<RwLock<HashMap<NodeId, std::net::SocketAddr>>>,
}

impl TcpTransport {
    pub fn new(addr: std::net::SocketAddr) -> Self {
        Self {
            addr,
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_peer(&self, id: NodeId, addr: std::net::SocketAddr) {
        self.peers.write().await.insert(id, addr);
    }
}

impl Transport for TcpTransport {
    async fn send(&self, target: NodeId, msg: RpcMessage) -> Result<RpcMessage, NetworkError> {
        let peers = self.peers.read().await;
        let target_addr = peers.get(&target).ok_or_else(|| NetworkError::Connection("Peer not found".into()))?;
        
        let mut stream = TcpStream::connect(*target_addr).await?;
        let data = crate::codec::encode_message(&msg)?;
        let len = data.len() as u32;
        
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(&data).await?;
        stream.flush().await?;
        
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;
        
        let mut resp_data = vec![0u8; resp_len];
        stream.read_exact(&mut resp_data).await?;
        
        crate::codec::decode_message(&resp_data)
    }

    async fn broadcast(&self, exclude: NodeId, msg: RpcMessage) -> Result<(), NetworkError> {
        let peers = self.peers.read().await;
        for (id, addr) in peers.iter() {
            if *id != exclude {
                let mut stream = TcpStream::connect(*addr).await?;
                let data = crate::codec::encode_message(&msg)?;
                let len = data.len() as u32;
                stream.write_all(&len.to_be_bytes()).await?;
                stream.write_all(&data).await?;
                stream.flush().await?;
            }
        }
        Ok(())
    }
}

pub struct InMemoryTransport {
    id: NodeId,
    network: Arc<RwLock<HashMap<NodeId, mpsc::Sender<(NodeId, RpcMessage)>>>>,
}

impl InMemoryTransport {
    pub fn new(id: NodeId, network: Arc<RwLock<HashMap<NodeId, mpsc::Sender<(NodeId, RpcMessage)>>>>) -> Self {
        Self { id, network }
    }
}

impl Transport for InMemoryTransport {
    async fn send(&self, target: NodeId, msg: RpcMessage) -> Result<RpcMessage, NetworkError> {
        let network = self.network.read().await;
        let sender = network.get(&target).ok_or_else(|| NetworkError::Connection("Target not connected".into()))?;
        let (tx, mut rx) = mpsc::channel(1);
        sender.send((self.id, RpcMessage::RequestVote { term: 0, candidate_id: 0, last_log_index: 0, last_log_term: 0 })).await.map_err(|_| NetworkError::Connection("Channel closed".into()))?;
        rx.recv().await.ok_or(NetworkError::Timeout)
    }

    async fn broadcast(&self, exclude: NodeId, msg: RpcMessage) -> Result<(), NetworkError> {
        let network = self.network.read().await;
        for (id, sender) in network.iter() {
            if *id != exclude && *id != self.id {
                sender.send((self.id, msg.clone())).await.map_err(|_| NetworkError::Connection("Channel closed".into()))?;
            }
        }
        Ok(())
    }
}
