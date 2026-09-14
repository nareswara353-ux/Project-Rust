use crate::{NetworkError, RpcMessage};
use raft_kv_core::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

pub trait Transport: Send + Sync {
    fn send(&self, target_id: NodeId, msg: RpcMessage) -> impl futures::future::Future<Output = Result<(), NetworkError>> + Send;
    fn broadcast(&self, exclude_id: NodeId, msg: RpcMessage) -> impl futures::future::Future<Output = Result<(), NetworkError>> + Send;
}

pub struct TcpTransport {
    peers: HashMap<NodeId, String>,
}

impl TcpTransport {
    pub fn new(peers: HashMap<NodeId, String>) -> Self {
        Self { peers }
    }
}

impl Transport for TcpTransport {
    async fn send(&self, target_id: NodeId, msg: RpcMessage) -> Result<(), NetworkError> {
        let addr = self.peers.get(&target_id)
            .ok_or_else(|| NetworkError::Connection(format!("Unknown peer: {}", target_id)))?;
        
        let stream = tokio::net::TcpStream::connect(addr).await?;
        let (mut reader, mut writer) = stream.into_split();

        let data = crate::codec::encode_message(&msg)?;
        let len = data.len() as u32;
        writer.write_all(&len.to_be_bytes()).await?;
        writer.write_all(&data).await?;
        writer.flush().await?;

        let mut resp_len_buf = [0u8; 4];
        reader.read_exact(&mut resp_len_buf).await?;
        let resp_len = u32::from_be_bytes(resp_len_buf) as usize;
        let mut resp_data = vec![0u8; resp_len];
        reader.read_exact(&mut resp_data).await?;

        let _resp_msg = crate::codec::decode_message(&resp_data)?;

        Ok(())
    }

    async fn broadcast(&self, exclude_id: NodeId, msg: RpcMessage) -> Result<(), NetworkError> {
        for (&peer_id, _) in &self.peers {
            if peer_id != exclude_id {
                let _ = self.send(peer_id, msg.clone()).await;
            }
        }
        Ok(())
    }
}

pub struct InMemoryTransport {
    channels: Arc<tokio::sync::RwLock<HashMap<NodeId, mpsc::Sender<RpcMessage>>>>,
}

impl InMemoryTransport {
    pub fn new() -> Self {
        Self {
            channels: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    pub fn register(&self, id: NodeId, tx: mpsc::Sender<RpcMessage>) {
        let mut channels = self.channels.blocking_write();
        channels.insert(id, tx);
    }
}

impl Transport for InMemoryTransport {
    async fn send(&self, target_id: NodeId, msg: RpcMessage) -> Result<(), NetworkError> {
        let channels = self.channels.read().await;
        let tx = channels.get(&target_id)
            .ok_or_else(|| NetworkError::Connection(format!("Unknown peer: {}", target_id)))?;
        
        tx.send(msg).await
            .map_err(|_| NetworkError::Connection("Channel closed".into()))?;
        
        Ok(())
    }

    async fn broadcast(&self, exclude_id: NodeId, msg: RpcMessage) -> Result<(), NetworkError> {
        let channels = self.channels.read().await;
        for (&id, tx) in channels.iter() {
            if id != exclude_id {
                let _ = tx.send(msg.clone()).await;
            }
        }
        Ok(())
    }
}
