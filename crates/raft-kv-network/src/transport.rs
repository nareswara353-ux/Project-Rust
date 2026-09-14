use crate::{NetworkError, RpcMessage};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};

pub trait Transport: Send + Sync {
    async fn send(&self, target: SocketAddr, msg: RpcMessage) -> Result<RpcMessage, NetworkError>;
}

pub struct TcpTransport;

impl TcpTransport {
    pub fn new() -> Self {
        Self
    }
}

impl Transport for TcpTransport {
    async fn send(&self, target: SocketAddr, msg: RpcMessage) -> Result<RpcMessage, NetworkError> {
        let stream = TcpStream::connect(target).await?;
        let (mut reader, mut writer) = stream.into_split();

        let encoded = crate::codec::encode_message(&msg)?;
        let len = encoded.len() as u32;
        writer.write_all(&len.to_be_bytes()).await?;
        writer.write_all(&encoded).await?;
        writer.flush().await?;

        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;

        let mut resp_buf = vec![0u8; resp_len];
        reader.read_exact(&mut resp_buf).await?;

        crate::codec::decode_message(&resp_buf)
    }
}

pub struct InMemoryTransport {
    peers: Arc<RwLock<HashMap<SocketAddr, mpsc::Sender<RpcMessage>>>>,
}

impl InMemoryTransport {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register(&self, addr: SocketAddr, tx: mpsc::Sender<RpcMessage>) {
        let mut peers = self.peers.write().await;
        peers.insert(addr, tx);
    }
}

impl Transport for InMemoryTransport {
    async fn send(&self, target: SocketAddr, msg: RpcMessage) -> Result<RpcMessage, NetworkError> {
        let peers = self.peers.read().await;
        let tx = peers.get(&target).ok_or_else(|| NetworkError::Connection("Peer not found".into()))?;
        
        tx.send(msg).await.map_err(|_| NetworkError::Connection("Channel closed".into()))?;
        
        Err(NetworkError::Timeout)
    }
}
