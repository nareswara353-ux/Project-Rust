use crate::{NetworkError, RpcMessage, Transport};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct Network<T: Transport> {
    transport: Arc<T>,
    peers: Arc<RwLock<HashMap<u64, SocketAddr>>>,
}

impl<T: Transport> Network<T> {
    pub fn new(transport: T) -> Self {
        Self {
            transport: Arc::new(transport),
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_peer(&self, id: u64, addr: SocketAddr) {
        let mut peers = self.peers.write().await;
        peers.insert(id, addr);
    }

    pub async fn remove_peer(&self, id: u64) {
        let mut peers = self.peers.write().await;
        peers.remove(&id);
    }

    pub async fn send(&self, target_id: u64, msg: RpcMessage) -> Result<RpcMessage, NetworkError> {
        let peers = self.peers.read().await;
        let addr = *peers.get(&target_id).ok_or_else(|| NetworkError::Connection("Peer not found".into()))?;
        drop(peers);
        
        self.transport.send(addr, msg).await
    }

    pub async fn broadcast(&self, exclude_id: Option<u64>, msg: RpcMessage) -> HashMap<u64, Result<RpcMessage, NetworkError>> {
        let peers = self.peers.read().await;
        let mut handles = Vec::new();
        let mut results = HashMap::new();

        for (&id, &addr) in peers.iter() {
            if Some(id) == exclude_id {
                continue;
            }
            
            let transport = self.transport.clone();
            let handle = tokio::spawn(async move {
                (id, transport.send(addr, msg.clone()).await)
            });
            handles.push(handle);
        }

        for handle in handles {
            if let Ok((id, result)) = handle.await {
                results.insert(id, result);
            }
        }

        results
    }
}

pub type TcpNetwork = Network<crate::TcpTransport>;
pub type InMemoryNetwork = Network<crate::InMemoryTransport>;
