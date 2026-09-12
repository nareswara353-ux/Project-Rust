use crate::{NetworkError, RpcMessage};
use raft_kv_core::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

type Tx = mpsc::Sender<RpcMessage>;
type Rx = mpsc::Receiver<RpcMessage>;

pub struct Transport {
    nodes: Arc<Mutex<HashMap<NodeId, Tx>>>,
}

impl Transport {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn register_node(&self, node_id: NodeId, tx: Tx) {
        let mut nodes = self.nodes.lock().await;
        nodes.insert(node_id, tx);
    }

    pub async fn unregister_node(&self, node_id: NodeId) {
        let mut nodes = self.nodes.lock().await;
        nodes.remove(&node_id);
    }

    pub async fn send(&self, target_id: NodeId, msg: RpcMessage) -> Result<(), NetworkError> {
        let nodes = self.nodes.lock().await;
        if let Some(tx) = nodes.get(&target_id) {
            tx.send(msg)
                .await
                .map_err(|_| NetworkError::Connection("Channel closed".into()))?;
            Ok(())
        } else {
            Err(NetworkError::Connection(format!(
                "Node {} not found",
                target_id
            )))
        }
    }

    pub async fn broadcast(&self, exclude_id: NodeId, msg: RpcMessage) -> Result<(), NetworkError> {
        let nodes = self.nodes.lock().await;
        for (id, tx) in nodes.iter() {
            if *id != exclude_id {
                let _ = tx.send(msg.clone()).await;
            }
        }
        Ok(())
    }
}

impl Default for Transport {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TransportPair {
    pub local_rx: Rx,
    pub remote_tx: Tx,
}

impl TransportPair {
    pub fn new(buffer_size: usize) -> (Self, Self) {
        let (tx1, rx1) = mpsc::channel(buffer_size);
        let (tx2, rx2) = mpsc::channel(buffer_size);

        let pair1 = TransportPair {
            local_rx: rx1,
            remote_tx: tx2,
        };
        let pair2 = TransportPair {
            local_rx: rx2,
            remote_tx: tx1,
        };

        (pair1, pair2)
    }
}
