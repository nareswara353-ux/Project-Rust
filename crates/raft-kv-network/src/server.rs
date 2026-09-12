use crate::transport::Transport;
use crate::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, NetworkError, RequestVoteRequest, RequestVoteResponse, RpcMessage,
};
use raft_kv_core::NodeId;
use std::sync::Arc;
use tokio::sync::mpsc;

pub type AppendEntriesHandler = dyn Fn(AppendEntriesRequest) -> AppendEntriesResponse + Send + Sync + 'static;
pub type RequestVoteHandler = dyn Fn(RequestVoteRequest) -> RequestVoteResponse + Send + Sync + 'static;
pub type InstallSnapshotHandler = dyn Fn(InstallSnapshotRequest) -> InstallSnapshotResponse + Send + Sync + 'static;

pub struct RpcServer {
    node_id: NodeId,
    transport: Arc<Transport>,
    append_entries_handler: Option<Arc<AppendEntriesHandler>>,
    request_vote_handler: Option<Arc<RequestVoteHandler>>,
    install_snapshot_handler: Option<Arc<InstallSnapshotHandler>>,
}

impl RpcServer {
    pub fn new(node_id: NodeId, transport: Arc<Transport>) -> Self {
        Self {
            node_id,
            transport,
            append_entries_handler: None,
            request_vote_handler: None,
            install_snapshot_handler: None,
        }
    }

    pub fn with_append_entries_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(AppendEntriesRequest) -> AppendEntriesResponse + Send + Sync + 'static,
    {
        self.append_entries_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_request_vote_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(RequestVoteRequest) -> RequestVoteResponse + Send + Sync + 'static,
    {
        self.request_vote_handler = Some(Arc::new(handler));
        self
    }

    pub fn with_install_snapshot_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(InstallSnapshotRequest) -> InstallSnapshotResponse + Send + Sync + 'static,
    {
        self.install_snapshot_handler = Some(Arc::new(handler));
        self
    }

    pub async fn start(&self) -> Result<(), NetworkError> {
        let (tx, mut rx) = mpsc::channel::<RpcMessage>(100);
        self.transport.register_node(self.node_id, tx).await;

        println!("RPC Server started for node {}", self.node_id);

        while let Some(msg) = rx.recv().await {
            match msg {
                RpcMessage::AppendEntriesRequest(req) => {
                    if let Some(handler) = &self.append_entries_handler {
                        let resp = handler(req);
                        let _ = self
                            .transport
                            .send(req.leader_id, RpcMessage::AppendEntriesResponse(resp))
                            .await;
                    }
                }
                RpcMessage::RequestVoteRequest(req) => {
                    if let Some(handler) = &self.request_vote_handler {
                        let resp = handler(req);
                        let _ = self
                            .transport
                            .send(req.candidate_id, RpcMessage::RequestVoteResponse(resp))
                            .await;
                    }
                }
                RpcMessage::InstallSnapshotRequest(req) => {
                    if let Some(handler) = &self.install_snapshot_handler {
                        let resp = handler(req);
                        let _ = self
                            .transport
                            .send(req.leader_id, RpcMessage::InstallSnapshotResponse(resp))
                            .await;
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }
}
