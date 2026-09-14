use crate::{NetworkError, RpcMessage};
use raft_kv_core::message::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    RequestVoteRequest, RequestVoteResponse,
};
use raft_kv_core::RaftError;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

pub type VoteHandler = Arc<RwLock<dyn Fn(RequestVoteRequest) -> Result<RequestVoteResponse, RaftError> + Send + Sync>>;
pub type AppendHandler = Arc<RwLock<dyn Fn(AppendEntriesRequest) -> Result<AppendEntriesResponse, RaftError> + Send + Sync>>;
pub type SnapshotHandler = Arc<RwLock<dyn Fn(InstallSnapshotRequest) -> Result<InstallSnapshotResponse, RaftError> + Send + Sync>>;

pub struct RpcServer {
    addr: SocketAddr,
    vote_handler: Option<VoteHandler>,
    append_handler: Option<AppendHandler>,
    snapshot_handler: Option<SnapshotHandler>,
}

impl RpcServer {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            vote_handler: None,
            append_handler: None,
            snapshot_handler: None,
        }
    }

    pub fn with_vote_handler(mut self, handler: VoteHandler) -> Self {
        self.vote_handler = Some(handler);
        self
    }

    pub fn with_append_handler(mut self, handler: AppendHandler) -> Self {
        self.append_handler = Some(handler);
        self
    }

    pub fn with_snapshot_handler(mut self, handler: SnapshotHandler) -> Self {
        self.snapshot_handler = Some(handler);
        self
    }

    pub async fn run(self) -> Result<(), NetworkError> {
        let listener = TcpListener::bind(self.addr).await?;

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let vote_h = self.vote_handler.clone();
            let append_h = self.append_handler.clone();
            let snapshot_h = self.snapshot_handler.clone();

            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, vote_h, append_h, snapshot_h).await {
                    eprintln!("Error handling connection from {}: {}", peer_addr, e);
                }
            });
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    vote_handler: Option<VoteHandler>,
    append_handler: Option<AppendHandler>,
    snapshot_handler: Option<SnapshotHandler>,
) -> Result<(), NetworkError> {
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).await?;
    if n == 0 {
        return Ok(());
    }

    let msg: RpcMessage = bincode::deserialize(&buf[..n])
        .map_err(|e| NetworkError::Serialization(e.to_string()))?;

    let response = match msg {
        RpcMessage::RequestVote(req) => {
            if let Some(handler) = vote_handler {
                let h = handler.read().await;
                let res = h(req).map_err(|e| NetworkError::Connection(e.to_string()))?;
                RpcMessage::RequestVoteResponse(res)
            } else {
                return Err(NetworkError::Connection("No vote handler registered".into()));
            }
        }
        RpcMessage::AppendEntries(req) => {
            if let Some(handler) = append_handler {
                let h = handler.read().await;
                let res = h(req).map_err(|e| NetworkError::Connection(e.to_string()))?;
                RpcMessage::AppendEntriesResponse(res)
            } else {
                return Err(NetworkError::Connection("No append handler registered".into()));
            }
        }
        RpcMessage::InstallSnapshot(req) => {
            if let Some(handler) = snapshot_handler {
                let h = handler.read().await;
                let res = h(req).map_err(|e| NetworkError::Connection(e.to_string()))?;
                RpcMessage::InstallSnapshotResponse(res)
            } else {
                return Err(NetworkError::Connection("No snapshot handler registered".into()));
            }
        }
        _ => return Err(NetworkError::Connection("Unknown message type".into())),
    };

    let resp_bytes = bincode::serialize(&response)
        .map_err(|e| NetworkError::Serialization(e.to_string()))?;
    
    stream.write_all(&resp_bytes).await?;
    stream.flush().await?;

    Ok(())
}
