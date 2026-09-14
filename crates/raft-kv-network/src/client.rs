use crate::{NetworkError, RpcMessage};
use raft_kv_core::message::*;
use raft_kv_core::RaftError;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

pub struct RpcClient {
    timeout_duration: Duration,
}

impl RpcClient {
    pub fn new(timeout_ms: u64) -> Self {
        Self {
            timeout_duration: Duration::from_millis(timeout_ms),
        }
    }

    pub async fn send_request_vote(
        &self,
        addr: &str,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse, RaftError> {
        let msg = RpcMessage::RequestVote(req);
        let resp_msg = self.send_message(addr, msg).await?;

        match resp_msg {
            RpcMessage::RequestVoteResponse(resp) => Ok(resp),
            _ => Err(RaftError::Network("Unexpected response type".into())),
        }
    }

    pub async fn send_append_entries(
        &self,
        addr: &str,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        let msg = RpcMessage::AppendEntries(req);
        let resp_msg = self.send_message(addr, msg).await?;

        match resp_msg {
            RpcMessage::AppendEntriesResponse(resp) => Ok(resp),
            _ => Err(RaftError::Network("Unexpected response type".into())),
        }
    }

    pub async fn send_install_snapshot(
        &self,
        addr: &str,
        req: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        let msg = RpcMessage::InstallSnapshot(req);
        let resp_msg = self.send_message(addr, msg).await?;

        match resp_msg {
            RpcMessage::InstallSnapshotResponse(resp) => Ok(resp),
            _ => Err(RaftError::Network("Unexpected response type".into())),
        }
    }

    async fn send_message(&self, addr: &str, msg: RpcMessage) -> Result<RpcMessage, RaftError> {
        let stream = timeout(self.timeout_duration, TcpStream::connect(addr))
            .await
            .map_err(|_| RaftError::Network("Connection timeout".into()))?
            .map_err(|e| RaftError::Network(format!("Connection failed: {}", e)))?;

        let (mut reader, mut writer) = stream.into_split();

        let data = bincode::serialize(&msg)
            .map_err(|e| RaftError::Network(format!("Serialize request error: {}", e)))?;

        let len = data.len() as u32;
        writer
            .write_all(&len.to_be_bytes())
            .await
            .map_err(|e| RaftError::Network(format!("Write length error: {}", e)))?;

        writer
            .write_all(&data)
            .await
            .map_err(|e| RaftError::Network(format!("Write data error: {}", e)))?;

        writer
            .flush()
            .await
            .map_err(|e| RaftError::Network(format!("Flush error: {}", e)))?;

        let mut len_buf = [0u8; 4];
        reader
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| RaftError::Network(format!("Read length error: {}", e)))?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;

        let mut resp_data = vec![0u8; resp_len];
        reader
            .read_exact(&mut resp_data)
            .await
            .map_err(|e| RaftError::Network(format!("Read data error: {}", e)))?;

        let resp_msg: RpcMessage = bincode::deserialize(&resp_data)
            .map_err(|e| RaftError::Network(format!("Deserialize response error: {}", e)))?;

        Ok(resp_msg)
    }
}
