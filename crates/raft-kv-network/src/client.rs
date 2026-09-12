use crate::codec::encode_message;
use crate::{NetworkError, RpcMessage};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::info;

pub struct RpcClient {
    connect_timeout: Duration,
    read_timeout: Duration,
}

impl Default for RpcClient {
    fn default() -> Self {
        Self::new()
    }
}

impl RpcClient {
    pub fn new() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(10),
        }
    }

    pub fn with_timeouts(mut self, connect_ms: u64, read_ms: u64) -> Self {
        self.connect_timeout = Duration::from_millis(connect_ms);
        self.read_timeout = Duration::from_millis(read_ms);
        self
    }

    pub async fn send(
        &self,
        addr: SocketAddr,
        msg: RpcMessage,
    ) -> Result<RpcMessage, NetworkError> {
        let stream = timeout(self.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| NetworkError::Timeout)?
            .map_err(NetworkError::Io)?;

        let (mut reader, mut writer) = stream.into_split();

        let encoded = encode_message(&msg)?;
        writer.write_all(&encoded).await?;
        writer.flush().await?;

        info!("Sent message to {}: {:?}", addr, msg);

        let mut buf = vec![0u8; 4096];
        let n = timeout(self.read_timeout, reader.read(&mut buf))
            .await
            .map_err(|_| NetworkError::Timeout)?
            .map_err(NetworkError::Io)?;

        if n == 0 {
            return Err(NetworkError::Connection("Connection closed by peer".into()));
        }

        use crate::codec::decode_message;
        let response = decode_message(&buf[..n])?;
        info!("Received response from {}: {:?}", addr, response);

        Ok(response)
    }

    pub async fn send_one_way(
        &self,
        addr: SocketAddr,
        msg: RpcMessage,
    ) -> Result<(), NetworkError> {
        let stream = timeout(self.connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| NetworkError::Timeout)?
            .map_err(NetworkError::Io)?;

        let (_, mut writer) = stream.into_split();

        let encoded = encode_message(&msg)?;
        writer.write_all(&encoded).await?;
        writer.flush().await?;

        info!("Sent one-way message to {}: {:?}", addr, msg);

        Ok(())
    }
}
