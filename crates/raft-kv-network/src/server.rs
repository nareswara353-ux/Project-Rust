use crate::{NetworkError, RpcMessage};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

pub struct RpcServer {
    addr: SocketAddr,
    handler: Arc<dyn Fn(RpcMessage) -> futures::future::BoxFuture<'static, Option<RpcMessage>> + Send + Sync>,
}

impl RpcServer {
    pub fn new<F, Fut>(addr: SocketAddr, handler: F) -> Self
    where
        F: Fn(RpcMessage) -> Fut + Send + Sync + 'static,
        Fut: futures::future::Future<Output = Option<RpcMessage>> + Send + 'static,
    {
        Self {
            addr,
            handler: Arc::new(move |msg| Box::pin(handler(msg))),
        }
    }

    pub async fn run(self) -> Result<(), NetworkError> {
        let listener = TcpListener::bind(self.addr).await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let handler = self.handler.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream, handler).await {
                    eprintln!("Connection error: {}", e);
                }
            });
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    handler: Arc<dyn Fn(RpcMessage) -> futures::future::BoxFuture<'static, Option<RpcMessage>> + Send + Sync>,
) -> Result<(), NetworkError> {
    let (mut reader, mut writer) = stream.into_split();

    let mut len_buf = [0u8; 4];
    while reader.read_exact(&mut len_buf).await.is_ok() {
        let msg_len = u32::from_be_bytes(len_buf) as usize;
        let mut buf = vec![0u8; msg_len];
        reader.read_exact(&mut buf).await?;

        let msg = crate::codec::decode_message(&buf)?;
        if let Some(response) = handler(msg).await {
            let resp_bytes = crate::codec::encode_message(&response)?;
            let resp_len = resp_bytes.len() as u32;
            writer.write_all(&resp_len.to_be_bytes()).await?;
            writer.write_all(&resp_bytes).await?;
        }
    }

    Ok(())
}
