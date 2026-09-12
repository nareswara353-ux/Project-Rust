use crate::{NetworkError, RpcMessage};
use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

pub struct RpcCodec;

impl Encoder<RpcMessage> for RpcCodec {
    type Error = NetworkError;

    fn encode(&mut self, msg: RpcMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = bincode::serialize(&msg)
            .map_err(|e| NetworkError::Serialization(e.to_string()))?;

        let len = bytes.len() as u32;
        dst.reserve(4 + bytes.len());
        dst.put_u32(len);
        dst.put_slice(&bytes);
        Ok(())
    }
}

impl Decoder for RpcCodec {
    type Item = RpcMessage;
    type Error = NetworkError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let len = src.get_u32() as usize;
        if src.len() < len {
            src.reserve(len - src.len());
            return Ok(None);
        }

        let bytes = src.split_to(len).freeze();
        let msg: RpcMessage = bincode::deserialize(&bytes)
            .map_err(|e| NetworkError::Serialization(e.to_string()))?;

        Ok(Some(msg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AppendEntriesRequest, AppendEntriesResponse};
    use raft_kv_core::{Command, LogEntry, NodeId};

    #[test]
    fn test_encode_decode_append_entries() {
        let mut codec = RpcCodec;
        let mut buffer = BytesMut::new();

        let req = RpcMessage::AppendEntriesRequest(AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term: 1,
                index: 1,
                command: Command::Put {
                    key: "foo".into(),
                    value: "bar".into(),
                },
            }],
            leader_commit: 0,
        });

        codec.encode(req.clone(), &mut buffer).unwrap();
        let decoded = codec.decode(&mut buffer).unwrap().unwrap();

        assert!(matches!(decoded, RpcMessage::AppendEntriesRequest(_)));
    }
}
