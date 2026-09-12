use crate::{NetworkError, RpcMessage};
use bytes::Bytes;

pub struct MessageCodec;

impl MessageCodec {
    pub fn encode(msg: &RpcMessage) -> Result<Bytes, NetworkError> {
        let serialized = bincode::serialize(msg)
            .map_err(|e| NetworkError::Serialization(e.to_string()))?;
        Ok(Bytes::from(serialized))
    }

    pub fn decode(bytes: &[u8]) -> Result<RpcMessage, NetworkError> {
        let msg: RpcMessage = bincode::deserialize(bytes)
            .map_err(|e| NetworkError::Serialization(e.to_string()))?;
        Ok(msg)
    }

    pub fn size_hint(msg: &RpcMessage) -> usize {
        bincode::serialized_size(msg).unwrap_or(0) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AppendEntriesRequest, AppendEntriesResponse};
    use raft_kv_core::{Command, LogEntry};

    #[test]
    fn test_encode_decode_append_entries() {
        let request = RpcMessage::AppendEntriesRequest(AppendEntriesRequest {
            term: 1,
            leader_id: 101,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term: 1,
                index: 1,
                command: Command::Put {
                    key: "foo".to_string(),
                    value: "bar".to_string(),
                },
            }],
            leader_commit: 0,
        });

        let encoded = MessageCodec::encode(&request).unwrap();
        let decoded = MessageCodec::decode(&encoded).unwrap();

        match decoded {
            RpcMessage::AppendEntriesRequest(req) => {
                assert_eq!(req.term, 1);
                assert_eq!(req.leader_id, 101);
                assert_eq!(req.entries.len(), 1);
            }
            _ => panic!("Decoded wrong message type"),
        }
    }

    #[test]
    fn test_encode_decode_response() {
        let response = RpcMessage::AppendEntriesResponse(AppendEntriesResponse {
            term: 1,
            success: true,
            conflict_index: None,
            conflict_term: None,
        });

        let encoded = MessageCodec::encode(&response).unwrap();
        let decoded = MessageCodec::decode(&encoded).unwrap();

        match decoded {
            RpcMessage::AppendEntriesResponse(resp) => {
                assert!(resp.success);
                assert_eq!(resp.term, 1);
            }
            _ => panic!("Decoded wrong message type"),
        }
    }
}
