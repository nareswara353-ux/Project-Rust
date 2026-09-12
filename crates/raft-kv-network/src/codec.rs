use crate::{NetworkError, RpcMessage};
use bytes::Bytes;

pub fn encode_message(msg: &RpcMessage) -> Result<Bytes, NetworkError> {
    let serialized =
        bincode::serialize(msg).map_err(|e| NetworkError::Serialization(e.to_string()))?;
    Ok(Bytes::from(serialized))
}

pub fn decode_message(bytes: &[u8]) -> Result<RpcMessage, NetworkError> {
    let msg: RpcMessage =
        bincode::deserialize(bytes).map_err(|e| NetworkError::Serialization(e.to_string()))?;
    Ok(msg)
}
