use crate::rpc::{Request, Response};
use raft_kv_core::{NodeId, Result};

pub struct Transport;

impl Transport {
    pub fn send(&self, _target: NodeId, _req: Request) -> Result<Response> {
        Err(raft_kv_core::RaftError::Network("Not implemented".into()))
    }
}
