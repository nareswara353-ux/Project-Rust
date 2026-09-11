use raft_kv_core::{NodeId, Term, Index};

pub struct Snapshot {
    pub last_index: Index,
    pub last_term: Term,
    pub data: Vec<u8>,
}

impl Snapshot {
    pub fn new(last_index: Index, last_term: Term) -> Self {
        Self {
            last_index,
            last_term,
            data: Vec::new(),
        }
    }
}
