use crate::error::{RaftError, Result};
use crate::message::{
    AppendEntriesRequest, AppendEntriesResponse, Command, LogEntry, RequestVoteRequest,
    RequestVoteResponse, Role,
};
use crate::node::{Node, NodeState};
use crate::storage::Storage;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

pub struct Raft<S: Storage> {
    state: Arc<RwLock<NodeState>>,
    storage: Arc<S>,
    peers: Vec<Node>,
}

impl<S: Storage> Raft<S> {
    pub fn new(node_id: u64, storage: S, peers: Vec<Node>) -> Self {
        let state = NodeState::new(node_id);
        Self {
            state: Arc::new(RwLock::new(state)),
            storage: Arc::new(storage),
            peers,
        }
    }

    pub async fn start_election(&self) -> Result<()> {
        let mut state = self.state.write().await;
        state.current_term += 1;
        state.role = Role::Candidate;
        state.voted_for = Some(state.id);

        self.storage
            .set_current_term(state.current_term)
            .await
            .map_err(|e| RaftError::StorageError(e.to_string()))?;
        self.storage
            .set_voted_for(Some(state.id))
            .await
            .map_err(|e| RaftError::StorageError(e.to_string()))?;

        info!(
            "Node {} starting election for term {}",
            state.id, state.current_term
        );

        let last_log_index = state.log.last().map(|e| e.index).unwrap_or(0);
        let last_log_term = state.log.last().map(|e| e.term).unwrap_or(0);

        let request = RequestVoteRequest {
            term: state.current_term,
            candidate_id: state.id,
            last_log_index,
            last_log_term,
        };

        drop(state);

        let mut votes = 1;
        let quorum = (self.peers.len() / 2) + 1;

        for peer in &self.peers {
            let req = request.clone();
            let storage = self.storage.clone();
            let state_arc = self.state.clone();

            tokio::spawn(async move {
                match send_vote_request(peer.address.clone(), req).await {
                    Ok(response) => {
                        if response.vote_granted {
                            let mut s = state_arc.write().await;
                            if s.current_term == response.term {
                                votes += 1;
                                if votes >= quorum && s.role == Role::Candidate {
                                    s.become_leader();
                                    info!(
                                        "Node {} became leader for term {}",
                                        s.id, s.current_term
                                    );
                                }
                            }
                        } else if response.term > s.current_term {
                            let mut s = state_arc.write().await;
                            s.step_down(response.term);
                        }
                    }
                    Err(e) => warn!("Failed to get vote from {}: {}", peer.address, e),
                }
            });
        }

        Ok(())
    }

    pub async fn handle_append_entries(
        &self,
        req: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse> {
        let mut state = self.state.write().await;

        if req.term < state.current_term {
            return Ok(AppendEntriesResponse {
                term: state.current_term,
                success: false,
                conflict_index: None,
                conflict_term: None,
            });
        }

        if req.term > state.current_term {
            state.step_down(req.term);
        }

        state.reset_election_timeout();

        if !state.match_log_entry(req.prev_log_index, req.prev_log_term) {
            let conflict_index = state.log.last().map(|e| e.index).unwrap_or(0);
            let conflict_term = state.log.last().map(|e| e.term);

            return Ok(AppendEntriesResponse {
                term: state.current_term,
                success: false,
                conflict_index: Some(conflict_index),
                conflict_term,
            });
        }

        for entry in &req.entries {
            if let Some(existing) = state.get_entry_at(entry.index) {
                if existing.term != entry.term {
                    state.truncate_from(entry.index);
                    state.log.push(entry.clone());
                }
            } else {
                state.log.push(entry.clone());
            }
        }

        if req.leader_commit > state.commit_index {
            state.commit_index = std::cmp::min(
                req.leader_commit,
                state.log.last().map(|e| e.index).unwrap_or(0),
            );
        }

        Ok(AppendEntriesResponse {
            term: state.current_term,
            success: true,
            conflict_index: None,
            conflict_term: None,
        })
    }

    pub async fn handle_request_vote(
        &self,
        req: RequestVoteRequest,
    ) -> Result<RequestVoteResponse> {
        let mut state = self.state.write().await;

        if req.term < state.current_term {
            return Ok(RequestVoteResponse {
                term: state.current_term,
                vote_granted: false,
            });
        }

        if req.term > state.current_term {
            state.step_down(req.term);
        }

        if state.voted_for.is_some() && state.voted_for != Some(req.candidate_id) {
            return Ok(RequestVoteResponse {
                term: state.current_term,
                vote_granted: false,
            });
        }

        let last_log_index = state.log.last().map(|e| e.index).unwrap_or(0);
        let last_log_term = state.log.last().map(|e| e.term).unwrap_or(0);

        if req.last_log_term < last_log_term
            || (req.last_log_term == last_log_term && req.last_log_index < last_log_index)
        {
            return Ok(RequestVoteResponse {
                term: state.current_term,
                vote_granted: false,
            });
        }

        state.voted_for = Some(req.candidate_id);
        self.storage
            .set_voted_for(Some(req.candidate_id))
            .await
            .map_err(|e| RaftError::StorageError(e.to_string()))?;

        state.reset_election_timeout();

        Ok(RequestVoteResponse {
            term: state.current_term,
            vote_granted: true,
        })
    }

    pub async fn submit_command(&self, command: Command) -> Result<u64> {
        let mut state = self.state.write().await;

        if state.role != Role::Leader {
            return Err(RaftError::NotLeader);
        }

        let next_index = state.log.last().map(|e| e.index).unwrap_or(0) + 1;
        let entry = LogEntry {
            term: state.current_term,
            index: next_index,
            command,
        };

        state.log.push(entry.clone());

        drop(state);

        self.replicate_to_peers(next_index).await?;

        Ok(next_index)
    }

    async fn replicate_to_peers(&self, index: u64) -> Result<()> {
        let state = self.state.read().await;
        let entries_to_send: Vec<LogEntry> = state
            .log
            .iter()
            .filter(|e| e.index >= index)
            .cloned()
            .collect();

        if entries_to_send.is_empty() {
            return Ok(());
        }

        let prev_log_index = if index > 1 { index - 1 } else { 0 };
        let prev_log_term = state
            .log
            .iter()
            .find(|e| e.index == prev_log_index)
            .map(|e| e.term)
            .unwrap_or(0);

        let request = AppendEntriesRequest {
            term: state.current_term,
            leader_id: state.id,
            prev_log_index,
            prev_log_term,
            entries: entries_to_send,
            leader_commit: state.commit_index,
        };

        drop(state);

        let mut successful_replications = 1;
        let quorum = (self.peers.len() / 2) + 1;
        let state_arc = self.state.clone();

        for peer in &self.peers {
            let req = request.clone();
            let storage = self.storage.clone();
            let state_clone = state_arc.clone();

            tokio::spawn(async move {
                match send_append_entries(peer.address.clone(), req).await {
                    Ok(response) => {
                        if response.success {
                            let mut s = state_clone.write().await;
                            if s.current_term == response.term {
                                successful_replications += 1;
                                if successful_replications >= quorum {
                                    let last_index = s.log.last().map(|e| e.index).unwrap_or(0);
                                    s.commit_index = last_index;
                                }
                            }
                        } else if response.term > s.current_term {
                            let mut s = state_clone.write().await;
                            s.step_down(response.term);
                        }
                    }
                    Err(e) => warn!("Replication to {} failed: {}", peer.address, e),
                }
            });
        }

        Ok(())
    }
}

async fn send_vote_request(
    address: String,
    req: RequestVoteRequest,
) -> Result<RequestVoteResponse> {
    todo!("Implement network call")
}

async fn send_append_entries(
    address: String,
    req: AppendEntriesRequest,
) -> Result<AppendEntriesResponse> {
    todo!("Implement network call")
}
