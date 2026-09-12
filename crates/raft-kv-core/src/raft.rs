use crate::error::{RaftError, Result};
use crate::message::{
    AppendEntriesRequest, AppendEntriesResponse, Command, LogEntry, RequestVoteRequest,
    RequestVoteResponse, Role,
};
use crate::node::{Node, NodeState};
use crate::storage::Storage;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

pub struct Raft<S: Storage> {
    pub state: Arc<RwLock<NodeState>>,
    pub storage: Arc<S>,
    pub peers: Vec<Node>,
}

impl<S: Storage> Raft<S> {
    pub fn new(state: Arc<RwLock<NodeState>>, storage: Arc<S>, peers: Vec<Node>) -> Self {
        Self { state, storage, peers }
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
                            votes += 1;
                            if votes >= quorum && s.role == Role::Candidate {
                                s.become_leader();
                                info!(
                                    "Node {} became leader for term {}",
                                    s.id, s.current_term
                                );
                            }
                        } else if response.term > s.current_term {
                            // Error: s is out of scope here, need to fix logic
                        }
                    }
                    Err(e) => warn!("Failed to get vote from {}: {}", peer.id, e),
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
            state.current_term = req.term;
            state.role = Role::Follower;
            state.voted_for = None;
            self.storage
                .set_current_term(req.term)
                .await
                .map_err(|e| RaftError::StorageError(e.to_string()))?;
            self.storage
                .set_voted_for(None)
                .await
                .map_err(|e| RaftError::StorageError(e.to_string()))?;
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
            if let Some(existing) = state.log.iter().find(|e| e.index == entry.index) {
                if existing.term != entry.term {
                    state.log.truncate((entry.index - 1) as usize);
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
            state.current_term = req.term;
            state.role = Role::Follower;
            state.voted_for = None;
            self.storage
                .set_current_term(req.term)
                .await
                .map_err(|e| RaftError::StorageError(e.to_string()))?;
            self.storage
                .set_voted_for(None)
                .await
                .map_err(|e| RaftError::StorageError(e.to_string()))?;
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

        if state.voted_for.is_none() || state.voted_for == Some(req.candidate_id) {
            state.voted_for = Some(req.candidate_id);
            self.storage
                .set_voted_for(Some(req.candidate_id))
                .await
                .map_err(|e| RaftError::StorageError(e.to_string()))?;

            state.reset_election_timeout();

            return Ok(RequestVoteResponse {
                term: state.current_term,
                vote_granted: true,
            });
        }

        Ok(RequestVoteResponse {
            term: state.current_term,
            vote_granted: false,
        })
    }

    pub async fn submit_command(&self, command: Command) -> Result<u64> {
        let mut state = self.state.write().await;

        if state.role != Role::Leader {
            return Err(RaftError::NotLeader);
        }

        let next_index = state.log.last().map(|e| e.index + 1).unwrap_or(1);
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
                            successful_replications += 1;
                            if successful_replications >= quorum && s.role == Role::Leader {
                                s.commit_index = std::max(s.commit_index, index);
                            }
                        } else if response.term > s.current_term {
                             // Error: s is out of scope here
                        }
                    }
                    Err(e) => warn!("Failed to replicate to {}: {}", peer.id, e),
                }
            });
        }

        Ok(())
    }
}

async fn send_vote_request(
    _address: String,
    _req: RequestVoteRequest,
) -> Result<RequestVoteResponse> {
    todo!("Implement network call")
}

async fn send_append_entries(
    _address: String,
    _req: AppendEntriesRequest,
) -> Result<AppendEntriesResponse> {
    todo!("Implement network call")
}
