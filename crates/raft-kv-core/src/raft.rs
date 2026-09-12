use crate::error::{RaftError, Result};
use crate::message::{
    AppendEntriesRequest, AppendEntriesResponse, Command, LogEntry, RequestVoteRequest,
    RequestVoteResponse,
};
use crate::node::{Node, NodeState, Role};
use crate::storage::Storage;
use std::cmp::max;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

pub struct Raft<S: Storage> {
    state: Arc<RwLock<NodeState>>,
    storage: Arc<S>,
    peers: Vec<Node>,
}

impl<S: Storage + 'static> Raft<S> {
    pub fn new(state: Arc<RwLock<NodeState>>, storage: Arc<S>, peers: Vec<Node>) -> Self {
        Self {
            state,
            storage,
            peers,
        }
    }

    pub async fn start_election(&self) -> Result<()> {
        let (last_log_index, last_log_term, current_term, node_id) = {
            let state = self.state.read().await;
            (state.last_log_index(), state.last_log_term(), state.current_term, state.id)
        };

        let new_term = current_term + 1;

        {
            let mut state = self.state.write().await;
            // Cek lagi apakah term belum berubah saat kita menunggu lock
            if state.current_term >= new_term {
                return Ok(()); 
            }
            state.current_term = new_term;
            state.role = Role::Candidate;
            state.voted_for = Some(node_id);
        }

        self.storage
            .set_current_term(new_term)
            .await
            .map_err(|e| RaftError::StorageError(e.to_string()))?;
        self.storage
            .set_voted_for(Some(node_id))
            .await
            .map_err(|e| RaftError::StorageError(e.to_string()))?;

        info!("Node {} starting election for term {}", node_id, new_term);

        let req = RequestVoteRequest {
            term: new_term,
            candidate_id: node_id,
            last_log_index,
            last_log_term,
        };

        let votes = Arc::new(RwLock::new(1));
        let quorum = (self.peers.len() / 2) + 1;
        let state_arc = self.state.clone();
        let storage = self.storage.clone();
        let peers = self.peers.clone();

        for peer in peers {
            let req = req.clone();
            let storage = storage.clone();
            let state_arc = state_arc.clone();
            let votes = votes.clone();
            let quorum = quorum;
            let peer_addr = peer.addr.clone();
            let peer_id = peer.id;

            tokio::spawn(async move {
                match send_vote_request(peer_addr, req).await {
                    Ok(response) => {
                        if response.vote_granted {
                            let mut s = state_arc.write().await;
                            if s.role == Role::Candidate {
                                let mut v = votes.write().await;
                                *v += 1;
                                let won = *v >= quorum;
                                drop(v);
                                if won && s.role == Role::Candidate {
                                    s.become_leader();
                                    info!("Node {} became leader for term {}", s.id, s.current_term);
                                }
                            }
                        } else if response.term > s.current_term {
                             // Perlu lock ulang di sini karena 's' sudah drop atau scope berbeda
                             // Tapi di struktur ini, kita bisa pakai 's' jika masih dalam scope yang sama.
                             // Masalahnya di kode sebelumnya adalah 's' dideklarasikan di blok 'if' sebelumnya.
                             // Mari kita perbaiki dengan logika yang lebih aman:
                             if response.term > s.current_term {
                                s.current_term = response.term;
                                s.role = Role::Follower;
                                s.voted_for = None;
                                let _ = storage.set_current_term(s.current_term).await;
                                let _ = storage.set_voted_for(None).await;
                             }
                        }
                    }
                    Err(e) => warn!("Failed to request vote from {}: {}", peer_id, e),
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
                .set_current_term(state.current_term)
                .await
                .map_err(|e| RaftError::StorageError(e.to_string()))?;
            self.storage
                .set_voted_for(None)
                .await
                .map_err(|e| RaftError::StorageError(e.to_string()))?;
        }

        state.reset_election_timeout();

        if !state.match_log_entry(req.prev_log_index, req.prev_log_term) {
            let conflict_index = state.last_log_index();
            let conflict_term = state.log.last().map(|e| e.term);

            return Ok(AppendEntriesResponse {
                term: state.current_term,
                success: false,
                conflict_index: Some(conflict_index),
                conflict_term,
            });
        }

        for entry in req.entries {
            if let Some(existing) = state.get_entry(entry.index) {
                if existing.term != entry.term {
                    state.log.retain(|e| e.index < entry.index);
                    state.log.push(entry);
                }
            } else {
                state.log.push(entry);
            }
        }

        if req.leader_commit > state.commit_index {
            state.commit_index = std::cmp::min(req.leader_commit, state.last_log_index());
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
                .set_current_term(state.current_term)
                .await
                .map_err(|e| RaftError::StorageError(e.to_string()))?;
            self.storage
                .set_voted_for(None)
                .await
                .map_err(|e| RaftError::StorageError(e.to_string()))?;
        }

        let last_log_index = state.last_log_index();
        let last_log_term = state.last_log_term();

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

            Ok(RequestVoteResponse {
                term: state.current_term,
                vote_granted: true,
            })
        } else {
            Ok(RequestVoteResponse {
                term: state.current_term,
                vote_granted: false,
            })
        }
    }

    pub async fn submit_command(&self, command: Command) -> Result<u64> {
        let mut state = self.state.write().await;

        if state.role != Role::Leader {
            return Err(RaftError::NotLeader);
        }

        let next_index = state.last_log_index() + 1;
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
        let (entries, pli, plt, current_term, node_id, commit_index) = {
            let state = self.state.read().await;
            let entries: Vec<LogEntry> = state
                .log
                .iter()
                .filter(|e| e.index >= index)
                .cloned()
                .collect();

            if entries.is_empty() {
                return Ok(());
            }

            let prev_log_index = if index > 1 { index - 1 } else { 0 };
            let prev_log_term = state
                .log
                .iter()
                .find(|e| e.index == prev_log_index)
                .map(|e| e.term)
                .unwrap_or(0);

            (
                entries,
                prev_log_index,
                prev_log_term,
                state.current_term,
                state.id,
                state.commit_index,
            )
        };

        let req = AppendEntriesRequest {
            term: current_term,
            leader_id: node_id,
            prev_log_index: pli,
            prev_log_term: plt,
            entries,
            leader_commit: commit_index,
        };

        let successful_replications = Arc::new(RwLock::new(1));
        let quorum = (self.peers.len() / 2) + 1;
        let state_clone = self.state.clone();
        let storage = self.storage.clone();
        let peers = self.peers.clone();

        for peer in peers {
            let req = req.clone();
            let storage = storage.clone();
            let state_clone = state_clone.clone();
            let successful_replications = successful_replications.clone();
            let quorum = quorum;
            let peer_addr = peer.addr.clone();
            let peer_id = peer.id;

            tokio::spawn(async move {
                match send_append_entries(peer_addr, req).await {
                    Ok(response) => {
                        if response.success {
                            let mut s = state_clone.write().await;
                            if s.role == Role::Leader {
                                let mut r = successful_replications.write().await;
                                *r += 1;
                                let reached = *r >= quorum;
                                drop(r);
                                if reached && s.role == Role::Leader {
                                    s.commit_index = max(s.commit_index, index);
                                }
                            }
                        } else if response.term > s.current_term {
                            if response.term > s.current_term {
                                s.current_term = response.term;
                                s.role = Role::Follower;
                                s.voted_for = None;
                                let _ = storage.set_current_term(s.current_term).await;
                                let _ = storage.set_voted_for(None).await;
                            }
                        }
                    }
                    Err(e) => warn!("Failed to replicate to {}: {}", peer_id, e),
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
