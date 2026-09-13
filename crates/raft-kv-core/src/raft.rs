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
        let (last_log_index, last_log_term, node_id) = {
            let state = self.state.read().await;
            (state.last_log_index(), state.last_log_term(), state.id)
        };

        // Update state awal election
        {
            let mut state = self.state.write().await;
            state.current_term += 1;
            state.role = Role::Candidate;
            state.voted_for = Some(node_id);
        }

        let new_term = {
            let state = self.state.read().await;
            state.current_term
        };

        self.storage
            .set_current_term(new_term)
            .await
            .map_err(|e| RaftError::Storage(e.to_string()))?;
        self.storage
            .set_voted_for(Some(node_id))
            .await
            .map_err(|e| RaftError::Storage(e.to_string()))?;

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
            let peer_addr = peer.addr.clone();
            let peer_id = peer.id;

            tokio::spawn(async move {
                match send_vote_request(peer_addr, req).await {
                    Ok(response) => {
                        // LOGIKA DIPERBAIKI: Semua operasi state ada di dalam satu scope lock
                        let s = state_arc.write().await;

                        if response.vote_granted && s.role == Role::Candidate {
                            // Hitung vote sambil masih memegang lock state
                            // Catatan: Idealnya votes lock terpisah, tapi untuk aman kita urutkan saja
                            // Atau lebih baik: cek quorum dulu baru update state

                            // Karena kita butuh akses votes (async lock) dan s (async lock),
                            // kita harus hati-hati urutan lock untuk hindari deadlock.
                            // Strategi aman: Lepas s sebentar untuk hitung vote? Tidak, race condition.
                            // Strategi terbaik: Hitung vote DULU, baru lock state untuk commit keputusan.

                            drop(s); // Lepas lock state sebentar untuk hitung vote

                            {
                                let mut v = votes.write().await;
                                *v += 1;
                                if *v >= quorum {
                                    // Re-acquire lock state untuk jadi leader
                                    let mut s_final = state_arc.write().await;
                                    if s_final.role == Role::Candidate {
                                        s_final.become_leader();
                                        info!(
                                            "Node {} became leader for term {}",
                                            s_final.id, s_final.current_term
                                        );
                                    }
                                }
                            }
                        }

                        // Cek term lebih tinggi (Step down)
                        // Kita harus re-acquire lock jika tadi sudah di-drop, atau pakai lock yang sama jika belum
                        // Karena flow di atas sudah drop(s), kita lock ulang di sini khusus untuk step down
                        if response.term > new_term {
                            // Pakai local var new_term sebagai pembanding awal, tapi harus cek state terkini
                            let mut s_step = state_arc.write().await;
                            if response.term > s_step.current_term {
                                s_step.current_term = response.term;
                                s_step.role = Role::Follower;
                                s_step.voted_for = None;
                                let _ = storage.set_current_term(s_step.current_term).await;
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
                .map_err(|e| RaftError::Storage(e.to_string()))?;
            self.storage
                .set_voted_for(None)
                .await
                .map_err(|e| RaftError::Storage(e.to_string()))?;
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
                .map_err(|e| RaftError::Storage(e.to_string()))?;
            self.storage
                .set_voted_for(None)
                .await
                .map_err(|e| RaftError::Storage(e.to_string()))?;
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
                .map_err(|e| RaftError::Storage(e.to_string()))?;

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

            let prev_log_index = index.saturating_sub(1);
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
                        }

                        // Step down jika term lebih tinggi
                        if response.term > current_term {
                            let mut s_step = state_clone.write().await;
                            if response.term > s_step.current_term {
                                s_step.current_term = response.term;
                                s_step.role = Role::Follower;
                                s_step.voted_for = None;
                                let _ = storage.set_current_term(s_step.current_term).await;
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
