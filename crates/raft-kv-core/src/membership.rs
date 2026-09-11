use crate::types::NodeId;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub nodes: HashSet<NodeId>,
    pub learners: HashSet<NodeId>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            nodes: HashSet::new(),
            learners: HashSet::new(),
        }
    }
}

impl ClusterConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_nodes(nodes: Vec<NodeId>) -> Self {
        let mut config = Self::new();
        for node in nodes {
            config.nodes.insert(node);
        }
        config
    }

    pub fn add_node(&mut self, node_id: NodeId) {
        self.nodes.insert(node_id);
        self.learners.remove(&node_id);
    }

    pub fn remove_node(&mut self, node_id: NodeId) {
        self.nodes.remove(&node_id);
        self.learners.remove(&node_id);
    }

    pub fn add_learner(&mut self, node_id: NodeId) {
        if !self.nodes.contains(&node_id) {
            self.learners.insert(node_id);
        }
    }

    pub fn promote_learner(&mut self, node_id: NodeId) -> bool {
        if self.learners.remove(&node_id) {
            self.nodes.insert(node_id);
            true
        } else {
            false
        }
    }

    pub fn contains(&self, node_id: NodeId) -> bool {
        self.nodes.contains(&node_id)
    }

    pub fn quorum_size(&self) -> usize {
        (self.nodes.len() / 2) + 1
    }

    pub fn is_majority(&self, count: usize) -> bool {
        count >= self.quorum_size()
    }
}

#[derive(Debug, Clone)]
pub enum MembershipChange {
    AddNode(NodeId),
    RemoveNode(NodeId),
    AddLearner(NodeId),
    PromoteLearner(NodeId),
}
