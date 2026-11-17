use crate::raft::roles::Role;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize)]
pub struct Peer {
    pub node_id: String,
    pub http_address: String,
    pub grpc_address: String,
    pub role: Role,
}

impl Peer {
    pub fn new(node_id: String, http_address: String, grpc_address: String, role: Role) -> Self {
        Self {
            node_id,
            http_address,
            grpc_address,
            role,
        }
    }
}

#[derive(Clone, Default)]
pub struct PeerRegistry {
    peers: HashMap<String, Peer>,
}

impl PeerRegistry {
    pub fn from_env(value: &str) -> Self {
        let peers = value
            .split(',')
            .filter_map(|entry| {
                let mut parts = entry.split('|');
                Some(Peer::new(
                    parts.next()?.to_string(),
                    parts.next()?.to_string(),
                    parts.next()?.to_string(),
                    Role::Undefined,
                ))
            })
            .map(|peer| (peer.node_id.clone(), peer))
            .collect();

        Self { peers }
    }

    pub fn upsert(&mut self, peer: Peer) {
        self.peers.insert(peer.node_id.clone(), peer);
    }

    pub fn all(&self) -> Vec<Peer> {
        self.peers.values().cloned().collect()
    }

    pub fn leader(&self) -> Option<Peer> {
        self.peers
            .values()
            .find(|peer| peer.role == Role::Leader)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructor_preserves_peer_identity_and_role() {
        let peer = Peer::new(
            "node-3000".to_string(),
            "127.0.0.1:3000".to_string(),
            "127.0.0.1:4000".to_string(),
            Role::Follower,
        );

        assert_eq!(peer.node_id, "node-3000");
        assert_eq!(peer.role, Role::Follower);
    }
}
