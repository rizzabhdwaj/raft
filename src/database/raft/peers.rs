use crate::database::raft::roles::Role;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize)]
pub struct Peer {
    pub node_id: String,
    pub http_address: String,
    pub grpc_address: String,
    pub role: Role,
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
                Some(Peer {
                    node_id: parts.next()?.to_string(),
                    http_address: parts.next()?.to_string(),
                    grpc_address: parts.next()?.to_string(),
                    role: Role::Undefined,
                })
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
