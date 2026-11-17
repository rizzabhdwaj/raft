use crate::raft::roles::Role;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;

pub const MIN_DUR_MS: u64 = 150;

pub const MAX_DUR_MS: u64 = 200;

#[derive(Clone, Debug, Serialize)]
pub struct Peer {
    pub node_id: String,
    pub http_address: String,
    pub grpc_address: String,
    pub role: Role,
    pub heartbeat_timeout: Duration,
}

impl Peer {
    pub fn new(
        node_id: String,
        http_address: String,
        grpc_address: String,
        role: Role,
        heartbeat_timeout_ms: u64,
    ) -> Self {
        Self {
            node_id,
            http_address,
            grpc_address,
            role,
            heartbeat_timeout: Duration::from_millis(heartbeat_timeout_ms),
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
                    MIN_DUR_MS,
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
    fn constructor_converts_milliseconds_to_duration() {
        let peer = Peer::new(
            "node-3000".to_string(),
            "127.0.0.1:3000".to_string(),
            "127.0.0.1:4000".to_string(),
            Role::Follower,
            175,
        );

        assert_eq!(peer.heartbeat_timeout, Duration::from_millis(175));
    }
}
