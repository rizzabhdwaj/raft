use crate::grpc::proto::{self, log_service_client::LogServiceClient};
use crate::raft::{
    log::Log,
    peers::{Peer, PeerRegistry},
    roles::Role,
};
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};
use tonic::Status;

pub const MIN_DUR_MS: u64 = 150;
pub const MAX_DUR_MS: u64 = 300;
const RPC_TIMEOUT: Duration = Duration::from_millis(100);

// Immutable startup membership; discovery announcements never change quorum.
#[derive(Clone)]
pub struct Membership {
    pub node_id: String,
    pub http_address: String,
    pub peers: HashMap<String, String>,
}

impl Membership {
    pub fn from_config(
        own: &Peer,
        peer_info: &str,
        addresses: &[String],
    ) -> Result<(Self, PeerRegistry), String> {
        let mut configured = Vec::new();
        for entry in peer_info
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let parts: Vec<_> = entry.split('|').map(str::trim).collect();
            if parts.len() != 3 || parts.iter().any(|s| s.is_empty()) {
                return Err("PEER_INFO entries must be node_id|http_address|grpc_address; explicit identities are required for multi-node elections".into());
            }
            configured.push(Peer::new(
                parts[0].into(),
                parts[1].into(),
                parts[2].into(),
                Role::Undefined,
            ));
        }
        let membership = Self::new(own, &configured, addresses)?;
        let mut registry = PeerRegistry::default();
        for peer in configured {
            registry.upsert(peer);
        }
        registry.upsert(own.clone());
        Ok((membership, registry))
    }

    fn new(own: &Peer, configured: &[Peer], addresses: &[String]) -> Result<Self, String> {
        let mut identities = HashMap::from([(
            own.node_id.clone(),
            (own.http_address.clone(), own.grpc_address.clone()),
        )]);
        let mut endpoints = HashMap::from([(own.grpc_address.clone(), own.node_id.clone())]);
        let mut peers = HashMap::new();
        for peer in configured {
            let routes = (peer.http_address.clone(), peer.grpc_address.clone());
            if let Some(previous) = identities.insert(peer.node_id.clone(), routes.clone()) {
                if previous != routes {
                    return Err(format!(
                        "PEER_INFO has conflicting routes for identity {}; use one canonical address per node, including self",
                        peer.node_id
                    ));
                }
            }
            if let Some(previous) =
                endpoints.insert(peer.grpc_address.clone(), peer.node_id.clone())
            {
                if previous != peer.node_id {
                    return Err(format!(
                        "PEER_INFO assigns gRPC address {} to multiple identities",
                        peer.grpc_address
                    ));
                }
            }
            if peer.node_id != own.node_id {
                peers.insert(peer.node_id.clone(), peer.grpc_address.clone());
            }
        }
        let mut supplied = HashSet::new();
        for address in addresses.iter().map(|a| a.trim()).filter(|a| !a.is_empty()) {
            if !endpoints.contains_key(address) {
                return Err(format!(
                    "PEERS address {address} has no explicit PEER_INFO identity; multi-node elections require PEER_INFO=node_id|http_address|grpc_address. Use matching canonical addresses; aliases are not inferred"
                ));
            }
            if address != own.grpc_address {
                supplied.insert(address.to_owned());
            }
        }
        if addresses.iter().any(|a| !a.trim().is_empty()) {
            let expected: HashSet<_> = peers.values().cloned().collect();
            if supplied != expected {
                return Err("PEERS and PEER_INFO disagree: PEERS must include every configured remote gRPC address (self is optional)".into());
            }
        }
        Ok(Self {
            node_id: own.node_id.clone(),
            http_address: own.http_address.clone(),
            peers,
        })
    }

    fn majority(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }
}

pub struct NodeState {
    pub role: Role,
    pub current_term: u64,
    pub voted_for: Option<String>,
    pub leader_id: Option<String>,
    pub heartbeat_timeout: Duration,
    pub last_heartbeat: Instant,
    pub heartbeat_deadline: Instant,
    pub votes: HashSet<String>,
}

impl Default for NodeState {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeState {
    pub fn new() -> Self {
        let now = Instant::now();
        let mut state = Self {
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            heartbeat_timeout: Duration::ZERO,
            last_heartbeat: now,
            heartbeat_deadline: now,
            votes: HashSet::new(),
        };
        state.reset_timer();
        state
    }

    fn reset_timer(&mut self) {
        self.heartbeat_timeout =
            Duration::from_millis(rand::thread_rng().gen_range(MIN_DUR_MS..=MAX_DUR_MS));
        self.last_heartbeat = Instant::now();
        self.heartbeat_deadline = self.last_heartbeat + self.heartbeat_timeout;
    }

    pub fn observe_term(&mut self, term: u64) {
        if term > self.current_term {
            self.current_term = term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.leader_id = None;
            self.votes.clear();
        }
    }

    pub fn request_vote(
        &mut self,
        request: proto::VoteRequest,
        last: (u64, u64),
    ) -> proto::VoteResponse {
        self.observe_term(request.term);
        let granted = request.term == self.current_term
            && !request.candidate_id.is_empty()
            && self
                .voted_for
                .as_ref()
                .is_none_or(|id| id == &request.candidate_id)
            && (request.last_log_term, request.last_log_index) >= last;
        if granted {
            self.voted_for = Some(request.candidate_id);
            self.reset_timer();
        }
        proto::VoteResponse {
            term: self.current_term,
            vote_granted: granted,
        }
    }

    pub fn heartbeat(
        &mut self,
        request: proto::HeartbeatRequest,
    ) -> Result<proto::HeartbeatResponse, Status> {
        self.observe_term(request.term);
        if !request.entries.is_empty() || request.leader_id.is_empty() {
            return Err(Status::invalid_argument(
                "heartbeat requires leader ID and empty entries",
            ));
        }
        let accepted = request.term == self.current_term;
        if accepted {
            self.role = Role::Follower;
            self.leader_id = Some(request.leader_id);
            self.votes.clear();
            self.reset_timer();
        }
        Ok(proto::HeartbeatResponse {
            term: self.current_term,
            accepted,
            leader_id: self.leader_id.clone().unwrap_or_default(),
        })
    }

    fn start_election(&mut self, membership: &Membership, last: (u64, u64)) -> proto::VoteRequest {
        self.current_term += 1;
        self.role = Role::Candidate;
        self.leader_id = None;
        self.voted_for = Some(membership.node_id.clone());
        self.votes = HashSet::from([membership.node_id.clone()]);
        self.reset_timer();
        self.check_majority(membership);
        proto::VoteRequest {
            term: self.current_term,
            candidate_id: membership.node_id.clone(),
            last_log_term: last.0,
            last_log_index: last.1,
        }
    }

    fn check_majority(&mut self, membership: &Membership) {
        if self.role == Role::Candidate && self.votes.len() >= membership.majority() {
            self.role = Role::Leader;
            self.leader_id = Some(membership.node_id.clone());
            println!(
                "Leader elected: id={}, term={}, route=http://{}/command",
                membership.node_id, self.current_term, membership.http_address
            );
        }
    }

    fn receive_vote(
        &mut self,
        membership: &Membership,
        peer: String,
        election: u64,
        response: proto::VoteResponse,
    ) {
        self.observe_term(response.term);
        if self.role == Role::Candidate
            && election == self.current_term
            && response.term == election
            && response.vote_granted
            && membership.peers.contains_key(&peer)
        {
            self.votes.insert(peer);
            self.check_majority(membership);
        }
    }
}

pub fn last_log(log: &Log) -> (u64, u64) {
    log.content
        .last()
        .map(|entry| (entry.term, entry.index))
        .unwrap_or((0, 0))
}

pub async fn run_elections(
    state: Arc<Mutex<NodeState>>,
    log: Arc<Mutex<Log>>,
    membership: Membership,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut pending = JoinSet::new();
    let mut active = HashSet::new();
    loop {
        tokio::select! {
            Some(result) = pending.join_next(), if !pending.is_empty() => {
                if let Ok(peer) = result { active.remove(&peer); }
            }
            _ = interval.tick() => {
                let (vote, term, leader) = {
                    let mut state = state.lock().unwrap();
                    let vote = if state.role != Role::Leader && Instant::now() >= state.heartbeat_deadline {
                        Some(state.start_election(&membership, last_log(&log.lock().unwrap())))
                    } else { None };
                    (vote, state.current_term, state.role == Role::Leader)
                };
                if vote.is_none() && !leader { continue; }
                for (peer, address) in &membership.peers {
                    if !active.insert(peer.clone()) { continue; }
                    let (peer, address, state, membership, vote) = (peer.clone(), address.clone(), state.clone(), membership.clone(), vote.clone());
                    pending.spawn(async move {
                        // At most one bounded connection/RPC per configured peer; no lock across I/O.
                        let _ = tokio::time::timeout(RPC_TIMEOUT, async {
                            let mut client = LogServiceClient::connect(format!("http://{address}")).await.map_err(|e| Status::unavailable(e.to_string()))?;
                            {
                                let state = state.lock().unwrap();
                                let expected = if vote.is_some() { Role::Candidate } else { Role::Leader };
                                if state.current_term != term || state.role != expected { return Ok(()); }
                            }
                            if let Some(request) = vote {
                                let response = client.request_vote(request).await?.into_inner();
                                state.lock().unwrap().receive_vote(&membership, peer.clone(), term, response);
                            } else {
                                let response = client.heartbeat(proto::HeartbeatRequest { entries: vec![], term, leader_id: membership.node_id }).await?.into_inner();
                                state.lock().unwrap().observe_term(response.term);
                            }
                            Ok::<_, Status>(())
                        }).await;
                        peer
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn membership(size: usize) -> Membership {
        Membership {
            node_id: "self".into(),
            http_address: "127.0.0.1:3000".into(),
            peers: (1..size)
                .map(|i| (i.to_string(), format!("127.0.0.1:{}", 4000 + i)))
                .collect(),
        }
    }

    fn vote(term: u64, id: &str, last: (u64, u64)) -> proto::VoteRequest {
        proto::VoteRequest {
            term,
            candidate_id: id.into(),
            last_log_term: last.0,
            last_log_index: last.1,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn voting_criteria_and_timer_rules() {
        let mut state = NodeState::new();
        assert_eq!(state.role, Role::Follower);
        let deadline = state.heartbeat_deadline;
        tokio::time::advance(Duration::from_millis(20)).await;
        assert!(
            !state
                .request_vote(vote(1, "a", (2, 100)), (3, 1))
                .vote_granted
        );
        assert_eq!(state.current_term, 1);
        assert_eq!(state.heartbeat_deadline, deadline);
        assert!(
            !state
                .request_vote(vote(1, "a", (3, 0)), (3, 1))
                .vote_granted
        );
        assert!(
            state
                .request_vote(vote(1, "a", (3, 1)), (3, 1))
                .vote_granted
        );
        assert_eq!(state.last_heartbeat, Instant::now());
        let deadline = state.heartbeat_deadline;
        assert!(
            !state
                .request_vote(vote(1, "b", (4, 0)), (3, 1))
                .vote_granted
        );
        assert!(
            !state
                .request_vote(vote(0, "a", (4, 0)), (3, 1))
                .vote_granted
        );
        assert_eq!(state.heartbeat_deadline, deadline);
        assert!(
            state
                .request_vote(vote(1, "a", (4, 0)), (3, 100))
                .vote_granted
        );
        state.role = Role::Leader;
        let deadline = state.heartbeat_deadline;
        assert!(
            !state
                .request_vote(vote(2, "b", (0, 0)), (3, 1))
                .vote_granted
        );
        assert_eq!(state.role, Role::Follower);
        assert_eq!(state.voted_for, None);
        assert_eq!(state.heartbeat_deadline, deadline);
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_terms_and_vote_preservation() {
        let mut state = NodeState::new();
        state.start_election(&membership(3), (0, 0));
        let deadline = state.heartbeat_deadline;
        let heartbeat = |term| proto::HeartbeatRequest {
            term,
            leader_id: "leader".into(),
            entries: vec![],
        };
        assert!(!state.heartbeat(heartbeat(0)).unwrap().accepted);
        assert_eq!(state.role, Role::Candidate);
        assert_eq!(state.heartbeat_deadline, deadline);
        assert!(state.heartbeat(heartbeat(1)).unwrap().accepted);
        assert_eq!(state.role, Role::Follower);
        assert_eq!(state.voted_for.as_deref(), Some("self"));
        assert!(
            !state
                .request_vote(vote(1, "other", (0, 0)), (0, 0))
                .vote_granted
        );
        state.heartbeat(heartbeat(2)).unwrap();
        assert_eq!(state.current_term, 2);
        assert_eq!(state.voted_for, None);
        assert_eq!(state.leader_id.as_deref(), Some("leader"));
        let mut invalid = heartbeat(2);
        invalid.entries.push(proto::LogEntry::default());
        assert!(state.heartbeat(invalid).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn unique_majority_stale_responses_and_retry() {
        let config = membership(5);
        let mut state = NodeState::new();
        state.start_election(&config, (0, 0));
        let grant = |term| proto::VoteResponse {
            term,
            vote_granted: true,
        };
        for peer in ["1", "1", "outsider"] {
            state.receive_vote(&config, peer.into(), 1, grant(1));
        }
        assert_eq!(state.role, Role::Candidate); // Two of five is only a plurality.
        state.start_election(&config, (0, 0));
        assert_eq!(state.current_term, 2);
        assert_eq!(state.votes.len(), 1);
        assert!(
            (Duration::from_millis(150)..=Duration::from_millis(300))
                .contains(&state.heartbeat_timeout)
        );
        state.receive_vote(&config, "2".into(), 1, grant(1));
        assert_eq!(state.votes.len(), 1);
        for peer in ["1", "2"] {
            state.receive_vote(&config, peer.into(), 2, grant(2));
        }
        assert_eq!(state.role, Role::Leader);
        state.receive_vote(&config, "1".into(), 1, grant(3));
        assert_eq!(state.role, Role::Follower);
        assert_eq!(state.current_term, 3);
        assert_eq!(state.voted_for, None);
    }

    #[tokio::test(start_paused = true)]
    async fn single_node_and_timeout_retry_loop() {
        for size in [1, 3] {
            let state = Arc::new(Mutex::new(NodeState::new()));
            let mut config = membership(size);
            // No dependency on services that might be running on the developer's ports.
            for address in config.peers.values_mut() {
                *address = "invalid address".into();
            }
            let task = tokio::spawn(run_elections(
                state.clone(),
                Arc::new(Mutex::new(Log::new_log())),
                config,
            ));
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_millis(310)).await;
            tokio::task::yield_now().await;
            assert_eq!(state.lock().unwrap().current_term, 1);
            assert_eq!(
                state.lock().unwrap().role,
                if size == 1 {
                    Role::Leader
                } else {
                    Role::Candidate
                }
            );
            tokio::time::advance(Duration::from_millis(310)).await;
            tokio::task::yield_now().await;
            assert_eq!(
                state.lock().unwrap().current_term,
                if size == 1 { 1 } else { 2 }
            );
            task.abort();
        }
    }

    #[test]
    fn membership_deduplicates_config_and_self() {
        let own = Peer::new(
            "self".into(),
            "".into(),
            "localhost:1".into(),
            Role::Follower,
        );
        let other = Peer::new(
            "other".into(),
            "".into(),
            "localhost:2".into(),
            Role::Follower,
        );
        let config = Membership::new(
            &own,
            &[own.clone(), other.clone(), other],
            &[
                "localhost:1".into(),
                "localhost:2".into(),
                "localhost:2".into(),
            ],
        )
        .unwrap();
        assert_eq!(config.peers.len(), 1);
        assert_eq!(config.majority(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn raw_addresses_and_self_aliases_cannot_add_votes() {
        let own = Peer::new(
            "node-3000".into(),
            "127.0.0.1:3000".into(),
            "127.0.0.1:4000".into(),
            Role::Follower,
        );
        let info =
            "node-3001|127.0.0.1:3001|127.0.0.1:4001,node-3002|127.0.0.1:3002|127.0.0.1:4002";
        for addresses in [vec!["localhost:4000".into()], vec!["127.0.0.1:4001".into()]] {
            assert!(Membership::from_config(&own, "", &addresses).is_err());
            assert!(Membership::from_config(&own, info, &addresses).is_err());
        }
        let (config, _) = Membership::from_config(&own, info, &[]).unwrap();
        let mut state = NodeState::new();
        state.start_election(&config, (0, 0));
        for peer in [
            "node-3000",
            "127.0.0.1:4000",
            "localhost:4000",
            "127.0.0.1:4001",
        ] {
            state.receive_vote(
                &config,
                peer.into(),
                1,
                proto::VoteResponse {
                    term: 1,
                    vote_granted: true,
                },
            );
        }
        assert_eq!(state.votes.len(), 1);
        assert_eq!(state.role, Role::Candidate);
    }

    #[test]
    fn startup_rejects_malformed_or_conflicting_identity_config() {
        let own = Peer::new(
            "node-3000".into(),
            "127.0.0.1:3000".into(),
            "127.0.0.1:4000".into(),
            Role::Follower,
        );
        for info in [
            "node-3001|127.0.0.1:3001",
            "|127.0.0.1:3001|127.0.0.1:4001",
            "node-3001||127.0.0.1:4001",
            "node-3001|127.0.0.1:3001|127.0.0.1:4001|extra",
            "node-3000|127.0.0.1:3000|localhost:4000",
            "node-3001|127.0.0.1:3001|127.0.0.1:4000",
            "node-3001|127.0.0.1:3001|127.0.0.1:4001,node-3001|127.0.0.1:3001|localhost:4001",
            "node-3001|127.0.0.1:3001|127.0.0.1:4001,node-3002|127.0.0.1:3002|127.0.0.1:4001",
        ] {
            assert!(
                Membership::from_config(&own, info, &[]).is_err(),
                "accepted {info}"
            );
        }
        let (single, registry) = Membership::from_config(&own, "", &[]).unwrap();
        assert!(single.peers.is_empty());
        assert_eq!(registry.all().len(), 1);
    }

    #[test]
    fn run_all_config_has_consistent_membership_and_routes() {
        let script = include_str!("../../run-all.sh");
        let value = |name: &str| {
            script
                .lines()
                .find_map(|line| line.strip_prefix(name))
                .unwrap()
                .trim_matches('"')
        };
        let info = value("PEER_INFO=");
        let addresses: Vec<_> = value("PEERS=").split(',').map(str::to_owned).collect();
        for port in 3000..3005 {
            let own = Peer::new(
                format!("node-{port}"),
                format!("127.0.0.1:{port}"),
                format!("127.0.0.1:{}", port + 1000),
                Role::Follower,
            );
            let (config, registry) = Membership::from_config(&own, info, &addresses).unwrap();
            assert_eq!(config.peers.len(), 4);
            assert_eq!(config.majority(), 3);
            assert!(!config.peers.contains_key(&own.node_id));
            for (id, address) in config.peers {
                let route = registry
                    .all()
                    .into_iter()
                    .find(|p| p.node_id == id)
                    .unwrap();
                assert_eq!(route.grpc_address, address);
            }
            let without_self: Vec<_> = addresses
                .iter()
                .filter(|a| **a != own.grpc_address)
                .cloned()
                .collect();
            assert!(Membership::from_config(&own, info, &without_self).is_ok());
            assert!(Membership::from_config(&own, info, &without_self[..3]).is_err());
        }
    }

    #[tokio::test]
    async fn three_node_election_and_failover() {
        use crate::{database::table::Table, grpc::LogService};
        let mut listeners = Vec::new();
        for _ in 0..3 {
            listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
        }
        let addresses: Vec<_> = listeners
            .iter()
            .map(|l| l.local_addr().unwrap().to_string())
            .collect();
        let mut states = Vec::new();
        let mut servers = Vec::new();
        let mut loops = Vec::new();
        for (i, listener) in listeners.into_iter().enumerate() {
            let state = Arc::new(Mutex::new(NodeState::new()));
            let log = Arc::new(Mutex::new(Log::new_log()));
            let config = Membership {
                node_id: i.to_string(),
                http_address: format!("127.0.0.1:{}", 3000 + i),
                peers: addresses
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| *j != i)
                    .map(|(j, a)| (j.to_string(), a.clone()))
                    .collect(),
            };
            let service = LogService {
                state: state.clone(),
                log: log.clone(),
                table: Arc::new(Mutex::new(Table::init_table("test".into()))),
                peers: addresses.clone(),
                self_address: addresses[i].clone(),
            };
            servers.push(tokio::spawn(async move {
                let incoming = futures_util::stream::unfold(listener, |listener| async {
                    Some((listener.accept().await.map(|(socket, _)| socket), listener))
                });
                tonic::transport::Server::builder()
                    .add_service(proto::log_service_server::LogServiceServer::new(service))
                    .serve_with_incoming(incoming)
                    .await
                    .unwrap();
            }));
            loops.push(tokio::spawn(run_elections(state.clone(), log, config)));
            states.push(state);
        }
        let wait = |excluded: Option<usize>| {
            let states = states.clone();
            async move {
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let leaders: Vec<_> = states
                            .iter()
                            .enumerate()
                            .filter(|(i, s)| {
                                Some(*i) != excluded && s.lock().unwrap().role == Role::Leader
                            })
                            .map(|(i, _)| i)
                            .collect();
                        if leaders.len() == 1 {
                            return leaders[0];
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .unwrap()
            }
        };
        let first = wait(None).await;
        tokio::time::sleep(Duration::from_millis(350)).await;
        assert_eq!(
            states
                .iter()
                .filter(|s| s.lock().unwrap().role == Role::Leader)
                .count(),
            1
        );
        let term = states[first].lock().unwrap().current_term;
        loops[first].abort();
        servers[first].abort();
        let next = wait(Some(first)).await;
        assert_ne!(first, next);
        assert!(states[next].lock().unwrap().current_term > term);
        for task in loops.into_iter().chain(servers) {
            task.abort();
        }
    }
}
