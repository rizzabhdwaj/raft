use crate::grpc::proto::{HeartbeatRequest, log_service_client::LogServiceClient};
use crate::raft::{peers::PeerRegistry, roles::Role};
use rand::Rng;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior};
use tonic::{Status, transport::Endpoint};

pub const MIN_DUR_MS: u64 = 150;
pub const MAX_DUR_MS: u64 = 200;
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(10);
const RPC_TIMEOUT: Duration = Duration::from_millis(100);

// Local state only: neither the timeout nor clock readings belong in peer announcements.
pub struct NodeState {
    pub role: Role,
    pub heartbeat_timeout: Duration,
    pub last_heartbeat: Instant,
    pub heartbeat_deadline: Instant,
}

impl NodeState {
    pub fn new() -> Self {
        let heartbeat_timeout =
            Duration::from_millis(rand::thread_rng().gen_range(MIN_DUR_MS..=MAX_DUR_MS));
        let now = Instant::now();
        Self {
            role: Role::Follower,
            heartbeat_timeout,
            last_heartbeat: now,
            heartbeat_deadline: now + heartbeat_timeout,
        }
    }
}

pub fn receive_heartbeat(
    state: &Mutex<NodeState>,
    request: HeartbeatRequest,
) -> Result<(), Status> {
    if !request.entries.is_empty() {
        return Err(Status::invalid_argument("heartbeat entries must be empty"));
    }
    let mut state = state
        .lock()
        .map_err(|_| Status::internal("node state lock poisoned"))?;
    state.last_heartbeat = Instant::now();
    state.heartbeat_deadline = state.last_heartbeat + state.heartbeat_timeout;
    Ok(())
}

pub async fn run_heartbeats(
    state: Arc<Mutex<NodeState>>,
    registry: Arc<Mutex<PeerRegistry>>,
    peers: Vec<String>,
    self_address: String,
) {
    let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut pending = JoinSet::new();
    let mut active = HashSet::new();
    loop {
        tokio::select! {
            Some(result) = pending.join_next(), if !pending.is_empty() => {
                if let Ok(address) = result {
                    active.remove(&address);
                }
            }
            _ = interval.tick() => {
                if state.lock().unwrap().role != Role::Leader {
                    pending.abort_all();
                    while pending.join_next().await.is_some() {}
                    active.clear();
                    continue;
                }
                let mut targets: HashSet<_> = peers.iter().cloned().collect();
                targets.extend(registry.lock().unwrap().all().into_iter().map(|p| p.grpc_address));
                targets.remove(&self_address);
                for address in targets {
                    if !active.insert(address.clone()) {
                        continue;
                    }
                    let state = state.clone();
                    pending.spawn(async move {
                        // Bound connection establishment and the RPC together. Each peer progresses independently.
                        let _ = tokio::time::timeout(RPC_TIMEOUT, async {
                            let endpoint = Endpoint::from_shared(format!("http://{address}"))
                                .map_err(|e| Status::invalid_argument(e.to_string()))?;
                            let channel = endpoint.connect().await
                                .map_err(|e| Status::unavailable(e.to_string()))?;
                            if state.lock().unwrap().role == Role::Leader {
                                LogServiceClient::new(channel)
                                    .heartbeat(HeartbeatRequest { entries: vec![] }).await?;
                            }
                            Ok::<_, Status>(())
                        }).await;
                        address
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::{command::Command, commands::apply_command, table::Table};
    use crate::grpc::{LogService, proto};
    use crate::raft::log::Log;
    use tokio::net::TcpListener;

    #[tokio::test(start_paused = true)]
    async fn timer_is_local_and_resets_only_for_empty_heartbeats() {
        let state = Mutex::new(NodeState::new());
        let initial = state.lock().unwrap().last_heartbeat;
        let timeout = state.lock().unwrap().heartbeat_timeout;
        assert!(
            (Duration::from_millis(MIN_DUR_MS)..=Duration::from_millis(MAX_DUR_MS))
                .contains(&timeout)
        );
        assert_eq!(state.lock().unwrap().role, Role::Follower);
        tokio::time::advance(Duration::from_millis(75)).await;
        receive_heartbeat(&state, HeartbeatRequest { entries: vec![] }).unwrap();
        let deadline = {
            let state = state.lock().unwrap();
            assert_eq!(state.last_heartbeat, initial + Duration::from_millis(75));
            assert_eq!(state.heartbeat_deadline, state.last_heartbeat + timeout);
            state.heartbeat_deadline
        };
        tokio::time::advance(Duration::from_millis(25)).await;
        let error = receive_heartbeat(
            &state,
            HeartbeatRequest {
                entries: vec![proto::LogEntry {
                    index: 1,
                    term: 0,
                    command_json: String::new(),
                }],
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert_eq!(state.lock().unwrap().heartbeat_deadline, deadline);
    }

    #[tokio::test(start_paused = true)]
    async fn nonleaders_do_not_connect_to_peers() {
        for role in [Role::Follower, Role::Candidate, Role::Undefined] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let state = Arc::new(Mutex::new(NodeState {
                role,
                ..NodeState::new()
            }));
            let task = tokio::spawn(run_heartbeats(
                state,
                Arc::new(Mutex::new(PeerRegistry::default())),
                vec![listener.local_addr().unwrap().to_string()],
                String::new(),
            ));
            assert!(
                tokio::time::timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err()
            );
            task.abort();
            let _ = task.await;
        }
    }

    #[tokio::test]
    async fn leader_heartbeats_reset_receiver_without_mutation_despite_stalled_peer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let stalled = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let self_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let self_address = self_listener.local_addr().unwrap().to_string();
        let service = LogService {
            table: Arc::new(Mutex::new(Table::init_table("test".into()))),
            log: Arc::new(Mutex::new(Log::new_log())),
            state: Arc::new(Mutex::new(NodeState::new())),
            peers: vec![],
            self_address: address.clone(),
        };
        apply_command(
            &mut service.table.lock().unwrap(),
            &mut service.log.lock().unwrap(),
            Command::CreateTable {
                table_id: 1,
                table_name: "preserved".into(),
            },
        );
        let before_log = serde_json::to_value(&*service.log.lock().unwrap()).unwrap();
        let before_table = serde_json::to_value(&*service.table.lock().unwrap()).unwrap();
        let receiver = service.state.clone();
        let initial = receiver.lock().unwrap().last_heartbeat;
        let server_service = service.clone();
        let server = tokio::spawn(async move {
            let incoming = futures_util::stream::unfold(listener, |listener| async {
                Some((listener.accept().await.map(|(socket, _)| socket), listener))
            });
            tonic::transport::Server::builder()
                .add_service(proto::log_service_server::LogServiceServer::new(
                    server_service,
                ))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        let sender = Arc::new(Mutex::new(NodeState::new()));
        let task = tokio::spawn(run_heartbeats(
            sender.clone(),
            Arc::new(Mutex::new(PeerRegistry::default())),
            vec![
                stalled.local_addr().unwrap().to_string(),
                address.clone(),
                self_address.clone(),
            ],
            self_address,
        ));
        // The running loop must see a role change, not a startup snapshot.
        sender.lock().unwrap().role = Role::Leader;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if receiver.lock().unwrap().last_heartbeat > initial {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            // Verify the loop survives a stalled request's timeout and keeps sending.
            tokio::time::sleep(RPC_TIMEOUT * 2).await;
            let previous = receiver.lock().unwrap().last_heartbeat;
            loop {
                if receiver.lock().unwrap().last_heartbeat > previous {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            serde_json::to_value(&*service.log.lock().unwrap()).unwrap(),
            before_log
        );
        assert_eq!(
            serde_json::to_value(&*service.table.lock().unwrap()).unwrap(),
            before_table
        );
        let mut client = LogServiceClient::connect(format!("http://{address}"))
            .await
            .unwrap();
        let error = client
            .heartbeat(HeartbeatRequest {
                entries: vec![proto::LogEntry {
                    index: 2,
                    term: 1,
                    command_json: "not a heartbeat".into(),
                }],
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        sender.lock().unwrap().role = Role::Follower;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let last = receiver.lock().unwrap().last_heartbeat;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(receiver.lock().unwrap().last_heartbeat, last);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), self_listener.accept())
                .await
                .is_err()
        );
        task.abort();
        server.abort();
        let _ = task.await;
        let _ = server.await;
    }
}
