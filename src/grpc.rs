use crate::database::command::Command;
use crate::database::commands::apply_command;
use crate::database::table::Table;
use crate::raft::log::Log;
use crate::raft::peers::{Peer, PeerRegistry};
use crate::raft::roles::Role;
use crate::raft::voting::NodeState;
use actix_web::{HttpResponse, web};
use serde::Serialize;
use std::sync::{Arc, Mutex};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub mod proto {
    tonic::include_proto!("raftlog");
}

pub mod peer_proto {
    tonic::include_proto!("raftpeer");
}

#[cfg(test)]
mod tests {
    use super::*;
    use peer_proto::peer_service_server::PeerService as _;
    use proto::log_service_server::LogService as _;

    #[tokio::test]
    async fn nonleaders_refuse_proposals() {
        for role in [Role::Follower, Role::Candidate, Role::Undefined] {
            let service = LogService {
                table: Arc::new(Mutex::new(Table::init_table("test".to_string()))),
                log: Arc::new(Mutex::new(Log::new_log())),
                state: Arc::new(Mutex::new(NodeState {
                    role,
                    ..NodeState::new()
                })),
                peers: vec![],
                self_address: String::new(),
            };
            let response = service
                .propose_command(Request::new(proto::CommandRequest {
                    command_json: serde_json::to_string(&Command::CreateTable {
                        table_id: 1,
                        table_name: "rejected".to_string(),
                    })
                    .unwrap(),
                    term: 0,
                }))
                .await
                .unwrap()
                .into_inner();
            assert!(!response.success);
            assert!(service.log.lock().unwrap().content.is_empty());
        }
    }

    #[tokio::test]
    async fn peer_announcement_preserves_identity_and_role() {
        let registry = Arc::new(Mutex::new(PeerRegistry::from_env(
            "node-3000|127.0.0.1:3000|127.0.0.1:4000",
        )));
        let service = PeerService {
            registry: registry.clone(),
        };

        service
            .broadcast_peer(Request::new(peer_proto::BroadcastPeerRequest {
                peer: Some(peer_proto::PeerInfo {
                    node_id: "node-3000".to_string(),
                    http_address: "127.0.0.1:3000".to_string(),
                    grpc_address: "127.0.0.1:4000".to_string(),
                    role: "Follower".to_string(),
                }),
            }))
            .await
            .unwrap();

        let peers = registry.lock().unwrap().all();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node_id, "node-3000");
        assert_eq!(peers[0].role, Role::Follower);
    }

    #[tokio::test]
    async fn command_paths_observe_terms_and_reject_stale_replication() {
        let service = LogService {
            table: Arc::new(Mutex::new(Table::init_table("test".into()))),
            log: Arc::new(Mutex::new(Log::new_log())),
            state: Arc::new(Mutex::new(NodeState::new())),
            peers: vec![],
            self_address: String::new(),
        };
        service.state.lock().unwrap().role = Role::Leader;
        let command = |term| {
            Request::new(proto::CommandRequest {
                command_json: serde_json::to_string(&Command::CreateTable {
                    table_id: 1,
                    table_name: "test".into(),
                })
                .unwrap(),
                term,
            })
        };
        let response = service
            .propose_command(command(4))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.success);
        assert_eq!(response.term, 4);
        assert_eq!(service.state.lock().unwrap().role, Role::Follower);
        assert!(service.log.lock().unwrap().content.is_empty());
        assert!(
            service
                .apply_command(command(5))
                .await
                .unwrap()
                .into_inner()
                .success
        );
        let response = service
            .apply_command(command(4))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.success);
        assert_eq!(response.term, 5);
        let log = service.log.lock().unwrap();
        assert_eq!(log.content.len(), 1);
        assert_eq!(log.content[0].term, 5);
    }
}

#[derive(Clone)]
pub struct LogService {
    pub table: Arc<Mutex<Table>>,
    pub log: Arc<Mutex<Log>>,
    pub state: Arc<Mutex<NodeState>>,
    pub peers: Vec<String>,
    pub self_address: String,
}

#[derive(Clone)]
pub struct PeerService {
    pub registry: Arc<Mutex<PeerRegistry>>,
}

#[derive(Serialize)]
pub struct PeerLog {
    pub peer: String,
    pub entries: Vec<PeerEntry>,
}

#[derive(Serialize)]
pub struct PeerEntry {
    pub index: u64,
    pub term: u64,
    pub command_json: String,
}

#[tonic::async_trait]
impl proto::log_service_server::LogService for LogService {
    async fn heartbeat(
        &self,
        request: Request<proto::HeartbeatRequest>,
    ) -> Result<Response<proto::HeartbeatResponse>, Status> {
        Ok(Response::new(
            self.state.lock().unwrap().heartbeat(request.into_inner())?,
        ))
    }

    async fn request_vote(
        &self,
        request: Request<proto::VoteRequest>,
    ) -> Result<Response<proto::VoteResponse>, Status> {
        let mut state = self.state.lock().unwrap();
        let last = crate::raft::voting::last_log(&self.log.lock().unwrap());
        Ok(Response::new(
            state.request_vote(request.into_inner(), last),
        ))
    }

    async fn propose_command(
        &self,
        request: Request<proto::CommandRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let request = request.into_inner();
        self.state.lock().unwrap().observe_term(request.term);
        let command = serde_json::from_str(&request.command_json)
            .map_err(|_| Status::invalid_argument("invalid command"))?;
        apply_and_broadcast(
            &self.table,
            &self.log,
            &self.state,
            &self.peers,
            &self.self_address,
            command,
        )
        .await
        .map(Response::new)
    }

    async fn get_log(
        &self,
        _request: Request<proto::Empty>,
    ) -> Result<Response<proto::LogResponse>, Status> {
        let log = self
            .log
            .lock()
            .map_err(|_| Status::internal("log lock poisoned"))?;
        let entries = log
            .content
            .iter()
            .map(|entry| {
                Ok(proto::LogEntry {
                    index: entry.index,
                    term: entry.term,
                    command_json: serde_json::to_string(&entry.command)
                        .map_err(|_| Status::internal("failed to serialize command"))?,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;

        Ok(Response::new(proto::LogResponse { entries }))
    }

    async fn apply_command(
        &self,
        request: Request<proto::CommandRequest>,
    ) -> Result<Response<proto::CommandResponse>, Status> {
        let request = request.into_inner();
        let mut state = self.state.lock().unwrap();
        state.observe_term(request.term);
        if request.term < state.current_term {
            return Ok(Response::new(proto::CommandResponse {
                success: false,
                message: "stale term".into(),
                term: state.current_term,
            }));
        }
        let command = serde_json::from_str(&request.command_json)
            .map_err(|_| Status::invalid_argument("invalid command"))?;
        let mut table = self
            .table
            .lock()
            .map_err(|_| Status::internal("table lock poisoned"))?;
        let mut log = self
            .log
            .lock()
            .map_err(|_| Status::internal("log lock poisoned"))?;
        apply_command(&mut table, &mut log, command, request.term);

        Ok(Response::new(proto::CommandResponse {
            success: true,
            message: "command applied".to_string(),
            term: state.current_term,
        }))
    }
}

#[tonic::async_trait]
impl peer_proto::peer_service_server::PeerService for PeerService {
    async fn broadcast_peer(
        &self,
        request: Request<peer_proto::BroadcastPeerRequest>,
    ) -> Result<Response<peer_proto::BroadcastPeerResponse>, Status> {
        let peer = request
            .into_inner()
            .peer
            .ok_or_else(|| Status::invalid_argument("peer is required"))?;

        self.registry.lock().unwrap().upsert(Peer::new(
            peer.node_id,
            peer.http_address,
            peer.grpc_address,
            crate::raft::roles::Role::from_str(&peer.role),
        ));

        Ok(Response::new(peer_proto::BroadcastPeerResponse {
            success: true,
        }))
    }
}

pub async fn fetch_log(
    address: String,
) -> Result<proto::LogResponse, Box<dyn std::error::Error + Send + Sync>> {
    let channel = tonic::transport::Channel::from_shared(format!("http://{address}"))?
        .connect()
        .await?;
    let mut client = proto::log_service_client::LogServiceClient::new(channel);
    Ok(client.get_log(proto::Empty {}).await?.into_inner())
}

pub async fn get_cluster_logs(peers: web::Data<Vec<String>>) -> HttpResponse {
    let mut logs = Vec::new();

    for peer in peers.iter() {
        match fetch_log(peer.clone()).await {
            Ok(response) => logs.push(PeerLog {
                peer: peer.clone(),
                entries: response
                    .entries
                    .into_iter()
                    .map(|entry| PeerEntry {
                        index: entry.index,
                        term: entry.term,
                        command_json: entry.command_json,
                    })
                    .collect(),
            }),
            Err(error) => logs.push(PeerLog {
                peer: format!("{peer} ({error})"),
                entries: Vec::new(),
            }),
        }
    }

    HttpResponse::Ok().json(logs)
}

pub fn start_server(service: LogService, peers: Arc<Mutex<PeerRegistry>>, port: u16) {
    tokio::spawn(async move {
        let server = Server::builder()
            .add_service(proto::log_service_server::LogServiceServer::new(service))
            .add_service(peer_proto::peer_service_server::PeerServiceServer::new(
                PeerService { registry: peers },
            ))
            .serve(([0, 0, 0, 0], port).into());

        if let Err(error) = server.await {
            eprintln!("gRPC server stopped: {error}");
        }
    });
}

pub async fn broadcast_peer(address: String, peer: Peer) -> Result<(), Status> {
    let channel = tonic::transport::Channel::from_shared(format!("http://{address}"))
        .map_err(|error| Status::internal(error.to_string()))?
        .connect()
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let mut client = peer_proto::peer_service_client::PeerServiceClient::new(channel);

    client
        .broadcast_peer(peer_proto::BroadcastPeerRequest {
            peer: Some(peer_proto::PeerInfo {
                node_id: peer.node_id,
                http_address: peer.http_address,
                grpc_address: peer.grpc_address,
                role: peer.role.as_str().to_string(),
            }),
        })
        .await
        .map(|_| ())
        .map_err(|error| Status::unavailable(error.to_string()))
}

pub async fn apply_and_broadcast(
    table: &Mutex<Table>,
    log: &Mutex<Log>,
    state: &Mutex<NodeState>,
    peers: &[String],
    self_address: &str,
    command: Command,
) -> Result<proto::CommandResponse, Status> {
    let term = {
        let state = state.lock().unwrap();
        if state.role != Role::Leader {
            return Ok(proto::CommandResponse {
                success: false,
                message: "node is not the leader".into(),
                term: state.current_term,
            });
        }
        let mut table = table
            .lock()
            .map_err(|_| Status::internal("table lock poisoned"))?;
        let mut log = log
            .lock()
            .map_err(|_| Status::internal("log lock poisoned"))?;
        apply_command(&mut table, &mut log, command.clone(), state.current_term);
        state.current_term
    };
    use futures_util::{StreamExt, stream};
    let targets: std::collections::HashSet<_> = peers
        .iter()
        .filter(|p| p.as_str() != self_address)
        .collect();
    let requests: Vec<_> = targets
        .into_iter()
        .map(|peer| {
            let peer = peer.clone();
            let command = command.clone();
            async move { send_command(&peer, &command, term).await }
        })
        .collect();
    let mut responses = stream::iter(requests).buffer_unordered(16);
    while let Some(result) = responses.next().await {
        if let Ok(response) = result {
            state.lock().unwrap().observe_term(response.term);
        }
    }
    let state = state.lock().unwrap();
    Ok(proto::CommandResponse {
        success: state.current_term == term && state.role == Role::Leader,
        message: "command applied locally (not quorum committed)".into(),
        term: state.current_term,
    })
}

pub async fn propose_command(
    address: &str,
    command: &Command,
    state: &Mutex<NodeState>,
) -> Result<(), Status> {
    let term = state.lock().unwrap().current_term;
    let response = tokio::time::timeout(std::time::Duration::from_millis(500), async {
        let channel = tonic::transport::Channel::from_shared(format!("http://{address}"))
            .map_err(|error| Status::internal(error.to_string()))?
            .connect()
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let mut client = proto::log_service_client::LogServiceClient::new(channel);
        let response = client
            .propose_command(proto::CommandRequest {
                command_json: serde_json::to_string(command)
                    .map_err(|error| Status::internal(error.to_string()))?,
                term,
            })
            .await?
            .into_inner();
        Ok::<_, Status>(response)
    })
    .await
    .map_err(|_| Status::deadline_exceeded("proposal timed out"))??;
    state.lock().unwrap().observe_term(response.term);
    if !response.success {
        return Err(Status::failed_precondition(response.message));
    }
    Ok(())
}

pub async fn send_command(
    address: &str,
    command: &Command,
    term: u64,
) -> Result<proto::CommandResponse, Box<dyn std::error::Error + Send + Sync>> {
    tokio::time::timeout(std::time::Duration::from_millis(100), async {
        let channel = tonic::transport::Channel::from_shared(format!("http://{address}"))?
            .connect()
            .await?;
        let mut client = proto::log_service_client::LogServiceClient::new(channel);

        let response = client
            .apply_command(proto::CommandRequest {
                command_json: serde_json::to_string(command)?,
                term,
            })
            .await?;

        Ok(response.into_inner())
    })
    .await?
}
