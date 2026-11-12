use crate::database::raft::log::Log;
use crate::database::raft::peers::{Peer, PeerRegistry};
use crate::database::commands::apply_command;
use crate::database::table::Table;
use actix_web::{HttpResponse, web};
use crate::database::command::Command;
use serde::Serialize;
use std::sync::{Arc, Mutex};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub mod proto {
    tonic::include_proto!("raftlog");
}

#[derive(Clone)]
pub struct LogService {
    pub table: Arc<Mutex<Table>>,
    pub log: Arc<Mutex<Log>>,
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
        let command = serde_json::from_str(&request.into_inner().command_json)
            .map_err(|_| Status::invalid_argument("invalid command"))?;
        let mut table = self.table.lock().map_err(|_| Status::internal("table lock poisoned"))?;
        let mut log = self.log.lock().map_err(|_| Status::internal("log lock poisoned"))?;
        apply_command(&mut table, &mut log, command);

        Ok(Response::new(proto::CommandResponse {
            success: true,
            message: "command applied".to_string(),
        }))
    }
}

#[tonic::async_trait]
impl crate::role_server::proto::peer_service_server::PeerService for PeerService {
    async fn broadcast_peer(
        &self,
        request: Request<crate::role_server::proto::BroadcastPeerRequest>,
    ) -> Result<Response<crate::role_server::proto::BroadcastPeerResponse>, Status> {
        let peer = request
            .into_inner()
            .peer
            .ok_or_else(|| Status::invalid_argument("peer is required"))?;

        self.registry.lock().unwrap().upsert(Peer {
            node_id: peer.node_id,
            http_address: peer.http_address,
            grpc_address: peer.grpc_address,
            role: crate::database::raft::roles::Role::from_str(&peer.role),
        });

        Ok(Response::new(
            crate::role_server::proto::BroadcastPeerResponse { success: true },
        ))
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

pub fn start_server(
    table: Arc<Mutex<Table>>,
    log: Arc<Mutex<Log>>,
    peers: Arc<Mutex<PeerRegistry>>,
    port: u16,
) {
    tokio::spawn(async move {
        let server = Server::builder()
            .add_service(proto::log_service_server::LogServiceServer::new(
                LogService { table, log },
            ))
            .add_service(
                crate::role_server::proto::peer_service_server::PeerServiceServer::new(
                    PeerService { registry: peers },
                ),
            )
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
    let mut client = crate::role_server::proto::peer_service_client::PeerServiceClient::new(channel);

    client
        .broadcast_peer(crate::role_server::proto::BroadcastPeerRequest {
            peer: Some(crate::role_server::proto::PeerInfo {
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

pub async fn broadcast_command(peers: &[String], self_address: &str, command: &Command) {
    for peer in peers {
        if peer == self_address {
            continue;
        }

        if let Err(error) = send_command(peer, command).await {
            eprintln!("failed to send command to {peer}: {error}");
        }
    }
}

pub async fn send_command(
    address: &str,
    command: &Command,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = tonic::transport::Channel::from_shared(format!("http://{address}"))?
        .connect()
        .await?;
    let mut client = proto::log_service_client::LogServiceClient::new(channel);

    client
        .apply_command(proto::CommandRequest {
            command_json: serde_json::to_string(command)?,
            term: 0,
        })
        .await?;

    Ok(())
}
