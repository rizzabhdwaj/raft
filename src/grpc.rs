use crate::database::raft::log::Log;
use actix_web::{web, HttpResponse};
use serde::Serialize;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};
use tonic::transport::Server;

pub mod proto {
    tonic::include_proto!("raftlog");
}

#[derive(Clone)]
pub struct LogService {
    pub log: Arc<Mutex<Log>>,
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
        let log = self.log.lock().map_err(|_| Status::internal("log lock poisoned"))?;
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

pub fn start_server(log: Arc<Mutex<Log>>, port: u16) {
    tokio::spawn(async move {
        let server = Server::builder()
            .add_service(proto::log_service_server::LogServiceServer::new(LogService { log }))
            .serve(([0, 0, 0, 0], port).into());

        if let Err(error) = server.await {
            eprintln!("gRPC server stopped: {error}");
        }
    });
}
