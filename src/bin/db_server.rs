use actix_cors::Cors;
use actix_web::{App, Error, HttpResponse, HttpServer, web};
use raft_db::database::command::Command;
use raft_db::raft::log::Log;
use raft_db::raft::peers::{Peer, PeerRegistry};
use raft_db::raft::roles::Role;
use raft_db::raft::voting::{NodeState, run_heartbeats};

use raft_db::database::table::{Table, TableOut};
use raft_db::grpc;
use serde::Serialize;
use std::env;
use std::sync::{Arc, Mutex};

#[derive(Serialize)]
struct NodeInfo {
    role: String,
    log: Log,
}

async fn get_all_values(data: web::Data<Mutex<Table>>) -> web::Json<TableOut> {
    let table = data.lock().unwrap();
    web::Json(table.get_all_values())
}

async fn get_log(data: web::Data<Mutex<Log>>) -> web::Json<Log> {
    web::Json(data.lock().unwrap().clone())
}

async fn get_info(
    log: web::Data<Mutex<Log>>,
    state: web::Data<Mutex<NodeState>>,
) -> web::Json<NodeInfo> {
    web::Json(NodeInfo {
        role: state.lock().unwrap().role.as_str().to_string(),
        log: log.lock().unwrap().clone(),
    })
}

async fn check_health() -> HttpResponse {
    HttpResponse::Ok().body("UP AND RUNNING")
}

async fn leader_process(
    data: web::Data<Mutex<Table>>,
    log: web::Data<Mutex<Log>>,
    cmd: web::Json<Command>,
    state: web::Data<Mutex<NodeState>>,
    peers: web::Data<Vec<String>>,
    self_address: web::Data<String>,
    peer_registry: web::Data<Mutex<PeerRegistry>>,
) -> Result<HttpResponse, Error> {
    let role = state.lock().unwrap().role;
    if role == Role::Undefined {
        return Err(actix_web::error::ErrorServiceUnavailable(
            "role is not assigned yet",
        ));
    }

    if role != Role::Leader {
        let leader = peer_registry
            .lock()
            .unwrap()
            .leader()
            .ok_or_else(|| actix_web::error::ErrorServiceUnavailable("leader is unknown"))?;

        grpc::propose_command(&leader.grpc_address, &cmd.into_inner())
            .await
            .map_err(actix_web::error::ErrorBadGateway)?;
        return Ok(HttpResponse::Ok().finish());
    }

    let command = cmd.into_inner();
    grpc::apply_and_broadcast(&data, &log, &peers, &self_address, command)
        .await
        .map_err(actix_web::error::ErrorInternalServerError)?;

    Ok(HttpResponse::Ok().finish())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let port: u16 = env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse()
        .expect("PORT must be a valid number");
    let grpc_port: u16 = env::var("GRPC_PORT")
        .unwrap_or_else(|_| (port + 1000).to_string())
        .parse()
        .expect("GRPC_PORT must be a valid number");
    let peers = web::Data::new(
        env::var("PEERS")
            .unwrap_or_default()
            .split(',')
            .filter(|peer| !peer.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>(),
    );

    let database = Arc::new(Mutex::new(Table::init_table("my_table".to_string())));
    let log = Arc::new(Mutex::new(Log::new_log()));
    let peer_registry = Arc::new(Mutex::new(PeerRegistry::from_env(
        &env::var("PEER_INFO").unwrap_or_default(),
    )));
    let state = Arc::new(Mutex::new(NodeState::new()));
    let own_peer = Peer::new(
        format!("node-{port}"),
        format!("127.0.0.1:{port}"),
        format!("127.0.0.1:{grpc_port}"),
        state.lock().unwrap().role,
    );
    peer_registry.lock().unwrap().upsert(own_peer.clone());

    let log_data = web::Data::from(log.clone());
    let database_data = web::Data::from(database.clone());
    grpc::start_server(
        grpc::LogService {
            table: database.clone(),
            log: log.clone(),
            state: state.clone(),
            peers: peers.get_ref().clone(),
            self_address: own_peer.grpc_address.clone(),
        },
        peer_registry.clone(),
        grpc_port,
    );

    tokio::spawn(run_heartbeats(
        state.clone(),
        peer_registry.clone(),
        peers.get_ref().clone(),
        own_peer.grpc_address.clone(),
    ));

    let targets = peer_registry.lock().unwrap().all();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        for peer in targets {
            if peer.node_id != own_peer.node_id {
                let _ = grpc::broadcast_peer(peer.grpc_address.clone(), own_peer.clone()).await;
            }
        }
    });

    /*
        let monitor_database = database.clone();
        let monitor_log = log_data.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));

            loop {
                interval.tick().await;
                println!("[server:{port}] table: {:?}", monitor_database.lock().unwrap());
                println!("[server:{port}] log: {:?}", monitor_log.lock().unwrap());
            }
        });
    */
    HttpServer::new(move || {
        App::new()
            .wrap(Cors::permissive())
            .app_data(database_data.clone())
            .app_data(log_data.clone())
            .app_data(peers.clone())
            .app_data(web::Data::from(peer_registry.clone()))
            .app_data(web::Data::from(state.clone()))
            .app_data(web::Data::new(format!("127.0.0.1:{grpc_port}")))
            .route("/values", web::get().to(get_all_values))
            .route("/log", web::get().to(get_log))
            .route("/info", web::get().to(get_info))
            .route("/cluster/logs", web::get().to(grpc::get_cluster_logs))
            .route("/command", web::post().to(leader_process))
            .route("/health", web::get().to(check_health))
            .route("/health", web::post().to(check_health))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{http::StatusCode, test};

    async fn serve(
        role: Role,
        peers: Vec<String>,
    ) -> (grpc::LogService, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let service = grpc::LogService {
            table: Arc::new(Mutex::new(Table::init_table("test".to_string()))),
            log: Arc::new(Mutex::new(Log::new_log())),
            state: Arc::new(Mutex::new(NodeState {
                role,
                ..NodeState::new()
            })),
            peers,
            self_address: listener.local_addr().unwrap().to_string(),
        };
        let server_service = service.clone();
        let task = tokio::spawn(async move {
            let incoming = futures_util::stream::unfold(listener, |listener| async {
                Some((listener.accept().await.map(|(socket, _)| socket), listener))
            });
            tonic::transport::Server::builder()
                .add_service(grpc::proto::log_service_server::LogServiceServer::new(
                    server_service,
                ))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        (service, task)
    }

    async fn post_command(leader_address: Option<String>) -> (StatusCode, String, usize) {
        let registry = web::Data::new(Mutex::new(PeerRegistry::default()));
        if let Some(address) = leader_address {
            registry.lock().unwrap().upsert(Peer::new(
                "leader".to_string(),
                "unused-http-address".to_string(),
                address,
                Role::Leader,
            ));
        }
        let log = web::Data::new(Mutex::new(Log::new_log()));
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(Mutex::new(Table::init_table(
                    "test".to_string(),
                ))))
                .app_data(log.clone())
                .app_data(web::Data::new(Mutex::new(NodeState::new())))
                .app_data(web::Data::new(Vec::<String>::new()))
                .app_data(web::Data::new(String::new()))
                .app_data(registry)
                .route("/command", web::post().to(leader_process)),
        )
        .await;
        let request = test::TestRequest::post()
            .uri("/command")
            .set_json(Command::CreateTable {
                table_id: 1,
                table_name: "forwarded".to_string(),
            })
            .to_request();
        let response = test::call_service(&app, request).await;
        let status = response.status();
        assert!(!response.headers().contains_key("Location"));
        let body = String::from_utf8(test::read_body(response).await.to_vec()).unwrap();
        let entries = log.lock().unwrap().content.len();
        (status, body, entries)
    }

    #[actix_web::test]
    async fn unknown_leader_still_returns_service_unavailable() {
        let (status, body, entries) = post_command(None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body, "leader is unknown");
        assert_eq!(entries, 0);
    }

    #[actix_web::test]
    async fn follower_forwards_proposal_and_leader_broadcasts() {
        let (replica, replica_task) = serve(Role::Follower, vec![]).await;
        let (leader, leader_task) = serve(Role::Leader, vec![replica.self_address.clone()]).await;
        let (status, _, entries) = post_command(Some(leader.self_address.clone())).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(entries, 0);
        for service in [&leader, &replica] {
            let log = service.log.lock().unwrap();
            assert_eq!(log.content.len(), 1);
            assert!(matches!(&log.content[0].command,
                Command::CreateTable { table_id: 1, table_name } if table_name == "forwarded"));
        }
        leader_task.abort();
        replica_task.abort();
    }

    #[actix_web::test]
    async fn stale_leader_returns_bad_gateway_without_applying() {
        let (service, task) = serve(Role::Follower, vec![]).await;
        let (status, _, entries) = post_command(Some(service.self_address.clone())).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(entries, 0);
        assert!(service.log.lock().unwrap().content.is_empty());
        task.abort();
    }
}
