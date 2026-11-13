use actix_cors::Cors;
use actix_web::{App, Error, HttpResponse, HttpServer, web};
use raft_db::database::command::Command;
use raft_db::database::commands::apply_command;
use raft_db::database::raft::log::Log;
use raft_db::database::raft::peers::{Peer, PeerRegistry};
use raft_db::database::raft::roles::Role;
use raft_db::role_server;

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

async fn get_info(log: web::Data<Mutex<Log>>, role: web::Data<Role>) -> web::Json<NodeInfo> {
    web::Json(NodeInfo {
        role: (**role).as_str().to_string(),
        log: log.lock().unwrap().clone(),
    })
}

async fn check_health() -> HttpResponse {
    HttpResponse::Ok().body("UP AND RUNNING")
}

// Leader will call this function through GRPC for all the follower node's to update the log
async fn process_command(
    data: web::Data<Mutex<Table>>,
    log: web::Data<Mutex<Log>>,
    command: Command,
) -> Result<(), Error> {
    let mut table = data.lock().unwrap();
    let mut log = log.lock().unwrap();
    apply_command(&mut table, &mut log, command);
    Ok(())
}

async fn leader_process(
    data: web::Data<Mutex<Table>>,
    log: web::Data<Mutex<Log>>,
    cmd: web::Json<Command>,
    role: web::Data<Role>,
    peers: web::Data<Vec<String>>,
    self_address: web::Data<String>,
    peer_registry: web::Data<Mutex<PeerRegistry>>,
) -> Result<HttpResponse, Error> {
    if **role == Role::Undefined {
        return Err(actix_web::error::ErrorServiceUnavailable(
            "role is not assigned yet",
        ));
    }

    if **role != Role::Leader {
        let leader = peer_registry
            .lock()
            .unwrap()
            .leader()
            .ok_or_else(|| actix_web::error::ErrorServiceUnavailable("leader is unknown"))?;

        return Ok(HttpResponse::TemporaryRedirect()
            .insert_header(("Location", format!("http://{}/command", leader.http_address)))
            .finish());
    }

    let command = cmd.into_inner();
    process_command(data, log, command.clone()).await?;
    grpc::broadcast_command(peers.get_ref(), self_address.get_ref(), &command).await;

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
    let role_server_address =
        env::var("ROLE_SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:5000".to_string());
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
    let instance_role = role_server::request_role(format!("node-{port}"), role_server_address)
        .await
        .unwrap_or_else(|_| Role::Undefined);
    let own_peer = Peer {
        node_id: format!("node-{port}"),
        http_address: format!("127.0.0.1:{port}"),
        grpc_address: format!("127.0.0.1:{grpc_port}"),
        role: instance_role,
    };
    peer_registry.lock().unwrap().upsert(own_peer.clone());

    // when the server starts, it should send  grpc request to a server to gain role .

    let log_data = web::Data::from(log.clone());
    let database_data = web::Data::from(database.clone());
    grpc::start_server(database.clone(), log.clone(), peer_registry.clone(), grpc_port);

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
            .app_data(web::Data::new(instance_role))
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
