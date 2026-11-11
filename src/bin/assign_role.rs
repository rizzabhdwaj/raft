use raft_db::role_server;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port: u16 = env::var("ROLE_GRPC_PORT")
        .unwrap_or_else(|_| "5000".to_string())
        .parse()
        .expect("ROLE_GRPC_PORT must be a valid number");
    let nodes = env::var("ROLE_NODES")
        .unwrap_or_default()
        .split(',')
        .filter(|node| !node.is_empty())
        .map(str::to_owned)
        .collect();

    role_server::start_server_with_nodes(port, nodes).await?;
    Ok(())
}
