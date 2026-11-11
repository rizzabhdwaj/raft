use crate::database::raft::roles::Role;
use rand::seq::SliceRandom;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};
use tonic::transport::Server;

pub mod proto {
    tonic::include_proto!("raftrole");
}

#[derive(Clone)]
pub struct RoleAssigner {
    leader: Arc<Mutex<Option<String>>>,
    nodes: Vec<String>,
}

impl RoleAssigner {
    pub fn new(nodes: Vec<String>) -> Self {
        Self {
            leader: Arc::new(Mutex::new(None)),
            nodes,
        }
    }
}

#[tonic::async_trait]
impl proto::role_service_server::RoleService for RoleAssigner {
    async fn assign_role(
        &self,
        request: Request<proto::AssignRoleRequest>,
    ) -> Result<Response<proto::RoleResponse>, Status> {
        let node_id = request.into_inner().node_id;

        if node_id.is_empty() {
            return Err(Status::invalid_argument("node_id is required"));
        }

        let mut leader = self
            .leader
            .lock()
            .map_err(|_| Status::internal("role lock poisoned"))?;
        if leader.is_none() {
            *leader = self
                .nodes
                .choose(&mut rand::thread_rng())
                .cloned()
                .or_else(|| Some(node_id.clone()));
        }

        let role = if leader.as_deref() == Some(node_id.as_str()) {
            Role::Leader
        } else {
            Role::Follower
        };

        Ok(Response::new(proto::RoleResponse {
            node_id,
            role: role.as_str().to_string(),
        }))
    }
}

pub async fn start_server_with_nodes(
    port: u16,
    nodes: Vec<String>,
) -> Result<(), tonic::transport::Error> {
    Server::builder()
        .add_service(proto::role_service_server::RoleServiceServer::new(
            RoleAssigner::new(nodes),
        ))
        .serve(([0, 0, 0, 0], port).into())
        .await
}

pub async fn request_role(
    node_id: String,
    address: String,
) -> Result<Role, Box<dyn std::error::Error + Send + Sync>> {
    let channel = tonic::transport::Channel::from_shared(format!("http://{address}"))?
        .connect()
        .await?;
    let mut client = proto::role_service_client::RoleServiceClient::new(channel);
    let response = client
        .assign_role(proto::AssignRoleRequest { node_id })
        .await?
        .into_inner();

    Ok(Role::from_str(&response.role))
}
