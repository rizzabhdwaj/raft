use actix_web::{web, App, HttpResponse, HttpServer};
use std::env;

/// Default backend port if none specified
/// Can be overridden via MANAGER_DEFAULT_PORT env var, otherwise 3000
fn default_backend_port() -> u16 {
    env::var("MANAGER_DEFAULT_PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse()
        .expect("MANAGER_DEFAULT_PORT must be a valid number")
}

/// Build backend URL for redirect
fn build_backend_url(port: u16, path: &str) -> String {
    format!("http://127.0.0.1:{port}{path}")
}

/// Redirect response to backend server
fn make_redirect(target_port: u16, path: &str) -> HttpResponse {
    HttpResponse::TemporaryRedirect()
        .insert_header(("Location", build_backend_url(target_port, path)))
        .finish()
}

/// Health check using default port (route: /health)
async fn health_default() -> HttpResponse {
    let port = default_backend_port();
    make_redirect(port, "/health")
}

/// Health check using specified port (route: /health/{port})
async fn health_specified(path: web::Path<u16>) -> HttpResponse {
    let target_port = path.into_inner();
    make_redirect(target_port, "/health")
}

/// Values redirect using default port (route: /values)
async fn values_default() -> HttpResponse {
    let port = default_backend_port();
    make_redirect(port, "/values")
}

/// Values redirect using specified port (route: /values/{port})
async fn values_specified(path: web::Path<u16>) -> HttpResponse {
    let target_port = path.into_inner();
    make_redirect(target_port, "/values")
}

/// Command redirect using default port (route: /command)
async fn command_default() -> HttpResponse {
    let port = default_backend_port();
    make_redirect(port, "/command")
}

/// Command redirect using specified port (route: /command/{port})
async fn command_specified(path: web::Path<u16>) -> HttpResponse {
    let target_port = path.into_inner();
    make_redirect(target_port, "/command")
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let manager_port: u16 = env::var("MANAGER_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .expect("MANAGER_PORT must be a valid number");

    HttpServer::new(|| {
        App::new()
            // Health check routes
            .route("/health", web::get().to(health_default))   // uses default port
            .route("/health/{port}", web::get().to(health_specified))  // uses specified port
            // Values routes
            .route("/values", web::get().to(values_default))   // uses default port
            .route("/values/{port}", web::get().to(values_specified))  // uses specified port
            // Command routes
            .route("/command", web::post().to(command_default))   // uses default port
            .route("/command/{port}", web::post().to(command_specified))  // uses specified port
    })
    .bind(("0.0.0.0", manager_port))?
    .run()
    .await
}