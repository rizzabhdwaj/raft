use actix_web::{web, App, HttpResponse, HttpServer};
use std::env;

fn backend_url(port: u16, path: &str) -> String {
    format!("http://127.0.0.1:{port}{path}")
}

async fn health(port: web::Path<u16>) -> HttpResponse {
    redirect(port.into_inner(), "/health")
}

async fn values(port: web::Path<u16>) -> HttpResponse {
    redirect(port.into_inner(), "/values")
}

async fn command(port: web::Path<u16>) -> HttpResponse {
    redirect(port.into_inner(), "/command")
}

fn redirect(port: u16, path: &str) -> HttpResponse {
    HttpResponse::TemporaryRedirect()
        .insert_header(("Location", backend_url(port, path)))
        .finish()
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let manager_port: u16 = env::var("MANAGER_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .expect("MANAGER_PORT must be a valid number");

    HttpServer::new(|| {
        App::new()
            .route("/health/{port}", web::get().to(health))
            .route("/health/{port}", web::post().to(health))
            .route("/values/{port}", web::get().to(values))
            .route("/command/{port}", web::post().to(command))
    })
    .bind(("0.0.0.0", manager_port))?
    .run()
    .await
}