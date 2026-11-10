use actix_cors::Cors;
use actix_web::{App, HttpServer, web};
use std::env;

mod mem_db;

struct AppState {
    db: web::Data<mem_db::Table>,
}

async fn get_all_values(data: web::Data<mem_db::Table>) -> web::Json<mem_db::TableOut> {
    let all = data.get_all_values();
    web::Json(all)
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let db = mem_db::Table::init_table("my_table".to_string());

    let shared_db = web::Data::new(db);

    let port: u16 = env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse()
        .expect("PORT must be a valid number");

    HttpServer::new(move || {
        let cors = Cors::permissive();
        App::new()
            .wrap(cors)
            .app_data(web::Data::clone(&shared_db))
            .route("/values", web::get().to(get_all_values))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}
