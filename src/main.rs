use actix_cors::Cors;
use actix_web::{App, HttpServer, web, HttpResponse};
use std::env;
use std::sync::Mutex;

mod mem_db;

struct AppState {
    db: web::Data<Mutex<mem_db::Table>>,
}

async fn get_all_values(data: web::Data<Mutex<mem_db::Table>>) -> web::Json<mem_db::TableOut> {
    let data = data.lock().unwrap();
    let all = data.get_all_values();
    web::Json(all)
}

async fn process_command(
    data: web::Data<Mutex<mem_db::Table>>,
    cmd: web::Json<mem_db::Command>,
) -> HttpResponse {
    let mut table = data.lock().unwrap();
    match cmd.0 {
        mem_db::Command::CreateTable { table_id: _, table_name } => {
            *table = mem_db::Table::init_table(table_name.clone());
        }
        mem_db::Command::Insert { table_id: _, schema } => {
            table.append_values(schema.clone());
        }
        mem_db::Command::Delete { table_id: _, id } => {
            table.delete_values(id);
        }
    }
    HttpResponse::Ok().finish()
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let db = mem_db::Table::init_table("my_table".to_string());

    let shared_db = web::Data::new(Mutex::new(db));

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
            .route("/command", web::post().to(process_command))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}
