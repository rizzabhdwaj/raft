use actix_cors::Cors;
use actix_web::{App, HttpResponse, HttpServer, web};
use raft_db::database::command::Command;
use raft_db::database::table::{Table, TableOut};
use std::env;
use std::sync::Mutex;

async fn get_all_values(data: web::Data<Mutex<Table>>) -> web::Json<TableOut> {
    let table = data.lock().unwrap();
    web::Json(table.get_all_values())
}

async fn check_health() -> HttpResponse {
    HttpResponse::Ok().body("UP AND RUNNING")
}

async fn process_command(data: web::Data<Mutex<Table>>, cmd: web::Json<Command>) -> HttpResponse {
    let mut table = data.lock().unwrap();

    match cmd.into_inner() {
        Command::CreateTable {
            table_id: _,
            table_name,
        } => {
            *table = Table::init_table(table_name);
        }
        Command::Insert {
            table_id: _,
            schema,
        } => {
            table.append_values(schema);
        }
        Command::Delete { table_id: _, id } => {
            table.delete_values(id);
        }
    }

    HttpResponse::Ok().finish()
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let port: u16 = env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse()
        .expect("PORT must be a valid number");
    // one global instance of a Database Table
    // is modified using the function process_command
    let database = web::Data::new(Mutex::new(Table::init_table("my_table".to_string())));

    HttpServer::new(move || {
        App::new()
            .wrap(Cors::permissive())
            .app_data(database.clone())
            .route("/values", web::get().to(get_all_values))
            .route("/command", web::post().to(process_command))
            .route("/health", web::get().to(check_health))
            .route("/health", web::post().to(check_health))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}
