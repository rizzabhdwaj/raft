use actix_cors::Cors;
use actix_web::{App, HttpResponse, HttpServer, web};
use raft_db::database::command::Command;
use raft_db::database::raft::log::Log;
use raft_db::database::table::{Table, TableOut};
use std::env;
use std::sync::Mutex;
async fn get_all_values(data: web::Data<Mutex<Table>>) -> web::Json<TableOut> {
    let table = data.lock().unwrap();
    web::Json(table.get_all_values())
}

async fn get_log(data: web::Data<Mutex<Log>>) -> web::Json<Log> {
    web::Json(data.lock().unwrap().clone())
}

async fn check_health() -> HttpResponse {
    HttpResponse::Ok().body("UP AND RUNNING")
}

async fn process_command(
    data: web::Data<Mutex<Table>>,
    log: web::Data<Mutex<Log>>,
    cmd: web::Json<Command>,
) -> HttpResponse {
    let command = cmd.into_inner();

    {
        let mut table = data.lock().unwrap();

        match &command {
            Command::CreateTable {
                table_id: _,
                table_name,
            } => {
                *table = Table::init_table(table_name.clone());
            }
            Command::Insert {
                table_id: _,
                schema,
            } => {
                table.append_values(schema.clone());
            }
            Command::Delete { table_id: _, id } => {
                table.delete_values(*id);
            }
        }
    }

    log.lock().unwrap().append(0, command);

    HttpResponse::Ok().finish()
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let port: u16 = env::var("PORT")
        .unwrap_or_else(|_| "3000".to_string())
        .parse()
        .expect("PORT must be a valid number");

    let database = web::Data::new(Mutex::new(Table::init_table("my_table".to_string())));
    let log = web::Data::new(Mutex::new(Log::new_log()));
    // web::Data is used to share state across the application without creating a seperate instance
    // web:Data uses Arc
    // Mutex::new -> Thread safe Arc -> Shared ownership of the Thread safe data

    HttpServer::new(move || {
        App::new()
            .wrap(Cors::permissive())
            .app_data(database.clone())
            .app_data(log.clone())
            .route("/values", web::get().to(get_all_values))
            .route("/log", web::get().to(get_log))
            .route("/command", web::post().to(process_command))
            .route("/health", web::get().to(check_health))
            .route("/health", web::post().to(check_health))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}
