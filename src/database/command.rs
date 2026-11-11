use serde::{Deserialize, Serialize};
use crate::database::schema::Schema;

#[derive(Clone, Serialize, Deserialize)]
pub enum Command {
    CreateTable {
        table_id: i32,
        table_name: String,
    },

    Insert {
        table_id: i32,
        schema: Schema,
    },

    Delete {
        table_id: i32,
        id: i32,
    },
}