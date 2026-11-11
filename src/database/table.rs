use super::schema::Schema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct Table {
    pub table_id: i32,
    pub table_name: String,
    pub content: Vec<Schema>,
}

impl Table {
    pub fn init_table(t_name: String) -> Table {
        Table {
            table_id: 0,
            table_name: t_name,
            content: vec![],
        }
    }

    pub fn append_values(&mut self, content: Schema) {
        self.content.push(content);
    }

    pub fn delete_values(&mut self, id: i32) {
        self.content.retain(|item| item.id != id);
    }

    pub fn get_all_values(&self) -> TableOut {
        TableOut {
            table_id: self.table_id,
            table_name: self.table_name.clone(),
            content: self.content.clone(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TableOut {
    pub table_id: i32,
    pub table_name: String,
    pub content: Vec<Schema>,
}