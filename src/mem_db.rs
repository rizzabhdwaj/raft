#[derive(Clone, serde::Serialize)]
pub struct Schema {
    pub id: i32,
    pub col1: String,
    pub col2: String,
    pub col3: String,
}

#[derive(Clone, serde::Serialize)]
pub struct TableOut {
    pub table_id: i32,
    pub table_name: String,
    pub content: Vec<Schema>,
}

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

    fn append_values(&mut self, content: Schema) {
        self.content.push(content);
    }

    fn delete_values(&mut self, id: i32) {
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