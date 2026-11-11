use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct Schema {
    pub id: i32,
    pub col1: String,
    pub col2: String,
    pub col3: String,
}