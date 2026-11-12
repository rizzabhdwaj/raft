use crate::database::command::Command;
use crate::database::raft::log::Log;
use crate::database::table::Table;

pub fn apply_command(table: &mut Table, log: &mut Log, command: Command) {
    match &command {
        Command::CreateTable { table_name, .. } => {
            *table = Table::init_table(table_name.clone());
        }
        Command::Insert { schema, .. } => {
            table.append_values(schema.clone());
        }
        Command::Delete { id, .. } => {
            table.delete_values(*id);
        }
    }

    log.append(0, command);
}
