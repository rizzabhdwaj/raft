use crate::database::command::Command;
pub struct Entry {
    pub index: u64,
    pub term: u64,
    pub command: Command,
}

pub struct Log {
    pub content: Vec<Entry>,
}

impl Log {
    pub fn new_log() -> Log {
        Log { content: vec![] }
    }
    pub fn append(&mut self, term: u64, command: Command) {
        let index = self.content.len() as u64 + 1;

        self.content.push(Entry {
            index,
            term,
            command,
        });
    }

    pub fn append_entries(&mut self, entries: Vec<Entry>) {
        self.content.extend(entries);
    }
}
