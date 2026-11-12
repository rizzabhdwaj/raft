use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Role {
    Leader,
    Candidate,
    Follower,
    Undefined,
}

impl Role {
    pub fn new_role() -> Role {
        Role::Undefined
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Role::Leader => "Leader",
            Role::Candidate => "Candidate",
            Role::Follower => "Follower",
            Role::Undefined => "Undefined",
        }
    }

    pub fn from_str(value: &str) -> Role {
        match value {
            "Leader" => Role::Leader,
            "Candidate" => Role::Candidate,
            "Follower" => Role::Follower,
            _ => Role::Undefined,
        }
    }
}
