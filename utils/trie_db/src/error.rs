use thiserror::Error;

#[derive(Error, Debug)]
pub enum TrieError {
    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Decoding error: {0}")]
    DecodeError(String),

    #[error("Invalid state: {0}")]
    InvalidState(String),

    #[error("Node not found: {0}")]
    NodeNotFound(String),
}

pub type Result<T> = std::result::Result<T, TrieError>;

impl From<Box<dyn std::error::Error + Send + Sync>> for TrieError {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        TrieError::DatabaseError(e.to_string())
    }
}
