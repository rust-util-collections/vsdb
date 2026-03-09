use thiserror::Error;

#[derive(Error, Debug)]
pub enum TrieError {
    #[error("Invalid state: {0}")]
    InvalidState(String),
}

pub type Result<T> = std::result::Result<T, TrieError>;
