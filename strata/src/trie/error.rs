use crate::common::error::VsdbError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TrieError {
    #[error("Invalid state: {0}")]
    InvalidState(String),
}

pub type Result<T> = std::result::Result<T, TrieError>;

impl From<TrieError> for VsdbError {
    fn from(e: TrieError) -> Self {
        Self::Trie {
            detail: e.to_string(),
        }
    }
}
