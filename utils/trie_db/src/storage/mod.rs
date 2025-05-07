use crate::error::Result;

pub trait TrieBackend: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn insert_batch(&mut self, batch: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()>;
    fn remove_batch(&mut self, keys: &[Vec<u8>]) -> Result<()>;
}

pub mod vsdb_impl;
