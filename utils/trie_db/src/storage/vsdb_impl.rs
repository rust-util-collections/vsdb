use super::TrieBackend;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use vsdb::MapxOrdRawKey;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct VsdbTrieBackend {
    inner: MapxOrdRawKey<Vec<u8>>,
}

impl VsdbTrieBackend {
    pub fn new() -> Self {
        Self {
            inner: MapxOrdRawKey::new(),
        }
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

impl Default for VsdbTrieBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl TrieBackend for VsdbTrieBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.inner.get(key))
    }

    fn insert_batch(&mut self, batch: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        for (k, v) in batch {
            self.inner.insert(&k, &v);
        }
        Ok(())
    }

    fn remove_batch(&mut self, keys: &[Vec<u8>]) -> Result<()> {
        for k in keys {
            self.inner.remove(k);
        }
        Ok(())
    }
}
