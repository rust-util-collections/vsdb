use super::TrieBackend;
use crate::error::{Result, TrieError};
use std::sync::{Arc, Mutex};
use vsdb::MapxOrdRawKey;

#[derive(Clone, Debug)]
pub struct VsdbTrieBackend {
    inner: Arc<Mutex<MapxOrdRawKey<Vec<u8>>>>,
}

impl VsdbTrieBackend {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MapxOrdRawKey::new())),
        }
    }

    pub fn clear(&mut self) {
        self.inner
            .lock()
            .expect("VsdbTrieBackend mutex poisoned")
            .clear();
    }
}

impl Default for VsdbTrieBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl TrieBackend for VsdbTrieBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| TrieError::DatabaseError("VsdbTrieBackend mutex poisoned".into()))?;
        Ok(guard.get(key))
    }

    fn insert_batch(&mut self, batch: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| TrieError::DatabaseError("VsdbTrieBackend mutex poisoned".into()))?;
        let mut b = guard.batch_entry();
        for (k, v) in batch {
            b.insert(&k, &v);
        }
        b.commit()
            .map_err(|e| crate::error::TrieError::DatabaseError(e.to_string()))?;
        Ok(())
    }

    fn remove_batch(&mut self, keys: &[Vec<u8>]) -> Result<()> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| TrieError::DatabaseError("VsdbTrieBackend mutex poisoned".into()))?;
        let mut b = guard.batch_entry();
        for k in keys {
            b.remove(k);
        }
        b.commit()
            .map_err(|e| crate::error::TrieError::DatabaseError(e.to_string()))?;
        Ok(())
    }
}
