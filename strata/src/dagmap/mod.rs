//!
//! # Directed Acyclic Graph (DAG) Map
//!
//! This module provides data structures for representing and managing directed
//! acyclic graphs (DAGs) on disk.
//!

/// A module for raw DAG maps.
pub mod raw;
/// A module for DAG maps with raw keys.
pub mod rawkey;

use crate::ValueEnDe;
use parking_lot::Mutex;
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::LazyLock,
};
use vsdb_core::basic::mapx_raw::MapxRaw;

/// A type alias for a DAG map ID.
pub type DagMapId = [u8];

/// Number of IDs per batch.  A filesystem `fsync` is performed once per
/// batch, so this controls the trade-off between crash-safety overhead
/// and the maximum ID gap after a crash (at most `DAG_ID_BATCH` IDs
/// are "wasted" per unclean shutdown).
const DAG_ID_BATCH: u128 = 128;

struct DagIdAllocator {
    next: u128,
    ceiling: u128,
    path: PathBuf,
}

impl DagIdAllocator {
    fn init() -> Self {
        Self::init_at(vsdb_core::common::vsdb_get_system_dir())
    }

    fn init_at(base: &Path) -> Self {
        let path = base.join("dag_id_ceiling");
        let current = match fs::read(&path) {
            Ok(bytes) if bytes.len() == 16 => {
                Some(u128::from_le_bytes(bytes.try_into().unwrap()))
            }
            // A present-but-malformed ceiling file must never silently
            // reset the counter to 0 — that would re-issue IDs that still
            // key live `children` entries. Fail loudly instead.
            Ok(bytes) => panic!(
                "dag_id_ceiling: corrupt ceiling file ({} bytes, expected 16)",
                bytes.len()
            ),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => panic!("dag_id_ceiling: read failed: {e}"),
        };
        let legacy_path = base.join("id_num");
        let legacy = Self::read_legacy_counter(&legacy_path);
        let ceiling = current.unwrap_or(0).max(legacy.unwrap_or(0));

        let allocator = DagIdAllocator {
            next: ceiling,
            ceiling,
            path,
        };
        if legacy.is_some() {
            // Fold before retiring the old handle file. A crash on either
            // side is safe: the next open repeats the take-max migration.
            allocator.persist_ceiling(ceiling);
            match fs::remove_file(&legacy_path) {
                Ok(()) => {
                    if let Some(parent) = legacy_path.parent() {
                        fs::File::open(parent)
                            .and_then(|dir| dir.sync_all())
                            .expect("legacy dag id counter: parent dir fsync failed");
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => panic!("legacy dag id counter: remove failed: {e}"),
            }
        }
        allocator
    }

    fn read_legacy_counter(path: &Path) -> Option<u128> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(e) => panic!("legacy dag id counter: read failed: {e}"),
        };
        // Pre-ceiling releases serialized `Orphan<u128>` transparently as
        // its inner MapxRaw handle. Decode that exact wire shape, then read
        // the counter value from the still-live legacy slot.
        let legacy: MapxRaw = postcard::from_bytes(&bytes)
            .unwrap_or_else(|e| panic!("legacy dag id counter: invalid handle: {e}"));
        let raw = legacy
            .get([])
            .unwrap_or_else(|| panic!("legacy dag id counter: missing value"));
        Some(
            u128::decode(&raw)
                .unwrap_or_else(|e| panic!("legacy dag id counter: invalid value: {e}")),
        )
    }

    fn alloc(&mut self) -> u128 {
        if self.next >= self.ceiling {
            let new_ceiling = self
                .next
                .checked_add(DAG_ID_BATCH)
                .expect("dag_id_ceiling: ID space exhausted");
            self.persist_ceiling(new_ceiling);
            self.ceiling = new_ceiling;
        }
        self.next = self
            .next
            .checked_add(1)
            .expect("dag_id_ceiling: ID space exhausted");
        self.next
    }

    /// Atomic write: tmp → fsync → rename.  Guarantees the ceiling
    /// is durable before any ID from the new batch is returned.
    fn persist_ceiling(&self, ceiling: u128) {
        let tmp = self.path.with_extension("tmp");
        let mut f = fs::File::create(&tmp).expect("dag_id_ceiling: create tmp failed");
        f.write_all(&ceiling.to_le_bytes())
            .expect("dag_id_ceiling: write failed");
        f.sync_all().expect("dag_id_ceiling: fsync failed");
        fs::rename(&tmp, &self.path).expect("dag_id_ceiling: rename failed");
        if let Some(parent) = self.path.parent() {
            fs::File::open(parent)
                .and_then(|dir| dir.sync_all())
                .expect("dag_id_ceiling: parent dir fsync failed");
        }
    }
}

/// Generates a new, unique ID for a DAG map.
///
/// Maintains a persistent monotonic counter backed by a crash-safe
/// ceiling file.  IDs are handed out from an in-memory counter;
/// a batch ceiling is advanced and `fsync`'d to disk *before* any ID
/// in the new batch is returned.
///
/// # Crash semantics
///
/// The ceiling file always stores a value **>= any ID ever returned**.
/// On recovery the counter resumes from the persisted ceiling.  IDs
/// between the last returned value and the ceiling are skipped (safe
/// gap of at most `DAG_ID_BATCH` entries).  No ID is ever reused,
/// even after power failure.
pub fn gen_dag_map_id_num() -> u128 {
    static ALLOC: LazyLock<Mutex<DagIdAllocator>> =
        LazyLock::new(|| Mutex::new(DagIdAllocator::init()));

    ALLOC.lock().alloc()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_counter_is_folded_into_new_ceiling() {
        let mut legacy = MapxRaw::new();
        legacy.insert([], 321u128.encode());

        let dir = std::env::temp_dir()
            .join(format!("vsdb_dag_id_migration_{}", rand::random::<u128>()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("id_num"), postcard::to_allocvec(&legacy).unwrap()).unwrap();

        let mut alloc = DagIdAllocator::init_at(&dir);
        assert_eq!(alloc.next, 321);
        assert!(!dir.join("id_num").exists());
        assert_eq!(alloc.alloc(), 322);
        let persisted = fs::read(dir.join("dag_id_ceiling")).unwrap();
        assert_eq!(u128::from_le_bytes(persisted.try_into().unwrap()), 449);

        fs::remove_dir_all(dir).unwrap();
    }
}
