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

/// A type alias for a DAG map ID.
pub type DagMapId = [u8];

/// Number of IDs per batch.  A filesystem `fsync` is performed once per
/// batch, so this controls the trade-off between crash-safety overhead
/// and the maximum ID gap after a crash (at most `DAG_ID_BATCH` IDs
/// are "wasted" per unclean shutdown).
const DAG_ID_BATCH: u128 = 128;

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
/// gap of at most [`DAG_ID_BATCH`] entries).  No ID is ever reused,
/// even after power failure.
pub fn gen_dag_map_id_num() -> u128 {
    use parking_lot::Mutex;
    use std::{fs, sync::LazyLock};

    struct DagIdAllocator {
        next: u128,
        ceiling: u128,
        path: std::path::PathBuf,
    }

    impl DagIdAllocator {
        fn init() -> Self {
            let base = vsdb_core::common::vsdb_get_system_dir().to_owned();
            let path = base.join("dag_id_ceiling");

            let ceiling = match fs::read(&path) {
                Ok(bytes) if bytes.len() == 16 => {
                    u128::from_le_bytes(bytes.try_into().unwrap())
                }
                _ => 0,
            };

            DagIdAllocator {
                next: ceiling,
                ceiling,
                path,
            }
        }

        fn alloc(&mut self) -> u128 {
            if self.next >= self.ceiling {
                let new_ceiling = self.next + DAG_ID_BATCH;
                self.persist_ceiling(new_ceiling);
                self.ceiling = new_ceiling;
            }
            self.next += 1;
            self.next
        }

        /// Atomic write: tmp → fsync → rename.  Guarantees the ceiling
        /// is durable before any ID from the new batch is returned.
        fn persist_ceiling(&self, ceiling: u128) {
            use std::io::Write;
            let tmp = self.path.with_extension("tmp");
            let mut f =
                fs::File::create(&tmp).expect("dag_id_ceiling: create tmp failed");
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

    static ALLOC: LazyLock<Mutex<DagIdAllocator>> =
        LazyLock::new(|| Mutex::new(DagIdAllocator::init()));

    ALLOC.lock().alloc()
}
