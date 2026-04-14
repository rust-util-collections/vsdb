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

/// Generates a new, unique ID for a DAG map.
///
/// Maintains a persistent monotonic counter (`Orphan<u128>`) to ensure
/// that each generated ID is globally unique.
///
/// # Crash semantics
///
/// The counter persists via `Orphan<u128>` in MMDB. If the process
/// crashes after the counter increment is flushed but before the child
/// entry is written, the counter advances permanently and the skipped
/// ID is never used (safe gap). If the WAL flush for the counter is
/// itself lost (pre-WAL crash), the counter reverts while a different
/// shard may already contain the child entry — this is a known
/// limitation accepted by the current design.
pub fn gen_dag_map_id_num() -> u128 {
    use crate::{Orphan, ValueEnDe};
    use parking_lot::Mutex;
    use ruc::*;
    use std::{fs, io::ErrorKind, sync::LazyLock};

    static ID_NUM: LazyLock<Mutex<Orphan<u128>>> = LazyLock::new(|| {
        let mut meta_path = vsdb_core::vsdb_get_system_dir().to_owned();
        meta_path.push("id_num");

        match fs::read(&meta_path) {
            Ok(m) => Mutex::new(ValueEnDe::decode(&m).unwrap()),
            Err(e) => match e.kind() {
                ErrorKind::NotFound => {
                    let i = Orphan::new(0);
                    fs::write(&meta_path, i.encode()).unwrap();
                    Mutex::new(i)
                }
                _ => {
                    pnk!(Err(eg!("Error!")))
                }
            },
        }
    });

    let mut hdr = ID_NUM.lock();
    let mut hdr = hdr.get_mut();
    *hdr += 1;
    *hdr
}
