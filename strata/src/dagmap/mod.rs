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
/// This function maintains a persistent counter to ensure that each generated
/// ID is unique.
pub fn gen_dag_map_id_num() -> u128 {
    use crate::{Orphan, ValueEnDe};
    use parking_lot::Mutex;
    use ruc::*;
    use std::{fs, io::ErrorKind, sync::LazyLock};

    static ID_NUM: LazyLock<Mutex<Orphan<u128>>> = LazyLock::new(|| {
        let mut meta_path = vsdb_core::vsdb_get_custom_dir().to_owned();
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
