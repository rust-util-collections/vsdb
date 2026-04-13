//!
//! # Common components
//!
//! This module provides common components and utilities used throughout the VSDB framework.
//! It includes type definitions, constants, macros, and functions for managing the
//! underlying database environment.
//!

pub(crate) mod engine;

pub use engine::BatchTrait;
use parking_lot::Mutex;
use ruc::*;
use std::{
    env, fs,
    mem::size_of,
    path::{Path, PathBuf},
    sync::{
        LazyLock,
        atomic::{AtomicBool, Ordering},
    },
};

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A constant representing a null or empty byte slice.
pub const NULL: &[u8] = &[];

/// A type alias for a vector of bytes, commonly used for raw data.
pub type RawBytes = Vec<u8>;
/// A type alias for a raw key, represented as a vector of bytes.
pub type RawKey = RawBytes;
/// A type alias for a raw value, represented as a vector of bytes.
pub type RawValue = RawBytes;

/// A type alias for a prefix, represented as a `u64`.
pub type Pre = u64;
/// The size of a prefix in bytes.
pub const PREFIX_SIZE: usize = size_of::<Pre>();
/// A type alias for a prefix represented as a byte array.
pub type PreBytes = [u8; PREFIX_SIZE];

/// A constant representing 1 kilobyte in bytes.
pub const KB: u64 = 1 << 10;
/// A constant representing 1 megabyte in bytes.
pub const MB: u64 = 1 << 20;
/// A constant representing 1 gigabyte in bytes.
pub const GB: u64 = 1 << 30;

/// The number of reserved IDs.
const RESERVED_ID_CNT: Pre = 4096_0000;
/// The biggest reserved ID.
pub const BIGGEST_RESERVED_ID: Pre = RESERVED_ID_CNT - 1;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

const BASE_DIR_VAR: &str = "VSDB_BASE_DIR";

static VSDB_BASE_DIR: LazyLock<Mutex<PathBuf>> =
    LazyLock::new(|| Mutex::new(gen_data_dir()));

static VSDB_CUSTOM_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    let mut d = VSDB_BASE_DIR.lock().clone();
    d.push("__CUSTOM__");
    pnk!(fs::create_dir_all(&d));
    // SAFETY: Called during `LazyLock` init, which runs exactly once and
    // blocks concurrent accessors. No other threads read this env var
    // before initialization completes.
    unsafe { env::set_var("VSDB_CUSTOM_DIR", d.as_os_str()) }
    d
});

static VSDB_SYSTEM_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    let mut d = VSDB_BASE_DIR.lock().clone();
    d.push("__SYSTEM__");
    pnk!(fs::create_dir_all(&d));
    d
});

static VSDB_META_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    let mut d = VSDB_SYSTEM_DIR.clone();
    d.push("__instance_meta__");
    pnk!(fs::create_dir_all(&d));
    d
});

/// Returns the instance-meta directory path for VSDB.
///
/// This directory (`{system_dir}/__instance_meta__/`) is used to persist
/// lightweight metadata (e.g. serialized handles) for individual VSDB
/// instances, keyed by their unique `instance_id`.
#[inline(always)]
pub fn vsdb_get_meta_dir() -> &'static Path {
    VSDB_META_DIR.as_path()
}

/// Returns the meta file path for a given instance ID.
#[inline(always)]
pub fn vsdb_meta_path(instance_id: u64) -> PathBuf {
    let mut p = VSDB_META_DIR.clone();
    p.push(format!("{:016x}", instance_id));
    p
}

/// The global instance of the VsDB database.
///
/// This static variable is lazily initialized and provides a single point of
/// access to the underlying database.
pub static VSDB: LazyLock<VsDB> = LazyLock::new(|| pnk!(VsDB::new()));

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A macro to parse a byte slice into a specified integer type.
///
/// # Arguments
///
/// * `$bytes` - The byte slice to parse.
/// * `$ty` - The integer type to parse the bytes into.
///
/// # Panics
///
/// This macro will panic if the byte slice cannot be converted into the specified integer type.
#[macro_export]
macro_rules! parse_int {
    ($bytes: expr, $ty: ty) => {{
        let array: [u8; std::mem::size_of::<$ty>()] = $bytes[..].try_into().unwrap();
        <$ty>::from_le_bytes(array)
    }};
}

/// A macro to parse a byte slice into a `Pre` type.
///
/// # Arguments
///
/// * `$bytes` - The byte slice to parse.
///
/// # Panics
///
/// This macro will panic if the byte slice cannot be converted into a `Pre` type.
#[macro_export]
macro_rules! parse_prefix {
    ($bytes: expr) => {
        $crate::parse_int!($bytes, $crate::common::Pre)
    };
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A struct representing the VsDB database.
///
/// This struct encapsulates the underlying storage engine and provides a
/// high-level interface for interacting with the database.
///
/// The storage engine is MMDB, a pure-Rust LSM-Tree engine.
pub struct VsDB {
    db: engine::Engine,
}

impl VsDB {
    #[inline(always)]
    fn new() -> Result<Self> {
        Ok(Self {
            db: engine::Engine::new().c(d!())?,
        })
    }

    #[inline(always)]
    fn flush(&self) {
        self.db.flush()
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[inline(always)]
fn gen_data_dir() -> PathBuf {
    // Compatible with Windows OS?
    let d = env::var(BASE_DIR_VAR)
        .or_else(|_| env::var("HOME").map(|h| format!("{h}/.vsdb")))
        .unwrap_or_else(|_| {
            let mut p = env::temp_dir();
            p.push(format!(".vsdb_{}", std::process::id()));
            let s = p.to_string_lossy().into_owned();
            eprintln!(
                "vsdb: neither VSDB_BASE_DIR nor HOME is set; \
                 using temporary directory {s} (data will not persist across restarts)"
            );
            s
        });
    pnk!(fs::create_dir_all(&d));
    PathBuf::from(d)
}

/// Returns the custom directory path for VSDB.
///
/// This directory (`{base_dir}/__CUSTOM__/`) is available for users to store
/// application-specific files alongside the VSDB data directory.
///
/// # Returns
///
/// A `&'static Path` to the custom directory.
#[inline(always)]
pub fn vsdb_get_custom_dir() -> &'static Path {
    VSDB_CUSTOM_DIR.as_path()
}

/// Returns the internal system directory path for VSDB.
///
/// This directory (`{base_dir}/__SYSTEM__/`) is reserved for VSDB internal use
/// (instance metadata, trie caches, ID counters). Not intended for external use.
#[inline(always)]
pub fn vsdb_get_system_dir() -> &'static Path {
    VSDB_SYSTEM_DIR.as_path()
}

/// Returns the base directory path for VSDB.
///
/// This function returns the path of the base directory, which is determined
/// by the `VSDB_BASE_DIR` environment variable, the `HOME` environment variable
/// (`$HOME/.vsdb`), or a process-private temporary directory as a last resort.
///
/// # Returns
///
/// A `PathBuf` to the base directory.
#[inline(always)]
pub fn vsdb_get_base_dir() -> PathBuf {
    VSDB_BASE_DIR.lock().clone()
}

/// Sets the base directory path for VSDB manually.
///
/// This function allows you to programmatically set the base directory for VSDB.
/// It can only be called once, before the database is initialized.
///
/// # Arguments
///
/// * `dir` - An object that can be converted into a `Path`.
///
/// # Errors
///
/// This function will return an error if the base directory has already been initialized.
#[inline(always)]
pub fn vsdb_set_base_dir(dir: impl AsRef<Path>) -> Result<()> {
    static HAS_INITED: AtomicBool = AtomicBool::new(false);

    if HAS_INITED.swap(true, Ordering::AcqRel) {
        Err(eg!("VSDB has been initialized !!"))
    } else {
        // SAFETY: Guarded by `HAS_INITED` swap — runs at most once.
        // Must be called before spawning worker threads; the caller is
        // responsible for ensuring no concurrent env readers exist.
        unsafe { env::set_var(BASE_DIR_VAR, dir.as_ref().as_os_str()) }
        *VSDB_BASE_DIR.lock() = dir.as_ref().to_path_buf();
        Ok(())
    }
}

/// Flushes all data to disk.
///
/// This function triggers a flush operation on the underlying database,
/// ensuring that all pending writes are persisted to disk. This operation
/// may take a long time to complete, depending on the amount of data to be flushed.
#[inline(always)]
pub fn vsdb_flush() {
    VSDB.flush();
}
