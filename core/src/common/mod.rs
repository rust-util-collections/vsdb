//!
//! # Common components
//!
//! This module provides common components and utilities used throughout the VSDB framework.
//! It includes type definitions, constants, macros, and functions for managing the
//! underlying database environment.
//!

pub(crate) mod engine;
/// Structured error types for the VSDB public API.
pub mod error;

pub use engine::BatchTrait;
use error::{Result, VsdbError};
use parking_lot::Mutex;
use ruc::*;
use std::{
    env, fs, io,
    mem::size_of,
    path::{Path, PathBuf},
    sync::{
        LazyLock,
        atomic::{AtomicBool, Ordering},
    },
};

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

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

/// The first allocatable prefix: everything below this value is reserved
/// (never issued by the allocator). Doubles as the allocator's initial
/// persisted value.
const PREFIX_ALLOC_START: Pre = 4096_0000;
/// The biggest reserved ID.
pub const BIGGEST_RESERVED_ID: Pre = PREFIX_ALLOC_START - 1;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

const BASE_DIR_VAR: &str = "VSDB_BASE_DIR";

static VSDB_BASE_DIR: LazyLock<Mutex<PathBuf>> =
    LazyLock::new(|| Mutex::new(gen_data_dir()));

static VSDB_CUSTOM_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    // Materializing a derived directory pins it to the current base
    // dir forever; freeze the base dir so a later `vsdb_set_base_dir`
    // fails loudly instead of silently splitting the directory tree
    // across two bases.
    vsdb_freeze_base_dir();
    let mut d = VSDB_BASE_DIR.lock().clone();
    d.push("__CUSTOM__");
    pnk!(fs::create_dir_all(&d));
    d
});

static VSDB_SYSTEM_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    // See VSDB_CUSTOM_DIR: derived paths freeze the base dir.
    vsdb_freeze_base_dir();
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

/// Atomically replaces the file at `path` with `bytes`.
///
/// Writes to a sibling `*.tmp` file, fsyncs it, then renames it over the
/// target — a crash mid-write can never leave a truncated file at `path`
/// (POSIX `rename` is atomic within a filesystem). Instance metas are
/// written under the SWMR contract, so the fixed tmp name cannot race.
pub fn atomic_write_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        let mut f = fs::File::create(&tmp)?;
        io::Write::write_all(&mut f, bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)
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
macro_rules! parse_int {
    ($bytes: expr, $ty: ty) => {{
        let array: [u8; std::mem::size_of::<$ty>()] = $bytes[..].try_into().unwrap();
        <$ty>::from_le_bytes(array)
    }};
}
pub(crate) use parse_int;

/// A macro to parse a byte slice into a `Pre` type.
///
/// # Arguments
///
/// * `$bytes` - The byte slice to parse.
///
/// # Panics
///
/// This macro will panic if the byte slice cannot be converted into a `Pre` type.
macro_rules! parse_prefix {
    ($bytes: expr) => {
        $crate::common::parse_int!($bytes, $crate::common::Pre)
    };
}
pub(crate) use parse_prefix;

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

/// Whether the base directory has been frozen (set manually or locked in
/// by the first database initialization).
static BASE_DIR_FROZEN: AtomicBool = AtomicBool::new(false);

/// Freezes the base directory without touching the process environment.
///
/// Called by the engine when the database is first opened so that any
/// later [`vsdb_set_base_dir`] call fails instead of silently diverging
/// from the directory already in use.  This deliberately performs no
/// `env::set_var` — it can run at an arbitrary point in a multithreaded
/// program, where mutating the environment would be unsound.
#[inline(always)]
pub(crate) fn vsdb_freeze_base_dir() {
    BASE_DIR_FROZEN.store(true, Ordering::Release);
}

/// Sets the base directory path for VSDB manually.
///
/// This function allows you to programmatically set the base directory for VSDB.
/// It can only be called once, before the database is initialized.
///
/// It also publishes the directory through the `VSDB_BASE_DIR`
/// environment variable (for child processes).  Because `env::set_var`
/// is unsound while other threads may be reading the environment, call
/// this **early in `main`, before spawning any threads**.  If you cannot
/// guarantee that, set the `VSDB_BASE_DIR` environment variable before
/// process start instead of calling this function.
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
    if BASE_DIR_FROZEN.swap(true, Ordering::AcqRel) {
        Err(VsdbError::BaseDirFrozen)
    } else {
        // SAFETY: Guarded by the `BASE_DIR_FROZEN` swap — runs at most
        // once.  The documented contract above requires the caller to
        // invoke this before spawning threads, so no concurrent
        // `getenv`/`setenv` can observe the mutation.
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
