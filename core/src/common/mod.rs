//!
//! # Common components
//!
//! This module provides common components and utilities used throughout the VSDB framework.
//! It includes type definitions, constants, macros, and functions for managing the
//! underlying database environment.
//!

pub(crate) mod engines;

use engines::Engine;
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
use threadpool::ThreadPool;

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
    unsafe { env::set_var("VSDB_CUSTOM_DIR", d.as_os_str()) }
    d
});

/// The global instance of the VsDB database.
///
/// This static variable is lazily initialized and provides a single point of
/// access to the underlying database. The backend is determined by the
/// feature flags passed at compile time.
#[cfg(feature = "rocks_backend")]
pub static VSDB: LazyLock<VsDB<engines::RocksDB>> = LazyLock::new(|| pnk!(VsDB::new()));

/// The global instance of the VsDB database.
///
/// This static variable is lazily initialized and provides a single point of
/// access to the underlying database. The backend is determined by the
/// feature flags passed at compile time.
#[cfg(feature = "parity_backend")]
pub static VSDB: LazyLock<VsDB<engines::ParityDB>> = LazyLock::new(|| pnk!(VsDB::new()));

/// A thread pool for cleaning up orphan instances in the background.
///
/// This static variable is lazily initialized and provides a thread pool
/// with a single thread and a large stack size to handle background cleanup tasks.
pub static TRASH_CLEANER: LazyLock<Mutex<ThreadPool>> = LazyLock::new(|| {
    let pool = threadpool::Builder::new()
        .num_threads(1)
        .thread_stack_size(512 * MB as usize) // use large stack size
        .build();
    Mutex::new(pool)
});

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
        <$ty>::from_be_bytes(array)
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
/// This struct encapsulates the underlying database engine and provides a
/// high-level interface for interacting with the database.
pub struct VsDB<T: Engine> {
    db: T,
}

impl<T: Engine> VsDB<T> {
    #[inline(always)]
    fn new() -> Result<Self> {
        Ok(Self {
            db: T::new().c(d!())?,
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
        .unwrap_or_else(|_| "/tmp/.vsdb".to_owned());
    pnk!(fs::create_dir_all(&d));
    PathBuf::from(d)
}

/// Returns the custom directory path for VSDB.
///
/// This function returns a static reference to the path of the custom directory,
/// which is set by the `VSDB_CUSTOM_DIR` environment variable.
///
/// # Returns
///
/// A `&'static Path` to the custom directory.
#[inline(always)]
pub fn vsdb_get_custom_dir() -> &'static Path {
    VSDB_CUSTOM_DIR.as_path()
}

/// Returns the base directory path for VSDB.
///
/// This function returns the path of the base directory, which is determined
/// by the `VSDB_BASE_DIR` environment variable, the `HOME` environment variable,
/// or a default path of `/tmp/.vsdb`.
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

    if HAS_INITED.swap(true, Ordering::Relaxed) {
        Err(eg!("VSDB has been initialized !!"))
    } else {
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
