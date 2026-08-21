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
/// Namespaces: anonymous placement groups (independent engine instances).
pub mod namespace;

pub use engine::BatchTrait;
use error::{Result, VsdbError};
pub use namespace::{
    DEFAULT_NS_ID, InstanceId, Namespace, NamespaceOpts, NsId, NsInfo, vsdb_ns_close,
    vsdb_ns_destroy, vsdb_ns_list, vsdb_ns_relocate,
};
use parking_lot::Mutex;
use ruc::*;
use std::{
    env, fs, io,
    mem::size_of,
    path::{Path, PathBuf},
    sync::{
        LazyLock,
        atomic::{AtomicBool, AtomicU8, Ordering},
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
    if !vsdb_is_read_only() {
        pnk!(fs::create_dir_all(&d));
    }
    d
});

static VSDB_SYSTEM_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    // See VSDB_CUSTOM_DIR: derived paths freeze the base dir.
    vsdb_freeze_base_dir();
    let mut d = VSDB_BASE_DIR.lock().clone();
    d.push("__SYSTEM__");
    if !vsdb_is_read_only() {
        pnk!(fs::create_dir_all(&d));
    }
    d
});

static VSDB_META_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    let mut d = VSDB_SYSTEM_DIR.clone();
    d.push("__instance_meta__");
    if !vsdb_is_read_only() {
        pnk!(fs::create_dir_all(&d));
    }
    d
});

/// Returns the instance-meta directory path of the **default
/// namespace** (its root is the base dir, so this equals
/// `Namespace::default_ns().meta_dir()`; non-default namespaces keep
/// their metas under their own roots — use [`Namespace::meta_dir`]).
///
/// This directory (`{system_dir}/__instance_meta__/`) is used to persist
/// lightweight metadata (e.g. serialized handles) for individual VSDB
/// instances, keyed by their unique `instance_id`.
/// In read-only mode this returns the path without creating the directory.
#[inline(always)]
pub fn vsdb_get_meta_dir() -> &'static Path {
    VSDB_META_DIR.as_path()
}

/// Returns the **default-namespace** meta file path for a given map ID
/// (a bare `u64` can only ever address a default-namespace instance;
/// namespace-resident metas live under [`Namespace::meta_dir`]).
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
///
/// # Errors
///
/// Returns [`VsdbError::ReadOnly`] before touching the filesystem in
/// read-only mode, or a storage error if the replacement fails.
pub fn atomic_write_file(path: &Path, bytes: &[u8]) -> Result<()> {
    if vsdb_is_read_only() {
        return Err(VsdbError::ReadOnly {
            operation: "atomic file write",
        });
    }
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        let mut f = fs::File::create(&tmp)?;
        io::Write::write_all(&mut f, bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
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
/// This struct provides a high-level interface for interacting with the
/// database. It is a thin delegate over the default namespace, which
/// owns the actual default engine (see `namespace::DEFAULT_NS`).
///
/// The storage engine is MMDB, a pure-Rust LSM-Tree engine.
pub struct VsDB {
    ns: namespace::Namespace,
}

impl VsDB {
    #[inline(always)]
    fn new() -> Result<Self> {
        Ok(Self {
            ns: namespace::Namespace::default_ns(),
        })
    }

    #[inline(always)]
    fn flush(&self) {
        self.ns.flush()
    }

    /// The default engine (test-only: the allocator tests exercise
    /// engine-handle allocation through the process-global singleton).
    #[cfg(test)]
    pub(crate) fn engine(&self) -> &engine::Engine {
        self.ns.engine()
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
    if !vsdb_is_read_only() {
        pnk!(fs::create_dir_all(&d));
    }
    PathBuf::from(d)
}

/// Returns the custom directory path for VSDB — the **app-level
/// bootstrap anchor**, deliberately one per universe (NOT per
/// namespace).
///
/// This directory (`{base_dir}/__CUSTOM__/`) is available for users to
/// store application-specific files alongside the VSDB data directory —
/// typically the serialized app state holding the top-level collection
/// handles. Those handles may live in *any* namespace (their metas
/// embed the owning `ns_id`, and deserialization auto-opens it), so
/// this blob is the app's index over the whole universe. That is why
/// the dir does not split per namespace: namespaces are reached
/// *through* handles, and the handles are bootstrapped from here — a
/// per-namespace location would be circular, and `vsdb_ns_destroy`
/// would silently take the app's root pointer with it. Users need to
/// remember exactly one thing: the base dir.
///
/// In read-only mode this returns the path without creating the directory.
///
/// # Returns
///
/// A `&'static Path` to the custom directory.
#[inline(always)]
pub fn vsdb_get_custom_dir() -> &'static Path {
    VSDB_CUSTOM_DIR.as_path()
}

/// Returns the internal system directory path of the **default
/// namespace** (≡ `Namespace::default_ns().system_dir()`; each
/// namespace has its own `__SYSTEM__` under its root — see
/// [`Namespace::system_dir`]). Also home to the registry-wide state
/// that deliberately stays global: the namespace registry, the prefix
/// allocator ceiling, and the DagMap ID counter.
///
/// This directory (`{base_dir}/__SYSTEM__/`) is reserved for VSDB internal use
/// (instance metadata, trie caches, ID counters). Not intended for external use.
/// In read-only mode this returns the path without creating the directory.
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

/// Access capability selected for the whole VSDB universe in this process.
///
/// VSDB handles persist namespace ids but not an open mode, so the mode is
/// deliberately process-wide: automatic namespace opens during deserialization
/// cannot accidentally regain write capability. The selection is one-shot;
/// separate reader and writer processes are required for different modes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
#[repr(u8)]
pub enum OpenMode {
    /// Normal read-write operation.
    #[default]
    ReadWrite = 0,
    /// Open existing data without modifying the store.
    ///
    /// Existing handles can be restored and queried, including data recovered
    /// from residual WAL records in memory. No directory, format marker,
    /// allocator state, metadata, WAL, SST, or automatic trie cache is written.
    ReadOnly = 1,
}

/// One-shot process configuration for a VSDB universe.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct VsdbOptions {
    /// Root of the default namespace and universe-wide metadata.
    pub base_dir: PathBuf,
    /// Capability used by the default and every non-default namespace.
    pub open_mode: OpenMode,
}

impl VsdbOptions {
    /// Configures a normal read-write universe at `base_dir`.
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
            open_mode: OpenMode::ReadWrite,
        }
    }

    /// Configures an existing universe for read-only access.
    ///
    /// The database must already be complete. All namespaces opened by the
    /// process inherit this capability; per-namespace mixed modes are not
    /// supported.
    pub fn read_only(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
            open_mode: OpenMode::ReadOnly,
        }
    }
}

static OPEN_MODE: AtomicU8 = AtomicU8::new(OpenMode::ReadWrite as u8);

/// Returns the process-wide access mode.
///
/// The default is [`OpenMode::ReadWrite`] until [`vsdb_configure`] selects a
/// mode. Calling this getter does not freeze the configuration.
#[inline(always)]
pub fn vsdb_open_mode() -> OpenMode {
    if OPEN_MODE.load(Ordering::Acquire) == OpenMode::ReadOnly as u8 {
        OpenMode::ReadOnly
    } else {
        OpenMode::ReadWrite
    }
}

#[inline(always)]
pub(crate) fn vsdb_is_read_only() -> bool {
    vsdb_open_mode() == OpenMode::ReadOnly
}

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

/// Selects the base directory and access capability before first use.
///
/// This is the entry point for [`OpenMode::ReadOnly`]. The choice is one-shot
/// and process-wide: it applies to the default namespace and every
/// non-default namespace opened through a restored handle. Call it at the
/// start of `main`, before spawning threads and before any other VSDB API.
///
/// Read-only mode opens only complete existing datasets. Reads and in-memory
/// WAL recovery are supported without filesystem changes. Fallible write
/// operations return [`VsdbError::ReadOnly`]; legacy infallible collection
/// mutations panic; maintenance-only flush and deferred-delete calls are
/// no-ops.
///
/// Like [`vsdb_set_base_dir`], this publishes `VSDB_BASE_DIR` in the process
/// environment for child processes.
///
/// # Errors
///
/// Returns [`VsdbError::BaseDirFrozen`] if configuration was already selected
/// or any API that materializes/freezes a VSDB path was used first.
///
/// # Safety contract
///
/// Updating a process environment with concurrently running threads is not
/// safe. The caller must invoke this during single-threaded startup, before
/// spawning threads.
pub fn vsdb_configure(options: VsdbOptions) -> Result<()> {
    if BASE_DIR_FROZEN.swap(true, Ordering::AcqRel) {
        return Err(VsdbError::BaseDirFrozen);
    }

    OPEN_MODE.store(options.open_mode as u8, Ordering::Release);
    // SAFETY: guarded by the one-shot freeze above; the public contract
    // requires configuration before other threads can access the environment.
    unsafe { env::set_var(BASE_DIR_VAR, &options.base_dir) }
    *VSDB_BASE_DIR.lock() = options.base_dir;
    Ok(())
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
    vsdb_configure(VsdbOptions::new(dir.as_ref()))
}

/// Flushes all data to disk — the default namespace and every open
/// non-default namespace.
///
/// This function triggers a flush operation on the underlying database,
/// ensuring that all pending writes are persisted to disk. This operation
/// may take a long time to complete, depending on the amount of data to be flushed.
/// It is a no-op when [`vsdb_open_mode`] is [`OpenMode::ReadOnly`].
#[inline(always)]
pub fn vsdb_flush() {
    if vsdb_is_read_only() {
        return;
    }
    VSDB.flush();
    namespace::flush_all_open();
}
