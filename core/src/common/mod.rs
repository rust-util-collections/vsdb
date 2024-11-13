//!
//! # Common components
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
        atomic::{AtomicBool, Ordering},
        LazyLock,
    },
};
use threadpool::ThreadPool;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub const NULL: &[u8] = &[];

pub type RawBytes = Vec<u8>;
pub type RawKey = RawBytes;
pub type RawValue = RawBytes;

pub type Pre = u64;
pub const PREFIX_SIZE: usize = size_of::<Pre>();
pub type PreBytes = [u8; PREFIX_SIZE];

pub const KB: u64 = 1 << 10;
pub const MB: u64 = 1 << 20;
pub const GB: u64 = 1 << 30;

const RESERVED_ID_CNT: Pre = 4096_0000;
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
    env::set_var("VSDB_CUSTOM_DIR", d.as_os_str());
    d
});

#[cfg(feature = "rocks_backend")]
pub static VSDB: LazyLock<VsDB<engines::RocksDB>> = LazyLock::new(|| pnk!(VsDB::new()));

#[cfg(feature = "parity_backend")]
pub static VSDB: LazyLock<VsDB<engines::ParityDB>> = LazyLock::new(|| pnk!(VsDB::new()));

#[cfg(feature = "sled_backend")]
pub static VSDB: LazyLock<VsDB<engines::SledDB>> = LazyLock::new(|| pnk!(VsDB::new()));
/// Clean orphan instances in background.
pub static TRASH_CLEANER: LazyLock<Mutex<ThreadPool>> = LazyLock::new(|| {
    let pool = threadpool::Builder::new()
        .num_threads(1)
        .thread_stack_size(512 * MB as usize) // use large stack size
        .build();
    Mutex::new(pool)
});

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// Parse bytes to a specified integer type.
#[macro_export]
macro_rules! parse_int {
    ($bytes: expr, $ty: ty) => {{
        let array: [u8; std::mem::size_of::<$ty>()] = $bytes[..].try_into().unwrap();
        <$ty>::from_be_bytes(array)
    }};
}

/// Parse bytes to a `Pre` type.
#[macro_export]
macro_rules! parse_prefix {
    ($bytes: expr) => {
        $crate::parse_int!($bytes, $crate::common::Pre)
    };
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

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
        .or_else(|_| env::var("HOME").map(|h| format!("{}/.vsdb", h)))
        .unwrap_or_else(|_| "/tmp/.vsdb".to_owned());
    pnk!(fs::create_dir_all(&d));
    PathBuf::from(d)
}

/// ${VSDB_CUSTOM_DIR}
#[inline(always)]
pub fn vsdb_get_custom_dir() -> &'static Path {
    VSDB_CUSTOM_DIR.as_path()
}

/// ${VSDB_BASE_DIR}
#[inline(always)]
pub fn vsdb_get_base_dir() -> PathBuf {
    VSDB_BASE_DIR.lock().clone()
}

/// Set ${VSDB_BASE_DIR} manually.
#[inline(always)]
pub fn vsdb_set_base_dir(dir: impl AsRef<Path>) -> Result<()> {
    static HAS_INITED: AtomicBool = AtomicBool::new(false);

    if HAS_INITED.swap(true, Ordering::Relaxed) {
        Err(eg!("VSDB has been initialized !!"))
    } else {
        env::set_var(BASE_DIR_VAR, dir.as_ref().as_os_str());
        *VSDB_BASE_DIR.lock() = dir.as_ref().to_path_buf();
        Ok(())
    }
}

/// Flush data to disk, may take a long time.
#[inline(always)]
pub fn vsdb_flush() {
    VSDB.flush();
}
