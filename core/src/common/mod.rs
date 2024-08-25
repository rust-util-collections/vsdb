//!
//! # Common components
//!

pub(crate) mod engines;

#[cfg(feature = "vs")]
pub use ruc::crypto::trie_root;

use engines::Engine;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    mem::size_of,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
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

pub type BranchID = [u8; size_of::<u64>()];
pub type VersionID = [u8; size_of::<u64>()];

pub const VER_ID_MAX: VersionID = VersionIDBase::MAX.to_be_bytes();

pub type BranchIDBase = u64;
pub type VersionIDBase = u64;

/// Avoid making mistakes between branch name and version name.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BranchName<'a>(pub &'a [u8]);
#[derive(Clone, Debug, Eq, PartialEq, Default, Serialize, Deserialize)]
pub struct BranchNameOwned(pub Vec<u8>);
/// Avoid making mistakes between branch name and version name.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default, Serialize, Deserialize)]
pub struct ParentBranchName<'a>(pub &'a [u8]);
#[derive(Clone, Debug, Eq, PartialEq, Default, Serialize, Deserialize)]
pub struct ParentBranchNameOwned(pub Vec<u8>);
/// Avoid making mistakes between branch name and version name.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct VersionName<'a>(pub &'a [u8]);
#[derive(Clone, Debug, Eq, PartialEq, Default, Serialize, Deserialize)]
pub struct VersionNameOwned(pub Vec<u8>);

pub const KB: u64 = 1 << 10;
pub const MB: u64 = 1 << 20;
pub const GB: u64 = 1 << 30;

const RESERVED_ID_CNT: Pre = 4096_0000;
pub const BIGGEST_RESERVED_ID: Pre = RESERVED_ID_CNT - 1;
pub const NULL_ID: BranchID = (BIGGEST_RESERVED_ID as BranchIDBase).to_be_bytes();

pub const INITIAL_BRANCH_ID: BranchIDBase = 0;
pub const INITIAL_BRANCH_NAME: BranchName<'static> = BranchName(b"master");

/// The default value for reserved number when pruning old data.
pub const RESERVED_VERSION_NUM_DEFAULT: usize = 10;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

const BASE_DIR_VAR: &str = "VSDB_BASE_DIR";

static VSDB_BASE_DIR: Lazy<Mutex<PathBuf>> = Lazy::new(|| Mutex::new(gen_data_dir()));

static VSDB_CUSTOM_DIR: Lazy<PathBuf> = Lazy::new(|| {
    let mut d = VSDB_BASE_DIR.lock().clone();
    d.push("__CUSTOM__");
    pnk!(fs::create_dir_all(&d));
    env::set_var("VSDB_CUSTOM_DIR", d.as_os_str());
    d
});

#[cfg(any(
    feature = "rocks_engine",
    all(feature = "rocks_engine", feature = "sled_engine"),
    all(not(feature = "rocks_engine"), not(feature = "sled_engine")),
))]
pub static VSDB: Lazy<VsDB<engines::RocksDB>> = Lazy::new(|| pnk!(VsDB::new()));

#[cfg(all(feature = "sled_engine", not(feature = "rocks_engine")))]
pub static VSDB: Lazy<VsDB<engines::Sled>> = Lazy::new(|| pnk!(VsDB::new()));

/// Clean orphan instances in background.
pub static TRASH_CLEANER: Lazy<Mutex<ThreadPool>> =
    Lazy::new(|| Mutex::new(ThreadPool::new(1)));

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
    pub fn alloc_br_id(&self) -> BranchIDBase {
        self.db.alloc_br_id()
    }

    #[inline(always)]
    pub fn alloc_ver_id(&self) -> VersionIDBase {
        self.db.alloc_ver_id()
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

macro_rules! impl_from_for_name {
    ($target: tt) => {
        impl<'a> From<&'a [u8]> for $target<'a> {
            fn from(t: &'a [u8]) -> Self {
                $target(t)
            }
        }
        impl<'a> From<&'a Vec<u8>> for $target<'a> {
            fn from(t: &'a Vec<u8>) -> Self {
                $target(t.as_slice())
            }
        }
        impl<'a> From<&'a str> for $target<'a> {
            fn from(t: &'a str) -> Self {
                $target(t.as_bytes())
            }
        }
        impl<'a> From<&'a String> for $target<'a> {
            fn from(t: &'a String) -> Self {
                $target(t.as_bytes())
            }
        }
    };
    ($target: tt, $($t: tt),+) => {
        impl_from_for_name!($target);
        impl_from_for_name!($($t), +);
    };
}

impl_from_for_name!(BranchName, ParentBranchName, VersionName);

impl Default for BranchName<'static> {
    fn default() -> Self {
        INITIAL_BRANCH_NAME
    }
}

impl BranchNameOwned {
    #[inline(always)]
    pub fn as_deref(&self) -> BranchName {
        BranchName(&self.0)
    }
}

impl<'a> From<&'a BranchNameOwned> for BranchName<'a> {
    fn from(b: &'a BranchNameOwned) -> Self {
        b.as_deref()
    }
}

impl From<BranchName<'_>> for BranchNameOwned {
    fn from(b: BranchName) -> Self {
        BranchNameOwned(b.0.to_vec())
    }
}

impl ParentBranchNameOwned {
    #[inline(always)]
    pub fn as_deref(&self) -> ParentBranchName {
        ParentBranchName(&self.0)
    }
}

impl<'a> From<&'a ParentBranchNameOwned> for ParentBranchName<'a> {
    fn from(b: &'a ParentBranchNameOwned) -> Self {
        b.as_deref()
    }
}

impl From<ParentBranchName<'_>> for ParentBranchNameOwned {
    fn from(b: ParentBranchName) -> Self {
        ParentBranchNameOwned(b.0.to_vec())
    }
}

impl VersionNameOwned {
    #[inline(always)]
    pub fn as_deref(&self) -> VersionName {
        VersionName(&self.0)
    }
}

impl<'a> From<&'a VersionNameOwned> for VersionName<'a> {
    fn from(b: &'a VersionNameOwned) -> Self {
        b.as_deref()
    }
}

impl From<VersionName<'_>> for VersionNameOwned {
    fn from(b: VersionName) -> Self {
        VersionNameOwned(b.0.to_vec())
    }
}
