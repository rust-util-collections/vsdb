//!
//! # Common components
//!

#![allow(dead_code)]

pub(crate) mod ende;
pub(crate) mod engines;

use {
    engines::Engine,
    once_cell::sync::Lazy,
    parking_lot::Mutex,
    ruc::*,
    std::{
        env, fs,
        mem::size_of,
        sync::atomic::{AtomicBool, Ordering},
    },
};

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub(crate) type RawBytes = Box<[u8]>;
pub(crate) type RawKey = RawBytes;
pub(crate) type RawValue = RawBytes;

pub(crate) type Prefix = u64;
pub(crate) type PrefixBytes = [u8; PREFIX_SIZ];
pub(crate) const PREFIX_SIZ: usize = size_of::<Prefix>();

pub(crate) type BranchID = u64;
pub(crate) type VersionID = u64;

/// Avoid making mistakes between branch name and version name.
#[derive(Clone, Copy)]
pub struct BranchName<'a>(pub &'a [u8]);
/// Avoid making mistakes between branch name and version name.
#[derive(Clone, Copy)]
pub struct ParentBranchName<'a>(pub &'a [u8]);
/// Avoid making mistakes between branch name and version name.
#[derive(Clone, Copy)]
pub struct VersionName<'a>(pub &'a [u8]);

const RESERVED_ID_CNT: Prefix = 4096_0000;
pub(crate) const BIGGEST_RESERVED_ID: Prefix = RESERVED_ID_CNT - 1;
pub(crate) const NULL: BranchID = BIGGEST_RESERVED_ID;

pub(crate) const INITIAL_BRANCH_ID: BranchID = 0;
pub(crate) const INITIAL_BRANCH_NAME: &[u8] = b"main";

/// How many ancestral branches at most one new branch can have.
pub const BRANCH_ANCESTORS_LIMIT: usize = 128;

// default value for reserved number when pruning old data
pub(crate) const RESERVED_VERSION_NUM_DEFAULT: usize = 10;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

static VSDB_BASE_DIR: Lazy<Mutex<String>> = Lazy::new(|| Mutex::new(gen_data_dir()));

#[cfg(all(feature = "sled_engine", not(feature = "rocks_engine")))]
pub(crate) static VSDB: Lazy<VsDB<engines::Sled>> = Lazy::new(|| pnk!(VsDB::new()));

#[cfg(all(feature = "rocks_engine", not(feature = "sled_engine")))]
pub(crate) static VSDB: Lazy<VsDB<engines::RocksDB>> = Lazy::new(|| pnk!(VsDB::new()));

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// Parse bytes to a specified integer type.
#[macro_export(crate)]
macro_rules! parse_int {
    ($bytes: expr, $ty: ty) => {{
        let array: [u8; std::mem::size_of::<$ty>()] = $bytes[..].try_into().unwrap();
        <$ty>::from_be_bytes(array)
    }};
}

/// Parse bytes to a `Prefix` type.
#[macro_export(crate)]
macro_rules! parse_prefix {
    ($bytes: expr) => {
        $crate::parse_int!($bytes, $crate::common::Prefix)
    };
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub(crate) struct VsDB<T: Engine> {
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
    pub(crate) fn alloc_branch_id(&self) -> BranchID {
        self.db.alloc_branch_id()
    }

    #[inline(always)]
    pub(crate) fn alloc_version_id(&self) -> VersionID {
        self.db.alloc_version_id()
    }

    #[inline(always)]
    fn flush(&self) {
        self.db.flush()
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

fn gen_data_dir() -> String {
    // Compatible with Windows OS?
    let d = env::var("VSDB_BASE_DIR")
        .or_else(|_| env::var("HOME").map(|h| format!("{}/.vsdb", h)))
        .unwrap_or_else(|_| "/tmp/.vsdb".to_owned());
    fs::create_dir_all(&d).unwrap();
    d
}

fn get_data_dir() -> String {
    VSDB_BASE_DIR.lock().clone()
}

/// Set ${VSDB_BASE_DIR} manually.
pub fn vsdb_set_base_dir(dir: String) -> Result<()> {
    static HAS_INITED: AtomicBool = AtomicBool::new(false);

    if HAS_INITED.swap(true, Ordering::Relaxed) {
        Err(eg!("VSDB has been initialized !!"))
    } else {
        *VSDB_BASE_DIR.lock() = dir;
        Ok(())
    }
}

/// Flush data to disk, may take a long time.
pub fn vsdb_flush() {
    VSDB.flush();
}
