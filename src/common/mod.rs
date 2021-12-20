//!
//! # Common components
//!

#![allow(dead_code)]

pub(crate) mod ende;
pub(crate) mod engines;

use {
    crc32fast::Hasher,
    engines::Engine,
    lazy_static::lazy_static,
    ruc::*,
    serde::{Deserialize, Serialize},
    std::{
        env, fs,
        mem::size_of,
        sync::atomic::{AtomicBool, Ordering},
        sync::{Arc, Mutex},
    },
};

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub(crate) type RawBytes = Box<[u8]>;
pub(crate) type RawKey = RawBytes;
pub(crate) type RawValue = RawBytes;

// Checksum of a version
pub(crate) type VerChecksum = [u8; size_of::<u32>()];

pub(crate) type Prefix = u64;
pub(crate) type PrefixBytes = [u8; PREFIX_SIZ];
pub(crate) const PREFIX_SIZ: usize = size_of::<Prefix>();

pub(crate) type BranchID = u64;
pub(crate) type VersionID = u64;

/// avoid making mistakes between branch name and version name
pub struct BranchName<'a>(pub &'a [u8]);
/// +1 above
pub struct ParentBranchName<'a>(pub &'a [u8]);
/// +1 above
pub struct VersionName<'a>(pub &'a [u8]);

const RESERVED_ID_CNT: Prefix = 4096;
pub(crate) const BIGGEST_RESERVED_ID: Prefix = RESERVED_ID_CNT - 1;
pub(crate) const NULL: BranchID = BIGGEST_RESERVED_ID;

pub(crate) const INITIAL_BRANCH_ID: BranchID = 0;
pub(crate) const INITIAL_BRANCH_NAME: &[u8] = b"main";

/// how many branches in one instance can be created
pub const BRANCH_CNT_LIMIT: usize = 1024;

// default value for reserved number when pruning old data
pub(crate) const RESERVED_VERSION_NUM_DEFAULT: usize = 10;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

lazy_static! {
    static ref VSDB_BASE_DIR: Arc<Mutex<String>> = Arc::new(Mutex::new(gen_data_dir()));
}

#[cfg(all(feature = "sled_engine", not(feature = "rocks_engine")))]
lazy_static! {
    pub(crate) static ref VSDB: VsDB<engines::Sled> = pnk!(VsDB::new());
}

#[cfg(all(feature = "rocks_engine", not(feature = "sled_engine")))]
lazy_static! {
    pub(crate) static ref VSDB: VsDB<engines::RocksDB> = pnk!(VsDB::new());
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// Parse bytes to a specified integer type.
#[macro_export(crate)]
macro_rules! parse_int {
    ($bytes: expr, $ty: ty) => {{
        let array: [u8; std::mem::size_of::<$ty>()] = $bytes.try_into().unwrap();
        <$ty>::from_be_bytes(array)
    }};
}

/// Parse bytes to Prefix
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
    VSDB_BASE_DIR.lock().unwrap().clone()
}

/// Set ${VSDB_BASE_DIR} manually
pub fn vsdb_set_base_dir(dir: String) -> Result<()> {
    lazy_static! {
        static ref HAS_INITED: AtomicBool = AtomicBool::new(false);
    }

    if HAS_INITED.swap(true, Ordering::Relaxed) {
        Err(eg!("VSDB has been initialized !!"))
    } else {
        *VSDB_BASE_DIR.lock().unwrap() = dir;
        Ok(())
    }
}

/// Flush data to disk, may take a long time.
pub fn vsdb_flush() {
    VSDB.flush();
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[derive(Deserialize, Serialize, Debug)]
pub(crate) struct InstanceCfg {
    pub(crate) prefix: PrefixBytes,
    pub(crate) item_cnt: u64,
    pub(crate) area_idx: usize,
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[inline(always)]
pub(crate) fn compute_checksum(ivec: &[&[u8]]) -> VerChecksum {
    let mut hasher = Hasher::new();
    for bytes in ivec {
        hasher.update(bytes);
    }
    hasher.finalize().to_be_bytes()
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
