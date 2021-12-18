//!
//! # Common components
//!

pub(crate) mod engines;

use {
    core::{fmt, result::Result as CoreResult},
    engines::Engine,
    lazy_static::lazy_static,
    ruc::*,
    serde::{de, Deserialize, Serialize},
    sha3::{Digest, Sha3_256},
    std::{
        env, fs,
        mem::size_of,
        sync::atomic::{AtomicBool, Ordering},
        sync::{Arc, Mutex},
    },
};

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

pub(crate) type Prefix = u64;
pub(crate) type PrefixBytes = [u8; PREFIX_SIZ];
pub(crate) const PREFIX_SIZ: usize = size_of::<Prefix>();

const RESERVED_ID_CNT: Prefix = 4096;
pub(crate) const BIGGEST_RESERVED_ID: Prefix = RESERVED_ID_CNT - 1;

pub(crate) type BranchID = u64;
pub(crate) type VersionID = u64;

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

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

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

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

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

pub(crate) struct VsDB<T: Engine> {
    db: T,
}

impl<T: Engine> VsDB<T> {
    fn new() -> Result<Self> {
        Ok(Self {
            db: T::new().c(d!())?,
        })
    }

    pub(crate) fn alloc_branch_id(&self) -> BranchID {
        self.db.alloc_branch_id()
    }

    pub(crate) fn alloc_version_id(&self) -> VersionID {
        self.db.alloc_version_id()
    }

    fn flush(&self) {
        self.db.flush()
    }
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

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

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

#[derive(Deserialize, Serialize)]
pub(crate) struct InstanceCfg {
    pub(crate) prefix: PrefixBytes,
    pub(crate) item_cnt: u64,
    pub(crate) area_idx: usize,
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

pub(crate) struct SimpleVisitor;

impl<'de> de::Visitor<'de> for SimpleVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Fatal !!")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> CoreResult<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v.to_vec())
    }
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////

pub(crate) fn compute_sig(ivec: &[&[u8]]) -> Vec<u8> {
    let mut hasher = Sha3_256::new();
    for bytes in ivec {
        hasher.update(bytes);
    }
    hasher.finalize().as_slice().to_vec()
}

//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
