//!
//! # vsdb
//!

#![deny(warnings)]
#![deny(missing_docs)]

pub mod mapx;
pub mod mapx_oc;
pub mod vecx;

pub use mapx::Mapx;
pub use mapx_oc::{MapxOC, OrderConsistKey};
pub use vecx::Vecx;

use {
    core::{fmt, result::Result as CoreResult},
    lazy_static::lazy_static,
    ruc::*,
    serde::{de, Deserialize, Serialize},
    sled::{Config, Db as DB, Mode, Tree},
    std::{
        env, fs,
        mem::size_of,
        sync::atomic::{AtomicBool, Ordering},
        sync::{Arc, Mutex},
    },
};

//////////////////////////////////////////////////////////////////////////

const TREE_NUM: usize = 4;
const ID_KEY: [u8; size_of::<u32>()] = u32::MAX.to_be_bytes();
static DATA_DIR: String = String::new();

lazy_static! {
    static ref VSDB_DATA_DIR: Arc<Mutex<String>> = Arc::new(Mutex::new(gen_data_dir()));
    static ref ROOT_DB: DB = pnk!(sled_open());
    static ref VSDB: Vec<Tree> =
        (0..TREE_NUM).map(|i| pnk!(sled_open_tree(i))).collect();
}

macro_rules! parse_int {
    ($bytes: expr, $ty: ty) => {{
        let array: [u8; size_of::<$ty>()] = $bytes.try_into().unwrap();
        <$ty>::from_be_bytes(array)
    }};
}

#[inline(always)]
fn sled_open() -> Result<DB> {
    let db = Config::new()
        .path(VSDB_DATA_DIR.lock().unwrap().clone())
        .mode(Mode::HighThroughput)
        .use_compression(true)
        .open()
        .c(d!())?;

    if db.get(ID_KEY).c(d!())?.is_none() {
        db.insert(ID_KEY, usize::MIN.to_be_bytes().as_slice())
            .c(d!())?;
    }

    // avoid setting DB after it has been opened
    info_omit!(set_data_dir(gen_data_dir()));

    Ok(db)
}

#[inline(always)]
fn sled_open_tree(idx: usize) -> Result<Tree> {
    ROOT_DB.open_tree(idx.to_string()).c(d!())
}

fn alloc_id() -> usize {
    let incr = |id_base: Option<&[u8]>| -> Option<Vec<u8>> {
        id_base.map(|bytes| (parse_int!(bytes, usize) + 1).to_be_bytes().to_vec())
    };

    parse_int!(
        ROOT_DB
            .update_and_fetch(ID_KEY, incr)
            .unwrap()
            .unwrap()
            .as_ref(),
        usize
    )
}

#[inline(always)]
fn gen_data_dir() -> String {
    let d = if DATA_DIR.is_empty() {
        // Is it necessary to be compatible with Windows OS?
        env::var("VSDB_DATA_DIR")
            .or_else(|_| env::var("HOME").map(|h| format!("{}/.vsdb", h)))
            .unwrap_or_else(|_| "/tmp/.vsdb".to_owned())
    } else {
        DATA_DIR.clone()
    };
    fs::create_dir_all(&d).unwrap();
    d
}

/// Set ${VSDB_DATA_DIR} manually
pub fn set_data_dir(dir: String) -> Result<()> {
    lazy_static! {
        static ref HAS_INITED: AtomicBool = AtomicBool::new(false);
    }

    if HAS_INITED.swap(true, Ordering::Relaxed) {
        Err(eg!("VSDB has been initialized !!"))
    } else {
        *VSDB_DATA_DIR.lock().unwrap() = dir;
        Ok(())
    }
}

/// Flush data to disk
#[inline(always)]
pub fn flush_data() {
    (0..TREE_NUM).for_each(|i| {
        VSDB[i].flush().unwrap();
    });
}

/// Delete all KVs and meta
pub fn reset_db() {
    for i in 0..TREE_NUM {
        VSDB[i].iter().keys().map(|k| k.unwrap()).for_each(|k| {
            pnk!(VSDB[i].remove(k));
        });
        pnk!(VSDB[i].flush());
    }
}

//////////////////////////////////////////////////////////////////////////

#[derive(Deserialize, Serialize)]
struct MetaInfo {
    obj_id: usize,
    item_cnt: usize,
    tree_idx: usize,
}

//////////////////////////////////////////////////////////////////////////

struct SimpleVisitor;

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
