//!
//! # vsdb
//!

#![deny(warnings)]
#![deny(missing_docs)]
#![recursion_limit = "512"]

pub mod mapx;
pub mod mapx_oc;
pub mod mapx_raw;
pub mod vecx;

pub use mapx::Mapx;
pub use mapx_oc::{MapxOC, OrderConsistKey};
pub use mapx_raw::MapxRaw;
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

lazy_static! {
    static ref VSDB_DATA_DIR: Arc<Mutex<String>> = Arc::new(Mutex::new(gen_data_dir()));
    static ref VSDB: VsDB = pnk!(VsDB::new());
}

macro_rules! parse_int {
    ($bytes: expr, $ty: ty) => {{
        let array: [u8; size_of::<$ty>()] = $bytes.try_into().unwrap();
        <$ty>::from_be_bytes(array)
    }};
}

const PREFIX_ID_SIZ: usize = size_of::<u64>();

struct VsDB {
    root: DB,
    trees: Vec<Tree>,
    next_prefix_id: [u8; PREFIX_ID_SIZ],
}

impl VsDB {
    fn new() -> Result<Self> {
        const TREE_NUM: u64 = 4;

        let root = sled_open().c(d!())?;
        let trees = (0..TREE_NUM)
            .map(|idx| root.open_tree(idx.to_be_bytes()).c(d!()))
            .collect::<Result<Vec<_>>>()?;
        let next_prefix_id = u64::MAX.to_be_bytes();

        if root.get(next_prefix_id).c(d!())?.is_none() {
            root.insert(next_prefix_id, u64::MIN.to_be_bytes().as_slice())
                .c(d!())?;
        }

        Ok(Self {
            root,
            trees,
            next_prefix_id,
        })
    }

    fn alloc_id(&self) -> u64 {
        let incr = |id_base: Option<&[u8]>| -> Option<Vec<u8>> {
            id_base.map(|bytes| (parse_int!(bytes, u64) + 1).to_be_bytes().to_vec())
        };

        parse_int!(
            self.root
                .update_and_fetch(self.next_prefix_id, incr)
                .unwrap()
                .unwrap()
                .as_ref(),
            u64
        )
    }

    fn flush_data(&self) {
        (0..self.trees.len()).for_each(|i| {
            self.trees[i].flush().unwrap();
        });
    }

    fn clear_data(&self) {
        for i in 0..self.trees.len() {
            self.trees[i].clear().unwrap();
        }
    }

    // // Delete all TREEs except the base one
    // fn destory_trees(&self) {
    //     for i in 0..self.trees.len() {
    //         info_omit!(self.root.drop_tree(i.to_be_bytes()));
    //     }
    // }
}

fn sled_open() -> Result<DB> {
    let db = Config::new()
        .path(VSDB_DATA_DIR.lock().unwrap().clone())
        .mode(Mode::HighThroughput)
        .use_compression(true)
        .open()
        .c(d!())?;

    // avoid setting DB after it has been opened
    info_omit!(set_data_dir(gen_data_dir()));

    Ok(db)
}

fn gen_data_dir() -> String {
    // Is it necessary to be compatible with Windows OS?
    let d = env::var("VSDB_DATA_DIR")
        .or_else(|_| env::var("HOME").map(|h| format!("{}/.vsdb", h)))
        .unwrap_or_else(|_| "/tmp/.vsdb".to_owned());
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

/// Flush data to disk.
///
/// NOTE:
/// This operation may take a long long time.
pub fn flush() {
    VSDB.flush_data();
}

/// Delete all KVs and meta,
/// mostly used in testing scene.
///
/// NOTE:
/// this operation may take a very long long time
/// if a large number of KVs have been stored in this DB.
pub fn clear() {
    VSDB.clear_data();
}

//////////////////////////////////////////////////////////////////////////

#[derive(Deserialize, Serialize)]
struct MetaInfo {
    obj_id: u64,
    item_cnt: u64,
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
