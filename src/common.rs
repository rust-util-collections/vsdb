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
    static ref VSDB_BASE_DIR: Arc<Mutex<String>> = Arc::new(Mutex::new(gen_data_dir()));
    pub(crate) static ref VSDB: VsDB = pnk!(VsDB::new());
}

macro_rules! parse_int {
    ($bytes: expr, $ty: ty) => {{
        let array: [u8; size_of::<$ty>()] = $bytes.try_into().unwrap();
        <$ty>::from_be_bytes(array)
    }};
    ($bytes: expr) => {
        parse_int!($bytes, Prefix)
    };
}

pub(crate) type Prefix = u64;
pub(crate) const PREFIX_SIZ: usize = size_of::<Prefix>();

pub(crate) struct VsDB {
    meta: DB,
    pub(crate) data_set: Vec<Tree>,
    // key of the prefix allocator in the 'meta'
    prefix_allocator: [u8; 1],
}

impl VsDB {
    fn new() -> Result<Self> {
        const DATA_SET_NUM: u64 = 4;
        const PREFIX_ALLOCATOR: [u8; 1] = 0_u8.to_be_bytes();

        let meta = sled_open().c(d!())?;

        let data_set = (0..DATA_SET_NUM)
            .map(|idx| meta.open_tree(idx.to_be_bytes()).c(d!()))
            .collect::<Result<Vec<_>>>()?;

        if meta.get(PREFIX_ALLOCATOR).c(d!())?.is_none() {
            meta.insert(PREFIX_ALLOCATOR, Prefix::MIN.to_be_bytes().as_slice())
                .c(d!())?;
        }

        Ok(VsDB {
            meta,
            data_set,
            prefix_allocator: PREFIX_ALLOCATOR,
        })
    }

    pub(crate) fn alloc_prefix(&self) -> Prefix {
        let incr = |id_base: Option<&[u8]>| -> Option<Vec<u8>> {
            id_base.map(|bytes| (parse_int!(bytes) + 1).to_be_bytes().to_vec())
        };

        parse_int!(self
            .meta
            .update_and_fetch(self.prefix_allocator, incr)
            .unwrap()
            .unwrap()
            .as_ref())
    }

    fn flush_data(&self) {
        (0..self.data_set.len()).for_each(|i| {
            self.data_set[i].flush().unwrap();
        });
    }
}

fn sled_open() -> Result<DB> {
    let db = Config::new()
        .path(VSDB_BASE_DIR.lock().unwrap().clone())
        .mode(Mode::HighThroughput)
        .use_compression(true)
        .open()
        .c(d!())?;

    // avoid setting DB after it has been opened
    info_omit!(vsdb_set_base_dir(gen_data_dir()));

    Ok(db)
}

fn gen_data_dir() -> String {
    // Is it necessary to be compatible with Windows OS?
    let d = env::var("VSDB_BASE_DIR")
        .or_else(|_| env::var("HOME").map(|h| format!("{}/.vsdb", h)))
        .unwrap_or_else(|_| "/tmp/.vsdb".to_owned());
    fs::create_dir_all(&d).unwrap();
    d
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

/// Flush data to disk.
///
/// NOTE:
/// This operation may take a long long time.
pub fn vsdb_flush() {
    VSDB.flush_data();
}

//////////////////////////////////////////////////////////////////////////

#[derive(Deserialize, Serialize)]
pub(crate) struct InstanceCfg {
    pub(crate) prefix: Vec<u8>,
    pub(crate) item_cnt: u64,
    pub(crate) data_set_idx: usize,
}

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
