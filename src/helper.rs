//!
//! # Common Types and Macros
//!

use lazy_static::lazy_static;
use rocksdb::{DBCompressionType, Options, SliceTransform, DB};
use ruc::*;
use std::{
    convert::TryInto,
    env, fs,
    io::{Read, Write},
    mem::size_of,
};

pub(crate) const CNTER: &str = "____cnter____";
pub(crate) const PREFIX: &str = "____prefix____";
const IDX_KEY: [u8; size_of::<u32>()] = u32::MAX.to_le_bytes();

lazy_static! {
    pub(crate) static ref BNC: DB = pnk!(rocksdb_open());

    /// Is it necessary to be compatible with Windows OS?
    pub static ref DATA_DIR: String = env::var("BNC_DATA_DIR")
        .unwrap_or_else(|_|"/tmp/.bnc".to_owned());
}

#[inline(always)]
fn rocksdb_open() -> Result<DB> {
    let mut cfg = Options::default();
    cfg.create_if_missing(true);
    cfg.set_compression_type(DBCompressionType::Lz4);
    cfg.set_max_open_files(81920);
    cfg.set_allow_mmap_writes(true);
    cfg.set_allow_mmap_reads(true);
    cfg.create_missing_column_families(true);
    cfg.set_atomic_flush(true);
    cfg.set_prefix_extractor(SliceTransform::create_fixed_prefix(size_of::<u32>()));

    let db = DB::open(&cfg, &*DATA_DIR).c(d!())?;

    if db.get(IDX_KEY).c(d!())?.is_none() {
        db.put(IDX_KEY, u32::MAX.to_le_bytes()).c(d!())?;
    }

    Ok(db)
}

#[inline(always)]
pub(crate) fn meta_check(path: &str) -> Result<()> {
    fs::create_dir_all(path).c(d!())?;

    let mut f = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(format!("{}/{}", path, PREFIX))
        .c(d!())?;
    let mut buf = [0u8; size_of::<u32>()];
    let nbytes = f.read(&mut buf).c(d!())?;
    if 0 == nbytes {
        let cur_idx = BNC.get(IDX_KEY).c(d!())?.c(d!())?;
        let cur_idx =
            u32::from_le_bytes(cur_idx[..size_of::<u32>()].try_into().unwrap());
        let new_idx = cur_idx.overflowing_sub(1).0.to_le_bytes();
        BNC.put(IDX_KEY, new_idx)
            .c(d!())
            .and_then(|_| f.write(&new_idx[..]).c(d!()))?;
        BNC.flush().c(d!())?;
    } else if size_of::<u32>() != nbytes {
        return Err(eg!("Fatal !!"));
    }

    Ok(())
}

#[inline(always)]
pub(crate) fn read_prefix_bytes(path: &str) -> Result<Vec<u8>> {
    fs::read(path).c(d!(path.to_owned()))
}

#[inline(always)]
pub(crate) fn read_db_len(path: &str) -> Result<usize> {
    fs::read(path).c(d!(path.to_owned())).map(|bytes| {
        usize::from_le_bytes(bytes[..size_of::<usize>()].try_into().unwrap())
    })
}

#[inline(always)]
pub(crate) fn write_db_len(path: &str, len: usize) -> Result<()> {
    fs::write(path, usize::to_le_bytes(len)).c(d!(path.to_owned()))
}
