//!
//! # Common Types and Macros
//!

use crate::{BNC_DATA_DIR, BNC_META_NAME, BNC_ROOT_DIR, DB_NUM};
use lazy_static::lazy_static;
use rocksdb::{DBCompressionType, Options, SliceTransform, DB};
use ruc::*;
use std::{
    collections::hash_map::DefaultHasher,
    convert::TryInto,
    fs,
    hash::{Hash, Hasher},
    io::{Read, Write},
    mem::size_of,
};

pub(crate) const PREFIX: &str = "____prefix____";
const IDX_KEY: [u8; size_of::<u32>()] = u32::MAX.to_le_bytes();

lazy_static! {
    pub(crate) static ref BNC: Vec<DB> =
        (0..DB_NUM).map(|i| pnk!(rocksdb_open(i))).collect();
}

#[inline(always)]
fn rocksdb_open(idx: usize) -> Result<DB> {
    let mut cfg = Options::default();
    cfg.create_if_missing(true);
    cfg.increase_parallelism(num_cpus::get() as i32);
    cfg.set_compression_type(DBCompressionType::Lz4);
    cfg.set_max_open_files(81920);
    cfg.set_allow_mmap_writes(true);
    cfg.set_allow_mmap_reads(true);
    cfg.create_missing_column_families(true);
    cfg.set_atomic_flush(true);
    cfg.set_prefix_extractor(SliceTransform::create_fixed_prefix(size_of::<u32>()));

    let db = DB::open(&cfg, crate::BNC_DATA_DIR[idx].as_str()).c(d!())?;

    if db.get(IDX_KEY).c(d!())?.is_none() {
        db.put(IDX_KEY, u32::MAX.to_le_bytes()).c(d!())?;
    }

    Ok(db)
}

#[inline(always)]
pub(crate) fn rocksdb_clear() {
    for i in 0..DB_NUM {
        BNC[i]
            .iterator(rocksdb::IteratorMode::Start)
            .for_each(|(k, _)| {
                pnk!(BNC[i].delete(k));
            });
        pnk!(BNC[i].put(IDX_KEY, u32::MAX.to_le_bytes()));
        pnk!(BNC[i].flush());
        omit!(fs::remove_dir_all(format!(
            "{}/{}",
            BNC_DATA_DIR[i].as_str(),
            BNC_META_NAME
        )));
    }
}

#[inline(always)]
pub(crate) fn meta_check(path: &str) -> Result<()> {
    let idx = hash(&path) % DB_NUM;
    let path = format!("{}/{}", BNC_ROOT_DIR.as_str(), path);
    fs::create_dir_all(&path).c(d!(path.clone()))?;

    let mut f = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(format!("{}/{}", &path, PREFIX))
        .c(d!(path))?;
    let mut buf = [0u8; size_of::<u32>()];
    let nbytes = f.read(&mut buf).c(d!())?;
    if 0 == nbytes {
        let cur_idx = BNC[idx].get(IDX_KEY).c(d!())?.c(d!())?;
        let cur_idx =
            u32::from_le_bytes(cur_idx[..size_of::<u32>()].try_into().unwrap());
        let new_idx = cur_idx.overflowing_sub(1).0.to_le_bytes();
        BNC[idx]
            .put(IDX_KEY, new_idx)
            .c(d!())
            .and_then(|_| f.write(&new_idx[..]).c(d!()))?;
        BNC[idx].flush().c(d!())?;
    } else if size_of::<u32>() != nbytes {
        return Err(eg!("Fatal !!"));
    }

    Ok(())
}

#[inline(always)]
pub(crate) fn read_prefix_bytes(path: &str) -> Result<Vec<u8>> {
    let path = format!("{}/{}", BNC_ROOT_DIR.as_str(), path);
    fs::read(&path).c(d!(path))
}

#[inline(always)]
pub(crate) fn hash<T: Hash>(t: &T) -> usize {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish() as usize
}
