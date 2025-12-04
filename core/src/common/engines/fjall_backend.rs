use crate::common::{
    Engine, PREFIX_SIZE, Pre, PreBytes, RESERVED_ID_CNT, RawKey, RawValue,
    vsdb_get_base_dir, vsdb_set_base_dir,
};
use fjall::{
    CompressionType, Database, Keyspace, KeyspaceCreateOptions, PersistMode,
    config::CompressionPolicy,
};
use parking_lot::Mutex;
use ruc::*;
use std::{
    borrow::Cow,
    fs,
    ops::{Bound, RangeBounds},
    sync::{
        LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
};

const DATA_SET_NUM: usize = 2;
const SHARD_CNT: usize = 16;

const META_KEY_MAX_KEYLEN: [u8; 1] = [u8::MAX];
const META_KEY_PREFIX_ALLOCATOR: [u8; 1] = [u8::MIN];

fn keyspace_create_options() -> KeyspaceCreateOptions {
    let mut opts = KeyspaceCreateOptions::default();

    #[cfg(feature = "compress")]
    {
        // When compress feature is enabled, use LZ4 compression for data blocks
        // L0 and L1 are uncompressed for performance, L2+ use LZ4
        opts = opts.data_block_compression_policy(CompressionPolicy::new([
            CompressionType::None,
            CompressionType::None,
            CompressionType::Lz4,
        ]));
    }

    #[cfg(not(feature = "compress"))]
    {
        opts = opts.data_block_compression_policy(CompressionPolicy::disabled());
    }

    opts
}

pub struct FjallEngine {
    meta: Keyspace,
    shards: Vec<Database>,
    shards_parts: Vec<Vec<Keyspace>>,
    prefix_allocator: PreAllocator,
    max_keylen: AtomicUsize,
}

impl FjallEngine {
    #[inline(always)]
    fn get_shard_idx(&self, prefix: PreBytes) -> usize {
        (prefix[0] as usize) % SHARD_CNT
    }

    #[inline(always)]
    fn get_part(&self, prefix: PreBytes) -> &Keyspace {
        let shard_idx = self.get_shard_idx(prefix);
        let part_idx = self.area_idx(prefix);
        &self.shards_parts[shard_idx][part_idx]
    }

    #[inline(always)]
    fn get_max_keylen(&self) -> usize {
        self.max_keylen.load(Ordering::Relaxed)
    }

    #[inline(always)]
    fn set_max_key_len(&self, len: usize) {
        self.max_keylen.store(len, Ordering::Relaxed);
        self.meta
            .insert(META_KEY_MAX_KEYLEN, len.to_be_bytes())
            .unwrap();
    }
}

impl Engine for FjallEngine {
    fn new() -> Result<Self> {
        let base_dir = vsdb_get_base_dir();
        // avoid setting again on an opened DB
        omit!(vsdb_set_base_dir(&base_dir));

        let mut shards = Vec::with_capacity(SHARD_CNT);
        let mut shards_parts = Vec::with_capacity(SHARD_CNT);

        // Ensure base dir exists
        fs::create_dir_all(&base_dir).c(d!())?;

        for i in 0..SHARD_CNT {
            let dir = base_dir.join(format!("shard_{}", i));
            let db = Database::builder(&dir).open().c(d!())?;

            let mut parts = Vec::with_capacity(DATA_SET_NUM);
            for j in 0..DATA_SET_NUM {
                let p = db
                    .keyspace(&format!("part_{}", j), keyspace_create_options)
                    .c(d!())?;
                parts.push(p);
            }
            shards.push(db);
            shards_parts.push(parts);
        }

        // Use a dedicated keyspace in shard 0 for meta
        let meta = shards[0]
            .keyspace("meta", keyspace_create_options)
            .c(d!())?;

        let (prefix_allocator, initial_value) = PreAllocator::init();

        if meta.get(META_KEY_MAX_KEYLEN).c(d!())?.is_none() {
            meta.insert(META_KEY_MAX_KEYLEN, 0_usize.to_be_bytes())
                .c(d!())?;
        }

        if meta.get(prefix_allocator.key).c(d!())?.is_none() {
            meta.insert(prefix_allocator.key, initial_value).c(d!())?;
        }

        let max_keylen = AtomicUsize::new(crate::parse_int!(
            meta.get(META_KEY_MAX_KEYLEN).unwrap().unwrap(),
            usize
        ));

        Ok(FjallEngine {
            meta,
            shards,
            shards_parts,
            prefix_allocator,
            // length of the raw key, exclude the meta prefix
            max_keylen,
        })
    }

    // 'step 1' and 'step 2' is not atomic in multi-threads scene,
    // so we use a `Mutex` lock for thread safe.
    #[allow(unused_variables)]
    fn alloc_prefix(&self) -> Pre {
        static LK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
        let x = LK.lock();

        // step 1
        let ret = crate::parse_prefix!(
            self.meta.get(self.prefix_allocator.key).unwrap().unwrap()
        );

        // step 2
        self.meta
            .insert(self.prefix_allocator.key, (1 + ret).to_be_bytes())
            .unwrap();

        ret
    }

    fn area_count(&self) -> usize {
        DATA_SET_NUM
    }

    fn flush(&self) {
        for db in &self.shards {
            db.persist(PersistMode::SyncAll).unwrap();
        }
    }

    fn iter(&self, meta_prefix: PreBytes) -> FjallIter {
        let part = self.get_part(meta_prefix);
        let inner = part
            .prefix(meta_prefix)
            .map(|guard| guard.into_inner().map(|(k, v)| (k.to_vec(), v.to_vec())));

        FjallIter {
            inner: Box::new(inner),
            prefix: meta_prefix,
        }
    }

    fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        meta_prefix: PreBytes,
        bounds: R,
    ) -> FjallIter {
        let part = self.get_part(meta_prefix);

        let mut b_lo = meta_prefix.to_vec();
        let start = match bounds.start_bound() {
            Bound::Included(lo) => {
                b_lo.extend_from_slice(lo);
                Bound::Included(b_lo.clone())
            }
            Bound::Excluded(lo) => {
                b_lo.extend_from_slice(lo);
                Bound::Excluded(b_lo.clone())
            }
            Bound::Unbounded => {
                Bound::Included(b_lo.clone()) // Start from prefix
            }
        };

        let mut b_hi = meta_prefix.to_vec();
        let end = match bounds.end_bound() {
            Bound::Included(hi) => {
                b_hi.extend_from_slice(hi);
                Bound::Included(b_hi.clone())
            }
            Bound::Excluded(hi) => {
                b_hi.extend_from_slice(hi);
                Bound::Excluded(b_hi.clone())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let inner = part
            .range((start, end))
            .map(|guard| guard.into_inner().map(|(k, v)| (k.to_vec(), v.to_vec())));

        FjallIter {
            inner: Box::new(inner),
            prefix: meta_prefix,
        }
    }

    fn get(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let part = self.get_part(meta_prefix);
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);
        part.get(k).unwrap().map(|v| v.to_vec())
    }

    fn insert(
        &self,
        meta_prefix: PreBytes,
        key: &[u8],
        value: &[u8],
    ) -> Option<RawValue> {
        let part = self.get_part(meta_prefix);
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);

        if key.len() > self.get_max_keylen() {
            self.set_max_key_len(key.len());
        }

        // Fjall insert does not return old value.
        // We must get it first to satisfy the trait.
        // This makes insert slower (read-modify-write).
        let old_v = part.get(&k).unwrap().map(|v| v.to_vec());
        part.insert(k, value).unwrap();
        old_v
    }

    fn remove(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue> {
        let part = self.get_part(meta_prefix);
        let mut k = meta_prefix.to_vec();
        k.extend_from_slice(key);

        let old_v = part.get(&k).unwrap().map(|v| v.to_vec());
        part.remove(k).unwrap();
        old_v
    }

    fn get_instance_len_hint(&self, instance_prefix: PreBytes) -> u64 {
        self.meta
            .get(instance_prefix)
            .unwrap()
            .map(|v| crate::parse_int!(v, u64))
            .unwrap_or(0)
    }

    fn set_instance_len_hint(&self, instance_prefix: PreBytes, new_len: u64) {
        self.meta
            .insert(instance_prefix, new_len.to_be_bytes())
            .unwrap();
    }
}

type FjallInnerIter =
    Box<dyn DoubleEndedIterator<Item = fjall::Result<(Vec<u8>, Vec<u8>)>>>;

pub struct FjallIter {
    inner: FjallInnerIter,
    prefix: PreBytes,
}

impl Iterator for FjallIter {
    type Item = (RawKey, RawValue);
    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok((k, v))) => {
                if !k.starts_with(&self.prefix) {
                    return None;
                }
                let mut k_vec = k;
                k_vec.drain(..PREFIX_SIZE);
                Some((k_vec, v))
            }
            Some(Err(e)) => {
                panic!("Fjall iteration error: {}", e);
            }
            None => None,
        }
    }
}

impl DoubleEndedIterator for FjallIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self.inner.next_back() {
            Some(Ok((k, v))) => {
                if !k.starts_with(&self.prefix) {
                    return None;
                }
                let mut k_vec = k;
                k_vec.drain(..PREFIX_SIZE);
                Some((k_vec, v))
            }
            Some(Err(e)) => {
                panic!("Fjall iteration error: {}", e);
            }
            None => None,
        }
    }
}

// key of the prefix allocator in the 'meta'
struct PreAllocator {
    key: [u8; 1],
}

impl PreAllocator {
    const fn init() -> (Self, PreBytes) {
        (
            Self {
                key: META_KEY_PREFIX_ALLOCATOR,
            },
            (RESERVED_ID_CNT + Pre::MIN).to_be_bytes(),
        )
    }
}
