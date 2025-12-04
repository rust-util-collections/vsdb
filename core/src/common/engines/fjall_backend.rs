use crate::common::{
    Engine, GB, PREFIX_SIZE, Pre, PreBytes, RESERVED_ID_CNT, RawKey, RawValue,
    vsdb_get_base_dir, vsdb_set_base_dir,
};
use fjall::{CompressionType, Config, Keyspace, Partition, PartitionCreateOptions};
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

pub struct FjallEngine {
    meta: Partition,
    shards: Vec<Keyspace>,
    shards_parts: Vec<Vec<Partition>>,
    prefix_allocator: PreAllocator,
    max_keylen: AtomicUsize,
}

impl FjallEngine {
    #[inline(always)]
    fn get_shard_idx(&self, prefix: PreBytes) -> usize {
        (prefix[0] as usize) % SHARD_CNT
    }

    #[inline(always)]
    fn get_part(&self, prefix: PreBytes) -> &Partition {
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

        let total_mem_budget = if cfg!(target_os = "linux") {
            let memsiz = fs::read_to_string("/proc/meminfo")
                .c(d!())?
                .lines()
                .find(|l| l.contains("MemAvailable"))
                .c(d!())?
                .replace(|ch: char| !ch.is_numeric(), "")
                .parse::<u64>()
                .c(d!())?
                * 1024;
            alt!((16 * GB) < memsiz, memsiz / 4, GB)
        } else {
            GB
        };

        // NOTE:
        // The current `get_shard_idx` implementation uses the most significant byte (MSB)
        // of the Big-Endian prefix. Since `alloc_prefix` increments a counter sequentially
        // starting from a small number, the MSB will remain 0 for practically forever
        // (until 2^56 collections are created).
        // This means effectively ONLY Shard 0 is used.
        // If we divide memory by SHARD_CNT (16), we starve the only active shard.
        // Therefore, we oversubscribe memory significantly, assuming that in practice
        // only a few shards (likely just one) will ever be active.
        // We set the budget per shard to half of the total available budget.
        let per_shard_budget = total_mem_budget / 2;
        let write_buffer_size = per_shard_budget / 4;
        let cache_size = per_shard_budget - write_buffer_size;

        for i in 0..SHARD_CNT {
            let dir = base_dir.join(format!("shard_{}", i));
            let ks = Config::new(dir)
                .max_write_buffer_size(write_buffer_size)
                .cache_size(cache_size)
                .open()
                .c(d!())?;

            let mut parts = Vec::with_capacity(DATA_SET_NUM);
            for j in 0..DATA_SET_NUM {
                let mut opts = PartitionCreateOptions::default();

                #[cfg(feature = "compress")]
                {
                    opts = opts.compression(CompressionType::Lz4);
                }

                #[cfg(not(feature = "compress"))]
                {
                    opts = opts.compression(CompressionType::None);
                }

                let p = ks.open_partition(&format!("part_{}", j), opts).c(d!())?;
                parts.push(p);
            }
            shards.push(ks);
            shards_parts.push(parts);
        }

        // Use a dedicated partition in shard 0 for meta
        let mut meta_opts = PartitionCreateOptions::default();

        #[cfg(feature = "compress")]
        {
            meta_opts = meta_opts.compression(CompressionType::Lz4);
        }

        #[cfg(not(feature = "compress"))]
        {
            meta_opts = meta_opts.compression(CompressionType::None);
        }

        let meta = shards[0].open_partition("meta", meta_opts).c(d!())?;

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
        for ks in &self.shards {
            // ks.persist(fjall::PersistMode::SyncAll).unwrap();
            ks.persist(fjall::PersistMode::Buffer).unwrap();
        }
    }

    fn iter(&self, meta_prefix: PreBytes) -> FjallIter {
        let part = self.get_part(meta_prefix);
        let inner = part
            .prefix(meta_prefix)
            .map(|res| res.map(|(k, v)| (k.to_vec(), v.to_vec())));

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
            .map(|res| res.map(|(k, v)| (k.to_vec(), v.to_vec())));

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
