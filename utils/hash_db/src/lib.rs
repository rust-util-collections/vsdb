pub use hash_db as sp_hash_db;
pub use sp_trie_db;
pub use vsdb;

use hash_db::{AsHashDB, HashDB, HashDBRef, Hasher as KeyHasher, Prefix};
use ruc::*;
use serde::{Deserialize, Serialize};
use sp_trie::{
    cache::{CacheSize, SharedTrieCache},
    NodeCodec,
};
use sp_trie_db::NodeCodec as _;
use vsdb::{basic::mapx_ord_rawkey::MapxOrdRawKey as Map, RawBytes, ValueEnDe};

pub use keccak_hasher::KeccakHasher;

const GB: usize = 1024 * 1024 * 1024;
const DEFAULT_SIZ: usize = GB;

pub type TrieBackend = VsBackend<KeccakHasher, Vec<u8>>;
type SharedCache = SharedTrieCache<KeccakHasher>;

pub trait TrieVar: AsRef<[u8]> + for<'a> From<&'a [u8]> {}

impl<T> TrieVar for T where T: AsRef<[u8]> + for<'a> From<&'a [u8]> {}

// NOTE: make it `!Clone`
pub struct VsBackend<H, T>
where
    H: KeyHasher,
    T: TrieVar,
{
    data: Map<Value<T>>,
    cache: Option<(SharedCache, usize)>,
    hashed_null_key: H::Out,
    null_node_data: T,
}

impl<H, T> VsBackend<H, T>
where
    H: KeyHasher,
    T: TrieVar,
{
    /// Create a new `VsBackend` from the default null key/data
    pub fn new(cache_size: Option<usize>) -> Self {
        let cache = cache_size.map(|mut n| {
            alt!(0 == n, n = DEFAULT_SIZ);
            (SharedCache::new(CacheSize::new(n)), n)
        });

        VsBackend {
            data: Map::new(),
            cache,
            hashed_null_key: NodeCodec::<H>::hashed_null_node(), // the initial root node
            null_node_data: [0u8].as_slice().into(),
        }
    }

    pub fn get_cache_hdr(&self) -> Option<&SharedCache> {
        self.cache.as_ref().map(|c| &c.0)
    }

    pub fn reset_cache(&mut self, size: Option<usize>) {
        if let Some(mut n) = size {
            if 0 == n {
                n = DEFAULT_SIZ;
            }
            let siz = CacheSize::new(n);
            self.cache.replace((SharedCache::new(siz), n));
        } else {
            self.cache.take();
        }
    }
}

impl<H, T> HashDB<H, T> for VsBackend<H, T>
where
    H: KeyHasher,
    T: TrieVar + Clone + Sync + Send + PartialEq + Default,
{
    fn get(&self, key: &<H as KeyHasher>::Out, prefix: Prefix) -> Option<T> {
        if key == &self.hashed_null_key {
            return Some(self.null_node_data.clone());
        }
        let key = prefixed_key::<H>(key, prefix);
        match self.data.get(key) {
            Some(Value { v, rc }) if rc > 0 => Some(v),
            _ => None,
        }
    }

    fn contains(&self, key: &<H as KeyHasher>::Out, prefix: Prefix) -> bool {
        if key == &self.hashed_null_key {
            return true;
        }
        let key = prefixed_key::<H>(key, prefix);
        matches!(self.data.get(key), Some(Value { v: _, rc }) if rc > 0)
    }

    fn emplace(&mut self, key: <H as KeyHasher>::Out, prefix: Prefix, value: T) {
        if value == self.null_node_data {
            return;
        }

        let key = prefixed_key::<H>(&key, prefix);

        if let Some(mut old) = self.data.get_mut(&key) {
            if old.rc == 0 {
                old.v = value;
                old.rc = 1;
            } else {
                old.rc += 1;
            }
            return;
        }

        self.data.insert(key, &Value { v: value, rc: 1 });
    }

    fn insert(&mut self, prefix: Prefix, value: &[u8]) -> <H as KeyHasher>::Out {
        let v = T::from(value);
        if v == self.null_node_data {
            return self.hashed_null_key;
        }

        let key = H::hash(value);
        HashDB::emplace(self, key, prefix, v);
        key
    }

    fn remove(&mut self, key: &<H as KeyHasher>::Out, prefix: Prefix) {
        if key == &self.hashed_null_key {
            return;
        }

        let key = prefixed_key::<H>(key, prefix);
        if let Some(mut v) = self.data.get_mut(&key) {
            if v.rc > 0 {
                v.rc -= 1;
            }
        }
    }
}

impl<H, T> HashDBRef<H, T> for VsBackend<H, T>
where
    H: KeyHasher,
    T: TrieVar + Clone + Sync + Send + Default + PartialEq,
{
    fn get(&self, key: &<H as KeyHasher>::Out, prefix: Prefix) -> Option<T> {
        HashDB::get(self, key, prefix)
    }
    fn contains(&self, key: &<H as KeyHasher>::Out, prefix: Prefix) -> bool {
        HashDB::contains(self, key, prefix)
    }
}

impl<H, T> AsHashDB<H, T> for VsBackend<H, T>
where
    H: KeyHasher,
    T: TrieVar + Clone + Sync + Send + Default + PartialEq,
{
    fn as_hash_db(&self) -> &dyn HashDB<H, T> {
        self
    }
    fn as_hash_db_mut(&mut self) -> &mut dyn HashDB<H, T> {
        self
    }
}

// Derive a database key from hash value of the node (key) and the node prefix.
fn prefixed_key<H: KeyHasher>(key: &H::Out, prefix: Prefix) -> Vec<u8> {
    let mut prefixed_key = Vec::with_capacity(key.as_ref().len() + prefix.0.len() + 1);
    prefixed_key.extend_from_slice(prefix.0);
    if let Some(last) = prefix.1 {
        prefixed_key.push(last);
    }
    prefixed_key.extend_from_slice(key.as_ref());
    prefixed_key
}

struct Value<T> {
    v: T,
    rc: i32,
}

const RC_BYTES_NUM: usize = i32::to_be_bytes(0).len();

impl<T> ValueEnDe for Value<T>
where
    T: TrieVar,
{
    fn try_encode(&self) -> Result<RawBytes> {
        Ok(self.encode())
    }

    fn encode(&self) -> RawBytes {
        let vbytes = self.v.as_ref();
        let mut r = Vec::with_capacity(RC_BYTES_NUM + vbytes.len());
        r.extend_from_slice(&i32::to_be_bytes(self.rc));
        r.extend_from_slice(vbytes);
        r
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < RC_BYTES_NUM {
            return Err(eg!("invalid length"));
        }
        let rcbytes = <[u8; RC_BYTES_NUM]>::try_from(&bytes[..RC_BYTES_NUM]).unwrap();
        Ok(Self {
            v: T::from(&bytes[RC_BYTES_NUM..]),
            rc: i32::from_be_bytes(rcbytes),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
struct VsBackendSerde<T>
where
    T: TrieVar,
{
    data: Map<Value<T>>,
    cache_size: Option<usize>,

    null_node_data: Vec<u8>,
}

impl<H, T> From<VsBackendSerde<T>> for VsBackend<H, T>
where
    H: KeyHasher,
    T: TrieVar,
{
    fn from(vbs: VsBackendSerde<T>) -> Self {
        Self {
            data: vbs.data,
            cache: vbs
                .cache_size
                .map(|n| (SharedCache::new(CacheSize::new(n)), n)),
            hashed_null_key: NodeCodec::<H>::hashed_null_node(),
            null_node_data: T::from(&vbs.null_node_data),
        }
    }
}

impl<H, T> From<&VsBackend<H, T>> for VsBackendSerde<T>
where
    H: KeyHasher,
    T: TrieVar,
{
    fn from(vb: &VsBackend<H, T>) -> Self {
        Self {
            data: unsafe { vb.data.shadow() },
            cache_size: vb.cache.as_ref().map(|c| c.1),
            null_node_data: vb.null_node_data.as_ref().to_vec(),
        }
    }
}

impl<H, T> ValueEnDe for VsBackend<H, T>
where
    H: KeyHasher,
    T: TrieVar,
{
    fn try_encode(&self) -> Result<RawBytes> {
        bcs::to_bytes(&VsBackendSerde::from(self)).c(d!())
    }

    fn encode(&self) -> RawBytes {
        pnk!(self.try_encode())
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        bcs::from_bytes::<VsBackendSerde<T>>(bytes)
            .c(d!())
            .map(Self::from)
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn print_null_value() {
        use super::*;
        println!("{:?}", KeccakHasher::hash(&[]));
        println!("{:?}", KeccakHasher::hash(&[0u8][..]));
    }
}
