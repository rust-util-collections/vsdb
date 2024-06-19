#![doc = include_str!("../README.md")]
#![deny(warnings)]
#![cfg_attr(test, warn(warnings))]

pub use hash_db as sp_hash_db;
pub use vsdb;

use hash_db::{AsHashDB, HashDB, HashDBRef, Hasher as KeyHasher, Prefix};
use ruc::*;
use serde::{Deserialize, Serialize};
use vsdb::{DagMapRaw, DagMapRawKey as Map, Orphan, RawBytes, ValueEnDe};

pub use keccak_hasher::KeccakHasher;

pub type TrieBackend = VsBackend<KeccakHasher, Vec<u8>>;

pub trait TrieVar: Clone + AsRef<[u8]> + for<'a> From<&'a [u8]> {}

impl<T> TrieVar for T where T: Clone + AsRef<[u8]> + for<'a> From<&'a [u8]> {}

// NOTE: make it `!Clone`
pub struct VsBackend<H, T>
where
    H: KeyHasher,
    T: TrieVar,
{
    data: Map<Value<T>>,
    hashed_null_key: H::Out,
    null_node_data: T,
}

impl<H, T> VsBackend<H, T>
where
    H: KeyHasher,
    T: TrieVar,
{
    /// Create a new `VsBackend` from the default null key/data
    pub fn new(raw_parent: &mut Orphan<Option<DagMapRaw>>) -> Result<Self> {
        Ok(VsBackend {
            data: Map::new(raw_parent).c(d!())?,
            hashed_null_key: Self::hashed_null_node(),
            null_node_data: [0u8].as_slice().into(),
        })
    }

    // The initial root node
    fn hashed_null_node() -> H::Out {
        H::hash(&[0])
    }

    /// # Safety
    ///
    /// This API breaks the semantic safety guarantees,
    /// but it is safe to use in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        Self {
            data: self.data.shadow(),
            hashed_null_key: self.hashed_null_key,
            null_node_data: self.null_node_data.clone(),
        }
    }

    /// # Safety
    ///
    /// This API breaks the semantic safety guarantees,
    /// but it is safe to use in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow_backend(&self) -> Map<Value<T>> {
        self.shadow().data
    }

    #[inline(always)]
    pub fn is_dead(&self) -> bool {
        self.data.is_dead()
    }

    #[inline(always)]
    pub fn no_children(&self) -> bool {
        self.data.no_children()
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.data.destroy();
    }

    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.data.is_the_same_instance(&other_hdr.data)
    }

    /// Return a new backend instance
    #[inline(always)]
    pub fn prune(self) -> Result<Self> {
        let data = self.data.prune().c(d!())?;
        Ok(Self {
            data,
            hashed_null_key: Self::hashed_null_node(),
            null_node_data: [0u8].as_slice().into(),
        })
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

pub struct Value<T> {
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
            hashed_null_key: Self::hashed_null_node(),
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
        msgpack::to_vec(&VsBackendSerde::from(self)).c(d!())
    }

    fn encode(&self) -> RawBytes {
        pnk!(self.try_encode())
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        msgpack::from_slice::<VsBackendSerde<T>>(bytes)
            .c(d!())
            .map(Self::from)
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn hash_db_print_null_value() {
        use super::*;
        println!("{:?}", KeccakHasher::hash(&[]));
        println!("{:?}", KeccakHasher::hash(&[0u8][..]));
    }
}
