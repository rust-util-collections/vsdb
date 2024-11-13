/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "rocks_backend")]
mod rocks_backend;

#[cfg(feature = "parity_backend")]
mod parity_backend;
#[cfg(feature = "sled_backend")]
mod sled_backend;
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "rocks_backend")]
pub(crate) use rocks_backend::RocksEngine as RocksDB;

#[cfg(feature = "rocks_backend")]
type EngineIter = rocks_backend::RocksIter;

#[cfg(feature = "parity_backend")]
pub(crate) use parity_backend::ParityEngine as ParityDB;

#[cfg(feature = "parity_backend")]
type EngineIter = parity_backend::ParityIter;

#[cfg(feature = "sled_backend")]
pub(crate) use sled_backend::SledEngine as SledDB;

#[cfg(feature = "sled_backend")]
type EngineIter = sled_backend::SledIter;
/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

use crate::common::{Pre, PreBytes, RawKey, RawValue, PREFIX_SIZE, VSDB};
use parking_lot::Mutex;
use ruc::*;
use serde::{de, Deserialize, Serialize};
use std::{
    borrow::Cow,
    fmt,
    mem::transmute,
    ops::{Deref, DerefMut, RangeBounds},
    result::Result as StdResult,
    sync::LazyLock,
};

static LEN_LK: LazyLock<Vec<Mutex<()>>> =
    LazyLock::new(|| (0..VSDB.db.area_count()).map(|_| Mutex::new(())).collect());

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// Low-level database interface.
pub trait Engine: Sized {
    fn new() -> Result<Self>;
    fn alloc_prefix(&self) -> Pre;
    fn area_count(&self) -> usize;

    // NOTE:
    // do NOT make the number of areas bigger than `u8::MAX - 1`
    fn area_idx(&self, meta_prefix: PreBytes) -> usize {
        meta_prefix[0] as usize % self.area_count()
    }

    fn flush(&self);

    fn iter(&self, meta_prefix: PreBytes) -> EngineIter;

    fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        meta_prefix: PreBytes,
        bounds: R,
    ) -> EngineIter;

    fn get(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue>;

    fn insert(
        &self,
        meta_prefix: PreBytes,
        key: &[u8],
        value: &[u8],
    ) -> Option<RawValue>;

    fn remove(&self, meta_prefix: PreBytes, key: &[u8]) -> Option<RawValue>;

    fn get_instance_len_hint(&self, instance_prefix: PreBytes) -> u64;

    fn set_instance_len_hint(&self, instance_prefix: PreBytes, new_len: u64);

    fn increase_instance_len_hint(&self, instance_prefix: PreBytes) {
        let x = LEN_LK[self.area_idx(instance_prefix)].lock();

        let l = self.get_instance_len_hint(instance_prefix);
        self.set_instance_len_hint(instance_prefix, l + 1);

        drop(x);
    }

    fn decrease_instance_len_hint(&self, instance_prefix: PreBytes) {
        let x = LEN_LK[self.area_idx(instance_prefix)].lock();

        let l = self.get_instance_len_hint(instance_prefix);
        self.set_instance_len_hint(instance_prefix, l.saturating_sub(1));

        drop(x);
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct Mapx {
    // the unique ID of each instance
    prefix: Prefix,
}

#[derive(Debug)]
enum Prefix {
    Recoverd(PreBytes),
    Created(LazyLock<PreBytes>),
}

impl Prefix {
    #[inline(always)]
    fn as_bytes(&self) -> &PreBytes {
        match self {
            Self::Recoverd(bytes) => bytes,
            Self::Created(lc) => LazyLock::force(lc),
        }
    }

    #[inline(always)]
    fn to_bytes(&self) -> PreBytes {
        *self.as_bytes()
    }

    fn hack_bytes(&mut self) -> PreBytes {
        match self {
            Self::Recoverd(bytes) => *bytes,
            Self::Created(lc) => {
                let b = *LazyLock::force(lc);
                *self = Self::Recoverd(b);
                b
            }
        }
    }

    #[inline(always)]
    fn from_bytes(b: PreBytes) -> Self {
        Self::Recoverd(b)
    }

    #[inline(always)]
    fn create() -> Self {
        Self::Created(LazyLock::new(|| {
            let prefix = VSDB.db.alloc_prefix();
            let prefix_bytes = prefix.to_be_bytes();
            debug_assert!(VSDB.db.iter(prefix_bytes).next().is_none());
            VSDB.db.set_instance_len_hint(prefix_bytes, 0);
            prefix_bytes
        }))
    }
}

impl Mapx {
    // # Safety
    //
    // This API breaks the semantic safety guarantees,
    // but it is safe to use in a race-free environment.
    pub(crate) unsafe fn shadow(&self) -> Self {
        Self {
            prefix: Prefix::from_bytes(self.prefix.to_bytes()),
        }
    }

    #[inline(always)]
    pub(crate) fn new() -> Self {
        Self {
            prefix: Prefix::create(),
        }
    }

    #[inline(always)]
    pub(crate) fn get(&self, key: &[u8]) -> Option<RawValue> {
        VSDB.db.get(self.prefix.to_bytes(), key)
    }

    #[inline(always)]
    pub(crate) fn get_mut(&mut self, key: &[u8]) -> Option<ValueMut> {
        let v = VSDB.db.get(self.prefix.hack_bytes(), key)?;

        Some(ValueMut {
            key: key.to_vec(),
            value: v,
            hdr: self,
        })
    }

    #[inline(always)]
    pub(crate) fn mock_value_mut(&mut self, key: RawValue, value: RawValue) -> ValueMut {
        ValueMut {
            key,
            value,
            hdr: self,
        }
    }

    #[inline(always)]
    pub(crate) fn len(&self) -> usize {
        VSDB.db.get_instance_len_hint(self.prefix.to_bytes()) as usize
    }

    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        0 == self.len()
    }

    #[inline(always)]
    pub(crate) fn iter(&self) -> MapxIter {
        MapxIter {
            db_iter: VSDB.db.iter(self.prefix.to_bytes()),
            _hdr: self,
        }
    }

    #[inline(always)]
    pub(crate) fn iter_mut(&mut self) -> MapxIterMut {
        MapxIterMut {
            db_iter: VSDB.db.iter(self.prefix.hack_bytes()),
            hdr: self,
        }
    }

    #[inline(always)]
    pub(crate) fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        bounds: R,
    ) -> MapxIter<'a> {
        MapxIter {
            db_iter: VSDB.db.range(self.prefix.to_bytes(), bounds),
            _hdr: self,
        }
    }

    #[inline(always)]
    pub(crate) fn range_mut<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a mut self,
        bounds: R,
    ) -> MapxIterMut<'a> {
        MapxIterMut {
            db_iter: VSDB.db.range(self.prefix.hack_bytes(), bounds),
            hdr: self,
        }
    }

    #[inline(always)]
    pub(crate) fn insert(&mut self, key: &[u8], value: &[u8]) -> Option<RawValue> {
        let prefix = self.prefix.hack_bytes();
        let ret = VSDB.db.insert(prefix, key, value);
        if ret.is_none() {
            VSDB.db.increase_instance_len_hint(prefix);
        }
        ret
    }

    #[inline(always)]
    pub(crate) fn remove(&mut self, key: &[u8]) -> Option<RawValue> {
        let prefix = self.prefix.hack_bytes();
        let ret = VSDB.db.remove(prefix, key);
        if ret.is_some() {
            VSDB.db.decrease_instance_len_hint(prefix);
        }
        ret
    }

    #[inline(always)]
    pub(crate) fn clear(&mut self) {
        let prefix = self.prefix.hack_bytes();
        VSDB.db.iter(prefix).for_each(|(k, _)| {
            VSDB.db.remove(prefix, &k);
        });
        VSDB.db.set_instance_len_hint(prefix, 0);
    }

    #[inline(always)]
    pub(crate) unsafe fn from_prefix_slice(s: impl AsRef<[u8]>) -> Self {
        debug_assert_eq!(s.as_ref().len(), PREFIX_SIZE);
        let mut prefix = PreBytes::default();
        prefix.copy_from_slice(s.as_ref());
        Self {
            prefix: Prefix::Recoverd(prefix),
        }
    }

    #[inline(always)]
    pub(crate) fn as_prefix_slice(&self) -> &PreBytes {
        self.prefix.as_bytes()
    }

    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.prefix.to_bytes() == other_hdr.prefix.to_bytes()
    }
}

impl Clone for Mapx {
    fn clone(&self) -> Self {
        let mut new_instance = Self::new();
        for (k, v) in self.iter() {
            new_instance.insert(&k, &v);
        }
        new_instance
    }
}

impl PartialEq for Mapx {
    fn eq(&self, other: &Mapx) -> bool {
        self.len() == other.len()
            && self
                .iter()
                .zip(other.iter())
                .all(|((k, v), (ko, vo))| k == ko && v == vo)
    }
}

impl Eq for Mapx {}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

pub(crate) struct SimpleVisitor;

impl<'de> de::Visitor<'de> for SimpleVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("bytes")
    }

    fn visit_str<E>(self, v: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v.as_bytes().to_vec())
    }

    fn visit_string<E>(self, v: String) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v.into_bytes())
    }

    fn visit_bytes<E>(self, v: &[u8]) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v.to_vec())
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v)
    }

    fn visit_seq<A>(self, mut seq: A) -> StdResult<Self::Value, A::Error>
    where
        A: de::SeqAccess<'de>,
    {
        let mut ret = vec![];
        loop {
            match seq.next_element() {
                Ok(i) => {
                    if let Some(i) = i {
                        ret.push(i);
                    } else {
                        break;
                    }
                }
                Err(e) => {
                    return Err(de::Error::custom(e));
                }
            }
        }
        Ok(ret)
    }
}

impl Serialize for Mapx {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.prefix.to_bytes())
    }
}

impl<'de> Deserialize<'de> for Mapx {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer
            .deserialize_byte_buf(SimpleVisitor)
            .map(|meta| unsafe { Self::from_prefix_slice(meta) })
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

pub struct MapxIter<'a> {
    db_iter: EngineIter,
    _hdr: &'a Mapx,
}

impl<'a> fmt::Debug for MapxIter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("MapxIter").field(&self._hdr).finish()
    }
}

impl<'a> Iterator for MapxIter<'a> {
    type Item = (RawKey, RawValue);
    fn next(&mut self) -> Option<Self::Item> {
        self.db_iter.next()
    }
}

impl<'a> DoubleEndedIterator for MapxIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.db_iter.next_back()
    }
}

pub struct MapxIterMut<'a> {
    db_iter: EngineIter,
    hdr: &'a mut Mapx,
}

impl<'a> fmt::Debug for MapxIterMut<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("MapxIterMut").field(&self.hdr).finish()
    }
}

impl<'a> Iterator for MapxIterMut<'a> {
    type Item = (RawKey, ValueIterMut<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let (k, v) = self.db_iter.next()?;

        let vmut = ValueIterMut {
            key: k.clone(),
            value: v,
            iter_mut: unsafe { transmute::<&'_ mut Self, &'a mut Self>(self) },
        };

        Some((k, vmut))
    }
}

impl<'a> DoubleEndedIterator for MapxIterMut<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let (k, v) = self.db_iter.next_back()?;

        let vmut = ValueIterMut {
            key: k.clone(),
            value: v,
            iter_mut: unsafe { transmute::<&'_ mut Self, &'a mut Self>(self) },
        };

        Some((k, vmut))
    }
}

#[derive(Debug)]
pub struct ValueIterMut<'a> {
    key: RawKey,
    value: RawValue,
    iter_mut: &'a mut MapxIterMut<'a>,
}

impl<'a> Drop for ValueIterMut<'a> {
    fn drop(&mut self) {
        self.iter_mut.hdr.insert(&self.key[..], &self.value[..]);
    }
}

impl<'a> Deref for ValueIterMut<'a> {
    type Target = RawValue;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a> DerefMut for ValueIterMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ValueMut<'a> {
    key: RawKey,
    value: RawValue,
    hdr: &'a mut Mapx,
}

impl<'a> Drop for ValueMut<'a> {
    fn drop(&mut self) {
        self.hdr.insert(&self.key[..], &self.value[..]);
    }
}

impl<'a> Deref for ValueMut<'a> {
    type Target = RawValue;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a> DerefMut for ValueMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
