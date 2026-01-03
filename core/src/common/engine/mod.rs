/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

mod rocksdb;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub(crate) use self::rocksdb::RocksDB;

type DbIter = self::rocksdb::RocksIter;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

use crate::common::{PREFIX_SIZE, PreBytes, RawKey, RawValue, VSDB};
use ruc::*;
use serde::{Deserialize, Serialize, de};
use std::{
    borrow::Cow,
    fmt,
    marker::PhantomData,
    ops::{Bound, Deref, DerefMut, RangeBounds},
    result::Result as StdResult,
    sync::LazyLock,
};

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// Trait for batch write operations
pub trait BatchTrait {
    fn insert(&mut self, key: &[u8], value: &[u8]);
    fn remove(&mut self, key: &[u8]);
    fn commit(&mut self) -> Result<()>;
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

    /// Force the prefix to be materialized (if lazily created)
    /// and return the bytes. Converts `Created` → `Recoverd`
    /// so subsequent calls avoid re-forcing the LazyLock.
    fn materialize(&mut self) -> PreBytes {
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
            prefix_bytes
        }))
    }
}

impl Mapx {
    // # Safety
    //
    // This API breaks Rust's semantic safety guarantees.
    // It creates a second handle to the same underlying prefix,
    // allowing two `&mut Mapx` references to coexist. This
    // bypasses the borrow checker's exclusivity guarantee.
    //
    // Callers MUST ensure:
    // - No concurrent reads and writes to the same key.
    // - No concurrent iteration and mutation.
    // - Essentially, the caller must uphold single-writer semantics
    //   externally.
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
    pub(crate) fn get_mut(&mut self, key: &[u8]) -> Option<ValueMut<'_>> {
        let v = VSDB.db.get(self.prefix.materialize(), key)?;

        Some(ValueMut {
            key: key.to_vec(),
            value: v,
            dirty: false,
            hdr: self,
        })
    }

    #[inline(always)]
    pub(crate) fn mock_value_mut(
        &mut self,
        key: RawValue,
        value: RawValue,
    ) -> ValueMut<'_> {
        ValueMut {
            key,
            value,
            dirty: true,
            hdr: self,
        }
    }

    #[inline(always)]
    pub(crate) fn iter(&self) -> MapxIter<'_> {
        MapxIter {
            db_iter: VSDB.db.iter(self.prefix.to_bytes()),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn iter_mut(&mut self) -> MapxIterMut<'_> {
        MapxIterMut {
            db_iter: VSDB.db.iter(self.prefix.materialize()),
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
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn range_detached<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &self,
        bounds: R,
    ) -> MapxIter<'a> {
        MapxIter {
            db_iter: VSDB.db.range(self.prefix.to_bytes(), bounds),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn range_mut<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a mut self,
        bounds: R,
    ) -> MapxIterMut<'a> {
        MapxIterMut {
            db_iter: VSDB.db.range(self.prefix.materialize(), bounds),
            hdr: self,
        }
    }

    #[inline(always)]
    pub(crate) fn insert(&mut self, key: &[u8], value: &[u8]) {
        let prefix = self.prefix.materialize();
        VSDB.db.insert(prefix, key, value);
    }

    #[inline(always)]
    pub(crate) fn remove(&mut self, key: &[u8]) {
        let prefix = self.prefix.materialize();
        VSDB.db.remove(prefix, key);
    }

    #[inline(always)]
    pub(crate) fn batch_begin(&mut self) -> Box<dyn BatchTrait + '_> {
        let prefix = self.prefix.materialize();
        VSDB.db.batch_begin(prefix)
    }

    #[inline(always)]
    pub(crate) fn clear(&mut self) {
        // Avoid collecting all keys into memory at once.
        // Instead, delete in chunks using repeated range scans.
        //
        // Important: we do not delete while holding an iterator alive.
        // Each loop creates a fresh iterator starting strictly after `last_key`.
        //
        // NOTE: This operation is NOT atomic. Concurrent readers (e.g.
        // via `shadow()`) may observe a partially-cleared state.
        const CLEAR_CHUNK: usize = 4096;

        let prefix = self.prefix.materialize();
        let mut last_key: Option<RawKey> = None;

        loop {
            let mut it = match &last_key {
                None => VSDB.db.iter(prefix),
                Some(k) => VSDB.db.range(
                    prefix,
                    (Bound::Excluded(Cow::Owned(k.clone())), Bound::Unbounded),
                ),
            };

            let mut keys = Vec::with_capacity(CLEAR_CHUNK);
            for _ in 0..CLEAR_CHUNK {
                let Some((k, _)) = it.next() else {
                    break;
                };
                last_key = Some(k.clone());
                keys.push(k);
            }

            // Drop the iterator before mutating the DB to avoid
            // holding a read snapshot across the batch delete.
            drop(it);

            if keys.is_empty() {
                break;
            }

            let mut batch = VSDB.db.batch_begin(prefix);
            for k in keys.iter() {
                batch.remove(k);
            }
            batch.commit().unwrap();
        }
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
        {
            let mut batch = new_instance.batch_begin();
            for (k, v) in self.iter() {
                batch.insert(&k, &v);
            }
            batch.commit().unwrap();
        }
        new_instance
    }
}

impl PartialEq for Mapx {
    fn eq(&self, other: &Mapx) -> bool {
        // Short-circuit: if both point to the same prefix, they are identical
        if self.prefix.to_bytes() == other.prefix.to_bytes() {
            return true;
        }

        // Compare all key-value pairs
        let mut self_iter = self.iter();
        let mut other_iter = other.iter();

        loop {
            match (self_iter.next(), other_iter.next()) {
                (Some((k1, v1)), Some((k2, v2))) => {
                    if k1 != k2 || v1 != v2 {
                        return false;
                    }
                }
                (None, None) => return true,
                _ => return false,
            }
        }
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
    db_iter: DbIter,
    _marker: PhantomData<&'a ()>,
}

impl fmt::Debug for MapxIter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("MapxIter").finish()
    }
}

impl Iterator for MapxIter<'_> {
    type Item = (RawKey, RawValue);
    fn next(&mut self) -> Option<Self::Item> {
        self.db_iter.next()
    }
}

impl DoubleEndedIterator for MapxIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.db_iter.next_back()
    }
}

pub struct MapxIterMut<'a> {
    db_iter: DbIter,
    hdr: &'a mut Mapx,
}

impl fmt::Debug for MapxIterMut<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("MapxIterMut").field(&self.hdr).finish()
    }
}

impl<'a> Iterator for MapxIterMut<'a> {
    type Item = (RawKey, ValueIterMut<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let (k, v) = self.db_iter.next()?;

        let vmut = ValueIterMut {
            prefix: self.hdr.prefix.to_bytes(),
            key: k.clone(),
            value: v,
            dirty: false,
            _marker: PhantomData,
        };

        Some((k, vmut))
    }
}

impl<'a> DoubleEndedIterator for MapxIterMut<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let (k, v) = self.db_iter.next_back()?;

        let vmut = ValueIterMut {
            prefix: self.hdr.prefix.to_bytes(),
            key: k.clone(),
            value: v,
            dirty: false,
            _marker: PhantomData,
        };

        Some((k, vmut))
    }
}

#[derive(Debug)]
pub struct ValueIterMut<'a> {
    prefix: PreBytes,
    key: RawKey,
    value: RawValue,
    dirty: bool,
    _marker: PhantomData<&'a mut ()>,
}

impl Drop for ValueIterMut<'_> {
    fn drop(&mut self) {
        if self.dirty {
            VSDB.db.insert(self.prefix, &self.key, &self.value);
        }
    }
}

impl Deref for ValueIterMut<'_> {
    type Target = RawValue;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for ValueIterMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.value
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ValueMut<'a> {
    key: RawKey,
    value: RawValue,
    dirty: bool,
    hdr: &'a mut Mapx,
}

impl Drop for ValueMut<'_> {
    fn drop(&mut self) {
        if self.dirty {
            self.hdr.insert(&self.key[..], &self.value[..]);
        }
    }
}

impl Deref for ValueMut<'_> {
    type Target = RawValue;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for ValueMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.value
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
