//!
//! A `BTreeMap`-like structure but storing data in disk.
//!
//! NOTE:
//! - Keys will **NOT** be encoded in this structure, but values will be
//!     - Values will be encoded by some `serde`-like methods
//! - It's your duty to ensure that the encoded key keeps a same order with the original key
//!
//! # Examples
//!
//! ```
//! use vsdb::MapxOrdRawKey;
//!
//! let mut l = MapxOrdRawKey::new();
//!
//! l.insert_ref(&[1], &0);
//! l.insert(vec![1].into_boxed_slice(), 0);
//! l.insert_ref(&[2], &0);
//!
//! l.iter().for_each(|(_, v)| {
//!     assert_eq!(v, 0);
//! });
//!
//! l.remove(&[2]);
//! assert_eq!(l.len(), 1);
//!
//! l.clear();
//! assert_eq!(l.len(), 0);
//! ```
//!

#[cfg(test)]
mod test;

use crate::{
    basic::mapx_raw::{MapxRaw, MapxRawIter},
    common::{
        ende::{SimpleVisitor, ValueEnDe},
        InstanceCfg, RawKey,
    },
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    mem::ManuallyDrop,
    ops::{Bound, Deref, DerefMut, RangeBounds},
    result::Result as StdResult,
};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `BTreeMap<_, _>`.
#[derive(PartialEq, Eq, Debug)]
pub struct MapxOrdRawKey<V>
where
    V: ValueEnDe,
{
    inner: MapxRaw,
    pd: PhantomData<V>,
}

impl<V> From<InstanceCfg> for MapxOrdRawKey<V>
where
    V: ValueEnDe,
{
    fn from(cfg: InstanceCfg) -> Self {
        Self {
            inner: MapxRaw::from(cfg),
            pd: PhantomData,
        }
    }
}

impl<V> Default for MapxOrdRawKey<V>
where
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<V> MapxOrdRawKey<V>
where
    V: ValueEnDe,
{
    /// Create an instance.
    #[inline(always)]
    pub fn new() -> Self {
        MapxOrdRawKey {
            inner: MapxRaw::new(),
            pd: PhantomData,
        }
    }

    // Get the database storage path
    pub(crate) fn get_instance_cfg(&self) -> InstanceCfg {
        self.inner.get_instance_cfg()
    }

    /// Imitate the behavior of 'BTreeMap<_>.get(...)'
    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Option<V> {
        self.inner
            .get(key)
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    /// Get the closest smaller value, include itself.
    #[inline(always)]
    pub fn get_le(&self, key: &[u8]) -> Option<(RawKey, V)> {
        self.inner
            .get_le(key)
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }

    /// Get the closest larger value, include itself.
    #[inline(always)]
    pub fn get_ge(&self, key: &[u8]) -> Option<(RawKey, V)> {
        self.inner
            .get_ge(key)
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }

    /// Imitate the behavior of 'BTreeMap<_>.get_mut(...)'
    #[inline(always)]
    pub fn get_mut(&mut self, key: &[u8]) -> Option<ValueMut<'_, V>> {
        self.inner.get(key).map(|v| {
            ValueMut::new(
                self,
                key.to_vec().into_boxed_slice(),
                <V as ValueEnDe>::decode(&v).unwrap(),
            )
        })
    }

    /// Imitate the behavior of 'BTreeMap<_>.len()'.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// A helper func
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Imitate the behavior of 'BTreeMap<_>.insert(...)'.
    #[inline(always)]
    pub fn insert(&mut self, key: RawKey, value: V) -> Option<V> {
        self.insert_ref(&key, &value)
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn insert_ref(&mut self, key: &[u8], value: &V) -> Option<V> {
        self.inner
            .insert(key, &value.encode())
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    /// Similar with `insert`, but ignore the old value.
    #[inline(always)]
    pub fn set_value(&mut self, key: RawKey, value: V) {
        self.set_value_ref(&key, &value);
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn set_value_ref(&mut self, key: &[u8], value: &V) {
        self.inner.insert(key, &value.encode());
    }

    /// Imitate the behavior of '.entry(...).or_insert(...)'
    #[inline(always)]
    pub fn entry(&mut self, key: RawKey) -> Entry<'_, V> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn entry_ref<'a>(&'a mut self, key: &'a [u8]) -> EntryRef<'a, V> {
        EntryRef { key, hdr: self }
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub fn iter(&self) -> MapxOrdRawKeyIter<V> {
        MapxOrdRawKeyIter {
            iter: self.inner.iter(),
            pd: PhantomData,
        }
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range<R: RangeBounds<RawKey>>(&self, bounds: R) -> MapxOrdRawKeyIter<V> {
        let start = match bounds.start_bound() {
            Bound::Included(s) => Bound::Included(&s[..]),
            Bound::Excluded(s) => Bound::Excluded(&s[..]),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end = match bounds.end_bound() {
            Bound::Included(e) => Bound::Included(&e[..]),
            Bound::Excluded(e) => Bound::Excluded(&e[..]),
            Bound::Unbounded => Bound::Unbounded,
        };

        self.range_ref((start, end))
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range_ref<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdRawKeyIter<V> {
        MapxOrdRawKeyIter {
            iter: self.inner.range(bounds),
            pd: PhantomData,
        }
    }

    /// First item
    #[inline(always)]
    pub fn first(&self) -> Option<(RawKey, V)> {
        self.iter().next()
    }

    /// Last item
    #[inline(always)]
    pub fn last(&self) -> Option<(RawKey, V)> {
        self.iter().next_back()
    }

    /// Check if a key is exists.
    #[inline(always)]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.inner.contains_key(key)
    }

    /// Remove a <V> from mem and disk.
    #[inline(always)]
    pub fn remove(&mut self, key: &[u8]) -> Option<V> {
        self.inner
            .remove(key)
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    /// Remove a <V> from mem and disk.
    #[inline(always)]
    pub fn unset_value(&mut self, key: &[u8]) {
        self.inner.remove(key);
    }

    /// Clear all data.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

/// Returned by `<MapxOrdRawKey>.get_mut(...)`
#[derive(Debug)]
pub struct ValueMut<'a, V>
where
    V: ValueEnDe,
{
    hdr: &'a mut MapxOrdRawKey<V>,
    key: ManuallyDrop<RawKey>,
    value: ManuallyDrop<V>,
}

impl<'a, V> ValueMut<'a, V>
where
    V: ValueEnDe,
{
    pub(crate) fn new(hdr: &'a mut MapxOrdRawKey<V>, key: RawKey, value: V) -> Self {
        ValueMut {
            hdr,
            key: ManuallyDrop::new(key),
            value: ManuallyDrop::new(value),
        }
    }
}

/// NOTE: Very Important !!!
impl<'a, V> Drop for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    fn drop(&mut self) {
        // This operation is safe within a `drop()`.
        // SEE: [**ManuallyDrop::take**](std::mem::ManuallyDrop::take)
        unsafe {
            self.hdr.set_value(
                ManuallyDrop::take(&mut self.key),
                ManuallyDrop::take(&mut self.value),
            );
        };
    }
}

impl<'a, V> Deref for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, V> DerefMut for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

/// Imitate the `btree_map/btree_map::Entry`.
pub struct Entry<'a, V>
where
    V: 'a + ValueEnDe,
{
    key: RawKey,
    hdr: &'a mut MapxOrdRawKey<V>,
}

impl<'a, V> Entry<'a, V>
where
    V: ValueEnDe,
{
    /// Imitate the `btree_map/btree_map::Entry.or_insert(...)`.
    pub fn or_insert(self, default: V) -> ValueMut<'a, V> {
        if !self.hdr.contains_key(&self.key) {
            self.hdr.set_value_ref(&self.key, &default);
        }
        pnk!(self.hdr.get_mut(&self.key))
    }
}

#[allow(missing_docs)]
pub struct EntryRef<'a, V>
where
    V: ValueEnDe,
{
    key: &'a [u8],
    hdr: &'a mut MapxOrdRawKey<V>,
}

impl<'a, V> EntryRef<'a, V>
where
    V: ValueEnDe,
{
    /// Imitate the `btree_map/btree_map::Entry.or_insert(...)`.
    pub fn or_insert_ref(self, default: &V) -> ValueMut<'a, V> {
        if !self.hdr.contains_key(self.key) {
            self.hdr.set_value_ref(self.key, default);
        }
        pnk!(self.hdr.get_mut(self.key))
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

#[allow(missing_docs)]
pub struct MapxOrdRawKeyIter<V>
where
    V: ValueEnDe,
{
    iter: MapxRawIter,
    pd: PhantomData<V>,
}

impl<V> Iterator for MapxOrdRawKeyIter<V>
where
    V: ValueEnDe,
{
    type Item = (RawKey, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }
}

impl<V> DoubleEndedIterator for MapxOrdRawKeyIter<V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|(k, v)| (k, <V as ValueEnDe>::decode(&v).unwrap()))
    }
}

impl<V> ExactSizeIterator for MapxOrdRawKeyIter<V> where V: ValueEnDe {}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<V> Serialize for MapxOrdRawKey<V>
where
    V: ValueEnDe,
{
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&<InstanceCfg as ValueEnDe>::encode(
            &self.get_instance_cfg(),
        ))
    }
}

impl<'de, V> Deserialize<'de> for MapxOrdRawKey<V>
where
    V: ValueEnDe,
{
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(SimpleVisitor).map(|cfg| {
            MapxOrdRawKey::from(<InstanceCfg as ValueEnDe>::decode(&cfg).unwrap())
        })
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
