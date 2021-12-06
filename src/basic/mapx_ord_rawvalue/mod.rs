//!
//! A `BTreeMap`-like structure but storing data in disk.
//!
//! NOTE:
//! - Values will **NOT** be encoded in this structure, but keys will be
//!     - Keys will be encoded by `KeyEnDeOrdered`
//! - It's your duty to ensure that the encoded key keeps a same order with the original key
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::mapx_ord_rawvalue::MapxOrdRawValue;
//!
//! let mut l = MapxOrdRawValue::new();
//!
//! l.insert_ref(&1, &[0]);
//! l.insert(1, Box::new([0]));
//! l.insert_ref(&2, &[0]);
//!
//! l.iter().for_each(|(k, v)| {
//!     assert!(k >= 1);
//!     assert_eq!(&v[..], &[0]);
//! });
//!
//! l.remove(&2);
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
        ende::{KeyEnDeOrdered, SimpleVisitor, ValueEnDe},
        InstanceCfg, RawValue,
    },
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Bound, Deref, DerefMut, RangeBounds},
    result::Result as StdResult,
};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `BTreeMap<_, _>`.
#[derive(PartialEq, Eq, Debug)]
pub struct MapxOrdRawValue<K>
where
    K: KeyEnDeOrdered,
{
    inner: MapxRaw,
    _pd: PhantomData<K>,
}

impl<K> From<InstanceCfg> for MapxOrdRawValue<K>
where
    K: KeyEnDeOrdered,
{
    fn from(cfg: InstanceCfg) -> Self {
        Self {
            inner: MapxRaw::from(cfg),
            _pd: PhantomData,
        }
    }
}

impl<K> Default for MapxOrdRawValue<K>
where
    K: KeyEnDeOrdered,
{
    fn default() -> Self {
        Self::new()
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<K> MapxOrdRawValue<K>
where
    K: KeyEnDeOrdered,
{
    /// Create an instance.
    #[inline(always)]
    pub fn new() -> Self {
        MapxOrdRawValue {
            inner: MapxRaw::new(),
            _pd: PhantomData,
        }
    }

    // Get the database storage path
    pub(crate) fn get_instance_cfg(&self) -> InstanceCfg {
        self.inner.get_instance_cfg()
    }

    /// Imitate the behavior of 'BTreeMap<_>.get(...)'
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<RawValue> {
        self.inner.get(&key.to_bytes())
    }

    /// Get the closest smaller value, include itself.
    #[inline(always)]
    pub fn get_le(&self, key: &K) -> Option<(K, RawValue)> {
        self.inner
            .get_le(&key.to_bytes())
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    /// Get the closest larger value, include itself.
    #[inline(always)]
    pub fn get_ge(&self, key: &K) -> Option<(K, RawValue)> {
        self.inner
            .get_ge(&key.to_bytes())
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }

    /// Imitate the behavior of 'BTreeMap<_>.get_mut(...)'
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, K>> {
        self.inner
            .get(&key.to_bytes())
            .map(|v| ValueMut::new(self, key.clone(), v))
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
    pub fn insert(&mut self, key: K, value: RawValue) -> Option<RawValue> {
        self.insert_ref(&key, &value)
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn insert_ref(&mut self, key: &K, value: &[u8]) -> Option<RawValue> {
        self.inner.insert(&key.to_bytes(), value)
    }

    /// Similar with `insert`, but ignore the old value.
    #[inline(always)]
    pub fn set_value(&mut self, key: K, value: RawValue) {
        self.set_value_ref(&key, &value);
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn set_value_ref(&mut self, key: &K, value: &[u8]) {
        self.inner.insert(&key.to_bytes(), value);
    }

    /// Imitate the behavior of '.entry(...).or_insert(...)'
    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, K> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn entry_ref<'a>(&'a mut self, key: &'a K) -> EntryRef<'a, K> {
        EntryRef { key, hdr: self }
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub fn iter(&self) -> MapxOrdRawValueIter<K> {
        MapxOrdRawValueIter {
            iter: self.inner.iter(),
            _pd: PhantomData,
        }
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range<R: RangeBounds<K>>(&self, bounds: R) -> MapxOrdRawValueIter<K> {
        self.range_ref((bounds.start_bound(), bounds.end_bound()))
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range_ref<'a, R: RangeBounds<&'a K>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdRawValueIter<K> {
        let ll;
        let l = match bounds.start_bound() {
            Bound::Included(lo) => {
                ll = lo.to_bytes();
                Bound::Included(&ll[..])
            }
            Bound::Excluded(lo) => {
                ll = lo.to_bytes();
                Bound::Excluded(&ll[..])
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let hh;
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                hh = hi.to_bytes();
                Bound::Included(&hh[..])
            }
            Bound::Excluded(hi) => {
                hh = hi.to_bytes();
                Bound::Excluded(&hh[..])
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        MapxOrdRawValueIter {
            iter: self.inner.range((l, h)),
            _pd: PhantomData,
        }
    }

    /// First item
    #[inline(always)]
    pub fn first(&self) -> Option<(K, RawValue)> {
        self.iter().next()
    }

    /// Last item
    #[inline(always)]
    pub fn last(&self) -> Option<(K, RawValue)> {
        self.iter().next_back()
    }

    /// Check if a key is exists.
    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(&key.to_bytes())
    }

    /// Remove a <K> from mem and disk.
    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<RawValue> {
        self.inner.remove(&key.to_bytes())
    }

    /// Remove a <K> from mem and disk.
    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.inner.remove(&key.to_bytes());
    }

    /// Clear all data.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

/// Returned by `<MapxOrdRawValue>.get_mut(...)`
#[derive(Debug)]
pub struct ValueMut<'a, K>
where
    K: KeyEnDeOrdered,
{
    hdr: &'a mut MapxOrdRawValue<K>,
    key: K,
    value: RawValue,
}

impl<'a, K> ValueMut<'a, K>
where
    K: KeyEnDeOrdered,
{
    pub(crate) fn new(hdr: &'a mut MapxOrdRawValue<K>, key: K, value: RawValue) -> Self {
        ValueMut { hdr, key, value }
    }
}

/// NOTE: Very Important !!!
impl<'a, K> Drop for ValueMut<'a, K>
where
    K: KeyEnDeOrdered,
{
    fn drop(&mut self) {
        self.hdr.set_value_ref(&self.key, &self.value);
    }
}

impl<'a, K> Deref for ValueMut<'a, K>
where
    K: KeyEnDeOrdered,
{
    type Target = RawValue;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K> DerefMut for ValueMut<'a, K>
where
    K: KeyEnDeOrdered,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

/// Imitate the `btree_map/btree_map::Entry`.
pub struct Entry<'a, K>
where
    K: KeyEnDeOrdered,
{
    key: K,
    hdr: &'a mut MapxOrdRawValue<K>,
}

impl<'a, K> Entry<'a, K>
where
    K: KeyEnDeOrdered,
{
    /// Imitate the `btree_map/btree_map::Entry.or_insert(...)`.
    pub fn or_insert(self, default: RawValue) -> ValueMut<'a, K> {
        if !self.hdr.contains_key(&self.key) {
            self.hdr.set_value_ref(&self.key, &default);
        }
        pnk!(self.hdr.get_mut(&self.key))
    }
}

#[allow(missing_docs)]
pub struct EntryRef<'a, K>
where
    K: KeyEnDeOrdered,
{
    key: &'a K,
    hdr: &'a mut MapxOrdRawValue<K>,
}

impl<'a, K> EntryRef<'a, K>
where
    K: KeyEnDeOrdered,
{
    /// Imitate the `btree_map/btree_map::Entry.or_insert(...)`.
    pub fn or_insert_ref(self, default: &[u8]) -> ValueMut<'a, K> {
        if !self.hdr.contains_key(self.key) {
            self.hdr.set_value_ref(self.key, default);
        }
        pnk!(self.hdr.get_mut(self.key))
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

#[allow(missing_docs)]
pub struct MapxOrdRawValueIter<K>
where
    K: KeyEnDeOrdered,
{
    iter: MapxRawIter,
    _pd: PhantomData<K>,
}

impl<K> Iterator for MapxOrdRawValueIter<K>
where
    K: KeyEnDeOrdered,
{
    type Item = (K, RawValue);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

impl<K> DoubleEndedIterator for MapxOrdRawValueIter<K>
where
    K: KeyEnDeOrdered,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|(k, v)| (pnk!(K::from_bytes(k)), v))
    }
}

impl<K> ExactSizeIterator for MapxOrdRawValueIter<K> where K: KeyEnDeOrdered {}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<K> Serialize for MapxOrdRawValue<K>
where
    K: KeyEnDeOrdered,
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

impl<'de, K> Deserialize<'de> for MapxOrdRawValue<K>
where
    K: KeyEnDeOrdered,
{
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(SimpleVisitor).map(|cfg| {
            MapxOrdRawValue::from(<InstanceCfg as ValueEnDe>::decode(&cfg).unwrap())
        })
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
