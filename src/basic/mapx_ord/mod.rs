//!
//! A `BTreeMap`-like structure but storing data in disk.
//!
//! NOTE:
//! - Both keys and values will be encoded in this structure
//!     - Keys will be encoded by `KeyEnDeOrdered`
//!     - Values will be encoded by some `serde`-like methods
//! - It's your duty to ensure that the encoded key keeps a same order with the original key
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::mapx_ord::MapxOrd;
//!
//! let mut l = MapxOrd::new();
//!
//! l.insert(1, 0);
//! l.insert_ref(&1, &0);
//! l.insert(2, 0);
//!
//! l.iter().for_each(|(k, v)| {
//!     assert!(k >= 1);
//!     assert_eq!(v, 0);
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
        InstanceCfg,
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
pub struct MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    inner: MapxRaw,
    _pd0: PhantomData<K>,
    _pd1: PhantomData<V>,
}

impl<K, V> From<InstanceCfg> for MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn from(cfg: InstanceCfg) -> Self {
        Self {
            inner: MapxRaw::from(cfg),
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }
}

impl<K, V> Default for MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<K, V> MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Create an instance.
    #[inline(always)]
    pub fn new() -> Self {
        MapxOrd {
            inner: MapxRaw::new(),
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    // Get the database storage path
    pub(crate) fn get_instance_cfg(&self) -> InstanceCfg {
        self.inner.get_instance_cfg()
    }

    /// Imitate the behavior of 'BTreeMap<_>.get(...)'
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner
            .get(&key.to_bytes())
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    /// Get the closest smaller value, include itself.
    #[inline(always)]
    pub fn get_le(&self, key: &K) -> Option<(K, V)> {
        self.inner.get_le(&key.to_bytes()).map(|(k, v)| {
            (
                pnk!(K::from_bytes(k)),
                <V as ValueEnDe>::decode(&v).unwrap(),
            )
        })
    }

    /// Get the closest larger value, include itself.
    #[inline(always)]
    pub fn get_ge(&self, key: &K) -> Option<(K, V)> {
        self.inner.get_ge(&key.to_bytes()).map(|(k, v)| {
            (
                pnk!(K::from_bytes(k)),
                <V as ValueEnDe>::decode(&v).unwrap(),
            )
        })
    }

    /// Imitate the behavior of 'BTreeMap<_>.get_mut(...)'
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, K, V>> {
        self.inner.get(&key.to_bytes()).map(|v| {
            ValueMut::new(self, key.clone(), <V as ValueEnDe>::decode(&v).unwrap())
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
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.insert_ref(&key, &value)
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn insert_ref(&mut self, key: &K, value: &V) -> Option<V> {
        self.inner
            .insert(&key.to_bytes(), &value.encode())
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    // used to support efficient versioned-implementations
    #[inline(always)]
    pub(crate) fn insert_ref_encoded_value(
        &mut self,
        key: &K,
        value: &[u8],
    ) -> Option<V> {
        self.inner
            .insert(&key.to_bytes(), value)
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    /// Similar with `insert`, but ignore the old value.
    #[inline(always)]
    pub fn set_value(&mut self, key: K, value: V) {
        self.set_value_ref(&key, &value);
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn set_value_ref(&mut self, key: &K, value: &V) {
        self.inner.insert(&key.to_bytes(), &value.encode());
    }

    /// Imitate the behavior of '.entry(...).or_insert(...)'
    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn entry_ref<'a>(&'a mut self, key: &'a K) -> EntryRef<'a, K, V> {
        EntryRef { key, hdr: self }
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub fn iter(&self) -> MapxOrdIter<K, V> {
        MapxOrdIter {
            iter: self.inner.iter(),
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range<R: RangeBounds<K>>(&self, bounds: R) -> MapxOrdIter<K, V> {
        self.range_ref((bounds.start_bound(), bounds.end_bound()))
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range_ref<'a, R: RangeBounds<&'a K>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdIter<K, V> {
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

        MapxOrdIter {
            iter: self.inner.range((l, h)),
            _pd0: PhantomData,
            _pd1: PhantomData,
        }
    }

    /// First item
    #[inline(always)]
    pub fn first(&self) -> Option<(K, V)> {
        self.iter().next()
    }

    /// Last item
    #[inline(always)]
    pub fn last(&self) -> Option<(K, V)> {
        self.iter().next_back()
    }

    /// Check if a key is exists.
    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(&key.to_bytes())
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner
            .remove(&key.to_bytes())
            .map(|v| <V as ValueEnDe>::decode(&v).unwrap())
    }

    /// Remove a <K, V> from mem and disk.
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

/// Returned by `<MapxOrd>.get_mut(...)`
#[derive(Debug)]
pub struct ValueMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    hdr: &'a mut MapxOrd<K, V>,
    key: ManuallyDrop<K>,
    value: ManuallyDrop<V>,
}

impl<'a, K, V> ValueMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    pub(crate) fn new(hdr: &'a mut MapxOrd<K, V>, key: K, value: V) -> Self {
        ValueMut {
            hdr,
            key: ManuallyDrop::new(key),
            value: ManuallyDrop::new(value),
        }
    }
}

/// NOTE: Very Important !!!
impl<'a, K, V> Drop for ValueMut<'a, K, V>
where
    K: KeyEnDeOrdered,
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

impl<'a, K, V> Deref for ValueMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K, V> DerefMut for ValueMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

/// Imitate the `btree_map/btree_map::Entry`.
pub struct Entry<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: 'a + ValueEnDe,
{
    key: K,
    hdr: &'a mut MapxOrd<K, V>,
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Imitate the `btree_map/btree_map::Entry.or_insert(...)`.
    pub fn or_insert(self, default: V) -> ValueMut<'a, K, V> {
        if !self.hdr.contains_key(&self.key) {
            self.hdr.set_value_ref(&self.key, &default);
        }
        pnk!(self.hdr.get_mut(&self.key))
    }
}

#[allow(missing_docs)]
pub struct EntryRef<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    key: &'a K,
    hdr: &'a mut MapxOrd<K, V>,
}

impl<'a, K, V> EntryRef<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Imitate the `btree_map/btree_map::Entry.or_insert(...)`.
    pub fn or_insert_ref(self, default: &V) -> ValueMut<'a, K, V> {
        if !self.hdr.contains_key(self.key) {
            self.hdr.set_value_ref(self.key, default);
        }
        pnk!(self.hdr.get_mut(self.key))
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

#[allow(missing_docs)]
pub struct MapxOrdIter<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    iter: MapxRawIter,
    _pd0: PhantomData<K>,
    _pd1: PhantomData<V>,
}

impl<K, V> Iterator for MapxOrdIter<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|(k, v)| {
            (
                pnk!(K::from_bytes(k)),
                <V as ValueEnDe>::decode(&v).unwrap(),
            )
        })
    }
}

impl<K, V> DoubleEndedIterator for MapxOrdIter<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(|(k, v)| {
            (
                pnk!(K::from_bytes(k)),
                <V as ValueEnDe>::decode(&v).unwrap(),
            )
        })
    }
}

impl<K, V> ExactSizeIterator for MapxOrdIter<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<K, V> Serialize for MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
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

impl<'de, K, V> Deserialize<'de> for MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer
            .deserialize_bytes(SimpleVisitor)
            .map(|cfg| MapxOrd::from(<InstanceCfg as ValueEnDe>::decode(&cfg).unwrap()))
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
