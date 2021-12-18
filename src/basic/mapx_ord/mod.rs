//!
//! A disk-storage replacement for the pure in-memory BTreeMap.
//!

mod backend;

#[cfg(test)]
mod test;

use crate::common::{
    ende::{KeyEnDeOrdered, SimpleVisitor, ValueEnDe},
    InstanceCfg,
};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
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
    inner: backend::MapxOrd<K, V>,
}

impl<K, V> From<InstanceCfg> for MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn from(cfg: InstanceCfg) -> Self {
        Self {
            inner: backend::MapxOrd::from(cfg),
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

///////////////////////////////////////////////
// Begin of the self-implementation for MapxOrd //
/*********************************************/

impl<K, V> MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Create an instance.
    #[inline(always)]
    pub fn new() -> Self {
        MapxOrd {
            inner: backend::MapxOrd::must_new(),
        }
    }

    // Get the database storage path
    pub(crate) fn get_instance_cfg(&self) -> InstanceCfg {
        self.inner.get_instance_cfg()
    }

    /// Imitate the behavior of 'BTreeMap<_>.get(...)'
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(key)
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn get_ref_bytes_k(&self, key: &[u8]) -> Option<V> {
        self.inner.get_ref_bytes_k(key)
    }

    /// Get the closest smaller value, include itself.
    #[inline(always)]
    pub fn get_le(&self, key: &K) -> Option<(K, V)> {
        self.inner.get_le(key)
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn get_le_ref_bytes_k(&self, key: &[u8]) -> Option<(K, V)> {
        self.inner.get_le_ref_bytes_k(key)
    }

    /// Get the closest larger value, include itself.
    #[inline(always)]
    pub fn get_ge(&self, key: &K) -> Option<(K, V)> {
        self.inner.get_ge(key)
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn get_ge_ref_bytes_k(&self, key: &[u8]) -> Option<(K, V)> {
        self.inner.get_ge_ref_bytes_k(key)
    }

    /// Imitate the behavior of 'BTreeMap<_>.get_mut(...)'
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, K, V>> {
        self.inner
            .get(key)
            .map(move |v| ValueMut::new(self, key.clone(), v))
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn get_mut_ref_bytes_k(&mut self, key: &[u8]) -> Option<ValueMut<'_, K, V>> {
        self.inner
            .get_ref_bytes_k(key)
            .and_then(|v| K::from_slice(key).ok().map(|k| ValueMut::new(self, k, v)))
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
        self.inner.insert_ref(key, value)
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn insert_ref_bytes_k(&mut self, key: &[u8], value: &V) -> Option<V> {
        self.inner.insert_ref_bytes_k(key, value)
    }

    /// same as the funtion without the '_' prefix, but use bytes key&value
    #[inline(always)]
    pub fn insert_ref_bytes_kv(&mut self, key: &[u8], value: &[u8]) -> Option<V> {
        self.inner.insert_ref_bytes_kv(key, value)
    }

    /// Similar with `insert`, but ignore the old value.
    #[inline(always)]
    pub fn set_value(&mut self, key: K, value: V) {
        self.set_value_ref(&key, &value);
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn set_value_ref(&mut self, key: &K, value: &V) {
        self.inner.set_value_ref(key, value);
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn set_value_ref_bytes_k(&mut self, key: &[u8], value: &V) {
        self.inner.set_value_ref_bytes_k(key, value);
    }

    /// same as the funtion without the '_' prefix, but use bytes key&value
    #[inline(always)]
    pub fn set_value_ref_bytes_kv(&mut self, key: &[u8], value: &[u8]) {
        self.inner.set_value_ref_bytes_kv(key, value);
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

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn entry_ref_bytes_key<'a>(
        &'a mut self,
        key: &'a [u8],
    ) -> EntryRefBytesKey<'a, K, V> {
        EntryRefBytesKey { key, hdr: self }
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub fn iter(&self) -> MapxOrdIter<K, V> {
        MapxOrdIter {
            iter: self.inner.iter(),
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
                Bound::Included(ll.as_slice())
            }
            Bound::Excluded(lo) => {
                ll = lo.to_bytes();
                Bound::Excluded(ll.as_slice())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let hh;
        let h = match bounds.end_bound() {
            Bound::Included(hi) => {
                hh = hi.to_bytes();
                Bound::Included(hh.as_slice())
            }
            Bound::Excluded(hi) => {
                hh = hi.to_bytes();
                Bound::Excluded(hh.as_slice())
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        MapxOrdIter {
            iter: self.inner.range((l, h)),
        }
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range_ref_bytes_k<'a, R: RangeBounds<&'a [u8]>>(
        &'a self,
        bounds: R,
    ) -> MapxOrdIter<K, V> {
        MapxOrdIter {
            iter: self.inner.range(bounds),
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
        self.inner.contains_key(key)
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn contains_key_ref_bytes_k(&self, key: &[u8]) -> bool {
        self.inner.contains_key_ref_bytes_k(key)
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(key)
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn remove_ref_bytes_k(&mut self, key: &[u8]) -> Option<V> {
        self.inner.remove_ref_bytes_k(key)
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.inner.unset_value(key);
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn unset_value_ref_bytes_k(&mut self, key: &[u8]) {
        self.inner.unset_value_ref_bytes_k(key);
    }

    /// Clear all data.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

/*******************************************/
// End of the self-implementation for MapxOrd //
/////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////
// Begin of the implementation of ValueMut(returned by `self.get_mut`) for MapxOrd //
/********************************************************************************/

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

/******************************************************************************/
// End of the implementation of ValueMut(returned by `self.get_mut`) for MapxOrd //
////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////
// Begin of the implementation of Entry for MapxOrd //
/*************************************************/

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

#[allow(missing_docs)]
pub struct EntryRefBytesKey<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    key: &'a [u8],
    hdr: &'a mut MapxOrd<K, V>,
}

impl<'a, K, V> EntryRefBytesKey<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Imitate the `btree_map/btree_map::Entry.or_insert(...)`.
    pub fn or_insert_ref(self, default: &V) -> ValueMut<'a, K, V> {
        if !self.hdr.contains_key_ref_bytes_k(self.key) {
            self.hdr.set_value_ref_bytes_k(self.key, default);
        }
        pnk!(self.hdr.get_mut_ref_bytes_k(self.key))
    }
}

/***********************************************/
// End of the implementation of Entry for MapxOrd //
/////////////////////////////////////////////////

//////////////////////////////////////////////////
// Begin of the implementation of Iter for MapxOrd //
/************************************************/

/// Iter over [MapxOrd](self::MapxOrd).
pub struct MapxOrdIter<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    iter: backend::MapxOrdIter<K, V>,
}

impl<K, V> Iterator for MapxOrdIter<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<K, V> DoubleEndedIterator for MapxOrdIter<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}

/**********************************************/
// End of the implementation of Iter for MapxOrd //
////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////
// Begin of the implementation of Serialize/Deserialize for MapxOrd //
/*****************************************************************/

impl<K, V> Serialize for MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.get_instance_cfg().encode())
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
        deserializer.deserialize_bytes(SimpleVisitor).map(|meta| {
            let meta = pnk!(<InstanceCfg as ValueEnDe>::decode(&meta));
            MapxOrd::from(meta)
        })
    }
}

/***************************************************************/
// End of the implementation of Serialize/Deserialize for MapxOrd //
/////////////////////////////////////////////////////////////////
