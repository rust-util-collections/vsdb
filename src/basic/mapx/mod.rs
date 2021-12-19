//!
//! A `HashMap`-like structure but storing data in disk.
//!
//! NOTE:
//! - Both keys and values will be encoded(serde) in this structure
//!     - Both of them will be encoded by some `serde`-like methods
//!

#[cfg(test)]
mod test;

use crate::{
    basic::mapx_ord::{Entry, MapxOrd, MapxOrdIter, ValueMut},
    common::{
        ende::{KeyEnDe, SimpleVisitor, ValueEnDe},
        InstanceCfg, RawKey,
    },
};
use std::{marker::PhantomData, result::Result as StdResult};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `HashMap<_, _>`.
#[derive(PartialEq, Eq, Debug)]
pub struct Mapx<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    inner: MapxOrd<RawKey, V>,
    _pd: PhantomData<K>,
}

impl<K, V> Default for Mapx<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> From<InstanceCfg> for Mapx<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    fn from(cfg: InstanceCfg) -> Self {
        Self {
            inner: MapxOrd::from(cfg),
            _pd: PhantomData,
        }
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<K, V> Mapx<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    /// Create an instance.
    #[inline(always)]
    pub fn new() -> Self {
        Mapx {
            inner: MapxOrd::new(),
            _pd: PhantomData,
        }
    }

    // Get the database storage path
    fn get_instance_cfg(&self) -> InstanceCfg {
        self.inner.get_instance_cfg()
    }

    /// Imitate the behavior of 'HashMap<_>.get(...)'
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(&key.encode())
    }

    /// Imitate the behavior of 'HashMap<_>.get_mut(...)'
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, RawKey, V>> {
        let k = key.encode();
        self.inner
            .get(&k)
            .map(move |v| ValueMut::new(&mut self.inner, k, v))
    }

    /// Imitate the behavior of 'HashMap<_>.len()'.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// A helper func
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Imitate the behavior of 'HashMap<_>.insert(...)'.
    #[inline(always)]
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.insert_ref(&key, &value)
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn insert_ref(&mut self, key: &K, value: &V) -> Option<V> {
        self.inner.insert_ref(&key.encode(), value)
    }

    /// Similar with `insert`, but ignore the old value.
    #[inline(always)]
    pub fn set_value(&mut self, key: K, value: V) {
        self.set_value_ref(&key, &value)
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn set_value_ref(&mut self, key: &K, value: &V) {
        self.inner.set_value_ref(&key.encode(), value);
    }

    /// Imitate the behavior of '.entry(...).or_insert(...)'
    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, RawKey, V> {
        self.inner.entry(key.encode())
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub fn iter(&self) -> MapxIter<K, V> {
        MapxIter {
            iter: self.inner.iter(),
            _pd: PhantomData,
        }
    }

    /// Check if a key is exists.
    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(&key.encode())
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(&key.encode())
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.inner.unset_value(&key.encode());
    }

    /// Clear all data.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

#[allow(missing_docs)]
pub struct MapxIter<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    iter: MapxOrdIter<RawKey, V>,
    _pd: PhantomData<K>,
}

impl<K, V> Iterator for MapxIter<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .map(|(k, v)| (<K as KeyEnDe>::decode(&k).unwrap(), v))
    }
}

impl<K, V> DoubleEndedIterator for MapxIter<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter
            .next_back()
            .map(|(k, v)| (<K as KeyEnDe>::decode(&k).unwrap(), v))
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<K, V> serde::Serialize for Mapx<K, V>
where
    K: KeyEnDe,
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

impl<'de, K, V> serde::Deserialize<'de> for Mapx<K, V>
where
    K: KeyEnDe,
    V: ValueEnDe,
{
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer
            .deserialize_bytes(SimpleVisitor)
            .map(|cfg| Mapx::from(<InstanceCfg as ValueEnDe>::decode(&cfg).unwrap()))
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
