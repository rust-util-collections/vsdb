//!
//! A `BTreeMap`-like structure that stores data on disk with raw keys.
//!
//! `MapxOrdRawKey` is an ordered map where keys are stored as raw bytes,
//! while values are encoded using `serde`-like methods. This is useful when
//! you need to work with keys that are already in a byte format and want to
//! avoid the overhead of encoding and decoding them.
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::mapx_ord_rawkey::MapxOrdRawKey;
//! use vsdb::{VsdbOptions, vsdb_configure, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = std::env::temp_dir().join(format!("vsdb-{}", rand::random::<u128>()));
//! vsdb_configure(VsdbOptions::new(&dir)).unwrap();
//!
//! let mut m: MapxOrdRawKey<String> = MapxOrdRawKey::new();
//!
//! // Insert key-value pairs
//! m.insert(&[1], &"hello".to_string());
//! m.insert(&[2], &"world".to_string());
//!
//! // Retrieve a value
//! assert_eq!(m.get(&[1]), Some("hello".to_string()));
//!
//! // Iterate over the map
//! for (k, v) in m.iter() {
//!     println!("key: {:?}, val: {}", k, v);
//! }
//!
//! // Remove a key-value pair
//! m.remove(&[2]);
//!
//! // Clear the entire map
//! m.clear();
//!
//! // Clean up the directory
//! fs::remove_dir_all(vsdb_get_base_dir()).unwrap();
//! ```
//!

#[cfg(test)]
mod test;

use crate::common::{
    RawKey, RawRangeBounds, ende::ValueEnDe, error::Result, macros::define_map_wrapper,
};
use std::marker::PhantomData;
use vsdb_core::basic::mapx_raw::{self, MapxRaw, MapxRawIter};

define_map_wrapper! {
    #[doc = "A disk-based, `BTreeMap`-like data structure with raw keys and typed values."]
    #[doc = ""]
    #[doc = "`MapxOrdRawKey` stores keys as raw bytes and values as encoded data."]
    pub struct MapxOrdRawKey<V> {
        pub(crate) inner: MapxRaw,
        _p: PhantomData<V>,
    }
    where V: ValueEnDe
}

impl<V> MapxOrdRawKey<V>
where
    V: ValueEnDe,
{
    /// Retrieves a value from the map for a given key.
    #[inline(always)]
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<V> {
        self.try_get(key).expect("vsdb: read and decode value")
    }

    /// Decodes directly from pinned backend bytes and propagates read or codec errors.
    pub fn try_get(&self, key: impl AsRef<[u8]>) -> Result<Option<V>> {
        self.inner.try_get_with(key, V::decode_value)
    }

    /// Retrieves a mutable reference to a value in the map.
    ///
    /// A mutable dereference marks the value for write-back. A shared
    /// dereference is read-only, including for interior-mutable values.
    pub fn get_mut(&mut self, key: impl AsRef<[u8]>) -> Option<ValueMut<'_, V>> {
        self.inner.get_mut(key).map(ValueMut::occupied)
    }

    pub(crate) fn try_get_mut(&mut self, key: &[u8]) -> Result<Option<ValueMut<'_, V>>> {
        self.inner
            .try_value_mut(key)?
            .map(ValueMut::try_occupied)
            .transpose()
    }

    pub fn update<R>(
        &mut self,
        key: impl AsRef<[u8]>,
        f: impl FnOnce(&mut V) -> R,
    ) -> Result<Option<R>> {
        crate::common::ensure_writable(&self.namespace(), "value update")?;
        let Some(mut guard) = self.try_get_mut(key.as_ref())? else {
            return Ok(None);
        };
        let result = f(&mut guard);
        guard.commit()?;
        Ok(Some(result))
    }

    /// Checks if the map contains a value for the specified key.
    #[inline(always)]
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> bool {
        self.inner.contains_key(key.as_ref())
    }

    /// Retrieves the last entry with a key less than or equal to the given key.
    #[inline(always)]
    pub fn get_le(&self, key: impl AsRef<[u8]>) -> Option<(RawKey, V)> {
        self.inner
            .get_le(key.as_ref())
            .map(|(k, v)| (k, <V as ValueEnDe>::decode_value(&v).unwrap()))
    }

    /// Retrieves the first entry with a key greater than or equal to the given key.
    #[inline(always)]
    pub fn get_ge(&self, key: impl AsRef<[u8]>) -> Option<(RawKey, V)> {
        self.inner
            .get_ge(key.as_ref())
            .map(|(k, v)| (k, <V as ValueEnDe>::decode_value(&v).unwrap()))
    }

    /// Inserts a key-value pair into the map.
    ///
    /// Does not return the old value for performance reasons.
    #[inline(always)]
    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: &V) {
        self.inner.insert(key.as_ref(), value.encode_value())
    }

    /// Gets an entry for a given key, allowing for in-place modification.
    #[inline(always)]
    pub fn entry(&mut self, key: &[u8]) -> Entry<'_, V> {
        match self.inner.entry(key) {
            mapx_raw::Entry::Occupied(e) => {
                Entry::Occupied(OccupiedEntry(ValueMut::occupied(e.into_mut())))
            }
            mapx_raw::Entry::Vacant(e) => Entry::Vacant(VacantEntry(e, PhantomData)),
        }
    }

    /// Returns an iterator over the map's entries.
    #[inline(always)]
    pub fn iter(&self) -> MapxOrdRawKeyIter<'_, V> {
        MapxOrdRawKeyIter {
            inner: self.inner.iter(),
            _p: PhantomData,
        }
    }

    /// Returns an iterator over the map's raw keys, in ascending order.
    ///
    /// Unlike `iter().map(|(k, _)| k)`, this never copies or decodes `V` —
    /// each entry's value bytes are skipped entirely instead of being
    /// decoded and immediately discarded, avoiding wasted work on a hot
    /// path (this matters most for nested-VSDB-collection value types,
    /// whose "decode" is itself an engine `deserialize` call).
    #[inline(always)]
    pub fn keys(&self) -> impl DoubleEndedIterator<Item = RawKey> + '_ {
        RawKeys(self.inner.iter())
    }

    /// Returns a mutable iterator over the map's entries.
    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxOrdRawKeyIterMut<'_, V> {
        MapxOrdRawKeyIterMut {
            inner: self.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    /// Returns an iterator over a range of entries in the map.
    #[inline(always)]
    pub fn range<'a, R: RawRangeBounds>(
        &'a self,
        bounds: R,
    ) -> MapxOrdRawKeyIter<'a, V> {
        MapxOrdRawKeyIter {
            inner: self.inner.range(bounds),
            _p: PhantomData,
        }
    }

    /// Returns a mutable iterator over a range of entries in the map.
    #[inline(always)]
    pub fn range_mut<'a, R: RawRangeBounds>(
        &'a mut self,
        bounds: R,
    ) -> MapxOrdRawKeyIterMut<'a, V> {
        MapxOrdRawKeyIterMut {
            inner: self.inner.range_mut(bounds),
            _p: PhantomData,
        }
    }

    /// Retrieves the first entry in the map.
    #[inline(always)]
    pub fn first(&self) -> Option<(RawKey, V)> {
        self.iter().next()
    }

    /// Retrieves the last entry in the map.
    #[inline(always)]
    pub fn last(&self) -> Option<(RawKey, V)> {
        self.iter().next_back()
    }

    /// Removes a key from the map.
    ///
    /// Does not return the old value for performance reasons.
    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) {
        self.inner.remove(key.as_ref())
    }

    /// Start a batch operation.
    ///
    /// This method allows you to perform multiple insert/remove operations
    /// and commit them atomically.
    ///
    /// A failed [`commit`](MapxOrdRawKeyBatch::commit) consumes
    /// the buffered operations. Preflight rejections leave them unapplied;
    /// persistence errors can be ambiguous after recovery and must not be
    /// retried automatically.
    ///
    /// # Examples
    ///
    /// ```
    /// use vsdb::basic::mapx_ord_rawkey::MapxOrdRawKey;
    /// use vsdb::{VsdbOptions, vsdb_configure};
    ///
    /// let dir = std::env::temp_dir()
    ///     .join(format!("vsdb_mapx_ord_rawkey_batch_{}", rand::random::<u128>()));
    /// vsdb_configure(VsdbOptions::new(&dir)).unwrap();
    /// let mut map: MapxOrdRawKey<String> = MapxOrdRawKey::new();
    ///
    /// let mut batch = map.batch();
    /// batch.insert(&[1], &"one".to_string());
    /// batch.insert(&[2], &"two".to_string());
    /// batch.commit().unwrap();
    ///
    /// assert_eq!(map.get(&[1]), Some("one".to_string()));
    /// assert_eq!(map.get(&[2]), Some("two".to_string()));
    /// # std::fs::remove_dir_all(dir).unwrap();
    /// ```
    #[inline(always)]
    pub fn batch(&mut self) -> MapxOrdRawKeyBatch<'_, V> {
        MapxOrdRawKeyBatch {
            inner: self.inner.batch(),
            _marker: PhantomData,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// Hand-written rather than `#[derive(PartialEq, Eq)]`: a derive would
// compare the *encoded* bytes of `V` (postcard encoding is not
// injective on decoded value in general — e.g. `0.0_f64` and
// `-0.0_f64` encode to different bytes despite comparing equal, while
// distinct NaN bit patterns can compare unequal despite identical
// bytes), silently diverging from `V`'s own logical `PartialEq`.
// `self.iter()` already decodes `V` (there is no `K` to decode here —
// keys are raw bytes), so comparing through it fixes the mismatch.
impl<V> PartialEq for MapxOrdRawKey<V>
where
    V: ValueEnDe + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl<V> Eq for MapxOrdRawKey<V> where V: ValueEnDe + Eq {}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A batch entry for `MapxOrdRawKey`.
pub struct MapxOrdRawKeyBatch<'a, V>
where
    V: ValueEnDe,
{
    inner: vsdb_core::basic::mapx_raw::MapxRawBatch<'a>,
    _marker: PhantomData<V>,
}

impl<'a, V> MapxOrdRawKeyBatch<'a, V>
where
    V: ValueEnDe,
{
    /// Insert a key-value pair into the batch.
    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: &V) {
        self.inner.insert(key.as_ref(), value.encode_value());
    }

    /// Remove a key in the batch.
    pub fn remove(&mut self, key: impl AsRef<[u8]>) {
        self.inner.remove(key.as_ref());
    }

    /// Commit the batch.
    pub fn commit(self) -> Result<()> {
        self.inner.commit()
    }

    /// Atomically publishes the batch after synchronizing its shard WAL.
    /// An empty batch does not fence earlier writes. Persistence failures can
    /// be ambiguous after recovery; they must not be retried automatically.
    pub fn commit_sync(self) -> Result<()> {
        self.inner.commit_sync()
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// Typed point and iterator edits share the same write-back semantics.
pub type ValueMut<'a, V> = crate::common::guard::ValueGuard<V, mapx_raw::ValueMut<'a>>;

pub enum Entry<'a, V: ValueEnDe> {
    Occupied(OccupiedEntry<'a, V>),
    Vacant(VacantEntry<'a, V>),
}
pub struct OccupiedEntry<'a, V: ValueEnDe>(ValueMut<'a, V>);
pub struct VacantEntry<'a, V: ValueEnDe>(mapx_raw::VacantEntry<'a>, PhantomData<V>);
impl<'a, V: ValueEnDe> OccupiedEntry<'a, V> {
    pub fn get(&self) -> &V {
        &self.0
    }
    pub fn get_mut(&mut self) -> &mut V {
        &mut self.0
    }
    pub fn insert(&mut self, value: V) -> V {
        std::mem::replace(&mut *self.0, value)
    }
    pub fn into_mut(self) -> ValueMut<'a, V> {
        self.0
    }
    pub fn remove(self) -> V {
        let (slot, value) = self.0.into_parts();
        slot.remove();
        value
    }
}
impl<'a, V: ValueEnDe> VacantEntry<'a, V> {
    pub fn insert(self, value: V) -> ValueMut<'a, V> {
        ValueMut::new(self.0.into_slot(), value, true)
    }
}
impl<'a, V: ValueEnDe> Entry<'a, V> {
    pub fn or_insert(self, value: V) -> ValueMut<'a, V> {
        self.or_insert_with(|| value)
    }
    pub fn or_insert_with(self, f: impl FnOnce() -> V) -> ValueMut<'a, V> {
        match self {
            Self::Occupied(e) => e.into_mut(),
            Self::Vacant(e) => e.insert(f()),
        }
    }
    pub fn or_default(self) -> ValueMut<'a, V>
    where
        V: Default,
    {
        self.or_insert_with(V::default)
    }
    pub fn and_modify(mut self, f: impl FnOnce(&mut V)) -> Self {
        if let Self::Occupied(e) = &mut self {
            f(e.get_mut());
        }
        self
    }
}

// Copies each key from the lent cursor row and skips its value bytes.
struct RawKeys<'a>(MapxRawIter<'a>);

impl Iterator for RawKeys<'_> {
    type Item = RawKey;
    fn next(&mut self) -> Option<RawKey> {
        self.0
            .try_step_next()
            .expect("vsdb: mmdb iteration failed")
            .map(|(k, _)| k.to_vec())
    }
}

impl DoubleEndedIterator for RawKeys<'_> {
    fn next_back(&mut self) -> Option<RawKey> {
        self.0
            .try_step_prev()
            .expect("vsdb: mmdb iteration failed")
            .map(|(k, _)| k.to_vec())
    }
}

/// An iterator over the entries of a `MapxOrdRawKey`.
pub struct MapxOrdRawKeyIter<'a, V> {
    inner: MapxRawIter<'a>,
    _p: PhantomData<V>,
}

impl<V> Iterator for MapxOrdRawKeyIter<'_, V>
where
    V: ValueEnDe,
{
    type Item = (RawKey, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(k, v)| (k, <V as ValueEnDe>::decode_value(&v).unwrap()))
    }
}

impl<V> DoubleEndedIterator for MapxOrdRawKeyIter<'_, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (k, <V as ValueEnDe>::decode_value(&v).unwrap()))
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A mutable iterator over the entries of a `MapxOrdRawKey`.
pub struct MapxOrdRawKeyIterMut<'a, V> {
    inner: mapx_raw::MapxRawIterMut<'a>,
    _p: PhantomData<V>,
}

impl<'a, V> Iterator for MapxOrdRawKeyIterMut<'a, V>
where
    V: ValueEnDe,
{
    type Item = (RawKey, ValueIterMut<'a, V>);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| (k, ValueMut::occupied(v)))
    }
}

impl<V> DoubleEndedIterator for MapxOrdRawKeyIterMut<'_, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (k, ValueMut::occupied(v)))
    }
}

pub type ValueIterMut<'a, V> = ValueMut<'a, V>;
