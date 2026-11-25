//!
//! A `BTreeMap`-like structure that stores data on disk.
//!
//! `MapxOrd` provides an ordered map where keys and values are encoded before
//! being persisted. Keys are encoded using `KeyEnDeOrdered` to ensure that
//! a lexicographical ordering of the encoded bytes maintains the original order of the keys.
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::mapx_ord::MapxOrd;
//! use vsdb::{VsdbOptions, vsdb_configure, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_configure(VsdbOptions::new(&dir)).unwrap();
//!
//! let mut m: MapxOrd<u32, String> = MapxOrd::new();
//!
//! // Insert key-value pairs
//! m.insert(&1, &"hello".to_string());
//! m.insert(&2, &"world".to_string());
//!
//! // Retrieve a value
//! assert_eq!(m.get(&1), Some("hello".to_string()));
//!
//! // Iterate over the map
//! for (k, v) in m.iter() {
//!     println!("key: {}, val: {}", k, v);
//! }
//!
//! // Remove a key-value pair
//! m.remove(&2);
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

pub use super::mapx_ord_rawkey::{Entry, OccupiedEntry, VacantEntry};
use crate::{
    basic::mapx_ord_rawkey::{
        MapxOrdRawKey, MapxOrdRawKeyBatch, MapxOrdRawKeyIter, MapxOrdRawKeyIterMut,
        ValueIterMut, ValueMut,
    },
    common::{
        ende::{
            KeyEnDeOrdered, OrderedBoundEndpoint, OrderedKeyLookup, OrderedRangeBounds,
            ValueEnDe,
        },
        error::Result,
        macros::define_map_wrapper,
    },
};
use std::marker::PhantomData;
define_map_wrapper! {
    #[doc = "A disk-based, `BTreeMap`-like data structure with typed, ordered keys and values."]
    #[doc = ""]
    #[doc = "`MapxOrd` stores key-value pairs on disk, ensuring that the keys are ordered."]
    pub struct MapxOrd<K, V> {
        inner: MapxOrdRawKey<V>,
        _p: PhantomData<K>,
    }
    where K: KeyEnDeOrdered, V: ValueEnDe
}

impl<K, V> MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Retrieves a value from the map for a given key.
    ///
    /// The key may be any borrowed form of `K` (`&str` for `String` keys,
    /// `&[u8]` for `Vec<u8>` keys), like `BTreeMap::get`.
    #[inline(always)]
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: OrderedKeyLookup<Q>,
        Q: ?Sized,
    {
        K::with_ordered_lookup(key, |bytes| self.inner.get(bytes)).flatten()
    }

    /// Retrieves a mutable reference to a value in the map (borrowed key
    /// forms as in [`get`](Self::get)).
    #[inline(always)]
    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<ValueMut<'_, V>>
    where
        K: OrderedKeyLookup<Q>,
        Q: ?Sized,
    {
        K::with_ordered_lookup(key, |bytes| self.inner.get_mut(bytes)).flatten()
    }

    /// Edits a present value and returns any encoding or storage error.
    pub fn update<Q: ?Sized, R>(
        &mut self,
        key: &Q,
        f: impl FnOnce(&mut V) -> R,
    ) -> Result<Option<R>>
    where
        K: OrderedKeyLookup<Q>,
    {
        let Some(mut guard) = self.get_mut(key) else {
            return Ok(None);
        };
        let result = f(&mut guard);
        guard.commit()?;
        Ok(Some(result))
    }

    /// Checks if the map contains a value for the specified key (borrowed
    /// key forms as in [`get`](Self::get)).
    #[inline(always)]
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: OrderedKeyLookup<Q>,
        Q: ?Sized,
    {
        K::with_ordered_lookup(key, |bytes| self.inner.contains_key(bytes))
            .unwrap_or(false)
    }

    /// Retrieves the last entry with a key less than or equal to the given
    /// key (borrowed key forms as in [`get`](Self::get)).
    ///
    /// ```compile_fail
    /// let map = vsdb::MapxOrd::<[u8; 64], u64>::new();
    /// map.get_le(&[0u8; 4][..]); // An array boundary needs the full width.
    /// ```
    #[inline(always)]
    pub fn get_le<E>(&self, key: E) -> Option<(K, V)>
    where
        K: OrderedBoundEndpoint<E>,
    {
        K::with_endpoint(&key, |bytes| self.inner.get_le(bytes))
            .map(|(k, v)| (K::from_bytes(k).unwrap(), v))
    }

    /// Retrieves the first entry with a key greater than or equal to the
    /// given key (borrowed key forms as in [`get`](Self::get)).
    #[inline(always)]
    pub fn get_ge<E>(&self, key: E) -> Option<(K, V)>
    where
        K: OrderedBoundEndpoint<E>,
    {
        K::with_endpoint(&key, |bytes| self.inner.get_ge(bytes))
            .map(|(k, v)| (K::from_bytes(k).unwrap(), v))
    }

    /// Inserts a key-value pair into the map.
    ///
    /// Does not return the old value for performance reasons.
    #[inline(always)]
    pub fn insert(&mut self, key: &K, value: &V) {
        key.with_ordered_bytes(|k| self.inner.insert(k, value))
    }

    /// Gets an entry for a given key, allowing for in-place modification.
    ///
    /// # Panics
    /// A slice addressing a fixed-array key must have the array's exact length.
    #[inline(always)]
    pub fn entry<Q: ?Sized>(&mut self, key: &Q) -> Entry<'_, V>
    where
        K: OrderedKeyLookup<Q>,
    {
        K::with_ordered_lookup(key, |bytes| self.inner.entry(bytes))
            .expect("invalid key length")
    }

    /// Returns an iterator over the map's entries.
    #[inline(always)]
    pub fn iter(&self) -> MapxOrdIter<'_, K, V> {
        MapxOrdIter {
            inner: self.inner.iter(),
            _p: PhantomData,
        }
    }

    /// Returns a mutable iterator over the map's entries.
    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxOrdIterMut<'_, K, V> {
        MapxOrdIterMut {
            inner: self.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    /// Returns an iterator over the map's values.
    #[inline(always)]
    pub fn values(&self) -> MapxOrdValues<'_, V> {
        MapxOrdValues {
            inner: self.inner.iter(),
        }
    }

    /// Returns a mutable iterator over the map's values.
    #[inline(always)]
    pub fn values_mut(&mut self) -> MapxOrdValuesMut<'_, V> {
        MapxOrdValuesMut {
            inner: self.inner.iter_mut(),
            _p: PhantomData,
        }
    }

    /// Returns an iterator over a range of entries in the map.
    #[inline(always)]
    pub fn range<R: OrderedRangeBounds<K>>(&self, bounds: R) -> MapxOrdIter<'_, K, V> {
        let (l, h) = bounds.ordered_bounds();

        MapxOrdIter {
            inner: self.inner.range((l, h)),
            _p: PhantomData,
        }
    }

    /// Returns a mutable iterator over a range of entries in the map.
    #[inline(always)]
    pub fn range_mut<R: OrderedRangeBounds<K>>(
        &mut self,
        bounds: R,
    ) -> MapxOrdIterMut<'_, K, V> {
        let (l, h) = bounds.ordered_bounds();

        MapxOrdIterMut {
            inner: self.inner.range_mut((l, h)),
            _p: PhantomData,
        }
    }

    /// Retrieves the first entry in the map.
    #[inline(always)]
    pub fn first(&self) -> Option<(K, V)> {
        self.iter().next()
    }

    /// Retrieves the last entry in the map.
    #[inline(always)]
    pub fn last(&self) -> Option<(K, V)> {
        self.iter().next_back()
    }

    /// Removes a key from the map (borrowed key forms as in
    /// [`get`](Self::get)).
    ///
    /// Does not return the old value for performance reasons.
    #[inline(always)]
    pub fn remove<Q>(&mut self, key: &Q)
    where
        K: OrderedKeyLookup<Q>,
        Q: ?Sized,
    {
        K::with_ordered_lookup(key, |bytes| self.inner.remove(bytes));
    }

    /// Start a batch operation.
    ///
    /// This method allows you to perform multiple insert/remove operations
    /// and commit them atomically.
    ///
    /// A failed [`commit`](MapxOrdBatch::commit) consumes
    /// the buffered operations. Preflight rejections leave them unapplied;
    /// persistence errors can be ambiguous after recovery and must not be
    /// retried automatically.
    ///
    /// # Examples
    ///
    /// ```
    /// use vsdb::basic::mapx_ord::MapxOrd;
    /// use vsdb::{VsdbOptions, vsdb_configure};
    ///
    /// let dir = std::env::temp_dir()
    ///     .join(format!("vsdb_mapx_ord_batch_{}", rand::random::<u128>()));
    /// vsdb_configure(VsdbOptions::new(&dir)).unwrap();
    /// let mut map: MapxOrd<u32, String> = MapxOrd::new();
    ///
    /// let mut batch = map.batch();
    /// batch.insert(&1, &"one".to_string());
    /// batch.insert(&2, &"two".to_string());
    /// batch.commit().unwrap();
    ///
    /// assert_eq!(map.get(&1), Some("one".to_string()));
    /// assert_eq!(map.get(&2), Some("two".to_string()));
    /// # std::fs::remove_dir_all(dir).unwrap();
    /// ```
    #[inline(always)]
    pub fn batch(&mut self) -> MapxOrdBatch<'_, K, V> {
        MapxOrdBatch {
            inner: self.inner.batch(),
            _marker: PhantomData,
        }
    }

    /// Returns an iterator over the map's keys in ascending order.
    ///
    /// Decodes only `K` — unlike `iter().map(|(k, _)| k)`, the value
    /// bytes are never decoded and discarded (see
    /// `MapxOrdRawKey::keys`'s doc comment for why this matters for
    /// nested-VSDB-collection value types).
    #[inline(always)]
    pub fn keys(&self) -> impl DoubleEndedIterator<Item = K> + '_ {
        self.inner.keys().map(|k| K::from_bytes(k).unwrap())
    }
}

impl<'a, K, V> IntoIterator for &'a MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = (K, V);
    type IntoIter = MapxOrdIter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

// Hand-written rather than `#[derive(PartialEq, Eq)]`: a derive would
// compare the *encoded* bytes of `V` (postcard encoding is not
// injective on decoded value in general — e.g. `0.0_f64` and
// `-0.0_f64` encode to different bytes despite comparing equal, while
// distinct NaN bit patterns can compare unequal despite identical
// bytes), silently diverging from `V`'s own logical `PartialEq`.
// Comparing through `self.inner` (raw key bytes + *decoded* `V`) needs
// no `K: PartialEq` bound — `KeyEnDeOrdered` already requires `Eq`, but
// comparing encoded bytes directly avoids a decode round-trip, matching
// this file's existing preference for skipping unnecessary `K` decodes
// (see `keys()`'s doc comment above).
impl<K, V> PartialEq for MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner.iter().eq(other.inner.iter())
    }
}

impl<K, V> Eq for MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe + Eq,
{
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<'a, K, V> IntoIterator for &'a mut MapxOrd<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = (K, ValueIterMut<'a, V>);
    type IntoIter = MapxOrdIterMut<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}

/// A batch entry for `MapxOrd`.
pub struct MapxOrdBatch<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyBatch<'a, V>,
    _marker: PhantomData<K>,
}

impl<'a, K, V> MapxOrdBatch<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Insert a key-value pair into the batch.
    pub fn insert(&mut self, key: &K, value: &V) {
        key.with_ordered_bytes(|k| self.inner.insert(k, value));
    }

    /// Remove a key in the batch.
    pub fn remove(&mut self, key: &K) {
        key.with_ordered_bytes(|k| self.inner.remove(k));
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

/// An iterator over the entries of a `MapxOrd`.
pub struct MapxOrdIter<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyIter<'a, V>,
    _p: PhantomData<K>,
}

impl<K, V> Iterator for MapxOrdIter<'_, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(k, v)| (K::from_bytes(k).unwrap(), v))
    }
}

impl<K, V> DoubleEndedIterator for MapxOrdIter<'_, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (K::from_bytes(k).unwrap(), v))
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// An iterator over the values of a `MapxOrd`.
pub struct MapxOrdValues<'a, V>
where
    V: ValueEnDe,
{
    /// The inner iterator over raw key-value pairs.
    pub(crate) inner: MapxOrdRawKeyIter<'a, V>,
}

impl<V> Iterator for MapxOrdValues<'_, V>
where
    V: ValueEnDe,
{
    type Item = V;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

impl<V> DoubleEndedIterator for MapxOrdValues<'_, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(_, v)| v)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A mutable iterator over the values of a `MapxOrd`.
pub struct MapxOrdValuesMut<'a, V>
where
    V: ValueEnDe,
{
    pub(crate) inner: MapxOrdRawKeyIterMut<'a, V>,
    pub(crate) _p: PhantomData<V>,
}

impl<'a, V> Iterator for MapxOrdValuesMut<'a, V>
where
    V: ValueEnDe,
{
    type Item = ValueIterMut<'a, V>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(_, v)| v)
    }
}

impl<V> DoubleEndedIterator for MapxOrdValuesMut<'_, V>
where
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|(_, v)| v)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A mutable iterator over the entries of a `MapxOrd`.
pub struct MapxOrdIterMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    inner: MapxOrdRawKeyIterMut<'a, V>,
    _p: PhantomData<(K, V)>,
}

impl<'a, K, V> Iterator for MapxOrdIterMut<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    type Item = (K, ValueIterMut<'a, V>);
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(k, v)| (<K as KeyEnDeOrdered>::from_bytes(k).unwrap(), v))
    }
}

impl<K, V> DoubleEndedIterator for MapxOrdIterMut<'_, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner
            .next_back()
            .map(|(k, v)| (<K as KeyEnDeOrdered>::from_bytes(k).unwrap(), v))
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
