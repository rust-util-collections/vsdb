//!
//! A `Map`-like structure that stores data on disk.
//!
//! This module provides `MapxRaw`, a key-value store that functions like a standard `Map`
//! but with the underlying data persisted to disk. It is "raw" because it does not
//! encode or transform keys and values; they are stored as-is.
//!
//! # Examples
//!
//! ```
//! use vsdb_core::basic::mapx_raw::MapxRaw;
//! use vsdb_core::{vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut m = MapxRaw::new();
//!
//! // Insert key-value pairs
//! m.insert(&[1], &[10]);
//! m.insert(&[2], &[20]);
//! m.insert(&[3], &[30]);
//!
//! // Retrieve a value
//! assert_eq!(m.get(&[2]), Some(vec![20]));
//!
//! // Iterate over the map
//! for (k, v) in m.iter() {
//!     println!("key: {:?}, val: {:?}", k, v);
//! }
//!
//! // Remove a key-value pair
//! m.remove(&[2]);
//! assert!(m.get(&[2]).is_none());
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
    DEFAULT_NS_ID, InstanceId, Namespace, PreBytes, RawKey, RawValue, engine,
    error::{Result, VsdbError},
};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, fs, ops::RangeBounds};

/// An iterator over the entries of a `MapxRaw`.
pub type MapxRawIter<'a> = engine::MapxIter<'a>;
/// A mutable iterator over the entries of a `MapxRaw`.
pub type MapxRawIterMut<'a> = engine::MapxIterMut<'a>;
/// A mutable reference to a value in a `MapxRaw`.
pub type ValueMut<'a> = engine::ValueMut<'a>;
/// A mutable iterator over the values of a `MapxRaw`.
pub type ValueIterMut<'a> = engine::ValueIterMut<'a>;

/// A raw, disk-based, key-value map.
///
/// `MapxRaw` provides a `Map`-like interface for storing and retrieving raw byte slices.
/// It is unversioned and does not perform any encoding on keys or values.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MapxRaw {
    inner: engine::Mapx,
}

impl Serialize for MapxRaw {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MapxRaw {
    /// # Aliasing warning
    ///
    /// Deserialization does **not** create an independent copy — the
    /// restored handle reconnects to the exact same underlying key
    /// range that produced the serialized metadata (the payload is the
    /// instance's raw prefix, not its contents).  This is semantically
    /// equivalent to [`shadow`](MapxRaw::shadow), reachable through
    /// safe code: if the original handle (or any other restore of it)
    /// is still alive in-process, the same SWMR discipline applies
    /// across every live alias — no concurrent writes to the same key
    /// through any handle.  Deserialization is intended to restore a
    /// handle after the original has gone out of scope (e.g. across a
    /// process restart); deserializing while the original is still
    /// live requires the same care as `shadow()`.
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        engine::Mapx::deserialize(deserializer).map(|inner| Self { inner })
    }
}

impl MapxRaw {
    /// Creates a "shadow" copy of the `MapxRaw` instance.
    ///
    /// This method creates a new `MapxRaw` that shares the same underlying data source.
    /// It is a lightweight operation that can be used to create multiple references
    /// to the same map without the overhead of cloning the entire structure.
    ///
    /// # Safety
    ///
    /// This API breaks the semantic safety guarantees of Rust's ownership and
    /// borrowing rules.  The caller must ensure no concurrent writes to the
    /// same key through any handle.  Multiple writers on disjoint keys are
    /// safe.  Concurrent reads alongside writes are safe (the engine provides
    /// snapshot isolation).
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        Self {
            // SAFETY: forwards this fn's `unsafe` contract — the caller
            // guarantees no concurrent writes to the same key through
            // any handle.
            inner: unsafe { self.inner.shadow() },
        }
    }

    /// Creates a new, empty `MapxRaw` in the current ambient namespace
    /// ([`Namespace::current`]; the default namespace unless inside a
    /// [`Namespace::scope`] block).
    #[inline(always)]
    pub fn new() -> Self {
        MapxRaw {
            inner: engine::Mapx::new(),
        }
    }

    /// Creates a new, empty `MapxRaw` placed in `ns` — the explicit
    /// form of the ambient-scope placement performed by
    /// [`new`](Self::new) (naming mirrors `Box::new_in`).
    #[inline(always)]
    pub fn new_in(ns: &Namespace) -> Self {
        MapxRaw {
            inner: engine::Mapx::new_in(ns),
        }
    }

    /// The namespace this map lives in — THE co-location primitive:
    /// `MapxRaw::new_in(&existing.namespace())` places new data
    /// together with `existing`.
    #[inline(always)]
    pub fn namespace(&self) -> Namespace {
        self.inner.namespace()
    }

    /// Deep-copies every entry into a brand-new instance placed in `ns`
    /// — the cross-namespace form of [`Clone`] (`clone()` copies into
    /// the *source's* namespace; `clone_in` chooses the target instead,
    /// mirroring [`new`](Self::new) vs [`new_in`](Self::new_in)).
    ///
    /// The copy runs in bounded chunks (never buffering the whole map
    /// in memory) and needs no atomicity: the target is a brand-new,
    /// unobservable instance until returned.
    ///
    /// # Errors
    ///
    /// If an engine-level write fails.  The partially-written target is
    /// reclaimed with a best-effort O(1) wipe; only if that wipe also
    /// fails is it abandoned as unreferenced, invisible garbage (the
    /// same residue a mid-`clone()` panic leaves behind).
    pub fn clone_in(&self, ns: &Namespace) -> Result<Self> {
        Ok(MapxRaw {
            inner: self.inner.clone_in(ns)?,
        })
    }

    /// Retrieves a value from the map corresponding to the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up.
    ///
    /// # Returns
    ///
    /// An `Option<RawValue>` containing the value if the key exists, or `None` otherwise.
    #[inline(always)]
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<RawValue> {
        self.inner.get(key.as_ref())
    }

    /// Retrieves a mutable reference to a value in the map.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up.
    ///
    /// # Returns
    ///
    /// An `Option<ValueMut<'_>>` containing a mutable reference to the value if the key exists,
    /// or `None` otherwise.
    #[inline(always)]
    pub fn get_mut(&mut self, key: impl AsRef<[u8]>) -> Option<ValueMut<'_>> {
        self.inner.get_mut(key.as_ref())
    }

    /// Mocks a mutable value, typically for use in scenarios where you need to
    /// create a `ValueMut` without actually inserting the value into the map yet.
    ///
    /// # Arguments
    ///
    /// * `key` - The key associated with the value.
    /// * `value` - The value to be mocked.
    ///
    /// # Returns
    ///
    /// A `ValueMut` instance.
    #[inline(always)]
    pub fn mock_value_mut(&mut self, key: RawValue, value: RawValue) -> ValueMut<'_> {
        self.inner.mock_value_mut(key, value)
    }

    /// Checks if the map contains a value for the specified key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check.
    ///
    /// # Returns
    ///
    /// `true` if the map contains the key, `false` otherwise.
    #[inline(always)]
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> bool {
        self.get(key.as_ref()).is_some()
    }

    /// Retrieves the last entry with a key less than or equal to the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to search for.
    ///
    /// # Returns
    ///
    /// An `Option<(RawKey, RawValue)>` containing the key-value pair if found, or `None` otherwise.
    #[inline(always)]
    pub fn get_le(&self, key: impl AsRef<[u8]>) -> Option<(RawKey, RawValue)> {
        self.range(..=Cow::Borrowed(key.as_ref())).next_back()
    }

    /// Retrieves the first entry with a key greater than or equal to the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to search for.
    ///
    /// # Returns
    ///
    /// An `Option<(RawKey, RawValue)>` containing the key-value pair if found, or `None` otherwise.
    #[inline(always)]
    pub fn get_ge(&self, key: impl AsRef<[u8]>) -> Option<(RawKey, RawValue)> {
        self.range(Cow::Borrowed(key.as_ref())..).next()
    }

    /// Gets an entry for the given key, allowing for in-place modification.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the entry.
    ///
    /// # Returns
    ///
    /// An `Entry` that allows for operations on the value.
    #[inline(always)]
    pub fn entry<'a>(&'a mut self, key: &'a [u8]) -> Entry<'a> {
        Entry { key, hdr: self }
    }

    /// Returns an iterator over the map's entries.
    ///
    /// # Returns
    ///
    /// A `MapxRawIter` that iterates over the key-value pairs.
    #[inline(always)]
    pub fn iter(&self) -> MapxRawIter<'_> {
        self.inner.iter()
    }

    /// Returns an iterator over a range of entries in the map.
    ///
    /// # Arguments
    ///
    /// * `bounds` - The range of keys to iterate over.
    ///
    /// # Returns
    ///
    /// A `MapxRawIter` that iterates over the key-value pairs in the specified range.
    #[inline(always)]
    pub fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        bounds: R,
    ) -> MapxRawIter<'a> {
        self.inner.range(bounds)
    }

    /// Returns a detached iterator over a range of entries in the map.
    ///
    /// This iterator is not tied to the lifetime of `&self`, allowing for concurrent
    /// modification of the map during iteration (though the iterator will see a snapshot).
    ///
    /// # Arguments
    ///
    /// * `bounds` - The range of keys to iterate over.
    ///
    /// # Returns
    ///
    /// A `MapxRawIter` that iterates over the key-value pairs in the specified range.
    #[inline(always)]
    pub fn range_detached<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &self,
        bounds: R,
    ) -> MapxRawIter<'a> {
        self.inner.range_detached(bounds)
    }

    /// Returns a mutable iterator over the map's entries.
    ///
    /// # Returns
    ///
    /// A `MapxRawIterMut` that allows for mutable iteration over the key-value pairs.
    #[inline(always)]
    pub fn iter_mut(&mut self) -> MapxRawIterMut<'_> {
        self.inner.iter_mut()
    }

    /// Returns a mutable iterator over a range of entries in the map.
    ///
    /// # Arguments
    ///
    /// * `bounds` - The range of keys to iterate over.
    ///
    /// # Returns
    ///
    /// A `MapxRawIterMut` that allows for mutable iteration over the key-value pairs in the specified range.
    #[inline(always)]
    pub fn range_mut<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a mut self,
        bounds: R,
    ) -> MapxRawIterMut<'a> {
        self.inner.range_mut(bounds)
    }

    /// Retrieves the last entry in the map.
    ///
    /// # Returns
    ///
    /// An `Option<(RawKey, RawValue)>` containing the last key-value pair, or `None` if the map is empty.
    #[inline(always)]
    pub fn last(&self) -> Option<(RawKey, RawValue)> {
        self.iter().next_back()
    }

    /// Inserts a key-value pair into the map.
    ///
    /// Does not return the old value for performance reasons.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert.
    /// * `value` - The value to associate with the key.
    #[inline(always)]
    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        self.inner.insert(key.as_ref(), value.as_ref())
    }

    /// Removes a key from the map.
    ///
    /// Does not return the old value for performance reasons.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove.
    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) {
        self.inner.remove(key.as_ref())
    }

    /// Marks a key for deferred removal via the compaction filter.
    ///
    /// The key remains readable until the underlying storage engine
    /// compacts the relevant level.  Use this for bulk cleanup (e.g.
    /// garbage collection) where immediate visibility is not required.
    ///
    /// Registrations are **not crash-durable**: mmdb holds dead-key
    /// registrations in memory only, so keys still pending at process
    /// exit survive the restart.  Callers must be able to re-register
    /// (e.g. by re-running a ref-count rebuild) after recovery.
    #[doc(hidden)]
    #[inline(always)]
    pub fn lazy_delete(&self, key: impl AsRef<[u8]>) {
        self.inner.lazy_delete(key.as_ref())
    }

    /// Batch version of [`lazy_delete`](Self::lazy_delete).
    #[doc(hidden)]
    #[inline(always)]
    pub fn lazy_delete_batch(&self, keys: impl IntoIterator<Item = impl AsRef<[u8]>>) {
        self.inner.lazy_delete_batch(keys)
    }

    /// Start a batch operation.
    ///
    /// This method allows you to perform multiple insert/remove operations
    /// and commit them atomically.
    ///
    /// A failed [`commit`](crate::common::BatchTrait::commit) consumes the
    /// buffered operations (none are applied) and is not retryable —
    /// re-stage the operations on a fresh batch instead.
    ///
    /// # Examples
    ///
    /// ```
    /// use vsdb_core::basic::mapx_raw::MapxRaw;
    /// use vsdb_core::vsdb_set_base_dir;
    ///
    /// vsdb_set_base_dir("/tmp/vsdb_core_mapx_raw_batch_entry").unwrap();
    /// let mut map = MapxRaw::new();
    ///
    /// {
    ///     let mut batch = map.batch_entry();
    ///     batch.insert(&[1], &[10]);
    ///     batch.insert(&[2], &[20]);
    ///     batch.commit().unwrap();
    /// }
    ///
    /// assert_eq!(map.get(&[1]), Some(vec![10]));
    /// assert_eq!(map.get(&[2]), Some(vec![20]));
    /// ```
    #[inline(always)]
    pub fn batch_entry(&mut self) -> Box<dyn crate::common::BatchTrait + '_> {
        self.inner.batch_begin()
    }

    /// Start a batch operation pre-staged with the removal of **every**
    /// existing entry of this map (one engine-level range tombstone).
    ///
    /// Operations added afterwards apply on top of the wipe, and the
    /// whole set — wipe included — commits in one atomic engine write
    /// batch: observers see either the pre-batch state or the fully
    /// applied result, never anything in between (even across a crash).
    ///
    /// The wipe belongs to the buffered operations: like them, it is
    /// consumed by the first [`commit`](crate::common::BatchTrait::commit)
    /// and does not re-arm afterwards.
    ///
    /// # Examples
    ///
    /// ```
    /// use vsdb_core::basic::mapx_raw::MapxRaw;
    /// use vsdb_core::vsdb_set_base_dir;
    ///
    /// vsdb_set_base_dir("/tmp/vsdb_core_mapx_raw_batch_entry_wiped").unwrap();
    /// let mut map = MapxRaw::new();
    /// map.insert([1], [10]);
    ///
    /// {
    ///     let mut batch = map.batch_entry_wiped();
    ///     batch.insert(&[2], &[20]);
    ///     batch.commit().unwrap();
    /// }
    ///
    /// assert_eq!(map.get(&[1]), None);
    /// assert_eq!(map.get(&[2]), Some(vec![20]));
    /// ```
    #[inline(always)]
    pub fn batch_entry_wiped(&mut self) -> Box<dyn crate::common::BatchTrait + '_> {
        self.inner.batch_begin_wiped()
    }

    /// Clears the map, removing all key-value pairs.
    ///
    /// The wipe is a single atomic engine write batch (one range
    /// tombstone): all-or-nothing, even across a crash.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Reconstructs a `MapxRaw` from the 8-byte prefix previously
    /// obtained via [`as_bytes`](Self::as_bytes), bound to the current
    /// ambient namespace ([`Namespace::current`]).
    ///
    /// # Safety
    ///
    /// The caller must ensure that `s` encodes a prefix they have unique
    /// ownership of and that the *ambient namespace's* engine still
    /// contains the data for this prefix (a raw prefix carries no
    /// namespace information of its own).  Passing arbitrary bytes is
    /// undefined behavior.
    #[inline(always)]
    pub unsafe fn from_bytes(s: impl AsRef<[u8]>) -> Self {
        Self {
            // SAFETY: forwards this fn's `unsafe` contract — the caller
            // guarantees `s` encodes a uniquely-owned prefix and that the
            // backing data still exists in the ambient namespace.
            inner: unsafe { engine::Mapx::from_prefix_slice(s) },
        }
    }

    /// [`from_bytes`](Self::from_bytes) bound to an explicit namespace.
    ///
    /// # Safety
    ///
    /// Same contract as `from_bytes`, with the data required to live in
    /// `ns`'s engine.
    #[inline(always)]
    pub unsafe fn from_bytes_in(ns: &Namespace, s: impl AsRef<[u8]>) -> Self {
        Self {
            // SAFETY: forwards this fn's `unsafe` contract.
            inner: unsafe { engine::Mapx::from_prefix_slice_in(ns, s) },
        }
    }

    /// Returns the 8-byte prefix that uniquely identifies this map's
    /// storage prefix range.
    #[inline(always)]
    pub fn as_bytes(&self) -> &PreBytes {
        self.inner.as_prefix_slice()
    }

    /// Returns the complete public identity of this `MapxRaw`:
    /// `{ map_id, ns }` — the same shape as the persisted meta bytes
    /// (`ns: None` ⇔ default namespace).  A pre-v16 bare `u64` id equals
    /// `InstanceId::from(u64)`.
    pub fn instance_id(&self) -> InstanceId {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.as_bytes());
        InstanceId::new(u64::from_le_bytes(bytes), self.inner.namespace().id())
    }

    /// Checks if this `MapxRaw` instance is the same as another.
    ///
    /// # Arguments
    ///
    /// * `other_hdr` - The other `MapxRaw` to compare against.
    ///
    /// # Returns
    ///
    /// `true` if both instances refer to the same underlying data, `false` otherwise.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }

    /// Persists this instance's metadata into its owning namespace's
    /// instance-meta directory so that it can be recovered later via
    /// [`from_meta`](Self::from_meta).
    ///
    /// Returns the [`InstanceId`] that can be passed to `from_meta`.
    pub fn save_meta(&self) -> Result<InstanceId> {
        let id = self.instance_id();
        let path = self.namespace().meta_path(id.map_id);
        fs::create_dir_all(path.parent().expect("has parent"))?;
        crate::common::atomic_write_file(&path, &self.inner.encode_prefix_meta())?;
        Ok(id)
    }

    /// Recovers a `MapxRaw` instance from previously saved metadata.
    ///
    /// Accepts anything convertible into an [`InstanceId`] — including a
    /// bare pre-v16 `u64` (⇒ default namespace). Resolution is
    /// deterministic, never a search: the token's `ns` names the meta
    /// directory (`None` ⇒ the default namespace's — its id is a fixed
    /// constant, there is nothing to look up); a miss is a clean error.
    ///
    /// The caller must ensure that the underlying VSDB database still
    /// contains the data referenced by this instance ID.
    pub fn from_meta(instance_id: impl Into<InstanceId>) -> Result<Self> {
        let id = instance_id.into();
        let id = InstanceId::new(id.map_id, id.ns.unwrap_or(DEFAULT_NS_ID));
        let ns = match id.ns {
            None => Namespace::default_ns(),
            Some(n) => Namespace::open(n)?,
        };
        let bytes = fs::read(ns.meta_path(id.map_id))?;
        let (prefix, embedded_ns) = engine::Mapx::decode_prefix_meta(&bytes)?;
        let found = InstanceId::new(
            u64::from_le_bytes(prefix),
            embedded_ns.unwrap_or(DEFAULT_NS_ID),
        );
        if found != id {
            return Err(VsdbError::Decode {
                detail: format!(
                    "metadata identity mismatch: requested {id}, payload names {found}"
                ),
            });
        }
        engine::Mapx::from_prefix_meta(&bytes).map(|inner| Self { inner })
    }
}

impl Default for MapxRaw {
    /// Creates a new, empty `MapxRaw`.
    ///
    /// # Returns
    ///
    /// A new `MapxRaw` instance.
    fn default() -> Self {
        Self::new()
    }
}

/// A view into a single entry in a map, which may either be vacant or occupied.
pub struct Entry<'a> {
    key: &'a [u8],
    hdr: &'a mut MapxRaw,
}

impl<'a> Entry<'a> {
    /// Ensures a value is in the entry by inserting the default if empty, and returns
    /// a mutable reference to the value.
    ///
    /// # Arguments
    ///
    /// * `default` - The default value to insert if the entry is empty.
    ///
    /// # Returns
    ///
    /// A `ValueMut` to the value in the entry.
    pub fn or_insert(self, default: &'a [u8]) -> ValueMut<'a> {
        let hdr = self.hdr as *mut MapxRaw;
        // SAFETY: `hdr` is derived from `self.hdr: &'a mut MapxRaw`.
        // The two dereferences are in mutually exclusive match arms and
        // never coexist; no aliasing occurs.
        match unsafe { &mut *hdr }.get_mut(self.key) {
            Some(v) => v,
            _ => {
                unsafe { &mut *hdr }.mock_value_mut(self.key.to_vec(), default.to_vec())
            }
        }
    }

    /// Ensures a value is in the entry by inserting the result of a function if empty,
    /// and returns a mutable reference to the value.
    ///
    /// # Arguments
    ///
    /// * `f` - A closure that returns the default value to insert if the entry is empty.
    ///
    /// # Returns
    ///
    /// A `ValueMut` to the value in the entry.
    pub fn or_insert_with<F>(self, f: F) -> ValueMut<'a>
    where
        F: FnOnce() -> RawValue,
    {
        let hdr = self.hdr as *mut MapxRaw;
        // SAFETY: `hdr` is derived from `self.hdr: &'a mut MapxRaw`.
        // The two dereferences are in mutually exclusive match arms and
        // never coexist; no aliasing occurs.
        match unsafe { &mut *hdr }.get_mut(self.key) {
            Some(v) => v,
            _ => unsafe { &mut *hdr }.mock_value_mut(self.key.to_vec(), f()),
        }
    }
}
