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
//! use vsdb_core::{VsdbOptions, vsdb_configure, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = std::env::temp_dir().join(format!("vsdb-{}", rand::random::<u128>()));
//! vsdb_configure(VsdbOptions::new(&dir)).unwrap();
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
    DEFAULT_NS_ID, InstanceId, Namespace, RawKey, RawRangeBounds, RawValue, engine,
    error::{Result, VsdbError},
};
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, fs};

/// An iterator over the entries of a `MapxRaw`.
pub type MapxRawIter<'a> = engine::MapxIter<'a>;
/// A read-only view whose queries share one captured committed state.
pub type MapxRawReadView<'a> = engine::MapxReadView<'a>;

/// A read-only handle sharing a map's identity without exposing mutation.
///
/// Cloning this reader shares the same map; cloning `MapxRaw` still deep-copies
/// data. The reader retains namespace ownership, so close is refused while
/// it remains alive. Views borrow the reader and keep their snapshot registered.
///
/// ```compile_fail
/// use vsdb_core::MapxRaw;
/// let map = MapxRaw::new();
/// let mut reader = map.reader();
/// reader.insert(b"key", b"value"); // No mutable capability.
/// ```
#[derive(Clone, Debug)]
pub struct MapxRawReader {
    inner: engine::MapxReader,
}

impl MapxRawReader {
    /// Identity shared with the writable map.
    pub fn instance_id(&self) -> InstanceId {
        self.inner.instance_id()
    }

    /// Owning namespace, retained for the lifetime of the reader.
    pub fn namespace(&self) -> Namespace {
        self.inner.namespace()
    }

    /// Reads the latest committed value. Fatal engine errors panic.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<RawValue> {
        self.inner.get(key.as_ref())
    }

    /// Decode a visible value from pinned engine bytes. The decoder receives
    /// empty values as an empty slice and is skipped when the key is absent.
    pub fn try_get_with<T>(
        &self,
        key: impl AsRef<[u8]>,
        decode: impl FnOnce(&[u8]) -> Result<T>,
    ) -> Result<Option<T>> {
        self.inner.try_get_with(key.as_ref(), decode)
    }

    /// Captures one committed state for repeated queries.
    pub fn read_view(&self) -> MapxRawReadView<'_> {
        self.inner.read_view()
    }
}
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
    /// Creates an immutable reader without introducing a mutable alias.
    pub fn reader(&self) -> MapxRawReader {
        MapxRawReader {
            inner: self.inner.reader(),
        }
    }

    /// Captures one state for point and range reads. The view borrows this map.
    /// Use `reader()` first when the original handle must remain writable.
    /// Long-lived views retain old versions and can delay reclamation.
    ///
    /// ```compile_fail
    /// use vsdb_core::MapxRaw;
    /// let view = {
    ///     let map = MapxRaw::new();
    ///     map.read_view()
    /// };
    /// let _ = view.get(b"key"); // The owner cannot be dropped first.
    /// ```
    pub fn read_view(&self) -> MapxRawReadView<'_> {
        self.inner.read_view()
    }

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
    /// safe. Concurrent immutable reads and iteration alongside writes are
    /// supported. Each iterator retains the committed view captured when it
    /// is created, including across later writes, clears and flushes. Separate
    /// reads/iterators can observe different views. Mutable iteration remains
    /// a write operation and requires the same exclusion of other writers.
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
    ///
    /// # Panics
    ///
    /// Panics in read-only mode. Restore an existing handle with
    /// [`from_meta`](Self::from_meta) instead.
    #[inline(always)]
    pub fn new() -> Self {
        MapxRaw {
            inner: engine::Mapx::new(),
        }
    }

    /// Creates a new, empty `MapxRaw` placed in `ns` — the explicit
    /// form of the ambient-scope placement performed by
    /// [`new`](Self::new) (naming mirrors `Box::new_in`).
    ///
    /// # Panics
    ///
    /// Panics if `ns` is read-only.
    #[inline(always)]
    pub fn new_in(ns: &Namespace) -> Self {
        MapxRaw {
            inner: engine::Mapx::new_in(ns),
        }
    }

    /// The namespace (engine placement group) this map lives in:
    /// `MapxRaw::new_in(&existing.namespace())` places a new map in the
    /// same namespace as `existing` (use [`new_colocated`](Self::new_colocated)
    /// when two maps must also share one shard WAL).
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
    /// Returns [`VsdbError::ReadOnly`] if `ns` is read-only, or an
    /// engine-level write error. The partially-written target is
    /// reclaimed with a best-effort O(1) wipe; only if that wipe also
    /// fails is it abandoned as unreferenced, invisible garbage (the
    /// same residue a mid-`clone()` panic leaves behind).
    pub fn clone_in(&self, ns: &Namespace) -> Result<Self> {
        Ok(MapxRaw {
            inner: self.inner.clone_in(ns)?,
        })
    }

    /// Creates a fresh, empty map in this map's namespace whose storage
    /// routes to the **same engine shard**.
    ///
    /// Co-located maps share one write-ahead log, so their combined write
    /// order is also their crash order: after a crash, the surviving
    /// writes across all of them are a prefix of the order in which they
    /// were issued. A composite structure built from co-located maps can
    /// therefore rely on program order where it would otherwise need a
    /// WAL fence between maps.
    ///
    /// # Panics
    ///
    /// Panics in read-only mode (no prefix can be allocated).
    pub fn new_colocated(&self) -> Self {
        MapxRaw {
            inner: self.inner.new_colocated(),
        }
    }

    /// Whether `self` and `other` are co-located: same namespace, same
    /// engine shard (see [`new_colocated`](Self::new_colocated)).
    pub fn is_colocated_with(&self, other: &Self) -> bool {
        self.inner.is_colocated_with(&other.inner)
    }

    /// Inserts rows in two maps with one asynchronous WAL batch. The maps
    /// must belong to the same open engine and route to the same shard.
    /// Keys are local to their respective map. Repeated keys take the value
    /// of the last put. When two handles name the same map, all `here` puts
    /// precede all `there` puts in that prefix. A rejected placement writes no rows.
    pub fn insert_colocated(
        &mut self,
        other: &mut Self,
        here: &[(&[u8], &[u8])],
        there: &[(&[u8], &[u8])],
    ) -> Result<()> {
        let ns = self.namespace();
        if ns.is_read_only() {
            return Err(VsdbError::ReadOnly {
                operation: "co-located insert",
            });
        }
        if !std::ptr::eq(ns.engine(), other.namespace().engine()) {
            return Err(VsdbError::InvalidConfig {
                detail: "co-located insert requires maps in one namespace".into(),
            });
        }
        let first = self.try_instance_id()?.map_id.to_le_bytes();
        let second = other.try_instance_id()?.map_id.to_le_bytes();
        ns.engine().insert_colocated(first, second, here, there)
    }

    /// Deep-copies every entry into a fresh instance co-located with
    /// `anchor` — the co-located form of [`Clone`], with the same chunked
    /// copy and error cleanup as [`clone_in`](Self::clone_in).
    ///
    /// # Errors
    ///
    /// Returns [`VsdbError::ReadOnly`] in read-only mode, or an
    /// engine-level write error (the partial target is wiped as in
    /// `clone_in`).
    pub fn clone_colocated(&self, anchor: &Self) -> Result<Self> {
        Ok(MapxRaw {
            inner: self.inner.clone_colocated(&anchor.inner)?,
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

    /// Decode a visible value from pinned engine bytes. The decoder receives
    /// empty values as an empty slice and is skipped when the key is absent.
    /// First use of a fresh map can reserve its prefix and return an allocator error.
    pub fn try_get_with<T>(
        &self,
        key: impl AsRef<[u8]>,
        decode: impl FnOnce(&[u8]) -> Result<T>,
    ) -> Result<Option<T>> {
        self.inner.try_get_with(key.as_ref(), decode)
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

    #[doc(hidden)]
    pub fn try_value_mut(
        &mut self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<ValueMut<'_>>> {
        self.inner.try_get_mut(key.as_ref())
    }

    /// Edits a present value and reports any storage error during write-back.
    pub fn update<R>(
        &mut self,
        key: impl AsRef<[u8]>,
        edit: impl FnOnce(&mut RawValue) -> R,
    ) -> Result<Option<R>> {
        if self.namespace().is_read_only() {
            return Err(VsdbError::ReadOnly {
                operation: "value update",
            });
        }
        let Some(mut guard) = self.try_value_mut(key)? else {
            return Ok(None);
        };
        let result = edit(&mut guard);
        guard.commit()?;
        Ok(Some(result))
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
        self.try_contains_key(key)
            .expect("vsdb: mmdb existence check failed")
    }

    /// Check the latest committed state without materializing a copy of the value.
    /// First use can reserve this map's prefix. Allocation and storage failures
    /// retain their source and classification.
    pub fn try_contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        self.inner.try_contains_key(key.as_ref())
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
    pub fn entry(&mut self, key: &[u8]) -> Entry<'_> {
        match self.get(key) {
            Some(value) => Entry::Occupied(OccupiedEntry(self.inner.value_slot(
                key.to_vec(),
                value,
                false,
            ))),
            None => Entry::Vacant(VacantEntry {
                key: key.to_vec(),
                hdr: self,
            }),
        }
    }

    /// Returns an iterator over the map's entries.
    ///
    /// The iterator retains its creation-time committed view. Later writes
    /// through another handle do not change the entries it returns.
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
    /// Retains one creation-time committed view, like [`iter`](Self::iter).
    ///
    /// # Arguments
    ///
    /// * `bounds` - The range of keys to iterate over.
    ///
    /// # Returns
    ///
    /// A `MapxRawIter` that iterates over the key-value pairs in the specified range.
    #[inline(always)]
    pub fn range<'a, R: RawRangeBounds>(&'a self, bounds: R) -> MapxRawIter<'a> {
        self.inner.range(bounds)
    }

    /// Opens a fallible cursor over the given bounds. Use its lending steps
    /// to propagate late storage errors while streaming each borrowed row.
    pub fn try_range<R: RawRangeBounds>(&self, bounds: R) -> Result<MapxRawIter<'_>> {
        self.inner.try_range(bounds)
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
    pub fn range_detached<'a, R: RawRangeBounds>(&self, bounds: R) -> MapxRawIter<'a> {
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
    pub fn range_mut<'a, R: RawRangeBounds>(
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
    /// # Size limits
    ///
    /// The storage engine validates every write before WAL/memtable
    /// admission: keys are capped at 8 MiB and whole entries
    /// (key + value) at ~64 MiB. A direct `insert` treats engine
    /// rejection as fatal and panics; staging the same entry through
    /// [`batch`](Self::batch) surfaces the rejection as an
    /// `Err` from `commit` instead.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert.
    /// * `value` - The value to associate with the key.
    ///
    /// # Panics
    ///
    /// Panics in read-only mode or if the engine rejects the write.
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
    ///
    /// # Panics
    ///
    /// Panics in read-only mode or if the engine rejects the write.
    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) {
        self.inner.remove(key.as_ref())
    }

    /// Makes prior successful writes to this map's shard durable against
    /// power loss, without forcing a memtable flush. Other maps on the same
    /// shard are synchronized too; other shards are unaffected.
    ///
    /// Ordinary inserts and batch commits are WAL-backed but do not fsync
    /// individually. Use this fence before publishing a reference from a
    /// different shard, and fence removal of that reference before reclaiming
    /// the referenced data. It does not make multiple shards transactional.
    /// Like namespace flush, this maintenance operation is a no-op read-only.
    ///
    /// # Panics
    ///
    /// Panics if the storage engine cannot synchronize its WAL.
    pub fn sync_wal(&self) {
        self.inner.sync_wal();
    }

    /// Fallible form of [`sync_wal`](Self::sync_wal).
    ///
    /// Makes prior successful writes on this map's shard durable without
    /// flushing its memtable. Other shards are unaffected. Returns engine
    /// synchronization errors to the caller; a read-only handle is a no-op.
    pub fn try_sync_wal(&self) -> Result<()> {
        self.inner.try_sync_wal()
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
    /// In read-only mode this operation is a no-op.
    #[doc(hidden)]
    #[inline(always)]
    pub fn lazy_delete(&self, key: impl AsRef<[u8]>) {
        self.inner.lazy_delete(key.as_ref())
    }

    /// Batch version of [`lazy_delete`](Self::lazy_delete). In read-only
    /// mode this operation is a no-op.
    #[doc(hidden)]
    #[inline(always)]
    pub fn lazy_delete_batch(&self, keys: impl IntoIterator<Item = impl AsRef<[u8]>>) {
        self.inner.lazy_delete_batch(keys)
    }

    /// Starts a write batch.
    ///
    /// Buffered insert/remove operations are applied atomically by
    /// [`MapxRawBatch::commit`], which consumes the batch. Preflight rejection
    /// leaves it unapplied; persistence failures can be ambiguous after
    /// recovery and must not be retried automatically.
    ///
    /// # Examples
    ///
    /// ```
    /// use vsdb_core::basic::mapx_raw::MapxRaw;
    /// use vsdb_core::{VsdbOptions, vsdb_configure};
    ///
    /// let dir = std::env::temp_dir()
    ///     .join(format!("vsdb_core_mapx_raw_batch_{}", rand::random::<u128>()));
    /// vsdb_configure(VsdbOptions::new(&dir)).unwrap();
    /// let mut map = MapxRaw::new();
    ///
    /// {
    ///     let mut batch = map.batch();
    ///     batch.insert(&[1], &[10]);
    ///     batch.insert(&[2], &[20]);
    ///     batch.commit().unwrap();
    /// }
    ///
    /// assert_eq!(map.get(&[1]), Some(vec![10]));
    /// assert_eq!(map.get(&[2]), Some(vec![20]));
    /// # std::fs::remove_dir_all(dir).unwrap();
    /// ```
    #[inline(always)]
    pub fn batch(&mut self) -> MapxRawBatch<'_> {
        MapxRawBatch {
            inner: self.inner.batch_begin(),
        }
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
    /// applied by [`MapxRawBatch::commit`]. Persistence failures may be
    /// ambiguous after recovery; dropping an uncommitted batch discards it.
    ///
    /// # Examples
    ///
    /// ```
    /// use vsdb_core::basic::mapx_raw::MapxRaw;
    /// use vsdb_core::{VsdbOptions, vsdb_configure};
    ///
    /// let dir = std::env::temp_dir()
    ///     .join(format!("vsdb_core_mapx_raw_batch_wiped_{}", rand::random::<u128>()));
    /// vsdb_configure(VsdbOptions::new(&dir)).unwrap();
    /// let mut map = MapxRaw::new();
    /// map.insert([1], [10]);
    ///
    /// {
    ///     let mut batch = map.batch_wiped();
    ///     batch.insert(&[2], &[20]);
    ///     batch.commit().unwrap();
    /// }
    ///
    /// assert_eq!(map.get(&[1]), None);
    /// assert_eq!(map.get(&[2]), Some(vec![20]));
    /// # std::fs::remove_dir_all(dir).unwrap();
    /// ```
    #[inline(always)]
    pub fn batch_wiped(&mut self) -> MapxRawBatch<'_> {
        MapxRawBatch {
            inner: self.inner.batch_begin_wiped(),
        }
    }

    /// Clears the map, removing all key-value pairs.
    ///
    /// The wipe is a single atomic engine write batch (one range
    /// tombstone): all-or-nothing, even across a crash.
    ///
    /// # Panics
    ///
    /// Panics in read-only mode or if the engine rejects the batch.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Returns the complete public identity of this `MapxRaw`:
    /// The token contains the map prefix and owning namespace. It uses
    /// `ns: None` for a map in the default namespace.
    pub fn instance_id(&self) -> InstanceId {
        self.inner.instance_id()
    }

    /// Returns the map's identity, propagating errors from the first durable
    /// prefix reservation for a newly created map.
    pub fn try_instance_id(&self) -> Result<InstanceId> {
        self.inner.try_instance_id()
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
    ///
    /// # Errors
    ///
    /// Returns [`VsdbError::ReadOnly`] in read-only mode, or an I/O/encoding
    /// error if the metadata cannot be written.
    pub fn save_meta(&self) -> Result<InstanceId> {
        if self.namespace().is_read_only() {
            return Err(VsdbError::ReadOnly {
                operation: "metadata save",
            });
        }
        let id = self.try_instance_id()?;
        let path = self.namespace().meta_path(id.map_id);
        crate::common::create_dir_all_durable(path.parent().expect("has parent"))?;
        crate::common::atomic_write_file(&path, &self.inner.encode_prefix_meta()?)?;
        Ok(id)
    }

    /// Recovers a `MapxRaw` instance from previously saved metadata.
    ///
    /// Accepts anything convertible into an [`InstanceId`]. A bare `u64`
    /// selects the default namespace. The token determines which namespace's
    /// metadata directory is read. The metadata must be in the v18 raw format.
    ///
    /// The caller must ensure that the underlying VSDB database still
    /// contains the data referenced by this instance ID.
    pub fn from_meta(instance_id: impl Into<InstanceId>) -> Result<Self> {
        let id = instance_id.into();
        let id = InstanceId::new(id.map_id, id.ns.unwrap_or(DEFAULT_NS_ID));
        let ns = Namespace::open(id.ns.unwrap_or(DEFAULT_NS_ID))?;
        let bytes = fs::read(ns.meta_path(id.map_id))?;
        let found = engine::Mapx::decode_prefix_meta(&bytes)?;
        if found != id {
            return Err(VsdbError::Decode {
                detail: format!(
                    "metadata identity mismatch: requested {id}, payload names {found}"
                ),
            });
        }
        Self::from_prefix_meta(&bytes)
    }

    #[doc(hidden)]
    pub fn from_prefix_meta(bytes: &[u8]) -> Result<Self> {
        engine::Mapx::from_prefix_meta(bytes).map(|inner| Self { inner })
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

/// An atomic write batch over one [`MapxRaw`], from [`MapxRaw::batch`] or
/// [`MapxRaw::batch_wiped`].
pub struct MapxRawBatch<'a> {
    inner: engine::Batch<'a>,
}

impl MapxRawBatch<'_> {
    /// Deletes keys in the half-open byte range `[start, end)` in this batch.
    /// Later inserts in the same batch take precedence.
    pub fn delete_range(&mut self, start: &[u8], end: &[u8]) {
        self.inner.delete_range(start, end);
    }

    /// Buffers an insert.
    #[inline(always)]
    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) {
        self.inner.insert(key.as_ref(), value.as_ref());
    }

    /// Buffers a removal.
    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) {
        self.inner.remove(key.as_ref());
    }

    /// Applies every buffered operation atomically, consuming the batch.
    ///
    /// # Errors
    ///
    /// Engine-side size rejections (keys over 8 MiB, entries over ~64 MiB)
    /// and [`VsdbError::ReadOnly`] leave the batch unapplied. Persistence
    /// failures can be ambiguous after recovery and must not be retried
    /// automatically.
    #[inline(always)]
    pub fn commit(self) -> Result<()> {
        self.inner.commit()
    }

    /// Atomically publishes this batch after synchronizing its shard WAL.
    ///
    /// Unlike `commit` followed by a WAL fence, readers cannot observe this
    /// batch before its synchronization succeeds. This does not force a
    /// memtable flush or create a transaction across collections or shards.
    /// An empty batch is a no-op, not a fence for previous writes.
    ///
    /// # Errors
    ///
    /// Reports capability, size and persistence errors. After an I/O failure,
    /// recovery may find WAL data even though this call did not acknowledge
    /// success; do not automatically retry an ambiguous failed write.
    pub fn commit_sync(self) -> Result<()> {
        self.inner.commit_sync()
    }

    /// Commit once with ordinary WAL durability. If admission would stall at
    /// L0 or the immutable queue, return [`crate::StorageErrorKind::Busy`].
    /// A busy batch wrote no rows or WAL records and may be rebuilt and retried.
    /// Group commit and file I/O can still take time.
    pub fn commit_no_slowdown(self) -> Result<()> {
        self.inner.commit_no_slowdown(false)
    }

    /// Apply the same admission rule as [`Self::commit_no_slowdown`] and
    /// synchronize a single accepted batch before acknowledging it.
    pub fn commit_sync_no_slowdown(self) -> Result<()> {
        self.inner.commit_no_slowdown(true)
    }
}

/// A view into a single entry in a map, which may either be vacant or occupied.
pub enum Entry<'a> {
    Occupied(OccupiedEntry<'a>),
    Vacant(VacantEntry<'a>),
}

pub struct OccupiedEntry<'a>(ValueMut<'a>);
impl<'a> OccupiedEntry<'a> {
    pub fn get(&self) -> &RawValue {
        &self.0
    }
    pub fn get_mut(&mut self) -> &mut RawValue {
        &mut self.0
    }
    pub fn into_mut(self) -> ValueMut<'a> {
        self.0
    }
    pub fn insert(&mut self, value: RawValue) -> RawValue {
        std::mem::replace(&mut *self.0, value)
    }
    pub fn remove(self) -> RawValue {
        self.0.remove()
    }
}

pub struct VacantEntry<'a> {
    key: RawKey,
    hdr: &'a mut MapxRaw,
}
impl<'a> VacantEntry<'a> {
    pub fn insert(self, value: RawValue) -> ValueMut<'a> {
        self.hdr.inner.value_slot(self.key, value, true)
    }
    /// An uninitialized guard for a layered codec. Dropping this guard
    /// before assigning a value leaves the entry vacant.
    pub fn into_slot(self) -> ValueMut<'a> {
        self.hdr.inner.value_slot(self.key, Vec::new(), false)
    }
}
impl<'a> Entry<'a> {
    pub fn or_insert(self, default: &[u8]) -> ValueMut<'a> {
        self.or_insert_with(|| default.to_vec())
    }
    pub fn or_insert_with(self, f: impl FnOnce() -> RawValue) -> ValueMut<'a> {
        match self {
            Self::Occupied(e) => e.into_mut(),
            Self::Vacant(e) => e.insert(f()),
        }
    }
    pub fn or_default(self) -> ValueMut<'a> {
        self.or_insert_with(Vec::new)
    }
    pub fn and_modify(mut self, f: impl FnOnce(&mut RawValue)) -> Self {
        if let Self::Occupied(e) = &mut self {
            f(e.get_mut());
        }
        self
    }
}
