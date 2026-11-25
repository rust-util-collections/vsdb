/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod clone_tests;
mod mmdb;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub(crate) use self::mmdb::{
    EngineSizing, MmDB as Engine, MmdbBatch as Batch, MmdbReadView, recover_ceiling,
    validate_completed_dataset, validate_pending_shards,
};

type DbIter = self::mmdb::MmdbIter;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

use crate::common::{
    Pre, PreBytes, RawKey, RawRangeBounds, RawValue,
    error::{Result, VsdbError},
    namespace::Namespace,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize, de, ser};
use std::{
    fmt,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    result::Result as StdResult,
    sync::atomic::{AtomicU64, Ordering},
    thread,
};

const MAPX_META_MAGIC: &[u8; 8] = b"VSMAPX02";

#[derive(Clone, Debug)]
pub(crate) struct MapxReader {
    prefix: PreBytes,
    ns: Namespace,
}

impl MapxReader {
    pub(crate) fn namespace(&self) -> Namespace {
        self.ns.clone()
    }

    pub(crate) fn instance_id(&self) -> crate::InstanceId {
        crate::InstanceId::new(u64::from_le_bytes(self.prefix), self.ns.id())
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<RawValue> {
        self.ns.engine().get(self.prefix, key)
    }

    pub(crate) fn try_get_with<T>(
        &self,
        key: &[u8],
        decode: impl FnOnce(&[u8]) -> Result<T>,
    ) -> Result<Option<T>> {
        self.ns.engine().try_get_with(self.prefix, key, decode)
    }

    pub(crate) fn read_view(&self) -> MapxReadView<'_> {
        MapxReadView {
            inner: self.ns.engine().read_view(self.prefix),
        }
    }
}

/// A borrowed immutable view of one ordinary map's committed state.
pub struct MapxReadView<'a> {
    inner: MmdbReadView<'a>,
}

impl MapxReadView<'_> {
    /// Reads a key at the captured state. Fatal engine errors panic.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<RawValue> {
        self.inner.get(key.as_ref())
    }

    /// Decode a single value directly from its pinned bytes at this snapshot.
    pub fn try_get_with<T>(
        &self,
        key: impl AsRef<[u8]>,
        decode: impl FnOnce(&[u8]) -> Result<T>,
    ) -> Result<Option<T>> {
        self.inner.try_get_with(key.as_ref(), decode)
    }

    /// Resolve up to [`crate::MAX_GROUPED_READ_KEYS`] slots at this snapshot.
    /// The visitor runs in input order, including duplicates and misses. All
    /// storage reads succeed before the first callback. A visitor error stops
    /// the remaining callbacks. Returned slices are borrowed only for each call.
    pub fn try_visit_many(
        &self,
        keys: &[&[u8]],
        visit: impl FnMut(usize, Option<&[u8]>) -> Result<()>,
    ) -> Result<()> {
        self.inner.try_visit_many(keys, visit)
    }

    /// Whether the captured state contains a key.
    pub fn contains_key(&self, key: impl AsRef<[u8]>) -> bool {
        self.try_contains_key(key)
            .expect("vsdb: mmdb snapshot existence check failed")
    }

    /// Fallible snapshot existence check, without copying the value.
    pub fn try_contains_key(&self, key: impl AsRef<[u8]>) -> Result<bool> {
        self.inner.try_contains_key(key.as_ref())
    }

    /// Iterates the captured state, borrowing this view.
    ///
    /// ```compile_fail
    /// use vsdb_core::MapxRaw;
    /// let map = MapxRaw::new();
    /// let iter = {
    ///     let view = map.read_view();
    ///     view.iter()
    /// };
    /// let _ = iter.count(); // The snapshot must remain alive.
    /// ```
    pub fn iter(&self) -> MapxIter<'_> {
        self.range(..)
    }

    /// Captures the source iterators at this view's sequence. The iterator
    /// retains those sources independently of the view.
    pub fn range_detached<R: RawRangeBounds>(&self, bounds: R) -> MapxIter<'static> {
        MapxIter {
            db_iter: self.inner.range(bounds),
            _marker: PhantomData,
        }
    }

    /// Opens a bounded cursor at this snapshot and preserves storage errors.
    pub fn try_range<R: RawRangeBounds>(&self, bounds: R) -> Result<MapxIter<'_>> {
        Ok(MapxIter {
            db_iter: self.inner.try_range(bounds)?,
            _marker: PhantomData,
        })
    }

    /// Iterates a range within the captured state. Fatal engine errors panic.
    pub fn range<'a, R: RawRangeBounds>(&'a self, bounds: R) -> MapxIter<'a> {
        MapxIter {
            db_iter: self.inner.range(bounds),
            _marker: PhantomData,
        }
    }
}
const MAPX_META_NS_LEN: usize = 24;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// Whether the thread was already unwinding when a write-back guard was
/// created.
///
/// A panic that begins while a guard is alive means its buffered edit may
/// be half-done: the guard discards it instead of persisting it (which also
/// avoids a second panic, and thus an abort, while unwinding). A guard
/// created during unwinding — e.g. inside a `Drop` — writes normally.
#[derive(Clone, Copy, Debug)]
struct UnwindMark(bool);

impl UnwindMark {
    #[inline(always)]
    fn new() -> Self {
        Self(thread::panicking())
    }

    /// True when a panic began after this mark was taken.
    #[inline(always)]
    fn interrupted(self) -> bool {
        !self.0 && thread::panicking()
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct Mapx {
    // Zero until the first use reserves an ID in the process-wide allocator.
    prefix: AtomicU64,
    // Serializes only the first assignment. Failed allocations can be retried.
    prefix_init: Mutex<()>,
    ns: Namespace,
}

impl Mapx {
    fn with_prefix(ns: Namespace, prefix: Pre) -> Self {
        Self {
            prefix: AtomicU64::new(prefix),
            prefix_init: Mutex::new(()),
            ns,
        }
    }

    #[inline(always)]
    fn prefix_bytes(&self) -> PreBytes {
        self.try_prefix()
            .expect("vsdb: prefix allocation failed")
            .to_le_bytes()
    }

    #[inline]
    fn try_prefix(&self) -> Result<Pre> {
        let id = self.prefix.load(Ordering::Acquire);
        if id == 0 {
            self.assign_prefix()
        } else {
            Ok(id)
        }
    }

    fn assign_prefix(&self) -> Result<Pre> {
        let _init = self.prefix_init.lock();
        let mut id = self.prefix.load(Ordering::Relaxed);
        if id == 0 {
            id = self.ns.engine().alloc_prefix()?;
            debug_assert!(self.ns.engine().iter(id.to_le_bytes()).next().is_none());
            self.prefix.store(id, Ordering::Release);
        }
        Ok(id)
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
    // - No concurrent writes to the same key through any handle.
    //   Multiple writers on disjoint keys are safe.
    // Immutable reads, including iteration, may run alongside writes. Each
    // iterator retains one committed view; separate reads can see newer views.
    // Mutable iteration is a write and follows the same key exclusion rule.
    pub(crate) unsafe fn shadow(&self) -> Self {
        Self::with_prefix(self.ns.clone(), self.instance_id().map_id)
    }

    #[inline(always)]
    pub(crate) fn new() -> Self {
        Self::new_in(&Namespace::current())
    }

    #[inline(always)]
    pub(crate) fn new_in(ns: &Namespace) -> Self {
        assert!(
            !ns.is_read_only(),
            "vsdb: cannot create a collection in read-only mode"
        );
        Self::with_prefix(ns.clone(), 0)
    }

    /// The owning namespace (cheap `Arc` clone).
    #[inline(always)]
    pub(crate) fn namespace(&self) -> Namespace {
        self.ns.clone()
    }

    pub(crate) fn sync_wal(&self) {
        if !self.ns.is_read_only() {
            let prefix = self.prefix.load(Ordering::Acquire);
            if prefix != 0 {
                self.ns.engine().sync_wal(prefix.to_le_bytes());
            }
        }
    }

    pub(crate) fn try_sync_wal(&self) -> Result<()> {
        if self.ns.is_read_only() {
            return Ok(());
        }
        // An untouched handle has no writes or assigned shard to fence.
        // Do not turn this fallible operation into an infallible prefix allocation.
        let prefix = self.prefix.load(Ordering::Acquire);
        if prefix == 0 {
            return Ok(());
        }
        self.ns.engine().try_sync_wal(prefix.to_le_bytes())
    }

    #[inline(always)]
    pub(crate) fn get(&self, key: &[u8]) -> Option<RawValue> {
        self.ns.engine().get(self.prefix_bytes(), key)
    }

    pub(crate) fn try_get_with<T>(
        &self,
        key: &[u8],
        decode: impl FnOnce(&[u8]) -> Result<T>,
    ) -> Result<Option<T>> {
        self.ns
            .engine()
            .try_get_with(self.try_prefix()?.to_le_bytes(), key, decode)
    }

    pub(crate) fn try_range<R: RawRangeBounds>(
        &self,
        bounds: R,
    ) -> Result<MapxIter<'_>> {
        Ok(MapxIter {
            db_iter: self
                .ns
                .engine()
                .try_range(self.try_prefix()?.to_le_bytes(), bounds)?,
            _marker: PhantomData,
        })
    }

    pub(crate) fn try_contains_key(&self, key: &[u8]) -> Result<bool> {
        let prefix = self.try_prefix()?.to_le_bytes();
        self.ns.engine().try_contains_key(prefix, key)
    }

    pub(crate) fn reader(&self) -> MapxReader {
        MapxReader {
            prefix: self.prefix_bytes(),
            ns: self.ns.clone(),
        }
    }

    pub(crate) fn read_view(&self) -> MapxReadView<'_> {
        MapxReadView {
            inner: self.ns.engine().read_view(self.prefix_bytes()),
        }
    }

    #[inline(always)]
    pub(crate) fn get_mut(&mut self, key: &[u8]) -> Option<ValueMut<'_>> {
        let prefix = self.prefix_bytes();
        let v = self.ns.engine().get(prefix, key)?;

        Some(self.value_slot(key.to_vec(), v, false))
    }

    pub(crate) fn value_slot(
        &mut self,
        key: RawKey,
        value: RawValue,
        dirty: bool,
    ) -> ValueMut<'_> {
        ValueMut {
            ns: self.ns.clone(),
            prefix: self.prefix_bytes(),
            key,
            value,
            dirty,
            unwind: UnwindMark::new(),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn iter(&self) -> MapxIter<'_> {
        MapxIter {
            db_iter: self.ns.engine().iter(self.prefix_bytes()),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn iter_mut(&mut self) -> MapxIterMut<'_> {
        let prefix = self.prefix_bytes();
        MapxIterMut {
            db_iter: self.ns.engine().iter(prefix),
            hdr: self,
        }
    }

    #[inline(always)]
    pub(crate) fn range<'a, R: RawRangeBounds>(&'a self, bounds: R) -> MapxIter<'a> {
        MapxIter {
            db_iter: self.ns.engine().range(self.prefix_bytes(), bounds),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn range_detached<'a, R: RawRangeBounds>(
        &self,
        bounds: R,
    ) -> MapxIter<'a> {
        MapxIter {
            db_iter: self.ns.engine().range(self.prefix_bytes(), bounds),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn range_mut<'a, R: RawRangeBounds>(
        &'a mut self,
        bounds: R,
    ) -> MapxIterMut<'a> {
        let prefix = self.prefix_bytes();
        MapxIterMut {
            db_iter: self.ns.engine().range(prefix, bounds),
            hdr: self,
        }
    }

    #[inline(always)]
    pub(crate) fn insert(&mut self, key: &[u8], value: &[u8]) {
        let prefix = self.prefix_bytes();
        self.ns.engine().insert(prefix, key, value);
    }

    #[inline(always)]
    pub(crate) fn remove(&mut self, key: &[u8]) {
        let prefix = self.prefix_bytes();
        self.ns.engine().remove(prefix, key);
    }

    /// Marks a key for deferred removal via compaction filter.
    ///
    /// Not crash-durable: registrations live in engine memory only and
    /// are lost on restart; callers must re-register after recovery.
    #[inline(always)]
    pub(crate) fn lazy_delete(&self, key: &[u8]) {
        if self.ns.is_read_only() {
            return;
        }
        self.ns.engine().lazy_delete(self.prefix_bytes(), key);
    }

    /// Batch version of [`lazy_delete`](Self::lazy_delete).
    #[inline(always)]
    pub(crate) fn lazy_delete_batch(
        &self,
        keys: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) {
        if self.ns.is_read_only() {
            return;
        }
        self.ns
            .engine()
            .lazy_delete_batch(self.prefix_bytes(), keys);
    }

    #[inline(always)]
    pub(crate) fn batch_begin(&mut self) -> Batch<'_> {
        let prefix = self.try_prefix().map(Pre::to_le_bytes);
        self.ns.engine().batch_begin(prefix)
    }

    /// A write batch pre-staged with the removal of every existing entry
    /// of this map (one engine-level range tombstone). Operations added
    /// afterwards apply on top of the wipe; the whole set — wipe
    /// included — commits in one atomic engine write batch.
    #[inline(always)]
    pub(crate) fn batch_begin_wiped(&mut self) -> Batch<'_> {
        let prefix = self.try_prefix().map(Pre::to_le_bytes);
        self.ns.engine().batch_begin_wiped(prefix)
    }

    #[inline(always)]
    pub(crate) fn clear(&mut self) {
        // One batch containing a single range tombstone covering the whole
        // prefix: atomic (all-or-nothing, even across a crash) and O(1),
        // unlike a chunked scan-and-delete loop. Concurrent readers (e.g.
        // via `shadow()`) can never observe a partially-cleared state.
        self.batch_begin_wiped()
            .commit()
            .expect("vsdb: batch delete failed during clear");
    }

    pub(crate) fn from_prefix_meta(meta: &[u8]) -> Result<Self> {
        let id = Self::decode_prefix_meta(meta)?;
        let prefix = id.map_id.to_le_bytes();
        let ns = Namespace::open(id.ns.unwrap_or_default())?;
        if !ns.engine().reserve_recovered_prefix(prefix)? {
            return Err(VsdbError::Decode {
                detail: format!(
                    "Mapx metadata prefix {} is outside the allocator-issued \
                     range or collides with a concurrently issued prefix",
                    u64::from_le_bytes(prefix)
                ),
            });
        }
        Ok(Self::with_prefix(ns, id.map_id))
    }

    /// Fixed-size raw handle. All integer fields are little-endian.
    pub(crate) fn encode_prefix_meta(&self) -> Result<[u8; MAPX_META_NS_LEN]> {
        let mut frame = [0; MAPX_META_NS_LEN];
        frame[..8].copy_from_slice(MAPX_META_MAGIC);
        frame[8..16].copy_from_slice(&self.try_prefix()?.to_le_bytes());
        frame[16..].copy_from_slice(&self.ns.id().to_le_bytes());
        Ok(frame)
    }

    pub(crate) fn decode_prefix_meta(meta: &[u8]) -> Result<crate::InstanceId> {
        if meta.len() != MAPX_META_NS_LEN || &meta[..8] != MAPX_META_MAGIC {
            return Err(VsdbError::Decode {
                detail: "invalid raw map handle".into(),
            });
        }
        Ok(crate::InstanceId::new(
            u64::from_le_bytes(meta[8..16].try_into().unwrap()),
            u64::from_le_bytes(meta[16..24].try_into().unwrap()),
        ))
    }

    #[inline(always)]
    pub(crate) fn instance_id(&self) -> crate::InstanceId {
        self.try_instance_id()
            .expect("vsdb: prefix allocation failed")
    }

    pub(crate) fn try_instance_id(&self) -> Result<crate::InstanceId> {
        Ok(crate::InstanceId::new(self.try_prefix()?, self.ns.id()))
    }

    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.ns.id() == other_hdr.ns.id()
            && self.prefix_bytes() == other_hdr.prefix_bytes()
    }

    /// Deep-copies every pair into a brand-new instance placed in `ns` —
    /// the cross-namespace form of [`Clone`] (`clone()` ≡
    /// `clone_in(&self.ns)`).
    ///
    /// Commits in bounded chunks — a single WriteBatch buffers every
    /// pair in memory, which would OOM on larger-than-RAM maps. Chunks
    /// are bounded by entry count AND by staged bytes: the engine
    /// accepts entries up to ~64 MiB each, so a count-only bound could
    /// still stage hundreds of GiB in one batch. Atomicity is not
    /// needed: the clone target is a brand-new, unobservable prefix.
    ///
    /// On error the chunks already committed are reclaimed with a
    /// best-effort wipe (one O(1) range tombstone — tiny enough to
    /// stand a chance where a full data batch just failed, e.g. on a
    /// full disk), so a failed or retried clone does not accumulate
    /// garbage under never-returned prefixes.  Only if the wipe itself
    /// also fails does the partial target stay behind as unreferenced,
    /// invisible garbage (the same residue a mid-`clone()` panic
    /// leaves).
    pub(crate) fn clone_in(&self, ns: &Namespace) -> Result<Self> {
        if ns.is_read_only() {
            return Err(VsdbError::ReadOnly {
                operation: "collection clone",
            });
        }
        self.copy_into(Self::new_in(ns))
    }

    /// Deep copy into a fresh instance co-located with `anchor` (see
    /// [`new_colocated`](Self::new_colocated)); same chunking and error
    /// cleanup as [`clone_in`](Self::clone_in).
    pub(crate) fn clone_colocated(&self, anchor: &Self) -> Result<Self> {
        if anchor.ns.is_read_only() {
            return Err(VsdbError::ReadOnly {
                operation: "collection clone",
            });
        }
        self.copy_into(anchor.try_new_colocated()?)
    }

    /// A fresh, empty map in `self`'s namespace whose prefix routes to
    /// the same engine shard as `self`, so writes to both land in one
    /// WAL. Prefixes skipped while searching for that shard are simply
    /// never used (they are never reissued either).
    pub(crate) fn new_colocated(&self) -> Self {
        self.try_new_colocated()
            .expect("vsdb: co-located prefix allocation failed")
    }

    fn try_new_colocated(&self) -> Result<Self> {
        let anchor = self.try_prefix()?.to_le_bytes();
        let engine = self.ns.engine();
        let shard = engine.shard_of(&anchor);
        loop {
            let prefix = engine.alloc_prefix()?;
            if engine.shard_of(&prefix.to_le_bytes()) == shard {
                return Ok(Self::with_prefix(self.ns.clone(), prefix));
            }
        }
    }

    /// Whether `self` and `other` live in the same namespace and route to
    /// the same engine shard (one WAL, one crash order).
    pub(crate) fn is_colocated_with(&self, other: &Self) -> bool {
        self.ns.id() == other.ns.id()
            && self.ns.engine().shard_of(&self.prefix_bytes())
                == self.ns.engine().shard_of(&other.prefix_bytes())
    }

    fn copy_into(&self, mut new_instance: Self) -> Result<Self> {
        const CLONE_CHUNK: usize = 4096;
        const CLONE_CHUNK_BYTES: usize = 16 * 1024 * 1024;

        let mut it = self.try_range(..)?;
        let result = (|| {
            while let Some((k, v)) = it.try_step_next()? {
                new_instance.try_prefix()?;
                let mut batch = new_instance.batch_begin();
                let mut entries = 1usize;
                let mut bytes = k.len() + v.len();
                batch.insert(k, v);
                while entries < CLONE_CHUNK && bytes < CLONE_CHUNK_BYTES {
                    let Some((k, v)) = it.try_step_next()? else {
                        break;
                    };
                    entries += 1;
                    bytes += k.len() + v.len();
                    batch.insert(k, v);
                }
                batch.commit()?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            // Earlier chunks may have committed before a source read or a
            // destination write failed. Preserve the original typed error.
            if new_instance.prefix.load(Ordering::Relaxed) != 0 {
                let _ = new_instance.batch_begin_wiped().commit();
            }
            return Err(error);
        }
        Ok(new_instance)
    }
}

impl Clone for Mapx {
    fn clone(&self) -> Self {
        // The copy lands in the SAME namespace as the source — a deep
        // copy is co-located with its original, never re-placed by the
        // ambient scope.
        self.clone_in(&self.ns)
            .expect("vsdb: clone failed — I/O error")
    }
}

impl PartialEq for Mapx {
    fn eq(&self, other: &Mapx) -> bool {
        // Short-circuit: two handles on the same instance are identical
        if self.is_the_same_instance(other) {
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

struct SimpleVisitor;

impl<'de> de::Visitor<'de> for SimpleVisitor {
    type Value = Mapx;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("bytes")
    }

    fn visit_str<E>(self, v: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_bytes(v.as_bytes())
    }

    fn visit_string<E>(self, v: String) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_bytes(v.as_bytes())
    }

    fn visit_bytes<E>(self, v: &[u8]) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        Mapx::from_prefix_meta(v).map_err(E::custom)
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        self.visit_bytes(&v)
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
        self.visit_bytes(&ret)
    }
}

impl Serialize for Mapx {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer
            .serialize_bytes(&self.encode_prefix_meta().map_err(ser::Error::custom)?)
    }
}

impl<'de> Deserialize<'de> for Mapx {
    // Deserialization restores a handle to the SAME underlying prefix
    // (a shallow alias, like `shadow()`), not an independent copy.
    // The SWMR obligations of every alias-producing path are documented
    // on the public wrappers' `Deserialize`/`from_meta` impls; the
    // prefix itself is validated against the allocator-reserved range
    // by `from_prefix_meta`.
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(SimpleVisitor)
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

pub struct MapxIter<'a> {
    db_iter: DbIter,
    _marker: PhantomData<&'a ()>,
}

impl MapxIter<'_> {
    /// Advance from the front and borrow the unprefixed key and value until
    /// the next cursor operation. Storage errors retain their classification.
    pub fn try_step_next(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.db_iter.try_step_next()
    }

    /// Advance from the back and borrow the unprefixed key and value.
    pub fn try_step_prev(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        self.db_iter.try_step_prev()
    }
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
            ns: self.hdr.ns.clone(),
            prefix: self.hdr.prefix_bytes(),
            key: k.clone(),
            value: v,
            dirty: false,
            unwind: UnwindMark::new(),
            _marker: PhantomData,
        };

        Some((k, vmut))
    }
}

impl<'a> DoubleEndedIterator for MapxIterMut<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let (k, v) = self.db_iter.next_back()?;

        let vmut = ValueIterMut {
            ns: self.hdr.ns.clone(),
            prefix: self.hdr.prefix_bytes(),
            key: k.clone(),
            value: v,
            dirty: false,
            unwind: UnwindMark::new(),
            _marker: PhantomData,
        };

        Some((k, vmut))
    }
}

pub struct ValueMut<'a> {
    // The owning namespace (an `Arc` clone), not a bare engine
    // reference: engines are owned by their `NsInner` (no leak), so a
    // stored reference must ride with its anchor.
    ns: Namespace,
    prefix: PreBytes,
    key: RawKey,
    value: RawValue,
    dirty: bool,
    unwind: UnwindMark,
    _marker: PhantomData<&'a mut ()>,
}

impl fmt::Debug for ValueMut<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValueMut")
            .field("prefix", &self.prefix)
            .field("key", &self.key)
            .field("value", &self.value)
            .field("dirty", &self.dirty)
            .finish()
    }
}

impl Drop for ValueMut<'_> {
    fn drop(&mut self) {
        if self.dirty && !self.unwind.interrupted() {
            self.flush().expect("vsdb: value write-back failed");
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

pub type ValueIterMut<'a> = ValueMut<'a>;

impl ValueMut<'_> {
    fn flush(&mut self) -> Result<()> {
        self.dirty = false;
        let mut batch = self.ns.engine().batch_begin(Ok(self.prefix));
        batch.insert(&self.key, &self.value);
        batch.commit()
    }

    /// Finishes this edit and returns a storage error to the caller.
    pub fn commit(mut self) -> Result<()> {
        if self.dirty { self.flush() } else { Ok(()) }
    }

    /// Removes the entry, returning its buffered value.
    pub fn remove(mut self) -> RawValue {
        self.dirty = false;
        self.ns.engine().remove(self.prefix, &self.key);
        std::mem::take(&mut self.value)
    }
}
