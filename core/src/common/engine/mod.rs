/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

mod mmdb;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

pub(crate) use self::mmdb::{
    EngineSizing, MmDB as Engine, validate_completed_dataset, write_file_durable,
};

type DbIter = self::mmdb::MmdbIter;

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

use crate::common::{
    PREFIX_SIZE, PreBytes, RawKey, RawValue,
    error::{Result, VsdbError},
    namespace::{DEFAULT_NS_ID, Namespace},
};
use serde::{Deserialize, Serialize, de};
use std::{
    borrow::Cow,
    fmt,
    marker::PhantomData,
    ops::{Deref, DerefMut, RangeBounds},
    result::Result as StdResult,
    sync::OnceLock,
};

const MAPX_META_MAGIC: &[u8; 8] = b"VSMAPX01";
/// Meta without the namespace suffix — the pre-v16 wire format,
/// still written verbatim for default-namespace handles.
const MAPX_META_LEN: usize = MAPX_META_MAGIC.len() + PREFIX_SIZE;
/// Meta with the trailing `ns_id` (non-default namespaces only).
/// The suffix *is* an `Option<NsId>` encoded by presence: absent ⇔
/// `None` ⇔ default namespace.
const MAPX_META_NS_LEN: usize = MAPX_META_LEN + size_of::<u64>();

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// Trait for batch write operations
pub trait BatchTrait {
    fn insert(&mut self, key: &[u8], value: &[u8]);
    fn remove(&mut self, key: &[u8]);
    /// Atomically applies all buffered operations.
    ///
    /// On error the buffered operations are consumed and lost (none were
    /// applied); a failed commit is **not retryable** — re-stage the
    /// operations on a fresh batch instead. Engine-side entry-size
    /// rejections (keys over 8 MiB, entries over ~64 MiB) are reported
    /// here.
    fn commit(&mut self) -> Result<()>;
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct Mapx {
    // the unique ID of each instance (within its namespace's engine;
    // prefixes are globally unique across namespaces by construction)
    prefix: Prefix,
    // the owning namespace, captured eagerly at creation (ambient
    // scope must be read at creation time, never at first use)
    ns: Namespace,
}

#[derive(Debug)]
enum Prefix {
    Recovered(PreBytes),
    /// Allocation is deferred to first use so that constructing an
    /// empty handle never burns an id; the id itself comes from the
    /// process-global allocator (namespace-independent).
    Created(OnceLock<PreBytes>),
}

impl Mapx {
    #[inline(always)]
    fn prefix_bytes(&self) -> PreBytes {
        match &self.prefix {
            Prefix::Recovered(bytes) => *bytes,
            Prefix::Created(cell) => *cell.get_or_init(|| {
                let prefix = self.ns.engine().alloc_prefix();
                let prefix_bytes = prefix.to_le_bytes();
                debug_assert!(self.ns.engine().iter(prefix_bytes).next().is_none());
                prefix_bytes
            }),
        }
    }

    /// Force the prefix to be materialized (if lazily created)
    /// and return the bytes. Converts `Created` → `Recovered`
    /// so subsequent calls take the branch-free path.
    fn materialize(&mut self) -> PreBytes {
        let b = self.prefix_bytes();
        if matches!(self.prefix, Prefix::Created(_)) {
            self.prefix = Prefix::Recovered(b);
        }
        b
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
    // - No concurrent iteration and mutation.
    pub(crate) unsafe fn shadow(&self) -> Self {
        Self {
            prefix: Prefix::Recovered(self.prefix_bytes()),
            ns: self.ns.clone(),
        }
    }

    #[inline(always)]
    pub(crate) fn new() -> Self {
        Self::new_in(&Namespace::current())
    }

    #[inline(always)]
    pub(crate) fn new_in(ns: &Namespace) -> Self {
        Self {
            prefix: Prefix::Created(OnceLock::new()),
            ns: ns.clone(),
        }
    }

    /// The owning namespace (cheap `Arc` clone).
    #[inline(always)]
    pub(crate) fn namespace(&self) -> Namespace {
        self.ns.clone()
    }

    #[inline(always)]
    pub(crate) fn get(&self, key: &[u8]) -> Option<RawValue> {
        self.ns.engine().get(self.prefix_bytes(), key)
    }

    #[inline(always)]
    pub(crate) fn get_mut(&mut self, key: &[u8]) -> Option<ValueMut<'_>> {
        let prefix = self.materialize();
        let v = self.ns.engine().get(prefix, key)?;

        Some(ValueMut {
            key: key.to_vec(),
            value: v,
            dirty: false,
            hdr: self,
        })
    }

    #[inline(always)]
    pub(crate) fn mock_value_mut(
        &mut self,
        key: RawValue,
        value: RawValue,
    ) -> ValueMut<'_> {
        ValueMut {
            key,
            value,
            dirty: true,
            hdr: self,
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
        let prefix = self.materialize();
        MapxIterMut {
            db_iter: self.ns.engine().iter(prefix),
            hdr: self,
        }
    }

    #[inline(always)]
    pub(crate) fn range<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a self,
        bounds: R,
    ) -> MapxIter<'a> {
        MapxIter {
            db_iter: self.ns.engine().range(self.prefix_bytes(), bounds),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn range_detached<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &self,
        bounds: R,
    ) -> MapxIter<'a> {
        MapxIter {
            db_iter: self.ns.engine().range(self.prefix_bytes(), bounds),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub(crate) fn range_mut<'a, R: RangeBounds<Cow<'a, [u8]>>>(
        &'a mut self,
        bounds: R,
    ) -> MapxIterMut<'a> {
        let prefix = self.materialize();
        MapxIterMut {
            db_iter: self.ns.engine().range(prefix, bounds),
            hdr: self,
        }
    }

    #[inline(always)]
    pub(crate) fn insert(&mut self, key: &[u8], value: &[u8]) {
        let prefix = self.materialize();
        self.ns.engine().insert(prefix, key, value);
    }

    #[inline(always)]
    pub(crate) fn remove(&mut self, key: &[u8]) {
        let prefix = self.materialize();
        self.ns.engine().remove(prefix, key);
    }

    /// Marks a key for deferred removal via compaction filter.
    ///
    /// Not crash-durable: registrations live in engine memory only and
    /// are lost on restart; callers must re-register after recovery.
    #[inline(always)]
    pub(crate) fn lazy_delete(&self, key: &[u8]) {
        self.ns.engine().lazy_delete(self.prefix_bytes(), key);
    }

    /// Batch version of [`lazy_delete`](Self::lazy_delete).
    #[inline(always)]
    pub(crate) fn lazy_delete_batch(
        &self,
        keys: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) {
        self.ns
            .engine()
            .lazy_delete_batch(self.prefix_bytes(), keys);
    }

    #[inline(always)]
    pub(crate) fn batch_begin(&mut self) -> Box<dyn BatchTrait + '_> {
        let prefix = self.materialize();
        self.ns.engine().batch_begin(prefix)
    }

    /// A write batch pre-staged with the removal of every existing entry
    /// of this map (one engine-level range tombstone). Operations added
    /// afterwards apply on top of the wipe; the whole set — wipe
    /// included — commits in one atomic engine write batch.
    #[inline(always)]
    pub(crate) fn batch_begin_wiped(&mut self) -> Box<dyn BatchTrait + '_> {
        let prefix = self.materialize();
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

    /// # Safety
    ///
    /// `s` must be exactly [`PREFIX_SIZE`] bytes encoding a `u64` prefix
    /// (little-endian) that the caller is entitled to treat as uniquely
    /// owned — i.e. produced by [`as_prefix_slice`](Self::as_prefix_slice)
    /// (or equivalent) on a valid instance, with no other live handle
    /// concurrently using the same prefix. Passing a prefix that is
    /// still owned by another live handle causes two independently
    /// type-checked handles to alias the same underlying key range — a
    /// logical data race indistinguishable from [`shadow`](Self::shadow)
    /// misuse, even though this function performs no memory-unsafe
    /// operation itself (a length mismatch panics via `copy_from_slice`,
    /// it never reads out of bounds).
    ///
    /// The handle is bound to `ns`; the caller must additionally
    /// guarantee the prefix's data lives in that namespace's engine (a
    /// raw prefix carries no namespace information of its own).
    #[inline(always)]
    pub(crate) unsafe fn from_prefix_slice_in(
        ns: &Namespace,
        s: impl AsRef<[u8]>,
    ) -> Self {
        debug_assert_eq!(s.as_ref().len(), PREFIX_SIZE);
        let mut prefix = PreBytes::default();
        prefix.copy_from_slice(s.as_ref());
        Self {
            prefix: Prefix::Recovered(prefix),
            ns: ns.clone(),
        }
    }

    /// [`from_prefix_slice_in`](Self::from_prefix_slice_in) bound to the
    /// current ambient namespace ([`Namespace::current`]).
    ///
    /// # Safety
    ///
    /// Same contract as `from_prefix_slice_in`.
    #[inline(always)]
    pub(crate) unsafe fn from_prefix_slice(s: impl AsRef<[u8]>) -> Self {
        // SAFETY: forwards this fn's `unsafe` contract verbatim — the
        // caller guarantees a uniquely-owned prefix whose data lives in
        // the current ambient namespace.
        unsafe { Self::from_prefix_slice_in(&Namespace::current(), s) }
    }

    pub(crate) fn from_prefix_meta(meta: &[u8]) -> Result<Self> {
        let (prefix, ns_id) = Self::decode_prefix_meta(meta)?;
        // Resolve the owning namespace first (auto-opens it through the
        // registry); prefix recovery then reserves on the one global
        // allocator, exactly as pre-v16.
        let ns = match ns_id {
            None => Namespace::default_ns(),
            Some(id) => Namespace::open(id)?,
        };
        if !ns.engine().reserve_recovered_prefix(prefix) {
            return Err(VsdbError::Decode {
                detail: format!(
                    "Mapx metadata prefix {} is outside the allocator-issued \
                     range or collides with a concurrently issued prefix",
                    u64::from_le_bytes(prefix)
                ),
            });
        }
        Ok(Self {
            prefix: Prefix::Recovered(prefix),
            ns,
        })
    }

    /// `"VSMAPX01" ‖ prefix_le(8)` for default-namespace handles —
    /// byte-identical to the pre-v16 format — with the owning `ns_id`
    /// appended (8 bytes LE) for non-default namespaces.
    #[inline(always)]
    pub(crate) fn encode_prefix_meta(&self) -> Vec<u8> {
        let mut meta = Vec::with_capacity(MAPX_META_NS_LEN);
        meta.extend_from_slice(MAPX_META_MAGIC);
        meta.extend_from_slice(&self.prefix_bytes());
        let ns_id = self.ns.id();
        if ns_id != DEFAULT_NS_ID {
            meta.extend_from_slice(&ns_id.to_le_bytes());
        }
        meta
    }

    /// Decodes the meta into `(prefix, Option<ns_id>)` — the suffix is
    /// an `Option` encoded by presence: 16 bytes ⇒ `None` (default
    /// namespace, the pre-v16 wire format verbatim), 24 bytes ⇒
    /// `Some(ns_id)`, anything else ⇒ error.
    pub(crate) fn decode_prefix_meta(meta: &[u8]) -> Result<(PreBytes, Option<u64>)> {
        if meta.len() != MAPX_META_LEN && meta.len() != MAPX_META_NS_LEN {
            return Err(VsdbError::Decode {
                detail: format!(
                    "invalid Mapx metadata length: expected {} or {}, got {}",
                    MAPX_META_LEN,
                    MAPX_META_NS_LEN,
                    meta.len()
                ),
            });
        }
        if &meta[..MAPX_META_MAGIC.len()] != MAPX_META_MAGIC {
            return Err(VsdbError::Decode {
                detail: "invalid Mapx metadata magic".to_owned(),
            });
        }
        let mut prefix = PreBytes::default();
        prefix.copy_from_slice(&meta[MAPX_META_MAGIC.len()..MAPX_META_LEN]);
        let ns_id = (meta.len() == MAPX_META_NS_LEN).then(|| {
            let mut b = [0u8; 8];
            b.copy_from_slice(&meta[MAPX_META_LEN..]);
            u64::from_le_bytes(b)
        });
        Ok((prefix, ns_id))
    }

    #[inline(always)]
    pub(crate) fn as_prefix_slice(&self) -> &PreBytes {
        match &self.prefix {
            Prefix::Recovered(bytes) => bytes,
            Prefix::Created(cell) => {
                self.prefix_bytes();
                cell.get().expect("just initialized")
            }
        }
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
        const CLONE_CHUNK: usize = 4096;
        const CLONE_CHUNK_BYTES: usize = 16 * 1024 * 1024;

        let mut new_instance = Self::new_in(ns);
        let mut it = self.iter();
        // Pull the chunk's first pair before opening a batch, so an
        // exhausted iterator ends the loop without staging an empty
        // batch just to drop it.
        while let Some((k, v)) = it.next() {
            let mut batch = new_instance.batch_begin();
            let mut entries = 1usize;
            let mut bytes = k.len() + v.len();
            batch.insert(&k, &v);
            while entries < CLONE_CHUNK && bytes < CLONE_CHUNK_BYTES {
                let Some((k, v)) = it.next() else { break };
                entries += 1;
                bytes += k.len() + v.len();
                batch.insert(&k, &v);
            }
            if let Err(e) = batch.commit() {
                drop(batch);
                // Reclaim the committed chunks; a wipe failure changes
                // nothing for the caller — the original error wins.
                let _ = new_instance.batch_begin_wiped().commit();
                return Err(e);
            }
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

pub(crate) struct SimpleVisitor;

impl<'de> de::Visitor<'de> for SimpleVisitor {
    type Value = Vec<u8>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("bytes")
    }

    fn visit_str<E>(self, v: &str) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v.as_bytes().to_vec())
    }

    fn visit_string<E>(self, v: String) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v.into_bytes())
    }

    fn visit_bytes<E>(self, v: &[u8]) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v.to_vec())
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> StdResult<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v)
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
        Ok(ret)
    }
}

impl Serialize for Mapx {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.encode_prefix_meta())
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
        deserializer
            .deserialize_byte_buf(SimpleVisitor)
            .and_then(|meta| Self::from_prefix_meta(&meta).map_err(de::Error::custom))
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

pub struct MapxIter<'a> {
    db_iter: DbIter,
    _marker: PhantomData<&'a ()>,
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
            _marker: PhantomData,
        };

        Some((k, vmut))
    }
}

pub struct ValueIterMut<'a> {
    // The owning namespace (an `Arc` clone), not a bare engine
    // reference: engines are owned by their `NsInner` (no leak), so a
    // stored reference must ride with its anchor.
    ns: Namespace,
    prefix: PreBytes,
    key: RawKey,
    value: RawValue,
    dirty: bool,
    _marker: PhantomData<&'a mut ()>,
}

impl fmt::Debug for ValueIterMut<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValueIterMut")
            .field("prefix", &self.prefix)
            .field("key", &self.key)
            .field("value", &self.value)
            .field("dirty", &self.dirty)
            .finish()
    }
}

impl Drop for ValueIterMut<'_> {
    fn drop(&mut self) {
        if self.dirty {
            self.ns.engine().insert(self.prefix, &self.key, &self.value);
        }
    }
}

impl Deref for ValueIterMut<'_> {
    type Target = RawValue;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl DerefMut for ValueIterMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.value
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ValueMut<'a> {
    key: RawKey,
    value: RawValue,
    dirty: bool,
    hdr: &'a mut Mapx,
}

impl Drop for ValueMut<'_> {
    fn drop(&mut self) {
        if self.dirty {
            self.hdr.insert(&self.key[..], &self.value[..]);
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
