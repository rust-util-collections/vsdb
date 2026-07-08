//! Runtime distance-metric selection for [`VecDex`].
//!
//! [`VecDexDyn`] wraps one [`VecDex`] per supported metric behind an
//! enum, so the metric becomes a *value* ([`MetricKind`]) chosen at
//! construction time instead of a compile-time type parameter.
//!
//! # Cost model
//!
//! Enum dispatch happens **once per public operation** (one `match`),
//! after which the statically monomorphized `VecDex` code runs
//! unchanged — the distance loops themselves are never dynamically
//! dispatched.  The iterators ([`keys`](VecDexDyn::keys) /
//! [`iter`](VecDexDyn::iter)) dispatch per *item* through the same
//! single `match` (an internal enum wrapper — no boxing, no heap
//! allocation).  Plain `VecDex` is untouched: code that pins its
//! metric at compile time pays nothing for this type's existence.
//!
//! # Persistence
//!
//! A `VecDexDyn` serializes as the metric tag plus the inner `VecDex`
//! metadata, so [`from_meta`](VecDexDyn::from_meta) restores the
//! creation-time metric without the caller re-stating it.  The metric
//! tag is a hand-frozen wire constant, not a source-order variant
//! index — reorganizing the enum cannot re-interpret existing metas.
//! The formats are deliberately distinct: a meta saved through
//! `VecDex<K, D, S>` cannot be loaded as `VecDexDyn<K, S>` or vice
//! versa — pick one handle type per index and stay with it.

use super::{
    HnswConfig, VecDex,
    distance::{Cosine, InnerProduct, L2, MetricKind, Scalar},
};
use crate::common::{
    InstanceId, Namespace,
    ende::{KeyEnDe, ValueEnDe},
    error::Result,
};
use serde::{Deserialize, Serialize, de, ser::SerializeTuple};
use std::{fmt, marker::PhantomData, result::Result as StdResult};

/// Runs `$body` once with `$idx` bound to the active variant's inner
/// [`VecDex`] — the single dispatch point of every delegated method.
macro_rules! dispatch {
    ($self:expr, $idx:ident => $body:expr) => {
        match $self {
            VecDexDyn::L2($idx) => $body,
            VecDexDyn::Cosine($idx) => $body,
            VecDexDyn::InnerProduct($idx) => $body,
        }
    };
}

/// Frozen wire tags — the persisted metric discriminant, decoupled
/// from the enum's source order so that reordering variants (or
/// inserting a new one anywhere) can never re-interpret existing
/// metas.  **Append-only**: a new metric takes the next fresh value;
/// existing values are permanent.  Byte-identical to the postcard
/// variant indices the derived impls wrote through v16.2.0.
const WIRE_TAG_L2: u8 = 0;
const WIRE_TAG_COSINE: u8 = 1;
const WIRE_TAG_INNER_PRODUCT: u8 = 2;

/// A [`VecDex`] whose distance metric is selected at **runtime** via
/// [`MetricKind`] — for callers that decide the metric from
/// configuration or user input instead of pinning it in the type.
///
/// The full `VecDex` API is mirrored one-to-one; see [`VecDex`] for
/// the semantics of each operation.
///
/// ```ignore
/// use vsdb::vecdex::{HnswConfig, VecDexDyn, distance::MetricKind};
///
/// let cfg = HnswConfig { dim: 4, ..Default::default() };
/// let mut idx: VecDexDyn<String> = VecDexDyn::new(MetricKind::Cosine, cfg);
///
/// idx.insert(&"doc-a".into(), &[0.1, 0.2, 0.3, 0.4]).unwrap();
/// let results = idx.search(&[0.1, 0.2, 0.3, 0.4], 1).unwrap();
/// assert_eq!(results[0].0, "doc-a");
/// assert_eq!(idx.metric(), MetricKind::Cosine);
/// ```
pub enum VecDexDyn<K, S: Scalar = f32>
where
    K: KeyEnDe + ValueEnDe + Clone + Eq,
{
    /// Euclidean (L2) squared distance.
    L2(VecDex<K, L2, S>),
    /// Cosine distance.
    Cosine(VecDex<K, Cosine, S>),
    /// Negated inner product.
    InnerProduct(VecDex<K, InnerProduct, S>),
}

impl<K, S> Serialize for VecDexDyn<K, S>
where
    K: KeyEnDe + ValueEnDe + Clone + Eq,
    S: Scalar,
{
    fn serialize<Ser>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error>
    where
        Ser: serde::Serializer,
    {
        let mut tup = serializer.serialize_tuple(2)?;
        match self {
            Self::L2(idx) => {
                tup.serialize_element(&WIRE_TAG_L2)?;
                tup.serialize_element(idx)?;
            }
            Self::Cosine(idx) => {
                tup.serialize_element(&WIRE_TAG_COSINE)?;
                tup.serialize_element(idx)?;
            }
            Self::InnerProduct(idx) => {
                tup.serialize_element(&WIRE_TAG_INNER_PRODUCT)?;
                tup.serialize_element(idx)?;
            }
        }
        tup.end()
    }
}

impl<'de, K, S> Deserialize<'de> for VecDexDyn<K, S>
where
    K: KeyEnDe + ValueEnDe + Clone + Eq,
    S: Scalar,
{
    fn deserialize<De>(deserializer: De) -> StdResult<Self, De::Error>
    where
        De: serde::Deserializer<'de>,
    {
        struct DynVisitor<K, S>(PhantomData<(K, S)>);

        impl<'de, K, S> de::Visitor<'de> for DynVisitor<K, S>
        where
            K: KeyEnDe + ValueEnDe + Clone + Eq,
            S: Scalar,
        {
            type Value = VecDexDyn<K, S>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a VecDexDyn meta (metric wire tag + inner VecDex)")
            }

            fn visit_seq<A>(self, mut seq: A) -> StdResult<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let truncated = || de::Error::custom("VecDexDyn: truncated meta");
                let tag: u8 = seq.next_element()?.ok_or_else(truncated)?;
                Ok(match tag {
                    WIRE_TAG_L2 => {
                        VecDexDyn::L2(seq.next_element()?.ok_or_else(truncated)?)
                    }
                    WIRE_TAG_COSINE => {
                        VecDexDyn::Cosine(seq.next_element()?.ok_or_else(truncated)?)
                    }
                    WIRE_TAG_INNER_PRODUCT => VecDexDyn::InnerProduct(
                        seq.next_element()?.ok_or_else(truncated)?,
                    ),
                    other => {
                        return Err(de::Error::custom(format!(
                            "VecDexDyn: unknown metric wire tag {other} \
                             (meta written by a newer version?)"
                        )));
                    }
                })
            }
        }

        deserializer.deserialize_tuple(2, DynVisitor(PhantomData))
    }
}

impl<K, S> fmt::Debug for VecDexDyn<K, S>
where
    K: KeyEnDe + ValueEnDe + Clone + Eq,
    S: Scalar,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VecDexDyn")
            .field("metric", &self.metric())
            .field("inner", dispatch!(self, idx => idx))
            .finish()
    }
}

impl<K, S> VecDexDyn<K, S>
where
    K: KeyEnDe + ValueEnDe + Clone + Eq,
    S: Scalar,
{
    /// Creates a new, empty index using `metric`; otherwise identical
    /// to [`VecDex::new`] (same config validation and panics).
    pub fn new(metric: MetricKind, config: HnswConfig) -> Self {
        match metric {
            MetricKind::L2 => Self::L2(VecDex::new(config)),
            MetricKind::Cosine => Self::Cosine(VecDex::new(config)),
            MetricKind::InnerProduct => Self::InnerProduct(VecDex::new(config)),
        }
    }

    /// [`new`](Self::new) placed in `ns` — every internal component
    /// lands in the same namespace (a composite never spans namespaces).
    pub fn new_in(ns: &Namespace, metric: MetricKind, config: HnswConfig) -> Self {
        ns.scope(|| Self::new(metric, config))
    }

    /// The metric this index was created with.
    pub fn metric(&self) -> MetricKind {
        match self {
            Self::L2(_) => MetricKind::L2,
            Self::Cosine(_) => MetricKind::Cosine,
            Self::InnerProduct(_) => MetricKind::InnerProduct,
        }
    }

    /// The namespace this structure lives in.
    pub fn namespace(&self) -> Namespace {
        dispatch!(self, idx => idx.namespace())
    }

    /// Returns the unique instance ID.
    #[inline(always)]
    pub fn instance_id(&self) -> InstanceId {
        dispatch!(self, idx => idx.instance_id())
    }

    /// Persists this instance's metadata (metric tag included) to disk
    /// so that it can be recovered later via [`from_meta`](Self::from_meta).
    pub fn save_meta(&self) -> Result<InstanceId> {
        let id = self.instance_id();
        crate::common::save_instance_meta(id, self)?;
        Ok(id)
    }

    /// Recovers a `VecDexDyn` from previously saved metadata; the
    /// creation-time metric is restored from the meta itself.
    pub fn from_meta(instance_id: impl Into<InstanceId>) -> Result<Self> {
        crate::common::load_instance_meta(instance_id.into())
    }

    /// Returns the number of indexed vectors.
    pub fn len(&self) -> u64 {
        dispatch!(self, idx => idx.len())
    }

    /// Returns `true` if the index contains no vectors.
    pub fn is_empty(&self) -> bool {
        dispatch!(self, idx => idx.is_empty())
    }

    /// Updates the default search beam width (see
    /// [`VecDex::set_ef_search`], panics included).
    pub fn set_ef_search(&mut self, ef: usize) {
        dispatch!(self, idx => idx.set_ef_search(ef))
    }

    /// Returns the vector associated with the given key, if it exists.
    pub fn get(&self, key: &K) -> Option<Vec<S>> {
        dispatch!(self, idx => idx.get(key))
    }

    /// Returns `true` if the index contains the given key.
    pub fn contains_key(&self, key: &K) -> bool {
        dispatch!(self, idx => idx.contains_key(key))
    }

    /// Returns an iterator over all indexed keys.
    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        match self {
            Self::L2(idx) => DynIter::L2(idx.keys()),
            Self::Cosine(idx) => DynIter::Cosine(idx.keys()),
            Self::InnerProduct(idx) => DynIter::InnerProduct(idx.keys()),
        }
    }

    /// Returns an iterator over all (key, vector) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (K, Vec<S>)> + '_ {
        match self {
            Self::L2(idx) => DynIter::L2(idx.iter()),
            Self::Cosine(idx) => DynIter::Cosine(idx.iter()),
            Self::InnerProduct(idx) => DynIter::InnerProduct(idx.iter()),
        }
    }

    /// Clears all indexed data (see [`VecDex::clear`], panics included).
    pub fn clear(&mut self) {
        dispatch!(self, idx => idx.clear())
    }

    /// Inserts a vector associated with a user key (see
    /// [`VecDex::insert`]).
    pub fn insert(&mut self, key: &K, vector: &[S]) -> Result<()> {
        dispatch!(self, idx => idx.insert(key, vector))
    }

    /// Inserts a batch of (key, vector) pairs (see
    /// [`VecDex::insert_batch`]).
    pub fn insert_batch(&mut self, items: &[(K, Vec<S>)]) -> Result<()> {
        dispatch!(self, idx => idx.insert_batch(items))
    }

    /// Searches for the `k` nearest neighbors of the query vector.
    pub fn search(&self, query: &[S], k: usize) -> Result<Vec<(K, S)>> {
        dispatch!(self, idx => idx.search(query, k))
    }

    /// Searches with a custom `ef` (beam width) for recall/speed tradeoff.
    pub fn search_ef(&self, query: &[S], k: usize, ef: usize) -> Result<Vec<(K, S)>> {
        dispatch!(self, idx => idx.search_ef(query, k, ef))
    }

    /// Searches with a key predicate evaluated during beam search (see
    /// [`VecDex::search_with_filter`]).
    pub fn search_with_filter(
        &self,
        query: &[S],
        k: usize,
        predicate: impl Fn(&K) -> bool,
    ) -> Result<Vec<(K, S)>> {
        dispatch!(self, idx => idx.search_with_filter(query, k, predicate))
    }

    /// Filtered search with a custom `ef` (beam width).
    pub fn search_ef_with_filter(
        &self,
        query: &[S],
        k: usize,
        ef: usize,
        predicate: impl Fn(&K) -> bool,
    ) -> Result<Vec<(K, S)>> {
        dispatch!(self, idx => idx.search_ef_with_filter(query, k, ef, predicate))
    }

    /// Removes a vector by user key. Returns `true` if the key existed
    /// (see [`VecDex::remove`]).
    pub fn remove(&mut self, key: &K) -> Result<bool> {
        dispatch!(self, idx => idx.remove(key))
    }

    /// Rebuilds the HNSW graph from the existing vectors (see
    /// [`VecDex::compact`]).
    pub fn compact(&mut self) -> Result<()> {
        dispatch!(self, idx => idx.compact())
    }
}

/// Zero-allocation iterator over the active variant's inner iterator —
/// what [`VecDexDyn::keys`]/[`VecDexDyn::iter`] return as
/// `impl Iterator`, mirroring `VecDex`'s unboxed iterator API.  Each
/// `next` pays the same single `match` every other delegated
/// operation pays once per call.
enum DynIter<A, B, C> {
    L2(A),
    Cosine(B),
    InnerProduct(C),
}

impl<T, A, B, C> Iterator for DynIter<A, B, C>
where
    A: Iterator<Item = T>,
    B: Iterator<Item = T>,
    C: Iterator<Item = T>,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        match self {
            Self::L2(it) => it.next(),
            Self::Cosine(it) => it.next(),
            Self::InnerProduct(it) => it.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::L2(it) => it.size_hint(),
            Self::Cosine(it) => it.size_hint(),
            Self::InnerProduct(it) => it.size_hint(),
        }
    }
}
