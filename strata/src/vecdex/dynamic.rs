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
//! dispatched.  Plain `VecDex` is untouched: code that pins its metric
//! at compile time pays nothing for this type's existence.
//!
//! # Persistence
//!
//! A `VecDexDyn` serializes as the metric tag plus the inner `VecDex`
//! metadata, so [`from_meta`](VecDexDyn::from_meta) restores the
//! creation-time metric without the caller re-stating it.  The formats
//! are deliberately distinct: a meta saved through `VecDex<K, D, S>`
//! cannot be loaded as `VecDexDyn<K, S>` or vice versa — pick one
//! handle type per index and stay with it.

use super::{
    HnswConfig, VecDex,
    distance::{Cosine, InnerProduct, L2, MetricKind, Scalar},
};
use crate::common::{
    InstanceId, Namespace,
    ende::{KeyEnDe, ValueEnDe},
    error::Result,
};
use serde::{Deserialize, Serialize};
use std::fmt;

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
#[derive(Serialize, Deserialize)]
#[serde(bound = "K: KeyEnDe + ValueEnDe + Clone + Eq, S: Scalar")]
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
    pub fn keys(&self) -> Box<dyn Iterator<Item = K> + '_> {
        dispatch!(self, idx => Box::new(idx.keys()))
    }

    /// Returns an iterator over all (key, vector) pairs.
    pub fn iter(&self) -> Box<dyn Iterator<Item = (K, Vec<S>)> + '_> {
        dispatch!(self, idx => Box::new(idx.iter()))
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
