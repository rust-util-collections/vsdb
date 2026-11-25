//! Shared immutable vector-index handles and point-in-time queries.
use super::*;
use crate::common::{MapReader, ReadableMap};

impl<K: KeyEnDe, D: DistanceMetric<S>, S: Scalar> ReadableMap for VecDex<K, D, S> {
    type Config = (HnswConfig, D);
}

impl<K: KeyEnDe, D: DistanceMetric<S>, S: Scalar> VecDex<K, D, S> {
    /// Creates a cheap, cloneable reader sharing this index's storage.
    pub fn reader(&self) -> MapReader<Self> {
        MapReader::new(
            self.store.reader(),
            (self.config.clone(), self.metric.clone()),
        )
    }

    /// Captures one committed state for several related graph queries.
    pub fn read_view(&self) -> VecDexReadView<'_, K, D, S> {
        VecDexReadView {
            raw: self.store.read_view(),
            dim: self.config.dim,
            metric: &self.metric,
            _p: PhantomData,
        }
    }
}

impl<K: KeyEnDe, D: DistanceMetric<S>, S: Scalar> MapReader<VecDex<K, D, S>> {
    /// Captures one committed state, retaining it for the lifetime of the view.
    pub fn read_view(&self) -> VecDexReadView<'_, K, D, S> {
        VecDexReadView {
            raw: self.raw.read_view(),
            dim: self.config.0.dim,
            metric: &self.config.1,
            _p: PhantomData,
        }
    }
}

/// Immutable reads at a single engine sequence number.
pub struct VecDexReadView<'a, K, D, S> {
    pub(super) raw: MapxRawReadView<'a>,
    dim: usize,
    metric: &'a D,
    _p: PhantomData<(K, S)>,
}

impl<K: KeyEnDe, D: DistanceMetric<S>, S: Scalar> VecDexReadView<'_, K, D, S> {
    /// Number of vectors in this captured state.
    pub fn len(&self) -> u64 {
        GraphState::read(self.raw.get(STATE_KEY))
            .expect("VecDex: corrupt graph state")
            .node_count
    }

    /// Whether this captured state contains any vectors.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Gets a vector by its user key.
    pub fn get(&self, key: &K) -> Option<Vec<S>> {
        let id = decode_node_id(&self.raw.get(user_key(&key.encode_key()))?);
        self.raw
            .get(node_key(TAG_VEC, id))
            .map(|bytes| decode_vector(&bytes))
    }

    /// Iterates the key and vector pairs visible at this view's sequence number.
    pub fn iter(&self) -> impl Iterator<Item = (K, Vec<S>)> + '_ {
        self.raw
            .range(tag_bounds(TAG_NODE2KEY))
            .map(|(k, v)| decode_pair(&self.raw, &k, &v))
    }

    /// Finds nearest neighbors in the captured graph.
    pub fn search(&self, query: &[S], k: usize) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, None, None)
    }

    /// Searches the captured graph with a custom `ef` (beam width).
    pub fn search_ef(&self, query: &[S], k: usize, ef: usize) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, Some(ef), None)
    }

    /// Searches the captured graph with a decoded-key predicate.
    ///
    /// Semantics and visit bounds match [`VecDex::search_with_filter`].
    pub fn search_with_filter(
        &self,
        query: &[S],
        k: usize,
        predicate: impl Fn(&K) -> bool,
    ) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, None, Some(Filter::Typed(&predicate)))
    }

    /// Searches the captured graph with a custom `ef` and decoded-key predicate.
    pub fn search_ef_with_filter(
        &self,
        query: &[S],
        k: usize,
        ef: usize,
        predicate: impl Fn(&K) -> bool,
    ) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, Some(ef), Some(Filter::Typed(&predicate)))
    }

    /// Searches the captured graph with a predicate over encoded key bytes.
    ///
    /// Avoids `K::decode_key` allocations on rejected candidates during
    /// traversal; only the final `<= k` results are decoded into `K`.
    /// See [`VecDex::search_with_raw_filter`] for the encoded-byte contract.
    pub fn search_with_raw_filter(
        &self,
        query: &[S],
        k: usize,
        predicate: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, None, Some(Filter::Raw(&predicate)))
    }

    /// Searches the captured graph with a custom `ef` and raw encoded-key predicate.
    ///
    /// See [`VecDex::search_with_raw_filter`] for the encoded-byte contract.
    pub fn search_ef_with_raw_filter(
        &self,
        query: &[S],
        k: usize,
        ef: usize,
        predicate: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, Some(ef), Some(Filter::Raw(&predicate)))
    }

    pub(super) fn search_internal(
        &self,
        query: &[S],
        k: usize,
        ef: Option<usize>,
        filter: Option<Filter<'_, K>>,
    ) -> Result<Vec<(K, S)>> {
        validate_vector(self.dim, query)?;
        self.metric.dispatch_search(Search {
            view: &self.raw,
            query,
            k,
            ef,
            filter,
        })
    }
}

pub(super) fn decode_pair<K: KeyEnDe, S: Scalar>(
    view: &MapxRawReadView<'_>,
    key: &[u8],
    value: &[u8],
) -> (K, Vec<S>) {
    let id = u64::from_be_bytes(key[1..].try_into().expect("VecDex node ID"));
    let key = K::decode_key(value).expect("VecDex: corrupt key bytes");
    let raw = view
        .get(node_key(TAG_VEC, id))
        .expect("VecDex: missing vector");
    (key, decode_vector(&raw))
}
