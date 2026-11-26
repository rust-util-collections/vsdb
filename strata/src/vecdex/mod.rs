//! Approximate nearest-neighbor vector index backed by the HNSW algorithm.
//!
//! [`VecDex`] is a persistent, disk-backed vector index that wraps VSDB's
//! core storage primitives.  It supports insert, delete, and k-nearest-neighbor
//! search with configurable distance metrics (`L2`, `Cosine`,
//! `InnerProduct`).
//!
//! [`VecDexDyn`] accepts a runtime [`crate::MetricKind`].
//! Serialize writers that share an index. [`VecDex::reader`] provides a
//! cloneable handle for concurrent queries against captured read views. A search
//! returns the first detected lookup or row-decoding error. Filter calls that
//! already ran can have side effects. On an error, callers must discard any partial work
//! recorded by their predicate.
//!
//! # Storage model (single-handle, crash-atomic)
//!
//! All persistent state lives in **one** [`vsdb_core::MapxRaw`] handle, partitioned
//! by a leading tag byte:
//!
//! ```text
//! [0x00 | node_id BE]            -> unframed IEEE-754 LE components
//! [0x01 | layer | node_id BE]    -> packed u64 LE neighbor list
//! [0x02 | key bytes]             -> node_id u64 LE
//! [0x03 | node_id BE]            -> key bytes
//! [0x04 | node_id BE]            -> node max_layer (postcard)
//! [0x05]                         -> graph state (34 bytes)
//! [0x06]                         -> immutable schema
//! ```
//!
//! Each mutation or bulk-insert chunk stages its rows through a read-your-writes
//! overlay and commits one atomic engine batch. Earlier chunks can remain
//! committed after a later error. Recovery verifies the schema and reads the
//! committed graph state. Power-loss durability requires a shard WAL fence.
//!
//! The serialized form of a `VecDex` (its typed handle metadata) is the
//! single handle prefix, layout version and immutable schema digest.
//! It is **create-time constant**: graph growth only writes ordinary
//! data rows, so metadata saved once at creation stays valid for the
//! lifetime of the instance.
//!
//! # Quick start
//!
//! ```ignore
//! use vsdb::vecdex::{VecDex, HnswConfig, distance::Cosine};
//!
//! let cfg = HnswConfig { dim: 4, ..Default::default() };
//! let mut idx: VecDex<String, Cosine> = VecDex::new(cfg).unwrap();
//!
//! idx.insert(&"doc-a".into(), &[0.1, 0.2, 0.3, 0.4]).unwrap();
//! idx.insert(&"doc-b".into(), &[0.5, 0.6, 0.7, 0.8]).unwrap();
//!
//! let results = idx.search(&[0.1, 0.2, 0.3, 0.4], 1).unwrap();
//! assert_eq!(results[0].0, "doc-a");
//! ```

pub mod distance;
mod reader;
pub use reader::VecDexReadView;
mod hnsw;

/// An index with a runtime-selected, persisted distance metric.
pub type VecDexDyn<K, S = f32> = VecDex<K, distance::MetricKind, S>;

use crate::common::{
    InstanceId,
    ende::{KeyEnDe, ValueEnDe},
    ensure_process_writable, ensure_writable,
    error::{Result, VsdbError},
    staged::StagedRows,
};
use distance::{DistanceMetric, MetricSearch, Scalar};
use hnsw::{
    AdjRead, adj_key, decode_neighbors, encode_neighbors, get_neighbors,
    prune_neighbors, prune_selection, random_layer, search_scored_layer,
    select_neighbors_heuristic,
};
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    marker::PhantomData,
    rc::Rc,
};
use vsdb_core::basic::mapx_raw::{MapxRaw, MapxRawReadView};

// Namespace tags (first key byte).
const TAG_VEC: u8 = 0x00;
pub(crate) const TAG_ADJ: u8 = 0x01;
const TAG_KEY2NODE: u8 = 0x02;
const TAG_NODE2KEY: u8 = 0x03;
const TAG_INFO: u8 = 0x04;
const TAG_STATE: u8 = 0x05;

const STATE_KEY: [u8; 1] = [TAG_STATE];

/// Serialized-payload layout version. Guards against positionally
/// decoding metadata written by the pre-single-handle layout.
const LAYOUT_VERSION: u8 = 2;
const META_KEY: [u8; 1] = [0x06];

/// Configuration for a [`VecDex`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HnswConfig {
    /// Max neighbors per node per layer (default 16).
    pub m: usize,
    /// Max neighbors at the base layer (default 32).
    pub m_max0: usize,
    /// Construction beam width (default 200).
    pub ef_construction: usize,
    /// Default search beam width (default 50).
    pub ef_search: usize,
    /// Vector dimensionality.
    pub dim: usize,
}

impl HnswConfig {
    /// Rejects configurations under which the graph cannot work.
    pub fn validate(&self) -> Result<()> {
        let invalid = |detail: &str| {
            Err(VsdbError::InvalidConfig {
                detail: format!("VecDex: {detail}"),
            })
        };
        if self.dim > u32::MAX as usize {
            return invalid("dim exceeds u32::MAX");
        }
        if self.dim == 0 {
            return invalid("dim must be > 0");
        }
        if self.m < 2 {
            return invalid("m must be >= 2");
        }
        if self.m_max0 < self.m {
            return invalid("m_max0 must be >= m");
        }
        if self.ef_construction == 0 {
            return invalid("ef_construction must be > 0");
        }
        Ok(())
    }
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            m_max0: 32,
            ef_construction: 200,
            ef_search: 50,
            dim: 0,
        }
    }
}

/// Mutable graph state, loaded from one fixed-width row per operation.
#[derive(Debug, Default)]
struct GraphState {
    entry_point: Option<u64>,
    max_layer: u8,
    node_count: u64,
    next_node_id: u64,
    ef_search: usize,
}

#[derive(Serialize, Deserialize)]
struct SchemaHeader {
    version: u8,
    scalar: u8,
    type_tag: u64,
    dim: u32,
    m: usize,
    m_max0: usize,
    ef_construction: usize,
    ef_search: usize,
}

/// The handle carries only a version and a truncated schema digest. A
/// parameter change invalidates an existing handle even for custom metrics.
fn schema_binding(bytes: &[u8]) -> [u8; 9] {
    use sha3::{Digest, Keccak256};
    let mut binding = [LAYOUT_VERSION; 9];
    binding[1..].copy_from_slice(&Keccak256::digest(bytes)[..8]);
    binding
}

fn corrupt(detail: impl Into<String>) -> VsdbError {
    VsdbError::Corrupt {
        detail: detail.into(),
    }
}

impl GraphState {
    fn encode(&self) -> [u8; 34] {
        let mut bytes = [0; 34];
        bytes[0] = u8::from(self.entry_point.is_some());
        bytes[1] = self.max_layer;
        bytes[2..10].copy_from_slice(&self.entry_point.unwrap_or(0).to_le_bytes());
        bytes[10..18].copy_from_slice(&self.node_count.to_le_bytes());
        bytes[18..26].copy_from_slice(&self.next_node_id.to_le_bytes());
        bytes[26..].copy_from_slice(&(self.ef_search as u64).to_le_bytes());
        bytes
    }

    fn read(bytes: Option<Vec<u8>>) -> Result<Self> {
        let bytes = bytes.ok_or_else(|| corrupt("missing VecDex state"))?;
        Self::decode(&bytes)
    }

    fn try_read(store: &MapxRaw) -> Result<Self> {
        store
            .try_get_with(STATE_KEY, Self::decode)?
            .ok_or_else(|| corrupt("missing VecDex state"))
    }

    fn try_read_view(view: &MapxRawReadView<'_>) -> Result<Self> {
        view.try_get_with(STATE_KEY, Self::decode)?
            .ok_or_else(|| corrupt("missing VecDex state"))
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 34 || bytes[0] > 1 {
            return Err(corrupt("invalid VecDex state record"));
        }
        let read = |i: usize| u64::from_le_bytes(bytes[i..i + 8].try_into().unwrap());
        let state = Self {
            entry_point: (bytes[0] == 1).then(|| read(2)),
            max_layer: bytes[1],
            node_count: read(10),
            next_node_id: read(18),
            ef_search: usize::try_from(read(26))
                .map_err(|_| corrupt("invalid ef_search"))?,
        };
        if state.max_layer > 32
            || (state.entry_point.is_none() && (read(2) != 0 || state.max_layer != 0))
            || state.node_count > state.next_node_id
            || state.entry_point.is_some() != (state.node_count > 0)
            || state.entry_point.is_some_and(|ep| ep >= state.next_node_id)
        {
            return Err(corrupt("inconsistent VecDex graph state"));
        }
        Ok(state)
    }

    fn cleared(&self) -> Self {
        Self {
            next_node_id: self.next_node_id,
            ef_search: self.ef_search,
            ..Self::default()
        }
    }
}

fn validate_vector<S: Scalar>(dim: usize, vector: &[S]) -> Result<()> {
    if vector.len() != dim {
        return Err(VsdbError::DimensionMismatch {
            expected: dim,
            found: vector.len(),
        });
    }
    if !vector.iter().all(|component| component.is_finite()) {
        return Err(VsdbError::InvalidConfig {
            detail: "VecDex vectors must contain finite components".to_owned(),
        });
    }
    Ok(())
}

fn decode_vector_into<S: Scalar>(
    raw: &[u8],
    dim: usize,
    out: &mut Vec<S>,
) -> Result<()> {
    if raw.len() != dim * size_of::<S>() {
        return Err(corrupt(
            "VecDex: vector length differs from graph dimensions",
        ));
    }
    S::decode_into(raw, out);
    Ok(())
}

fn decode_vector<S: Scalar>(raw: &[u8], dim: usize) -> Result<Vec<S>> {
    let mut vector = Vec::new();
    decode_vector_into(raw, dim, &mut vector)?;
    Ok(vector)
}
// =========================================================================
// Key codecs
// =========================================================================

#[inline]
fn node_key(tag: u8, node_id: u64) -> [u8; 9] {
    let mut buf = [0u8; 9];
    buf[0] = tag;
    buf[1..9].copy_from_slice(&node_id.to_be_bytes());
    buf
}

fn user_key(key_bytes: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + key_bytes.len());
    v.push(TAG_KEY2NODE);
    v.extend_from_slice(key_bytes);
    v
}

fn decode_node_id(raw: &[u8]) -> Result<u64> {
    raw.try_into()
        .map(u64::from_le_bytes)
        .map_err(|_| corrupt("invalid VecDex forward mapping width"))
}

fn decode_info_key(key: &[u8]) -> Result<u64> {
    let [TAG_INFO, id @ ..] = key else {
        return Err(corrupt("invalid VecDex node-info tag"));
    };
    id.try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| corrupt("invalid VecDex node-info key width"))
}

fn decode_layer(raw: &[u8]) -> Result<u8> {
    match raw {
        [layer @ 0..=32] => Ok(*layer),
        _ => Err(corrupt("invalid VecDex node layer")),
    }
}

// =========================================================================
// Mutation transaction
// =========================================================================

type VecCache<S> = HashMap<u64, Rc<Vec<S>>>;

/// One mutation's staged rows plus a working copy of the graph state.
/// Reads observe the operation's own uncommitted writes.
struct Txn<'a, S> {
    store: &'a MapxRaw,
    rows: StagedRows,
    state: GraphState,
    dim: usize,
    /// Exclusive ID limit before this chunk allocated or linked any new nodes.
    committed_next_node_id: u64,
    /// Per-operation decoded-vector cache: HNSW linking re-reads the
    /// same vectors many times across layers/heuristics (and across 64-item
    /// chunks inside `insert_batch`); decoding each row once removes the
    /// dominant redundant cost while remaining bounded at 32 MiB.
    vec_cache: RefCell<VecCache<S>>,
}

/// Graph reads for public searches, with lazy live-node seeds for disconnected
/// components. The persistent graph and construction-time searches are unchanged.
struct SearchRows<'a, 'db>(&'a MapxRawReadView<'db>);

impl AdjRead for SearchRows<'_, '_> {
    fn neighbors_into(&self, layer: u8, id: u64, out: &mut Vec<u64>) -> Result<()> {
        self.0.neighbors_into(layer, id, out)
    }

    fn fallback_nodes(&self) -> Result<Option<hnsw::NodeSeeds<'_>>> {
        let mut rows = self.0.try_range(tag_bounds(TAG_INFO))?;
        Ok(Some(Box::new(std::iter::from_fn(move || {
            rows.try_step_next().transpose().map(|row| {
                let (key, _) = row?;
                decode_info_key(key)
            })
        }))))
    }
}

impl<S> Txn<'_, S> {
    /// A committed row predates every ID allocated in this chunk. An overlay
    /// row may already refer to nodes added earlier in the same chunk.
    fn with_neighbor_row<T>(
        &self,
        key: &[u8],
        decode: impl FnOnce(&[[u8; 8]]) -> T,
    ) -> Result<Option<T>> {
        let checked = |bytes: &[u8], next_node_id: u64| {
            let (ids, tail) = bytes.as_chunks::<8>();
            if !tail.is_empty() {
                return Err(corrupt("invalid VecDex neighbor row width"));
            }
            if ids.iter().any(|id| u64::from_le_bytes(*id) >= next_node_id) {
                return Err(corrupt("VecDex neighbor ID exceeds the issued range"));
            }
            Ok(decode(ids))
        };
        match self.rows.staged_value(key) {
            Some(staged) => staged
                .map(|bytes| checked(bytes, self.state.next_node_id))
                .transpose(),
            None => self
                .store
                .try_get_with(key, |bytes| checked(bytes, self.committed_next_node_id)),
        }
    }
}

impl<S> AdjRead for Txn<'_, S> {
    fn neighbors_into(&self, layer: u8, id: u64, out: &mut Vec<u64>) -> Result<()> {
        out.clear();
        self.with_neighbor_row(&adj_key(layer, id), |ids| {
            out.extend(ids.iter().map(|id| u64::from_le_bytes(*id)))
        })?;
        Ok(())
    }
}

impl<'a, S: Scalar> Txn<'a, S> {
    fn new(store: &'a MapxRaw, state: GraphState, dim: usize) -> Self {
        Self::with_cache(store, state, dim, HashMap::new())
    }

    fn with_cache(
        store: &'a MapxRaw,
        state: GraphState,
        dim: usize,
        cache: VecCache<S>,
    ) -> Self {
        Self {
            store,
            rows: StagedRows::new(),
            committed_next_node_id: state.next_node_id,
            state,
            dim,
            vec_cache: RefCell::new(cache),
        }
    }

    fn try_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.rows.try_get_over(self.store, key)
    }

    fn try_contains(&self, key: &[u8]) -> Result<bool> {
        self.rows.try_contains_over(self.store, key)
    }

    fn read_vec(&self, node_id: u64) -> Result<Option<Rc<Vec<S>>>> {
        if let Some(v) = self.vec_cache.borrow().get(&node_id) {
            return Ok(Some(Rc::clone(v)));
        }
        let key = node_key(TAG_VEC, node_id);
        let decode = |raw: &[u8]| decode_vector(raw, self.dim).map(Rc::new);
        let vector = match self.rows.staged_value(&key) {
            Some(staged) => staged.map(decode).transpose()?,
            None => self.store.try_get_with(key, decode)?,
        };
        if let Some(v) = &vector {
            self.cache(node_id, v);
        }
        Ok(vector)
    }

    /// Stages a vector row and pre-warms the decode cache.
    fn put_vec(&mut self, node_id: u64, vector: &[S]) {
        self.rows
            .put(node_key(TAG_VEC, node_id).to_vec(), S::encode_slice(vector));
        self.cache(node_id, &Rc::new(vector.to_vec()));
    }

    fn cache(&self, node_id: u64, vector: &Rc<Vec<S>>) {
        // Every vector has the same validated dimension. Account for the
        // payload plus map entry, Rc and Vec header; never cache an oversized row.
        let bytes_per_entry = size_of_val(vector.as_slice()).saturating_add(96);
        let max_entries = (32 << 20) / bytes_per_entry;
        let mut cache = self.vec_cache.borrow_mut();
        if max_entries != 0 {
            if cache.len() >= max_entries {
                cache.clear();
            }
            cache.insert(node_id, Rc::clone(vector));
        }
    }

    fn del_vec(&mut self, node_id: u64) {
        self.rows.del(node_key(TAG_VEC, node_id).to_vec());
        self.vec_cache.get_mut().remove(&node_id);
    }

    fn set_neighbors(&mut self, layer: u8, node_id: u64, neighbors: &[u64]) {
        let key = adj_key(layer, node_id).to_vec();
        if neighbors.is_empty() {
            self.rows.del(key);
        } else {
            self.rows.put(key, encode_neighbors(neighbors));
        }
    }

    fn remove_adjacency(&mut self, layer: u8, node_id: u64) {
        self.rows.del(adj_key(layer, node_id).to_vec());
    }

    fn apply_pruned_selection(
        &mut self,
        node_id: u64,
        layer: u8,
        mut pruned: Vec<u64>,
        mut evicted: Vec<u64>,
        keep: Option<u64>,
    ) -> Result<bool> {
        if let Some(keep_id) = keep
            && evicted.contains(&keep_id)
            && let Some(pos) = pruned.iter().rposition(|&id| id != keep_id)
        {
            let displaced = std::mem::replace(&mut pruned[pos], keep_id);
            evicted.retain(|&id| id != keep_id);
            if !evicted.contains(&displaced) {
                evicted.push(displaced);
            }
        }
        self.set_neighbors(layer, node_id, &pruned);
        let kept = keep.is_none_or(|k| !evicted.contains(&k));
        for evicted_id in evicted {
            let mut e_list = get_neighbors(self, layer, evicted_id)?;
            e_list.retain(|&x| x != node_id);
            self.set_neighbors(layer, evicted_id, &e_list);
        }
        Ok(kept)
    }

    /// Applies the pruning result of
    /// [`prune_selection`](hnsw::prune_selection) for `(node, layer)` and
    /// detaches the evicted back-edges, mirroring the insert-time
    /// neighbor-eviction protocol. When `keep` is present, it is retained
    /// by displacing the heuristic's last choice if necessary.
    fn prune_and_detach<D: DistanceMetric<S>>(
        &mut self,
        metric: &D,
        node_id: u64,
        layer: u8,
        m_max: usize,
        keep: Option<u64>,
    ) -> Result<bool> {
        let pruned = {
            let gv = |id: u64| self.read_vec(id);
            prune_selection::<S, D, Self>(metric, node_id, layer, m_max, self, &gv)?
        };
        let Some((pruned, evicted)) = pruned else {
            return Ok(true);
        };
        self.apply_pruned_selection(node_id, layer, pruned, evicted, keep)
    }

    /// Appends `added` to `(neighbor, layer)` in a single adjacency read,
    /// bypassing decode/prune when the list stays within `m_max`.
    fn append_neighbor_and_prune<D: DistanceMetric<S>>(
        &mut self,
        metric: &D,
        neighbor: u64,
        added: u64,
        layer: u8,
        m_max: usize,
        keep: Option<u64>,
    ) -> Result<()> {
        let key = adj_key(layer, neighbor);
        let mut raw = self
            .with_neighbor_row(&key, |ids| ids.as_flattened().to_vec())?
            .unwrap_or_default();
        if raw.len() / 8 < m_max {
            raw.extend_from_slice(&added.to_le_bytes());
            self.rows.put(key.to_vec(), raw);
            return Ok(());
        }
        let mut neighbors = decode_neighbors(&raw);
        neighbors.push(added);
        let pruned = {
            let gv = |id: u64| self.read_vec(id);
            prune_neighbors::<S, D>(metric, neighbor, &neighbors, m_max, &gv)?
        };
        if let Some((pruned, evicted)) = pruned {
            self.apply_pruned_selection(neighbor, layer, pruned, evicted, keep)?;
        } else {
            self.set_neighbors(layer, neighbor, &neighbors);
        }
        Ok(())
    }

    /// Stages the state row and hands back `(rows, vec_cache)` for chunked batching.
    fn finish_with_cache(mut self) -> (StagedRows, VecCache<S>) {
        self.rows
            .put(STATE_KEY.to_vec(), self.state.encode().to_vec());
        (self.rows, self.vec_cache.into_inner())
    }

    /// Stages the state row and hands the parts back for commit.
    fn finish(self) -> StagedRows {
        self.finish_with_cache().0
    }
}

fn encode_value<T: ValueEnDe>(v: &T) -> Vec<u8> {
    v.encode_value()
}

/// A persistent, disk-backed approximate nearest-neighbor index
/// using the HNSW (Hierarchical Navigable Small World) algorithm.
///
/// Type parameters:
/// - `K`: user-facing key type.
/// - `D`: distance metric ([`L2`](distance::L2), [`Cosine`](distance::Cosine),
///   [`InnerProduct`](distance::InnerProduct)).
/// - `S`: scalar type for vector components (`f32` or `f64`, default `f32`).
///
/// Each mutation or bulk-insert chunk uses one atomic engine batch for rows
/// and graph state. This preserves their consistency across recovery under
/// the ownership contract below; it does not imply power-loss durability
/// for every successful call.
///
/// # Handle ownership
///
/// Serde deserialization and [`from_meta`](Self::from_meta) restore a handle
/// onto the same graph. Serialize all writers to that graph, including
/// writers using distinct restored handles. Sequential writers see each
/// other's committed state. Readers can query while a writer runs.
/// Searches and iteration use a single engine snapshot. Point lookups use
/// never-reused node IDs and can return `None` if a concurrent write removes
/// the referenced vector.
pub struct VecDex<K, D, S: Scalar = f32>
where
    K: KeyEnDe,
    D: DistanceMetric<S>,
{
    store: MapxRaw,
    config: HnswConfig,
    metric: D,
    _p: PhantomData<(K, S)>,
}

impl<K, D, S> Serialize for VecDex<K, D, S>
where
    K: KeyEnDe,
    D: DistanceMetric<S>,
    S: Scalar,
{
    fn serialize<Ser>(&self, serializer: Ser) -> std::result::Result<Ser::Ok, Ser::Error>
    where
        Ser: serde::Serializer,
    {
        let schema = self.schema().map_err(serde::ser::Error::custom)?;
        let extra = schema_binding(&schema);
        crate::common::serialize_prefix_handle::<Self, _>(
            &self.store,
            &extra,
            serializer,
        )
    }
}

impl<'de, K, D, S> Deserialize<'de> for VecDex<K, D, S>
where
    K: KeyEnDe,
    D: DistanceMetric<S>,
    S: Scalar,
{
    fn deserialize<De>(deserializer: De) -> std::result::Result<Self, De::Error>
    where
        De: serde::Deserializer<'de>,
    {
        use crate::common::PrefixFrame;
        crate::common::deserialize_prefix_handle::<Self, _, _>(deserializer, |frame| {
            let PrefixFrame(store, extra) = frame;
            Self::hydrate(store, extra)
        })
    }
}

impl<K, D, S> std::fmt::Debug for VecDex<K, D, S>
where
    K: KeyEnDe,
    D: DistanceMetric<S>,
    S: Scalar,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VecDex")
            .field("node_count", &self.len())
            .field("dim", &self.config.dim)
            .field("max_layer", &self.state().max_layer)
            .finish()
    }
}

// Convenience aliases (f32)
pub type VecDexL2<K> = VecDex<K, distance::L2>;
pub type VecDexCosine<K> = VecDex<K, distance::Cosine>;

// f64 aliases
pub type VecDexL2F64<K> = VecDex<K, distance::L2, f64>;
pub type VecDexCosineF64<K> = VecDex<K, distance::Cosine, f64>;

impl<K, D, S> VecDex<K, D, S>
where
    K: KeyEnDe,
    D: DistanceMetric<S>,
    S: Scalar,
{
    /// [`new`](Self::new) placed in `ns` — every internal component
    /// lands in the same namespace (a composite never spans namespaces).
    pub fn new_in(ns: &crate::common::Namespace, config: HnswConfig) -> Result<Self> {
        ns.scope(|| Self::new(config))
    }

    /// The namespace this structure lives in.
    pub fn namespace(&self) -> crate::common::Namespace {
        self.store.namespace()
    }

    /// Creates a new, empty `VecDex` with the given configuration.
    ///
    /// # Errors
    ///
    /// [`VsdbError::InvalidConfig`] unless `dim > 0`, `m >= 2`,
    /// `m_max0 >= m` (else base-layer nodes have no edges and become
    /// unreachable) and `ef_construction > 0` (else a layer search returns
    /// no candidates). [`VsdbError::ReadOnly`] in read-only mode.
    pub fn new(config: HnswConfig) -> Result<Self> {
        Self::new_with_metric(D::default(), config)
    }

    /// Creates an index with the supplied metric and its persisted parameters.
    pub fn new_with_metric(metric: D, config: HnswConfig) -> Result<Self> {
        config.validate()?;
        ensure_process_writable("vector index new")?;
        let state = GraphState {
            ef_search: config.ef_search,
            ..GraphState::default()
        };
        let mut index = Self {
            store: MapxRaw::new(),
            config,
            metric,
            _p: PhantomData,
        };
        let schema = index.schema()?;
        let mut batch = index.store.batch();
        batch.insert(META_KEY, schema);
        batch.insert(STATE_KEY, state.encode());
        batch.commit()?;
        Ok(index)
    }

    /// Creates an index in the specified namespace with a configured metric.
    pub fn new_with_metric_in(
        ns: &crate::Namespace,
        metric: D,
        config: HnswConfig,
    ) -> Result<Self> {
        ns.scope(|| Self::new_with_metric(metric, config))
    }

    /// Returns the configured metric and its parameters.
    pub fn metric(&self) -> D {
        self.metric.clone()
    }

    fn schema(&self) -> Result<Vec<u8>> {
        let header = SchemaHeader {
            version: LAYOUT_VERSION,
            scalar: size_of::<S>() as u8,
            type_tag: crate::common::type_tag::<Self>(),
            dim: self.config.dim as u32,
            m: self.config.m,
            m_max0: self.config.m_max0,
            ef_construction: self.config.ef_construction,
            ef_search: self.config.ef_search,
        };
        Ok(postcard::to_allocvec(&(header, &self.metric))?)
    }

    fn hydrate(store: MapxRaw, binding: &[u8]) -> Result<Self> {
        let (config, metric) = Self::validate_store(&store, binding)?;
        Ok(Self {
            store,
            config,
            metric,
            _p: PhantomData,
        })
    }

    fn validate_store(store: &MapxRaw, binding: &[u8]) -> Result<(HnswConfig, D)> {
        let schema = store
            .try_get_with(META_KEY, |raw| Ok(raw.to_vec()))?
            .ok_or_else(|| corrupt("missing VecDex schema"))?;
        if binding != schema_binding(&schema) {
            return Err(corrupt("VecDex schema binding mismatch"));
        }
        let (header, parameters): (SchemaHeader, _) =
            postcard::take_from_bytes(&schema)?;
        if header.version != LAYOUT_VERSION
            || header.scalar != size_of::<S>() as u8
            || header.type_tag != crate::common::type_tag::<Self>()
        {
            return Err(corrupt("VecDex schema type mismatch"));
        }
        let (metric, tail) = postcard::take_from_bytes(parameters)?;
        if !tail.is_empty() {
            return Err(corrupt("trailing VecDex schema parameters"));
        }
        let config = HnswConfig {
            dim: header.dim as usize,
            m: header.m,
            m_max0: header.m_max0,
            ef_construction: header.ef_construction,
            ef_search: header.ef_search,
        };
        config.validate()?;
        GraphState::try_read(store)?;
        Ok((config, metric))
    }

    fn state(&self) -> GraphState {
        GraphState::read(self.store.get(STATE_KEY)).expect("VecDex: corrupt graph state")
    }

    /// Deep-copies the entire index, preserving its graph, into a fresh prefix.
    pub fn clone_in(&self, ns: &crate::Namespace) -> Result<Self> {
        let binding = schema_binding(&self.schema()?);
        let mut store = self.store.clone_in(ns)?;
        match Self::validate_store(&store, &binding) {
            Ok((config, metric)) => Ok(Self {
                store,
                config,
                metric,
                _p: PhantomData,
            }),
            Err(err) => {
                let _ = store.batch_wiped().commit();
                Err(err)
            }
        }
    }

    /// Returns the unique instance ID.
    #[inline(always)]
    pub fn instance_id(&self) -> InstanceId {
        self.store.instance_id()
    }

    /// Persists this instance's metadata to disk so that it can be
    /// recovered later via [`from_meta`](Self::from_meta).
    ///
    /// The metadata is create-time constant (single handle + creation
    /// config), so calling this once after creation is sufficient for
    /// the lifetime of the instance.
    pub fn save_meta(&self) -> Result<InstanceId> {
        let id = self.instance_id();
        crate::common::save_instance_meta(id, self)?;
        Ok(id)
    }

    /// Recovers a `VecDex` from previously saved metadata.
    ///
    /// Verifies the metric parameters, scalar width and creation-time schema
    /// against the index's immutable schema row before serving any reads.
    pub fn from_meta(instance_id: impl Into<InstanceId>) -> Result<Self> {
        let id = instance_id.into();
        crate::common::load_instance_meta_checked(
            id,
            |crate::common::PrefixFrame(store, extra)| Self::hydrate(store, extra),
            Self::instance_id,
        )
    }

    /// Returns the number of indexed vectors.
    pub fn len(&self) -> u64 {
        self.state().node_count
    }

    /// Returns `true` if the index contains no vectors.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Updates the default search beam width (persisted).
    ///
    /// # Errors
    ///
    /// [`VsdbError::ReadOnly`] in read-only mode, or the commit error;
    /// preflight rejection leaves state unchanged. Runtime state updates
    /// only after commit succeeds. A persistence error can leave recoverable
    /// WAL data and must not be retried automatically.
    pub fn set_ef_search(&mut self, ef: usize) -> Result<()> {
        ensure_writable(&self.namespace(), "vector index ef update")?;
        let mut txn: Txn<'_, S> = Txn::new(
            &self.store,
            GraphState::try_read(&self.store)?,
            self.config.dim,
        );
        txn.state.ef_search = ef;
        let rows = txn.finish();
        rows.commit(&mut self.store)?;
        Ok(())
    }

    /// Returns the vector associated with the given key, if it exists.
    ///
    /// Panics if the stored forward mapping or vector has an invalid width.
    pub fn get(&self, key: &K) -> Option<Vec<S>> {
        let node_id = decode_node_id(&self.store.get(user_key(&key.encode_key()))?)
            .expect("VecDex: corrupt forward mapping");
        self.store.get(node_key(TAG_VEC, node_id)).map(|raw| {
            decode_vector(&raw, self.config.dim).expect("VecDex: corrupt vector")
        })
    }

    /// Returns `true` if the index contains the given key.
    pub fn contains_key(&self, key: &K) -> bool {
        self.store.contains_key(user_key(&key.encode_key()))
    }

    /// Returns an iterator over all indexed keys.
    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.scan_tag(TAG_KEY2NODE).map(|(raw_key, _)| {
            K::decode_key(&raw_key[1..]).expect("VecDex: corrupt key bytes")
        })
    }

    /// Returns an iterator over all (key, vector) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (K, Vec<S>)> + '_ {
        let view = self.store.read_view();
        let rows = view.range_detached(tag_bounds(TAG_NODE2KEY));
        let dim = self.config.dim;
        rows.map(move |(key, value)| reader::decode_pair(&view, &key, &value, dim))
    }

    fn scan_tag(
        &self,
        tag: u8,
    ) -> impl Iterator<Item = (vsdb_core::common::RawKey, vsdb_core::common::RawValue)> + '_
    {
        self.store.range(tag_bounds(tag))
    }

    /// Clears all indexed data.
    ///
    /// The wipe (one engine-level range tombstone) and the reset graph
    /// state row — which preserves the live `ef_search`, also across a
    /// later restore — commit in **one atomic engine write batch**: a
    /// crash can never expose a partially-cleared index.
    ///
    /// # Errors
    ///
    /// [`VsdbError::ReadOnly`] in read-only mode, or the commit error;
    /// preflight rejection leaves state unchanged. Runtime state updates
    /// only after commit succeeds. A persistence error can leave recoverable
    /// WAL data and must not be retried automatically.
    pub fn clear(&mut self) -> Result<()> {
        ensure_writable(&self.namespace(), "vector index clear")?;
        let mut txn: Txn<'_, S> = Txn::new(
            &self.store,
            GraphState::try_read(&self.store)?.cleared(),
            self.config.dim,
        );
        txn.rows.wipe();
        txn.rows.put(
            META_KEY.to_vec(),
            self.store
                .try_get_with(META_KEY, |raw| Ok(raw.to_vec()))?
                .ok_or_else(|| corrupt("missing VecDex schema"))?,
        );
        let rows = txn.finish();
        rows.commit(&mut self.store)?;
        Ok(())
    }

    /// Inserts a finite vector associated with a user key.
    ///
    /// If the key already exists, removal of the old node and insertion
    /// of the replacement are staged in the same atomic batch.
    ///
    /// All rows of the insert proper — the vector, both key mappings,
    /// the node info, every adjacency update, and the graph state — are
    /// committed through one atomic engine batch.
    ///
    /// # Errors
    ///
    /// Returns a dimension/capability error, a batch commit error, or
    /// [`VsdbError::Corrupt`] when linking encounters a malformed stored row.
    /// Validation and preflight rejection leave state unchanged. Runtime
    /// state updates only after commit succeeds. A persistence error can
    /// leave recoverable WAL data and must not be retried automatically.
    pub fn insert(&mut self, key: &K, vector: &[S]) -> Result<()> {
        ensure_writable(&self.namespace(), "vector index insert")?;
        validate_vector(self.config.dim, vector)?;
        let mut encoded_key = Vec::new();
        key.encode_key_into(&mut encoded_key)?;

        let mut txn = Txn::new(
            &self.store,
            GraphState::try_read(&self.store)?,
            self.config.dim,
        );
        Self::stage_remove(&self.metric, &mut txn, &self.config, &encoded_key)?;
        Self::stage_insert(&self.metric, &mut txn, &self.config, encoded_key, vector)?;
        let rows = txn.finish();
        rows.commit(&mut self.store)?;
        Ok(())
    }

    /// Stages one whole insert (rows + graph linking) into `txn`.
    fn stage_insert(
        metric: &D,
        txn: &mut Txn<'_, S>,
        config: &HnswConfig,
        encoded_key: Vec<u8>,
        vector: &[S],
    ) -> Result<()> {
        let node_id = txn.state.next_node_id;
        let node_layer = random_layer(config.m);

        txn.put_vec(node_id, vector);
        txn.rows
            .put(user_key(&encoded_key), node_id.to_le_bytes().to_vec());
        txn.rows
            .put(node_key(TAG_NODE2KEY, node_id).to_vec(), encoded_key);
        txn.rows.put(
            node_key(TAG_INFO, node_id).to_vec(),
            encode_value(&node_layer),
        );

        txn.state.next_node_id = node_id
            .checked_add(1)
            .ok_or_else(|| corrupt("VecDex node ID exhausted"))?;
        txn.state.node_count += 1;

        Self::link_node(metric, txn, config, node_id, vector, node_layer)
    }

    /// Inserts a batch of (key, vector) pairs.
    ///
    /// Semantically equivalent to calling [`insert`](Self::insert) in a
    /// loop, but the inserts are staged in chunks that share one
    /// transaction (and thus one atomic engine write batch) each —
    /// amortizing the per-commit cost, which dominates bulk loads.
    ///
    /// Pre-existing keys are removed and reinserted inside their chunk's
    /// atomic transaction, and duplicate keys inside `items` collapse to
    /// the last occurrence.
    pub fn insert_batch(&mut self, items: &[(K, Vec<S>)]) -> Result<()> {
        ensure_writable(&self.namespace(), "vector index batch insert")?;
        // Bounded chunks keep the staged set (and the engine batch)
        // at a sane size for arbitrarily large bulk loads.
        const CHUNK: usize = 64;

        for (_, vec) in items {
            validate_vector(self.config.dim, vec)?;
        }

        // Last occurrence of each key wins (matching the final state of
        // a serial insert loop), preserving the order of survivors.
        let mut seen: HashSet<Vec<u8>> = HashSet::with_capacity(items.len());
        let mut dedup: Vec<(Vec<u8>, &[S])> = Vec::with_capacity(items.len());
        for (key, vec) in items.iter().rev() {
            let mut encoded_key = Vec::new();
            key.encode_key_into(&mut encoded_key)?;
            if !seen.contains(encoded_key.as_slice()) {
                seen.insert(encoded_key.clone());
                dedup.push((encoded_key, vec.as_slice()));
            }
        }
        drop(seen);
        dedup.reverse();

        let mut vec_cache = VecCache::new();
        for chunk in dedup.chunks_mut(CHUNK) {
            let mut txn = Txn::with_cache(
                &self.store,
                GraphState::try_read(&self.store)?,
                self.config.dim,
                vec_cache,
            );
            for (encoded_key, vec) in chunk {
                let encoded_key = std::mem::take(encoded_key);
                Self::stage_remove(&self.metric, &mut txn, &self.config, &encoded_key)?;
                Self::stage_insert(
                    &self.metric,
                    &mut txn,
                    &self.config,
                    encoded_key,
                    vec,
                )?;
            }
            let (rows, next_cache) = txn.finish_with_cache();
            rows.commit(&mut self.store)?;
            vec_cache = next_cache;
        }
        Ok(())
    }

    /// Wires `node_id` into the HNSW graph (the linking phases of an
    /// insert): greedy descent from the entry point, then heuristic
    /// neighbor selection and bidirectional edge creation on every layer
    /// from `min(node_layer, max_layer)` down to 0, raising the entry
    /// point afterwards if `node_layer` exceeds the current maximum.
    ///
    /// All reads observe the transaction's own staged writes.
    fn link_node(
        metric: &D,
        txn: &mut Txn<'_, S>,
        config: &HnswConfig,
        node_id: u64,
        vector: &[S],
        node_layer: u8,
    ) -> Result<()> {
        let Some(ep) = txn.state.entry_point else {
            txn.state.entry_point = Some(node_id);
            txn.state.max_layer = node_layer;
            return Ok(());
        };
        if ep == node_id {
            return Ok(());
        }

        let cur_max = txn.state.max_layer;

        // Phase 1: Greedy descent from top layer to node_layer + 1.
        let mut cur_ep = vec![ep];
        for l in (node_layer.saturating_add(1)..=cur_max).rev() {
            let res = {
                let dist =
                    |id| Ok(txn.read_vec(id)?.map(|v| metric.distance(vector, &v)));
                search_scored_layer(dist, &cur_ep, 1, l, txn, None)?
            };
            if let Some(&(_, id)) = res.iter().find(|&&(_, id)| id != node_id) {
                cur_ep = vec![id];
            }
        }

        // Phase 2: Insert at layers node_layer..0 with heuristic selection.
        let top = node_layer.min(cur_max);
        for l in (0..=top).rev() {
            let m_max = if l == 0 { config.m_max0 } else { config.m };

            let candidates = {
                let dist =
                    |id| Ok(txn.read_vec(id)?.map(|v| metric.distance(vector, &v)));
                search_scored_layer(dist, &cur_ep, config.ef_construction, l, txn, None)?
            };
            let neighbor_pool: Vec<(S, u64)> = candidates
                .iter()
                .copied()
                .filter(|&(_, id)| id != node_id)
                .collect();

            let selected = {
                let gv = |id: u64| txn.read_vec(id);
                select_neighbors_heuristic::<S, D>(metric, &neighbor_pool, m_max, &gv)?
            };

            txn.set_neighbors(l, node_id, &selected);

            for (i, &neighbor) in selected.iter().enumerate() {
                // At least one reciprocal edge per linked layer must
                // survive pruning; otherwise every neighbor can evict the
                // new node and commit an unreachable entry point.
                let keep = (i == 0).then_some(node_id);
                txn.append_neighbor_and_prune(
                    metric, neighbor, node_id, l, m_max, keep,
                )?;
            }

            cur_ep = neighbor_pool.iter().map(|&(_, id)| id).collect();
            if cur_ep.is_empty() {
                cur_ep = vec![ep];
            }
        }

        if node_layer > cur_max {
            txn.state.entry_point = Some(node_id);
            txn.state.max_layer = node_layer;
        }
        Ok(())
    }

    /// Searches for the `k` nearest neighbors of the query vector.
    pub fn search(&self, query: &[S], k: usize) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, None, None)
    }

    /// Searches with a custom `ef` (beam width) for recall/speed tradeoff.
    ///
    /// If a graph component is exhausted before filling the beam, resumes
    /// from unvisited live nodes. An unfiltered beam at least as large as
    /// [`len`](Self::len) visits every vector, including in older disconnected
    /// graphs, without rebuilding stored edges. Smaller beams remain approximate.
    pub fn search_ef(&self, query: &[S], k: usize, ef: usize) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, Some(ef), None)
    }

    /// Searches with a key predicate evaluated during beam search.
    ///
    /// Non-matching nodes still participate in graph traversal to maintain
    /// connectivity, but are excluded from the result set. The search keeps
    /// expanding until it holds `max(ef, k)` matching candidates and the
    /// frontier is farther than the worst of them, so a selective predicate
    /// costs more visited nodes (roughly ∝ 1 / selectivity) rather than
    /// fewer results; a predicate that almost nothing satisfies stops at a
    /// visit cap of `max(64 × max(ef, k), 4096)` evaluated nodes.
    /// Exhausted components resume from unvisited live nodes; those restart
    /// candidates count toward the same cap.
    ///
    /// Use [`search_ef_with_filter`](Self::search_ef_with_filter) to trade
    /// recall against work explicitly.
    pub fn search_with_filter(
        &self,
        query: &[S],
        k: usize,
        predicate: impl Fn(&K) -> bool,
    ) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, None, Some(Filter::Typed(&predicate)))
    }

    /// Filtered search with a custom `ef` (beam width).
    pub fn search_ef_with_filter(
        &self,
        query: &[S],
        k: usize,
        ef: usize,
        predicate: impl Fn(&K) -> bool,
    ) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, Some(ef), Some(Filter::Typed(&predicate)))
    }

    /// Searches with a predicate evaluated directly against encoded key bytes.
    ///
    /// Rejected candidates do not invoke [`KeyEnDe::decode_key`] (avoiding
    /// heap key allocations during beam expansion); only the final `<= k`
    /// winners are decoded into `K`.
    ///
    /// # Encoded-key & visit-bound contract
    ///
    /// - The slice passed to `predicate` is the exact output of
    ///   [`KeyEnDe::encode_key`]. Under the blanket `postcard` implementation,
    ///   integers use varint encoding and variable-length containers carry a
    ///   leading length varint — raw prefix checks apply only to
    ///   self-delimiting leading fields or custom fixed-width [`KeyEnDe`]
    ///   implementations, not bytewise range comparisons on `postcard` numbers.
    /// - Graph traversal visits and row reads match [`search_with_filter`](Self::search_with_filter)
    ///   (stopping at `max(64 × max(ef, k), 4096)` evaluated nodes); use
    ///   [`search_ef_with_raw_filter`](Self::search_ef_with_raw_filter) to tune
    ///   `ef` explicitly for ultra-selective predicates.
    pub fn search_with_raw_filter(
        &self,
        query: &[S],
        k: usize,
        predicate: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, None, Some(Filter::Raw(&predicate)))
    }

    /// Raw encoded-key filtered search with a custom `ef` (beam width).
    ///
    /// See [`search_with_raw_filter`](Self::search_with_raw_filter) for the
    /// encoded-byte contract.
    pub fn search_ef_with_raw_filter(
        &self,
        query: &[S],
        k: usize,
        ef: usize,
        predicate: impl Fn(&[u8]) -> bool,
    ) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, Some(ef), Some(Filter::Raw(&predicate)))
    }

    /// Iterates `(node_id, max_layer)` over the committed node-info rows.
    #[cfg(test)]
    fn node_layers(&self) -> impl Iterator<Item = (u64, u8)> + '_ {
        self.scan_tag(TAG_INFO).map(|(raw_key, raw_val)| {
            (
                decode_info_key(&raw_key).unwrap(),
                decode_layer(&raw_val).unwrap(),
            )
        })
    }

    fn search_internal(
        &self,
        query: &[S],
        k: usize,
        ef: Option<usize>,
        filter: Option<Filter<'_, K>>,
    ) -> Result<Vec<(K, S)>> {
        self.read_view().search_internal(query, k, ef, filter)
    }

    /// Removes a vector by user key. Returns `true` if the key existed.
    ///
    /// Former neighbors of the removed node are reconnected to each
    /// other (best-effort) to preserve graph connectivity.  Every row
    /// update — edge rewires, row removals, and the graph state — is
    /// committed through one atomic engine batch.
    ///
    /// # Errors
    ///
    /// Read-only and preflight rejection leave state unchanged. Runtime
    /// state updates only after commit succeeds. A persistence error can
    /// leave recoverable WAL data and must not be retried automatically.
    /// A malformed mapping or node-info row encountered during removal returns
    /// [`VsdbError::Corrupt`] before the mutation is published.
    pub fn remove(&mut self, key: &K) -> Result<bool> {
        ensure_writable(&self.namespace(), "vector index remove")?;
        key.try_with_encoded_key(|encoded_key| {
            let mut txn = Txn::new(
                &self.store,
                GraphState::try_read(&self.store)?,
                self.config.dim,
            );
            if !Self::stage_remove(&self.metric, &mut txn, &self.config, encoded_key)? {
                return Ok(false);
            }
            let rows = txn.finish();
            rows.commit(&mut self.store)?;
            Ok(true)
        })
    }

    fn stage_remove(
        metric: &D,
        txn: &mut Txn<'_, S>,
        config: &HnswConfig,
        encoded_key: &[u8],
    ) -> Result<bool> {
        let Some(raw) = txn.try_get(&user_key(encoded_key))? else {
            return Ok(false);
        };
        let node_id = decode_node_id(&raw)?;
        if node_id >= txn.state.next_node_id
            || txn.try_get(&node_key(TAG_NODE2KEY, node_id))?.as_deref()
                != Some(encoded_key)
        {
            return Err(corrupt("VecDex forward and reverse mappings disagree"));
        }
        if txn.read_vec(node_id)?.is_none() {
            return Err(corrupt("missing VecDex vector for live node"));
        }
        let raw_info = txn
            .try_get(&node_key(TAG_INFO, node_id))?
            .ok_or_else(|| corrupt("missing VecDex node info"))?;
        let max_layer = decode_layer(&raw_info)?;
        if max_layer > txn.state.max_layer {
            return Err(corrupt("VecDex node layer exceeds graph maximum"));
        }

        // Phase 1: Remove edges and collect former neighbors per layer.
        let mut former_neighbors: Vec<Vec<u64>> =
            Vec::with_capacity(max_layer as usize + 1);
        for l in 0..=max_layer {
            let neighbors = get_neighbors(&*txn, l, node_id)?;
            for &n in &neighbors {
                let mut n_list = get_neighbors(&*txn, l, n)?;
                n_list.retain(|&x| x != node_id);
                txn.set_neighbors(l, n, &n_list);
            }
            txn.remove_adjacency(l, node_id);
            former_neighbors.push(neighbors);
        }

        // Phase 2: Reconnect former neighbors (best-effort).
        // Runs before the vector row is removed so distance computation
        // still works.
        for l in 0..=max_layer {
            let m_max = if l == 0 { config.m_max0 } else { config.m };
            let fns = &former_neighbors[l as usize];
            for &n in fns {
                let cur = get_neighbors(&*txn, l, n)?;
                if cur.len() >= m_max {
                    continue;
                }
                let slots = m_max - cur.len();
                let cur_set: HashSet<u64> = cur.iter().copied().collect();
                let mut added = 0usize;
                let mut keep = None;
                for &candidate in fns {
                    if added >= slots {
                        break;
                    }
                    if candidate == n || cur_set.contains(&candidate) {
                        continue;
                    }
                    let mut n_list = get_neighbors(&*txn, l, n)?;
                    n_list.push(candidate);
                    txn.set_neighbors(l, n, &n_list);

                    let mut c_list = get_neighbors(&*txn, l, candidate)?;
                    c_list.push(n);
                    txn.set_neighbors(l, candidate, &c_list);

                    if txn.prune_and_detach(metric, candidate, l, m_max, Some(n))? {
                        keep.get_or_insert(candidate);
                        added += 1;
                    }
                }
                if added > 0 {
                    txn.prune_and_detach(metric, n, l, m_max, keep)?;
                }
            }
        }

        // Phase 3: Clean up rows and state.
        txn.del_vec(node_id);
        txn.rows.del(user_key(encoded_key));
        txn.rows.del(node_key(TAG_NODE2KEY, node_id).to_vec());
        txn.rows.del(node_key(TAG_INFO, node_id).to_vec());

        txn.state.node_count = txn
            .state
            .node_count
            .checked_sub(1)
            .ok_or_else(|| corrupt("inconsistent VecDex node count"))?;

        if Self::entry_point_needs_repair(txn, node_id)? {
            Self::repair_entry_point(metric, txn, config)?;
        }
        Ok(true)
    }

    /// Whether removing `removed` can invalidate the entry point.
    ///
    /// A surviving entry point at the global maximum layer needs no
    /// re-election, and removing another top-layer node cannot lower the
    /// maximum. Only a lost, missing/mismatched, or base-disconnected entry
    /// point requires the full scan in [`Self::repair_entry_point`].
    fn entry_point_needs_repair(txn: &Txn<'_, S>, removed: u64) -> Result<bool> {
        if txn.state.node_count == 0 {
            return Ok(true);
        }
        let Some(entry) = txn.state.entry_point else {
            return Ok(true);
        };
        if entry == removed {
            return Ok(true);
        }
        if !txn.try_contains(&node_key(TAG_VEC, entry))? {
            return Ok(true);
        }
        let layer = txn
            .try_get(&node_key(TAG_INFO, entry))?
            .as_deref()
            .map(decode_layer)
            .transpose()?;
        if layer != Some(txn.state.max_layer) {
            return Ok(true);
        }
        if txn.state.node_count <= 1 {
            return Ok(false);
        }
        for neighbor in get_neighbors(txn, 0, entry)? {
            if txn.try_contains(&node_key(TAG_VEC, neighbor))? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn repair_entry_point(
        metric: &D,
        txn: &mut Txn<'_, S>,
        config: &HnswConfig,
    ) -> Result<()> {
        #[cfg(test)]
        ENTRY_REPAIRS.with(|count| count.set(count.get() + 1));
        let mut live = Vec::new();
        let mut info_scan = txn.rows.try_scan_prefix(txn.store, &[TAG_INFO])?;
        while let Some((key, value)) = info_scan.try_next()? {
            let id = decode_info_key(&key)?;
            if id >= txn.state.next_node_id {
                return Err(corrupt("VecDex node-info ID exceeds the issued range"));
            }
            if txn.try_contains(&node_key(TAG_VEC, id))? {
                live.push((id, decode_layer(&value)?));
            }
        }
        if live.len() as u64 != txn.state.node_count {
            return Err(corrupt(
                "VecDex live node-info count disagrees with graph state",
            ));
        }
        let Some(true_max) = live.iter().map(|(_, layer)| *layer).max() else {
            txn.state.entry_point = None;
            txn.state.max_layer = 0;
            return Ok(());
        };

        let mut best: Option<(u64, bool)> = None;
        for &(id, layer) in &live {
            if layer != true_max {
                continue;
            }
            let mut connected = false;
            for neighbor in get_neighbors(txn, 0, id)? {
                if txn.try_contains(&node_key(TAG_VEC, neighbor))? {
                    connected = true;
                    break;
                }
            }
            // max_by_key previously chose the last equally ranked candidate.
            if best.is_none_or(|(_, old)| connected || !old) {
                best = Some((id, connected));
            }
        }
        let (new_ep, connected) = best.expect("true maximum came from a live node");
        if live.len() > 1 && !connected {
            let ep_vec = txn
                .read_vec(new_ep)?
                .expect("live entry-point candidate has a vector");
            let mut nearest: Option<(S, u64)> = None;
            for &(id, _) in &live {
                if id != new_ep
                    && let Some(vector) = txn.read_vec(id)?
                {
                    let distance = metric.distance(&ep_vec, &vector);
                    if nearest.is_none_or(|(best, _)| distance.total_cmp(&best).is_lt())
                    {
                        nearest = Some((distance, id));
                    }
                }
            }
            if let Some((_, neighbor)) = nearest {
                txn.set_neighbors(0, new_ep, &[neighbor]);
                let mut reverse = get_neighbors(txn, 0, neighbor)?;
                if !reverse.contains(&new_ep) {
                    reverse.push(new_ep);
                    txn.set_neighbors(0, neighbor, &reverse);
                }
                txn.prune_and_detach(metric, neighbor, 0, config.m_max0, Some(new_ep))?;
            }
        }

        txn.state.entry_point = Some(new_ep);
        txn.state.max_layer = true_max;
        Ok(())
    }

    /// Rebuilds the HNSW graph from the existing vectors.
    ///
    /// Useful after many deletions to restore graph quality and recall.
    /// Vectors are re-inserted in random order for better graph quality.
    ///
    /// The rebuild checks vector rows and both key mappings against the captured
    /// graph state before it stages a replacement. Malformed or incomplete
    /// mappings leave the original rows intact.
    ///
    /// The whole rebuild is staged through one wiped transaction: the
    /// range tombstone and every row of the new graph commit in a
    /// **single atomic engine write batch**, so a crash (or an error
    /// return) leaves either the old graph or the new one — never
    /// anything in between.  This is a cold maintenance API: materialized
    /// keys and vectors are consumed as the replacement graph is built. Staged
    /// graph rows and the engine write batch both need memory before commit,
    /// alongside a decoded vector cache capped at 32 MiB. To rebuild a
    /// very large index under bounded memory, stream [`iter`](Self::iter) in
    /// chunks into [`insert_batch`](Self::insert_batch) on a fresh [`VecDex`]
    /// (new [`InstanceId`]) and atomically swap the application's persisted
    /// handle/ID reference before dropping the old index.
    pub fn compact(&mut self) -> Result<()> {
        ensure_writable(&self.namespace(), "vector index compaction")?;
        use rand::seq::SliceRandom;

        let view = self.store.read_view();
        let state = GraphState::try_read_view(&view)?;
        let mut pairs = Vec::new();
        let mut vectors = view.try_range(tag_bounds(TAG_VEC))?;
        let mut reverse = view.try_range(tag_bounds(TAG_NODE2KEY))?;
        let mut canonical_key = Vec::new();
        // Vector and reverse-map rows share the same big-endian node ID order.
        while let Some((reverse_key, key)) = reverse.try_step_next()? {
            let id = u64::from_be_bytes(
                reverse_key[1..]
                    .try_into()
                    .map_err(|_| corrupt("compact: invalid reverse mapping ID"))?,
            );
            let (vector_key, raw) = vectors
                .try_step_next()?
                .ok_or_else(|| corrupt("compact: missing vector"))?;
            if vector_key != node_key(TAG_VEC, id) || id >= state.next_node_id {
                return Err(corrupt("compact: vector and reverse mapping disagree"));
            }
            let forward_matches = view
                .try_get_with(user_key(key), |v| Ok(v == id.to_le_bytes()))?
                .unwrap_or(false);
            if !forward_matches {
                return Err(corrupt("compact: forward and reverse mappings disagree"));
            }
            canonical_key.clear();
            K::decode_key(key)?.encode_key_into(&mut canonical_key)?;
            if canonical_key != key {
                return Err(corrupt("compact: noncanonical key"));
            }
            let vector = decode_vector::<S>(raw, self.config.dim)?;
            if !vector.iter().all(|component| component.is_finite()) {
                return Err(corrupt(
                    "compact: stored vector contains nonfinite components",
                ));
            }
            pairs.push((key.to_vec(), vector));
        }
        let mut forward = view.try_range(tag_bounds(TAG_KEY2NODE))?;
        let mut forward_count = 0usize;
        while forward.try_step_next()?.is_some() {
            forward_count += 1;
        }
        if vectors.try_step_next()?.is_some()
            || pairs.len() as u64 != state.node_count
            || forward_count != pairs.len()
        {
            return Err(corrupt("compact: inconsistent vector or mapping count"));
        }
        drop(forward);
        drop(reverse);
        drop(vectors);
        drop(view);
        pairs.shuffle(&mut rand::rng());

        let mut txn = Txn::new(&self.store, state.cleared(), self.config.dim);
        txn.rows.wipe();
        txn.rows.put(
            META_KEY.to_vec(),
            self.store
                .try_get_with(META_KEY, |raw| Ok(raw.to_vec()))?
                .ok_or_else(|| corrupt("missing VecDex schema"))?,
        );
        for (key, vec) in pairs {
            Self::stage_insert(&self.metric, &mut txn, &self.config, key, &vec)?;
        }
        let rows = txn.finish();
        rows.commit(&mut self.store)?;

        Ok(())
    }
}

impl<K: KeyEnDe, D: DistanceMetric<S>, S: Scalar> Clone for VecDex<K, D, S> {
    fn clone(&self) -> Self {
        self.clone_in(&self.namespace())
            .expect("VecDex: deep clone failed")
    }
}

fn tag_bounds(tag: u8) -> std::ops::Range<[u8; 1]> {
    [tag]..[tag + 1]
}

pub(super) enum Filter<'a, K> {
    Typed(&'a dyn Fn(&K) -> bool),
    Raw(&'a dyn Fn(&[u8]) -> bool),
}

impl<K> Clone for Filter<'_, K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K> Copy for Filter<'_, K> {}

struct Search<'a, 'db, K, S> {
    view: &'a MapxRawReadView<'db>,
    query: &'a [S],
    k: usize,
    ef: Option<usize>,
    filter: Option<Filter<'a, K>>,
}

impl<K: KeyEnDe, S: Scalar> MetricSearch<S> for Search<'_, '_, K, S> {
    type Output = Result<Vec<(K, S)>>;

    fn run<D: DistanceMetric<S>>(self, metric: &D) -> Self::Output {
        let state = self
            .view
            .try_get_with(STATE_KEY, GraphState::decode)?
            .ok_or_else(|| corrupt("missing VecDex state"))?;
        let Some(mut ep) = state.entry_point else {
            return Ok(Vec::new());
        };
        if self.k == 0 {
            return Ok(Vec::new());
        }
        let mut scratch = Vec::with_capacity(self.query.len());
        let mut distance_to = |id| {
            self.view.try_get_with(node_key(TAG_VEC, id), |bytes| {
                decode_vector_into(bytes, self.query.len(), &mut scratch)?;
                Ok(metric.distance(self.query, &scratch))
            })
        };
        let missing_key = || corrupt("missing VecDex reverse key for a live vector");
        let key_for = |id| {
            self.view
                .try_get_with(node_key(TAG_NODE2KEY, id), |bytes| {
                    K::decode_key(bytes).map_err(|error| {
                        corrupt(format!("invalid VecDex result key: {error}"))
                    })
                })?
                .ok_or_else(missing_key)
        };
        let filtered_keys = RefCell::new(HashMap::new());
        let filter = self.filter.as_ref().map(|f| {
            |id| match *f {
                Filter::Typed(predicate) => {
                    let key = key_for(id)?;
                    if !predicate(&key) {
                        return Ok(false);
                    }
                    let mut keys = filtered_keys.borrow_mut();
                    if keys.len() < self.k {
                        keys.insert(id, key);
                    }
                    Ok(true)
                }
                Filter::Raw(predicate) => self
                    .view
                    .try_get_with(node_key(TAG_NODE2KEY, id), |raw| Ok(predicate(raw)))?
                    .ok_or_else(missing_key),
            }
        });
        let graph = SearchRows(self.view);
        for layer in (1..=state.max_layer).rev() {
            if let Some((_, next)) =
                search_scored_layer(&mut distance_to, &[ep], 1, layer, self.view, None)?
                    .first()
            {
                ep = *next;
            }
        }
        let ef = self.ef.unwrap_or(state.ef_search).max(self.k);
        let results = search_scored_layer(
            distance_to,
            &[ep],
            ef,
            0,
            &graph,
            filter.as_ref().map(|f| f as &dyn Fn(u64) -> Result<bool>),
        )?;
        let mut keys = filtered_keys.into_inner();
        let mut out = Vec::with_capacity(self.k.min(results.len()));
        for (dist, id) in results.into_iter().take(self.k) {
            let key = match keys.remove(&id) {
                Some(key) => key,
                None => key_for(id)?,
            };
            out.push((key, dist));
        }
        Ok(out)
    }
}

impl AdjRead for MapxRawReadView<'_> {
    fn neighbors_into(&self, layer: u8, id: u64, out: &mut Vec<u64>) -> Result<()> {
        out.clear();
        self.try_get_with(adj_key(layer, id), |raw| hnsw::extend_neighbors(raw, out))?;
        Ok(())
    }
}

fn _assert_send_sync() {
    fn require<T: Send + Sync>() {}
    require::<VecDex<String, distance::L2>>();
    require::<VecDex<String, distance::L2, f64>>();
    require::<VecDexDyn<String>>();
    require::<VecDexDyn<String, f64>>();
}

#[cfg(test)]
thread_local! {
    static ENTRY_REPAIRS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn entry_repair_count() -> usize {
    ENTRY_REPAIRS.with(std::cell::Cell::get)
}

#[cfg(test)]
mod test;
