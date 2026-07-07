//! Approximate nearest-neighbor vector index backed by the HNSW algorithm.
//!
//! [`VecDex`] is a persistent, disk-backed vector index that wraps VSDB's
//! core storage primitives.  It supports insert, delete, and k-nearest-neighbor
//! search with configurable distance metrics ([`L2`], [`Cosine`],
//! [`InnerProduct`]).
//!
//! For detailed documentation see [VecDex docs](../../docs/vecdex.md).
//!
//! # Storage model (single-handle, crash-atomic)
//!
//! All persistent state lives in **one** [`MapxRaw`] handle, namespaced
//! by a leading tag byte:
//!
//! ```text
//! [0x00 | node_id BE]            -> vector (postcard Vec<S>)
//! [0x01 | layer | node_id BE]    -> packed u64 LE neighbor list
//! [0x02 | key bytes]             -> node_id u64 LE
//! [0x03 | node_id BE]            -> key bytes
//! [0x04 | node_id BE]            -> node max_layer (postcard)
//! [0x05]                         -> graph state (postcard)
//! ```
//!
//! Every mutation stages its rows through a read-your-writes overlay and
//! commits them in a **single atomic engine write batch**, so on-disk
//! state is always internally consistent: there is no dirty flag, no
//! reconcile pass, and no rebuild-on-recovery path.
//!
//! The serialized form of a `VecDex` (its typed handle metadata) is the
//! raw prefix of the single handle plus the creation-time [`HnswConfig`].
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
//! let mut idx: VecDex<String, Cosine> = VecDex::new(cfg);
//!
//! idx.insert(&"doc-a".into(), &[0.1, 0.2, 0.3, 0.4]).unwrap();
//! idx.insert(&"doc-b".into(), &[0.5, 0.6, 0.7, 0.8]).unwrap();
//!
//! let results = idx.search(&[0.1, 0.2, 0.3, 0.4], 1).unwrap();
//! assert_eq!(results[0].0, "doc-a");
//! ```

pub mod distance;
mod hnsw;

use crate::common::{
    ende::{KeyEnDe, ValueEnDe},
    error::{Result, VsdbError},
    staged::StagedRows,
};
use distance::{DistanceMetric, Scalar};
use hnsw::{
    AdjRead, adj_key, encode_neighbors, get_neighbors, prune_selection, random_layer,
    search_layer, select_neighbors_heuristic,
};
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    marker::PhantomData,
    rc::Rc,
};
use vsdb_core::basic::mapx_raw::MapxRaw;

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

/// Mutable graph state, persisted as an ordinary data row and mirrored
/// in memory.  `node_count` is a plain count — crash consistency comes
/// from atomic mutation batches, not from a dirty flag.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct GraphState {
    entry_point: Option<u64>,
    max_layer: u8,
    node_count: u64,
    next_node_id: u64,
    ef_search: usize,
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

fn decode_node_id(raw: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&raw[..8]);
    u64::from_le_bytes(b)
}

// =========================================================================
// Mutation transaction
// =========================================================================

/// One mutation's staged rows plus a working copy of the graph state.
/// Reads observe the operation's own uncommitted writes.
struct Txn<'a, S> {
    store: &'a MapxRaw,
    rows: StagedRows,
    state: GraphState,
    /// Per-transaction decoded-vector cache: HNSW linking re-reads the
    /// same vectors many times across layers/heuristics; decoding each
    /// row once per transaction removes the dominant redundant cost.
    vec_cache: RefCell<HashMap<u64, Rc<Vec<S>>>>,
}

impl<S> AdjRead for Txn<'_, S> {
    #[inline]
    fn adj_row(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.rows.get_over(self.store, key)
    }
}

impl<'a, S: Scalar> Txn<'a, S> {
    fn new(store: &'a MapxRaw, state: GraphState) -> Self {
        Self {
            store,
            rows: StagedRows::new(),
            state,
            vec_cache: RefCell::new(HashMap::new()),
        }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.rows.get_over(self.store, key)
    }

    fn read_vec(&self, node_id: u64) -> Option<Rc<Vec<S>>> {
        if let Some(v) = self.vec_cache.borrow().get(&node_id) {
            return Some(Rc::clone(v));
        }
        let raw = self.get(&node_key(TAG_VEC, node_id))?;
        let v = Rc::new(decode_value::<Vec<S>>(&raw));
        self.vec_cache.borrow_mut().insert(node_id, Rc::clone(&v));
        Some(v)
    }

    /// Stages a vector row and pre-warms the decode cache.
    fn put_vec(&mut self, node_id: u64, vector: &[S]) {
        let v: Vec<S> = vector.to_vec();
        self.rows
            .put(node_key(TAG_VEC, node_id).to_vec(), encode_value(&v));
        self.vec_cache.get_mut().insert(node_id, Rc::new(v));
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

    /// Applies the pruning result of
    /// [`prune_selection`](hnsw::prune_selection) for `(node, layer)` and
    /// detaches the evicted back-edges, mirroring the insert-time
    /// neighbor-eviction protocol. Returns whether `keep` survived the
    /// pruning (`true` when `keep` is `None` or was not evicted).
    fn prune_and_detach<D: DistanceMetric<S>>(
        &mut self,
        node_id: u64,
        layer: u8,
        m_max: usize,
        keep: Option<u64>,
    ) -> bool {
        let pruned = {
            let gv = |id: u64| self.read_vec(id);
            prune_selection::<S, D, Self>(node_id, layer, m_max, self, &gv)
        };
        let Some((pruned, evicted)) = pruned else {
            return true;
        };
        self.set_neighbors(layer, node_id, &pruned);
        let kept = keep.is_none_or(|k| !evicted.contains(&k));
        for evicted_id in evicted {
            let mut e_list = get_neighbors(self, layer, evicted_id);
            e_list.retain(|&x| x != node_id);
            self.set_neighbors(layer, evicted_id, &e_list);
        }
        kept
    }

    /// Stages the state row and hands the parts back for commit.
    fn finish(mut self) -> (StagedRows, GraphState) {
        self.rows.put(STATE_KEY.to_vec(), encode_value(&self.state));
        (self.rows, self.state)
    }
}

fn encode_value<T: ValueEnDe>(v: &T) -> Vec<u8> {
    v.encode()
}

fn decode_value<T: ValueEnDe>(raw: &[u8]) -> T {
    T::decode(raw).expect("VecDex: corrupt row payload")
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
/// Every mutation is applied through a single atomic engine write batch,
/// so a crash can never leave the index internally inconsistent.
pub struct VecDex<K, D, S: Scalar = f32>
where
    K: KeyEnDe + ValueEnDe + Clone + Eq,
    D: DistanceMetric<S>,
{
    store: MapxRaw,
    config: HnswConfig,
    /// In-memory mirror of the persisted graph-state row.
    state: GraphState,
    _p: PhantomData<(K, D, S)>,
}

impl<K, D, S> Serialize for VecDex<K, D, S>
where
    K: KeyEnDe + ValueEnDe + Clone + Eq,
    D: DistanceMetric<S>,
    S: Scalar,
{
    fn serialize<Ser>(&self, serializer: Ser) -> std::result::Result<Ser::Ok, Ser::Error>
    where
        Ser: serde::Serializer,
    {
        // The distance metric `D` occurs in no field type, so the
        // typed-handle envelope (tagged with `VecDex<K, D, S>`) is the
        // only guard against restoring an index under a different metric.
        crate::common::serialize_typed_handle_meta::<Self, Ser>(
            &(LAYOUT_VERSION, &self.store, &self.config),
            serializer,
        )
    }
}

impl<'de, K, D, S> Deserialize<'de> for VecDex<K, D, S>
where
    K: KeyEnDe + ValueEnDe + Clone + Eq,
    D: DistanceMetric<S>,
    S: Scalar,
{
    fn deserialize<De>(deserializer: De) -> std::result::Result<Self, De::Error>
    where
        De: serde::Deserializer<'de>,
    {
        let (version, store, config) = crate::common::deserialize_typed_handle_meta::<
            Self,
            (u8, MapxRaw, HnswConfig),
            De,
        >(deserializer)?;
        if version != LAYOUT_VERSION {
            return Err(serde::de::Error::custom(format!(
                "VecDex: unsupported layout version {version} (expected {LAYOUT_VERSION})"
            )));
        }
        Ok(Self::hydrate(store, config))
    }
}

impl<K, D, S> std::fmt::Debug for VecDex<K, D, S>
where
    K: KeyEnDe + ValueEnDe + Clone + Eq,
    D: DistanceMetric<S>,
    S: Scalar,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VecDex")
            .field("node_count", &self.state.node_count)
            .field("dim", &self.config.dim)
            .field("max_layer", &self.state.max_layer)
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
    K: KeyEnDe + ValueEnDe + Clone + Eq,
    D: DistanceMetric<S>,
    S: Scalar,
{
    /// Creates a new, empty `VecDex` with the given configuration.
    pub fn new(config: HnswConfig) -> Self {
        assert!(config.dim > 0, "VecDex: dim must be > 0");
        assert!(config.m >= 2, "VecDex: m must be >= 2");
        assert!(
            config.m_max0 >= config.m,
            "VecDex: m_max0 must be >= m (else base-layer nodes have no edges and become unreachable)"
        );
        assert!(
            config.ef_construction > 0,
            "VecDex: ef_construction must be > 0 (else search_layer returns no candidates)"
        );
        let state = GraphState {
            ef_search: config.ef_search,
            ..GraphState::default()
        };
        Self {
            store: MapxRaw::new(),
            config,
            state,
            _p: PhantomData,
        }
    }

    /// Reconnects to an existing store and reads the persisted graph
    /// state (absent on a never-mutated index).
    fn hydrate(store: MapxRaw, config: HnswConfig) -> Self {
        let state = store
            .get(STATE_KEY)
            .map(|raw| decode_value::<GraphState>(&raw))
            .unwrap_or_else(|| GraphState {
                ef_search: config.ef_search,
                ..GraphState::default()
            });
        Self {
            store,
            config,
            state,
            _p: PhantomData,
        }
    }

    /// Returns the unique instance ID.
    #[inline(always)]
    pub fn instance_id(&self) -> u64 {
        self.store.instance_id()
    }

    /// Persists this instance's metadata to disk so that it can be
    /// recovered later via [`from_meta`](Self::from_meta).
    ///
    /// The metadata is create-time constant (single handle + creation
    /// config), so calling this once after creation is sufficient for
    /// the lifetime of the instance.
    pub fn save_meta(&self) -> Result<u64> {
        let id = self.instance_id();
        crate::common::save_instance_meta(id, self)?;
        Ok(id)
    }

    /// Recovers a `VecDex` from previously saved metadata.
    ///
    /// Every mutation is applied atomically, so the recovered state is
    /// always internally consistent — there is no rebuild path.
    pub fn from_meta(instance_id: u64) -> Result<Self> {
        crate::common::load_instance_meta(instance_id)
    }

    /// Returns the number of indexed vectors.
    pub fn len(&self) -> u64 {
        self.state.node_count
    }

    /// Returns `true` if the index contains no vectors.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Updates the default search beam width.
    ///
    /// # Panics
    ///
    /// Panics if the engine-level commit fails (matching the behavior of
    /// the plain collection types on engine write failure).
    pub fn set_ef_search(&mut self, ef: usize) {
        let mut txn: Txn<'_, S> = Txn::new(&self.store, self.state.clone());
        txn.state.ef_search = ef;
        let (rows, state) = txn.finish();
        rows.commit(&mut self.store)
            .expect("vsdb: VecDex set_ef_search commit failed");
        self.state = state;
    }

    /// Returns the vector associated with the given key, if it exists.
    pub fn get(&self, key: &K) -> Option<Vec<S>> {
        let node_id = decode_node_id(&self.store.get(user_key(&KeyEnDe::encode(key)))?);
        self.store
            .get(node_key(TAG_VEC, node_id))
            .map(|raw| decode_value::<Vec<S>>(&raw))
    }

    /// Returns `true` if the index contains the given key.
    pub fn contains_key(&self, key: &K) -> bool {
        self.store.contains_key(user_key(&KeyEnDe::encode(key)))
    }

    /// Returns an iterator over all indexed keys.
    pub fn keys(&self) -> impl Iterator<Item = K> + '_ {
        self.scan_tag(TAG_KEY2NODE).map(|(raw_key, _)| {
            <K as KeyEnDe>::decode(&raw_key[1..]).expect("VecDex: corrupt key bytes")
        })
    }

    /// Returns an iterator over all (key, vector) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (K, Vec<S>)> + '_ {
        self.scan_tag(TAG_NODE2KEY)
            .filter_map(|(raw_key, raw_val)| {
                let mut b = [0u8; 8];
                b.copy_from_slice(&raw_key[1..9]);
                let node_id = u64::from_be_bytes(b);
                let key = decode_value::<K>(&raw_val);
                self.store
                    .get(node_key(TAG_VEC, node_id))
                    .map(|raw| (key, decode_value::<Vec<S>>(&raw)))
            })
    }

    fn scan_tag(
        &self,
        tag: u8,
    ) -> impl Iterator<Item = (vsdb_core::common::RawKey, vsdb_core::common::RawValue)> + '_
    {
        use std::{borrow::Cow, ops::Bound};
        self.store.range((
            Bound::Included(Cow::Owned(vec![tag])),
            Bound::Excluded(Cow::Owned(vec![tag + 1])),
        ))
    }

    /// Clears all indexed data.
    ///
    /// The wipe (one engine-level range tombstone) and the reset graph
    /// state row — which preserves the live `ef_search`, also across a
    /// later restore — commit in **one atomic engine write batch**: a
    /// crash can never expose a partially-cleared index.
    ///
    /// # Panics
    ///
    /// Panics if the engine-level commit fails (matching the behavior of
    /// the plain collection types on engine write failure).
    pub fn clear(&mut self) {
        let mut txn: Txn<'_, S> = Txn::new(
            &self.store,
            GraphState {
                ef_search: self.state.ef_search,
                ..GraphState::default()
            },
        );
        txn.rows.wipe();
        let (rows, state) = txn.finish();
        rows.commit(&mut self.store)
            .expect("vsdb: VecDex clear commit failed");
        self.state = state;
    }

    /// Inserts a vector associated with a user key.
    ///
    /// If the key already exists, the old vector is replaced and the
    /// graph connections are rebuilt (two atomic operations: the removal
    /// of the old node, then the insert of the new one — each leaves the
    /// index in a consistent state).
    ///
    /// All rows of the insert proper — the vector, both key mappings,
    /// the node info, every adjacency update, and the graph state — are
    /// committed through one atomic engine batch.
    ///
    /// # Errors
    ///
    /// If the vector dimension mismatches the index configuration, or if
    /// the batch commit fails (in which case neither the on-disk state
    /// nor the in-memory state is modified).
    pub fn insert(&mut self, key: &K, vector: &[S]) -> Result<()> {
        if vector.len() != self.config.dim {
            return Err(VsdbError::Other {
                detail: format!(
                    "dimension mismatch: expected {}, got {}",
                    self.config.dim,
                    vector.len()
                ),
            });
        }

        if self.contains_key(key) {
            self.remove(key)?;
        }

        let mut txn = Txn::new(&self.store, self.state.clone());
        Self::stage_insert(&mut txn, &self.config, key, vector);
        let (rows, state) = txn.finish();
        rows.commit(&mut self.store)?;
        self.state = state;
        Ok(())
    }

    /// Stages one whole insert (rows + graph linking) into `txn`.
    fn stage_insert(txn: &mut Txn<'_, S>, config: &HnswConfig, key: &K, vector: &[S]) {
        let node_id = txn.state.next_node_id;
        let node_layer = random_layer(config.m);

        txn.put_vec(node_id, vector);
        txn.rows.put(
            user_key(&KeyEnDe::encode(key)),
            node_id.to_le_bytes().to_vec(),
        );
        txn.rows
            .put(node_key(TAG_NODE2KEY, node_id).to_vec(), encode_value(key));
        txn.rows.put(
            node_key(TAG_INFO, node_id).to_vec(),
            encode_value(&node_layer),
        );

        txn.state.next_node_id = node_id + 1;
        txn.state.node_count += 1;

        Self::link_node(txn, config, node_id, vector, node_layer);
    }

    /// Inserts a batch of (key, vector) pairs.
    ///
    /// Semantically equivalent to calling [`insert`](Self::insert) in a
    /// loop, but the inserts are staged in chunks that share one
    /// transaction (and thus one atomic engine write batch) each —
    /// amortizing the per-commit cost, which dominates bulk loads.
    ///
    /// Pre-existing keys are replaced (their removals are individually
    /// atomic), and duplicate keys inside `items` collapse to the last
    /// occurrence.
    pub fn insert_batch(&mut self, items: &[(K, Vec<S>)]) -> Result<()> {
        // Bounded chunks keep the staged set (and the engine batch)
        // at a sane size for arbitrarily large bulk loads.
        const CHUNK: usize = 64;

        for (_, vec) in items {
            if vec.len() != self.config.dim {
                return Err(VsdbError::Other {
                    detail: format!(
                        "dimension mismatch: expected {}, got {}",
                        self.config.dim,
                        vec.len()
                    ),
                });
            }
        }

        // Last occurrence of each key wins (matching the final state of
        // a serial insert loop), preserving the order of survivors.
        let mut seen: HashSet<Vec<u8>> = HashSet::with_capacity(items.len());
        let mut dedup: Vec<&(K, Vec<S>)> = Vec::with_capacity(items.len());
        for item in items.iter().rev() {
            if seen.insert(KeyEnDe::encode(&item.0)) {
                dedup.push(item);
            }
        }
        dedup.reverse();

        for (key, _) in &dedup {
            if self.contains_key(key) {
                self.remove(key)?;
            }
        }

        for chunk in dedup.chunks(CHUNK) {
            let mut txn = Txn::new(&self.store, self.state.clone());
            for (key, vec) in chunk {
                Self::stage_insert(&mut txn, &self.config, key, vec);
            }
            let (rows, state) = txn.finish();
            rows.commit(&mut self.store)?;
            self.state = state;
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
        txn: &mut Txn<'_, S>,
        config: &HnswConfig,
        node_id: u64,
        vector: &[S],
        node_layer: u8,
    ) {
        let Some(ep) = txn.state.entry_point else {
            txn.state.entry_point = Some(node_id);
            txn.state.max_layer = node_layer;
            return;
        };
        if ep == node_id {
            return;
        }

        let cur_max = txn.state.max_layer;

        // Phase 1: Greedy descent from top layer to node_layer + 1.
        let mut cur_ep = vec![ep];
        for l in (node_layer.saturating_add(1)..=cur_max).rev() {
            let res = {
                let gv = |id: u64| txn.read_vec(id);
                search_layer::<S, D, _>(vector, &cur_ep, 1, l, &gv, txn, None)
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
                let gv = |id: u64| txn.read_vec(id);
                search_layer::<S, D, _>(
                    vector,
                    &cur_ep,
                    config.ef_construction,
                    l,
                    &gv,
                    txn,
                    None,
                )
            };
            let neighbor_pool: Vec<(S, u64)> = candidates
                .iter()
                .copied()
                .filter(|&(_, id)| id != node_id)
                .collect();

            let selected = {
                let gv = |id: u64| txn.read_vec(id);
                select_neighbors_heuristic::<S, D>(&neighbor_pool, m_max, &gv)
            };

            txn.set_neighbors(l, node_id, &selected);

            for &neighbor in &selected {
                let mut n_neighbors = get_neighbors(txn, l, neighbor);
                n_neighbors.push(node_id);
                txn.set_neighbors(l, neighbor, &n_neighbors);
                txn.prune_and_detach::<D>(neighbor, l, m_max, None);
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
    }

    /// Searches for the `k` nearest neighbors of the query vector.
    pub fn search(&self, query: &[S], k: usize) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, self.state.ef_search, None)
    }

    /// Searches with a custom `ef` (beam width) for recall/speed tradeoff.
    pub fn search_ef(&self, query: &[S], k: usize, ef: usize) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, ef, None)
    }

    /// Searches with a key predicate evaluated during beam search.
    ///
    /// Non-matching nodes still participate in graph traversal to maintain
    /// connectivity, but are excluded from the result set.  Distance-based
    /// pruning is disabled when filtering to avoid missing matches reachable
    /// only through non-matching bridge nodes; traversal is still bounded by
    /// an inflated `ef` visit budget.
    ///
    /// For very large indexes or highly selective predicates, use
    /// [`search_ef_with_filter`](Self::search_ef_with_filter) with an
    /// increased `ef` to collect more candidate results.
    pub fn search_with_filter(
        &self,
        query: &[S],
        k: usize,
        predicate: impl Fn(&K) -> bool,
    ) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, self.state.ef_search, Some(&predicate))
    }

    /// Filtered search with a custom `ef` (beam width).
    pub fn search_ef_with_filter(
        &self,
        query: &[S],
        k: usize,
        ef: usize,
        predicate: impl Fn(&K) -> bool,
    ) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, ef, Some(&predicate))
    }

    fn node_user_key(&self, node_id: u64) -> Option<K> {
        self.store
            .get(node_key(TAG_NODE2KEY, node_id))
            .map(|raw| decode_value::<K>(&raw))
    }

    /// Iterates `(node_id, max_layer)` over the committed node-info rows.
    #[cfg(test)]
    fn node_layers(&self) -> impl Iterator<Item = (u64, u8)> + '_ {
        self.scan_tag(TAG_INFO).map(|(raw_key, raw_val)| {
            let mut b = [0u8; 8];
            b.copy_from_slice(&raw_key[1..9]);
            (u64::from_be_bytes(b), decode_value::<u8>(&raw_val))
        })
    }

    fn search_internal(
        &self,
        query: &[S],
        k: usize,
        ef: usize,
        predicate: Option<&dyn Fn(&K) -> bool>,
    ) -> Result<Vec<(K, S)>> {
        if query.len() != self.config.dim {
            return Err(VsdbError::Other {
                detail: format!(
                    "dimension mismatch: expected {}, got {}",
                    self.config.dim,
                    query.len()
                ),
            });
        }

        let Some(ep) = self.state.entry_point else {
            return Ok(vec![]);
        };

        if k == 0 {
            return Ok(vec![]);
        }

        let cache = RefCell::new(HashMap::<u64, Rc<Vec<S>>>::new());
        let get_vec = |id: u64| -> Option<Rc<Vec<S>>> {
            if let Some(v) = cache.borrow().get(&id) {
                // Cheap refcount bump — no vector data is copied, unlike
                // the pre-Rc version's full-vector `.clone()` on every
                // cache hit (this closure's entry point is looked up
                // repeatedly per query, e.g. once as the layer-0 entry
                // point right after the layer-descent loop ends).
                return Some(Rc::clone(v));
            }
            let raw = self.store.get(node_key(TAG_VEC, id))?;
            let v = Rc::new(decode_value::<Vec<S>>(&raw));
            cache.borrow_mut().insert(id, Rc::clone(&v));
            Some(v)
        };

        let node_filter: Option<Box<dyn Fn(u64) -> bool + '_>> =
            predicate.map(|pred| -> Box<dyn Fn(u64) -> bool + '_> {
                Box::new(move |node_id: u64| {
                    self.node_user_key(node_id).is_some_and(|k| pred(&k))
                })
            });
        let filter_ref: Option<&dyn Fn(u64) -> bool> =
            node_filter.as_ref().map(|f| f.as_ref());

        let mut cur_ep = vec![ep];
        for l in (1..=self.state.max_layer).rev() {
            let res = search_layer::<S, D, _>(
                query,
                &cur_ep,
                1,
                l,
                &get_vec,
                &self.store,
                None,
            );
            if let Some(&(_, id)) = res.first() {
                cur_ep = vec![id];
            }
        }

        // Saturating: `ef`/`k` are unrestricted public inputs, and the
        // ×4/×2 filter budget must not overflow for extreme values.
        let search_ef = if predicate.is_some() {
            ef.saturating_mul(4).max(k.saturating_mul(2))
        } else {
            ef.max(k)
        };
        let results = search_layer::<S, D, _>(
            query,
            &cur_ep,
            search_ef,
            0,
            &get_vec,
            &self.store,
            filter_ref,
        );

        let mut out = Vec::with_capacity(k.min(results.len()));
        for (dist, node_id) in results.into_iter().take(k) {
            if let Some(key) = self.node_user_key(node_id) {
                out.push((key, dist));
            }
        }

        Ok(out)
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
    /// If the batch commit fails, neither the on-disk state nor the
    /// in-memory state is modified.
    pub fn remove(&mut self, key: &K) -> Result<bool> {
        let Some(raw) = self.store.get(user_key(&KeyEnDe::encode(key))) else {
            return Ok(false);
        };
        let node_id = decode_node_id(&raw);

        let mut txn = Txn::new(&self.store, self.state.clone());

        let max_layer = txn
            .get(&node_key(TAG_INFO, node_id))
            .map(|raw| decode_value::<u8>(&raw))
            .unwrap_or(txn.state.max_layer);

        // Phase 1: Remove edges and collect former neighbors per layer.
        let mut former_neighbors: Vec<Vec<u64>> =
            Vec::with_capacity(max_layer as usize + 1);
        for l in 0..=max_layer {
            let neighbors = get_neighbors(&txn, l, node_id);
            for &n in &neighbors {
                let mut n_list = get_neighbors(&txn, l, n);
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
            let m_max = if l == 0 {
                self.config.m_max0
            } else {
                self.config.m
            };
            let fns = &former_neighbors[l as usize];
            for &n in fns {
                let cur = get_neighbors(&txn, l, n);
                if cur.len() >= m_max {
                    continue;
                }
                let slots = m_max - cur.len();
                let cur_set: HashSet<u64> = cur.iter().copied().collect();
                let mut added = 0usize;
                for &candidate in fns {
                    if added >= slots {
                        break;
                    }
                    if candidate == n || cur_set.contains(&candidate) {
                        continue;
                    }
                    let mut n_list = get_neighbors(&txn, l, n);
                    n_list.push(candidate);
                    txn.set_neighbors(l, n, &n_list);

                    let mut c_list = get_neighbors(&txn, l, candidate);
                    c_list.push(n);
                    txn.set_neighbors(l, candidate, &c_list);

                    if txn.prune_and_detach::<D>(candidate, l, m_max, Some(n)) {
                        added += 1;
                    }
                }
                if added > 0 {
                    txn.prune_and_detach::<D>(n, l, m_max, None);
                }
            }
        }

        // Phase 3: Clean up rows and state.
        txn.del_vec(node_id);
        txn.rows.del(user_key(&KeyEnDe::encode(key)));
        txn.rows.del(node_key(TAG_NODE2KEY, node_id).to_vec());
        txn.rows.del(node_key(TAG_INFO, node_id).to_vec());

        txn.state.node_count = txn.state.node_count.saturating_sub(1);

        if txn.state.entry_point == Some(node_id) {
            // Re-elect: prefer candidates that still have base-layer
            // edges so an isolated node cannot become the entry point
            // and hide the rest of the graph; among equals the higher
            // layer wins.
            let mut best: Option<(u64, u8, bool)> = None;
            let info_rows: Vec<(u64, u8)> = txn
                .rows
                .scan_prefix(txn.store, &[TAG_INFO])
                .map(|(raw_key, raw_val)| {
                    let mut b = [0u8; 8];
                    b.copy_from_slice(&raw_key[1..9]);
                    (u64::from_be_bytes(b), decode_value::<u8>(&raw_val))
                })
                .collect();
            for (nid, layer) in info_rows {
                if txn.get(&node_key(TAG_VEC, nid)).is_none() {
                    continue;
                }
                let linked = txn.adj_row(&adj_key(0, nid)).is_some();
                let better = match best {
                    None => true,
                    Some((_, bl, blinked)) => (linked, layer) > (blinked, bl),
                };
                if better {
                    best = Some((nid, layer, linked));
                }
            }
            if let Some((new_ep, new_max, _)) = best {
                txn.state.entry_point = Some(new_ep);
                txn.state.max_layer = new_max;
            } else {
                txn.state.entry_point = None;
                txn.state.max_layer = 0;
            }
        }

        let (rows, state) = txn.finish();
        rows.commit(&mut self.store)?;
        self.state = state;
        Ok(true)
    }

    /// Rebuilds the HNSW graph from the existing vectors.
    ///
    /// Useful after many deletions to restore graph quality and recall.
    /// Vectors are re-inserted in random order for better graph quality.
    ///
    /// The whole rebuild is staged through one wiped transaction: the
    /// range tombstone and every row of the new graph commit in a
    /// **single atomic engine write batch**, so a crash (or an error
    /// return) leaves either the old graph or the new one — never
    /// anything in between.  This is a cold maintenance API: the new
    /// graph is staged in memory before the commit, so expect transient
    /// memory proportional to the index size.
    pub fn compact(&mut self) -> Result<()> {
        use rand::seq::SliceRandom;

        let mut pairs: Vec<(K, Vec<S>)> = self.iter().collect();

        pairs.shuffle(&mut rand::rng());

        // Defense-in-depth: impossible for vectors that passed insert
        // validation, but `stage_insert` below performs no per-insert
        // dimension check, so re-check before rebuilding. An error here
        // (or anywhere before the commit) leaves the store untouched.
        for (_, vec) in &pairs {
            if vec.len() != self.config.dim {
                return Err(VsdbError::Other {
                    detail: format!(
                        "compact: stored vector dimension {} != index dimension {}",
                        vec.len(),
                        self.config.dim
                    ),
                });
            }
        }

        let mut txn = Txn::new(
            &self.store,
            GraphState {
                ef_search: self.state.ef_search,
                ..GraphState::default()
            },
        );
        txn.rows.wipe();
        for (key, vec) in &pairs {
            Self::stage_insert(&mut txn, &self.config, key, vec);
        }
        let (rows, state) = txn.finish();
        rows.commit(&mut self.store)?;
        self.state = state;

        Ok(())
    }
}

fn _assert_send_sync() {
    fn require<T: Send + Sync>() {}
    require::<VecDex<String, distance::L2>>();
    require::<VecDex<String, distance::L2, f64>>();
}

#[cfg(test)]
mod test;
