//! Approximate nearest-neighbor vector index backed by the HNSW algorithm.
//!
//! [`VecDex`] is a persistent, disk-backed vector index that wraps VSDB's
//! core storage primitives.  It supports insert, delete, and k-nearest-neighbor
//! search with configurable distance metrics ([`L2`], [`Cosine`],
//! [`InnerProduct`]).
//!
//! For detailed documentation see [VecDex docs](../docs/vecdex.md).
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

use crate::{
    Mapx, MapxOrd,
    basic::orphan::Orphan,
    common::ende::{KeyEnDe, ValueEnDe},
    common::error::{Result, VsdbError},
};
use distance::{DistanceMetric, Scalar};
use hnsw::{
    get_neighbors, prune_neighbors, random_layer, remove_adjacency, search_layer,
    select_neighbors_heuristic, set_neighbors,
};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use vsdb_core::basic::mapx_raw::MapxRaw;

/// Configuration for a [`VecDex`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HnswConfig {
    /// Max neighbors per node per layer (default 16).
    pub m: usize,
    /// Max neighbors at the base layer (default 2 * m).
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct HnswMeta {
    entry_point: Option<u64>,
    max_layer: u8,
    node_count: u64,
    next_node_id: u64,
    m: usize,
    m_max0: usize,
    ef_construction: usize,
    ef_search: usize,
    dim: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct NodeInfo {
    max_layer: u8,
}

/// A persistent, disk-backed approximate nearest-neighbor index
/// using the HNSW (Hierarchical Navigable Small World) algorithm.
///
/// Type parameters:
/// - `K`: user-facing key type.
/// - `D`: distance metric ([`L2`](distance::L2), [`Cosine`](distance::Cosine),
///   [`InnerProduct`](distance::InnerProduct)).
/// - `S`: scalar type for vector components (`f32` or `f64`, default `f32`).
#[derive(Serialize, Deserialize)]
#[serde(bound = "K: Serialize + serde::de::DeserializeOwned, S: Scalar")]
pub struct VecDex<K, D, S: Scalar = f32>
where
    K: KeyEnDe + ValueEnDe + Clone + Eq,
    D: DistanceMetric<S>,
{
    vectors: MapxOrd<u64, Vec<S>>,
    adjacency: MapxRaw,
    key_to_node: Mapx<K, u64>,
    node_to_key: MapxOrd<u64, K>,
    node_info: MapxOrd<u64, NodeInfo>,
    meta: Orphan<HnswMeta>,
    #[serde(skip)]
    _metric: PhantomData<D>,
}

impl<K, D, S> std::fmt::Debug for VecDex<K, D, S>
where
    K: KeyEnDe + ValueEnDe + Clone + Eq,
    D: DistanceMetric<S>,
    S: Scalar,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let m = self.meta.get_value();
        f.debug_struct("VecDex")
            .field("node_count", &m.node_count)
            .field("dim", &m.dim)
            .field("max_layer", &m.max_layer)
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
    K: KeyEnDe + ValueEnDe + Clone + Eq + Serialize + serde::de::DeserializeOwned,
    D: DistanceMetric<S>,
    S: Scalar,
{
    /// Creates a new, empty `VecDex` with the given configuration.
    pub fn new(config: HnswConfig) -> Self {
        assert!(config.dim > 0, "VecDex: dim must be > 0");
        assert!(config.m > 0, "VecDex: m must be > 0");
        let meta = HnswMeta {
            entry_point: None,
            max_layer: 0,
            node_count: 0,
            next_node_id: 0,
            m: config.m,
            m_max0: config.m_max0,
            ef_construction: config.ef_construction,
            ef_search: config.ef_search,
            dim: config.dim,
        };
        Self {
            vectors: MapxOrd::new(),
            adjacency: MapxRaw::new(),
            key_to_node: Mapx::new(),
            node_to_key: MapxOrd::new(),
            node_info: MapxOrd::new(),
            meta: Orphan::new(meta),
            _metric: PhantomData,
        }
    }

    /// Returns the unique instance ID.
    #[inline(always)]
    pub fn instance_id(&self) -> u64 {
        self.vectors.instance_id()
    }

    /// Persists metadata for later recovery via [`from_meta`](Self::from_meta).
    pub fn save_meta(&self) -> Result<u64> {
        let id = self.instance_id();
        crate::common::save_instance_meta(id, self)?;
        Ok(id)
    }

    /// Recovers a `VecDex` from previously saved metadata.
    pub fn from_meta(instance_id: u64) -> Result<Self> {
        crate::common::load_instance_meta(instance_id)
    }

    /// Returns the number of indexed vectors.
    pub fn len(&self) -> u64 {
        self.meta.get_value().node_count
    }

    /// Returns `true` if the index contains no vectors.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears all indexed data.
    pub fn clear(&mut self) {
        self.vectors.clear();
        self.adjacency.clear();
        self.key_to_node.clear();
        self.node_to_key.clear();
        self.node_info.clear();
        let mut m = self.meta.get_mut();
        m.entry_point = None;
        m.max_layer = 0;
        m.node_count = 0;
        m.next_node_id = 0;
    }

    /// Inserts a vector associated with a user key.
    ///
    /// If the key already exists, the old vector is replaced and the
    /// graph connections are rebuilt.
    pub fn insert(&mut self, key: &K, vector: &[S]) -> Result<()> {
        let meta = self.meta.get_value().clone();
        if vector.len() != meta.dim {
            return Err(VsdbError::Other {
                detail: format!(
                    "dimension mismatch: expected {}, got {}",
                    meta.dim,
                    vector.len()
                ),
            });
        }

        if self.key_to_node.contains_key(key) {
            self.remove(key)?;
        }

        let node_id = meta.next_node_id;
        let node_layer = random_layer(meta.m);

        self.vectors.insert(&node_id, &vector.to_vec());
        self.key_to_node.insert(key, &node_id);
        self.node_to_key.insert(&node_id, key);
        self.node_info.insert(
            &node_id,
            &NodeInfo {
                max_layer: node_layer,
            },
        );

        {
            let mut m = self.meta.get_mut();
            m.next_node_id = node_id + 1;
            m.node_count += 1;
        }

        if meta.entry_point.is_none() {
            let mut m = self.meta.get_mut();
            m.entry_point = Some(node_id);
            m.max_layer = node_layer;
            for l in 0..=node_layer {
                set_neighbors(&mut self.adjacency, l, node_id, &[]);
            }
            return Ok(());
        }

        let ep = meta.entry_point.unwrap();
        let vector = vector.to_vec();
        let get_vec = |id: u64| -> Option<Vec<S>> { self.vectors.get(&id) };

        // Phase 1: Greedy descent from top layer to node_layer + 1.
        let mut cur_ep = vec![ep];
        let cur_max = self.meta.get_value().max_layer;
        for l in (node_layer.saturating_add(1)..=cur_max).rev() {
            let res = search_layer::<S, D>(
                &vector,
                &cur_ep,
                1,
                l,
                &get_vec,
                &self.adjacency,
                None,
            );
            if let Some(&(_, id)) = res.first() {
                cur_ep = vec![id];
            }
        }

        // Phase 2: Insert at layers node_layer..0 with heuristic selection.
        let top = node_layer.min(cur_max);
        for l in (0..=top).rev() {
            let m_max = if l == 0 { meta.m_max0 } else { meta.m };

            let candidates = search_layer::<S, D>(
                &vector,
                &cur_ep,
                meta.ef_construction,
                l,
                &get_vec,
                &self.adjacency,
                None,
            );

            let selected =
                select_neighbors_heuristic::<S, D>(&candidates, m_max, &get_vec);

            set_neighbors(&mut self.adjacency, l, node_id, &selected);

            for &neighbor in &selected {
                let mut n_neighbors = get_neighbors(&self.adjacency, l, neighbor);
                n_neighbors.push(node_id);
                set_neighbors(&mut self.adjacency, l, neighbor, &n_neighbors);
                prune_neighbors::<S, D>(
                    neighbor,
                    l,
                    m_max,
                    &mut self.adjacency,
                    &get_vec,
                );
            }

            cur_ep = candidates.iter().map(|&(_, id)| id).collect();
        }

        if node_layer > cur_max {
            let mut m = self.meta.get_mut();
            m.entry_point = Some(node_id);
            m.max_layer = node_layer;
            for l in (cur_max + 1)..=node_layer {
                if get_neighbors(&self.adjacency, l, node_id).is_empty() {
                    set_neighbors(&mut self.adjacency, l, node_id, &[]);
                }
            }
        }

        Ok(())
    }

    /// Inserts a batch of (key, vector) pairs.
    ///
    /// Equivalent to calling [`insert`](Self::insert) in a loop but
    /// provides a clear semantic entry point for bulk loading.
    pub fn insert_batch(&mut self, items: &[(K, Vec<S>)]) -> Result<()> {
        for (key, vec) in items {
            self.insert(key, vec)?;
        }
        Ok(())
    }

    /// Searches for the `k` nearest neighbors of the query vector.
    pub fn search(&self, query: &[S], k: usize) -> Result<Vec<(K, S)>> {
        let ef = self.meta.get_value().ef_search;
        self.search_internal(query, k, ef, None)
    }

    /// Searches with a custom `ef` (beam width) for recall/speed tradeoff.
    pub fn search_ef(&self, query: &[S], k: usize, ef: usize) -> Result<Vec<(K, S)>> {
        self.search_internal(query, k, ef, None)
    }

    /// Searches with a key predicate evaluated during beam search.
    ///
    /// Non-matching nodes still participate in graph traversal to maintain
    /// connectivity, but are excluded from the result set.
    pub fn search_with_filter(
        &self,
        query: &[S],
        k: usize,
        predicate: impl Fn(&K) -> bool,
    ) -> Result<Vec<(K, S)>> {
        let ef = self.meta.get_value().ef_search;
        self.search_internal(query, k, ef, Some(&predicate))
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

    fn search_internal(
        &self,
        query: &[S],
        k: usize,
        ef: usize,
        predicate: Option<&dyn Fn(&K) -> bool>,
    ) -> Result<Vec<(K, S)>> {
        let meta = self.meta.get_value().clone();
        if query.len() != meta.dim {
            return Err(VsdbError::Other {
                detail: format!(
                    "dimension mismatch: expected {}, got {}",
                    meta.dim,
                    query.len()
                ),
            });
        }

        let Some(ep) = meta.entry_point else {
            return Ok(vec![]);
        };

        if k == 0 {
            return Ok(vec![]);
        }

        let get_vec = |id: u64| -> Option<Vec<S>> { self.vectors.get(&id) };

        let node_filter: Option<Box<dyn Fn(u64) -> bool + '_>> =
            predicate.map(|pred| -> Box<dyn Fn(u64) -> bool + '_> {
                Box::new(move |node_id: u64| {
                    self.node_to_key.get(&node_id).map_or(false, |k| pred(&k))
                })
            });
        let filter_ref: Option<&dyn Fn(u64) -> bool> =
            node_filter.as_ref().map(|f| f.as_ref());

        let mut cur_ep = vec![ep];
        for l in (1..=meta.max_layer).rev() {
            let res = search_layer::<S, D>(
                query,
                &cur_ep,
                1,
                l,
                &get_vec,
                &self.adjacency,
                None,
            );
            if let Some(&(_, id)) = res.first() {
                cur_ep = vec![id];
            }
        }

        let search_ef = if predicate.is_some() {
            (ef * 2).max(k)
        } else {
            ef.max(k)
        };
        let results = search_layer::<S, D>(
            query,
            &cur_ep,
            search_ef,
            0,
            &get_vec,
            &self.adjacency,
            filter_ref,
        );

        let mut out = Vec::with_capacity(k.min(results.len()));
        for (dist, node_id) in results.into_iter().take(k) {
            if let Some(key) = self.node_to_key.get(&node_id) {
                out.push((key, dist));
            }
        }

        Ok(out)
    }

    /// Removes a vector by user key. Returns `true` if the key existed.
    pub fn remove(&mut self, key: &K) -> Result<bool> {
        let Some(node_id) = self.key_to_node.get(key) else {
            return Ok(false);
        };

        let info = self.node_info.get(&node_id).unwrap_or_default();

        for l in 0..=info.max_layer {
            let neighbors = get_neighbors(&self.adjacency, l, node_id);
            for &n in &neighbors {
                let mut n_list = get_neighbors(&self.adjacency, l, n);
                n_list.retain(|&x| x != node_id);
                set_neighbors(&mut self.adjacency, l, n, &n_list);
            }
            remove_adjacency(&mut self.adjacency, l, node_id);
        }

        self.vectors.remove(&node_id);
        self.key_to_node.remove(key);
        self.node_to_key.remove(&node_id);
        self.node_info.remove(&node_id);

        {
            let mut m = self.meta.get_mut();
            m.node_count -= 1;

            if m.entry_point == Some(node_id) {
                m.entry_point = None;
                if let Some((id, new_info)) = self.node_info.iter().next() {
                    m.entry_point = Some(id);
                    m.max_layer = new_info.max_layer;
                } else {
                    m.max_layer = 0;
                }
            }
        }

        Ok(true)
    }

    /// Rebuilds the HNSW graph from the existing vectors.
    ///
    /// Useful after many deletions to restore graph quality and recall.
    pub fn compact(&mut self) -> Result<()> {
        let pairs: Vec<(K, Vec<S>)> = self
            .node_to_key
            .iter()
            .filter_map(|(node_id, key)| self.vectors.get(&node_id).map(|v| (key, v)))
            .collect();

        self.clear();

        for (key, vec) in &pairs {
            self.insert(key, vec)?;
        }

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
