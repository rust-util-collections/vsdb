//! Core HNSW algorithm: layer assignment, graph search, neighbor selection.

use super::distance::{DistanceMetric, Scalar};
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::rc::Rc;
use vsdb_core::basic::mapx_raw::MapxRaw;

// ---- Ordered scalar wrapper (for BinaryHeap) ----------------------------

#[derive(Clone, Copy, PartialEq)]
pub(crate) struct OrdS<S: Scalar>(pub S);

impl<S: Scalar> Eq for OrdS<S> {}

impl<S: Scalar> PartialOrd for OrdS<S> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: Scalar> Ord for OrdS<S> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

// ---- Layer assignment --------------------------------------------------

/// Assigns a random layer for a new node using exponential decay.
pub(crate) fn random_layer(m: usize) -> u8 {
    let ml = 1.0 / (m as f64).ln();
    let r: f64 = rand::random();
    let l = (-r.max(f64::MIN_POSITIVE).ln() * ml).floor() as u8;
    l.min(32)
}

// ---- Adjacency read abstraction ------------------------------------------

/// Read access to adjacency rows.  Implemented by the raw store (for
/// search paths) and by the staged transaction (for mutation paths,
/// where reads must observe the operation's own uncommitted writes).
pub(crate) trait AdjRead {
    fn adj_row(&self, key: &[u8]) -> Option<Vec<u8>>;
}

impl AdjRead for MapxRaw {
    #[inline]
    fn adj_row(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get(key)
    }
}

// ---- Adjacency key encoding --------------------------------------------

/// Compound key: `[TAG_ADJ][layer: u8][node_id: u64 BE]` = 10 bytes.
#[inline]
pub(crate) fn adj_key(layer: u8, node_id: u64) -> [u8; 10] {
    let mut buf = [0u8; 10];
    buf[0] = super::TAG_ADJ;
    buf[1] = layer;
    buf[2..10].copy_from_slice(&node_id.to_be_bytes());
    buf
}

/// Encode neighbor list as raw bytes (little-endian packed u64s).
pub(crate) fn encode_neighbors(neighbors: &[u64]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(neighbors.len() * 8);
    for &n in neighbors {
        buf.extend_from_slice(&n.to_le_bytes());
    }
    buf
}

/// Decode neighbor list from raw bytes.
pub(crate) fn decode_neighbors(bytes: &[u8]) -> Vec<u64> {
    bytes
        .chunks_exact(8)
        .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
        .collect()
}

/// Get neighbors of a node at a given layer.
pub(crate) fn get_neighbors<A: AdjRead + ?Sized>(
    adj: &A,
    layer: u8,
    node_id: u64,
) -> Vec<u64> {
    let key = adj_key(layer, node_id);
    adj.adj_row(&key)
        .map(|v| decode_neighbors(&v))
        .unwrap_or_default()
}

/// Decode neighbors into a reusable buffer, avoiding allocation on hot paths.
pub(crate) fn get_neighbors_into<A: AdjRead + ?Sized>(
    adj: &A,
    layer: u8,
    node_id: u64,
    buf: &mut Vec<u64>,
) {
    buf.clear();
    let key = adj_key(layer, node_id);
    if let Some(v) = adj.adj_row(&key) {
        for chunk in v.chunks_exact(8) {
            buf.push(u64::from_le_bytes(chunk.try_into().unwrap()));
        }
    }
}

// ---- Graph search ------------------------------------------------------

/// Greedy search within a single layer, returning the `ef` closest nodes.
///
/// When `filter` is `Some`, only nodes accepted by the predicate are counted
/// toward the result set.  Rejected nodes still participate in graph traversal.
/// Distance-based pruning is disabled when filtering to avoid missing
/// filter-passing nodes that are reachable only through unfiltered bridge nodes.
pub(crate) fn search_layer<S: Scalar, D: DistanceMetric<S>, A: AdjRead + ?Sized>(
    query: &[S],
    entry_points: &[u64],
    ef: usize,
    layer: u8,
    get_vector: &dyn Fn(u64) -> Option<Rc<Vec<S>>>,
    adj: &A,
    filter: Option<&dyn Fn(u64) -> bool>,
) -> Vec<(S, u64)> {
    let mut candidates: BinaryHeap<Reverse<(OrdS<S>, u64)>> = BinaryHeap::new();
    let mut result: BinaryHeap<(OrdS<S>, u64)> = BinaryHeap::new();
    let mut visited = HashSet::new();

    let has_filter = filter.is_some();
    let passes = |id: u64| -> bool { filter.is_none_or(|f| f(id)) };
    let visit_budget = has_filter.then_some(ef.max(entry_points.len()).max(1));

    for &ep in entry_points {
        if let Some(vec) = get_vector(ep) {
            let dist = D::distance(query, &vec);
            candidates.push(Reverse((OrdS(dist), ep)));
            if passes(ep) {
                result.push((OrdS(dist), ep));
            }
            visited.insert(ep);
        }
    }

    let mut neighbor_buf = Vec::new();
    while let Some(Reverse((OrdS(c_dist), c_id))) = candidates.pop() {
        if visit_budget.is_some_and(|budget| visited.len() >= budget) {
            break;
        }

        // Standard HNSW early termination: stop when the nearest unvisited
        // candidate is farther than the k-th result.  This is only sound
        // without filtering — when a filter is active, unfiltered bridge nodes
        // can connect to closer filter-passing neighbors, so we skip it.
        if !has_filter
            && let Some(&(OrdS(f_dist), _)) = result.peek()
            && c_dist.total_cmp(&f_dist) == Ordering::Greater
            && result.len() >= ef
        {
            break;
        }

        get_neighbors_into(adj, layer, c_id, &mut neighbor_buf);
        for &n_id in &neighbor_buf {
            if visit_budget.is_some_and(|budget| visited.len() >= budget) {
                break;
            }
            if !visited.insert(n_id) {
                continue;
            }
            let Some(n_vec) = get_vector(n_id) else {
                continue;
            };
            let n_dist = D::distance(query, &n_vec);

            let should_add = if has_filter {
                // When filtering, add every unvisited neighbor to the candidate
                // pool.  The visited set and natural distance decay bound the
                // search; the caller's inflated `ef` budgets the work.
                true
            } else {
                // Standard HNSW pruning: skip neighbors that are farther than
                // the worst result when we already have enough results.
                let result_full = result.len() >= ef;
                let worse_than_worst = result_full
                    && result.peek().is_some_and(|&(OrdS(f), _)| {
                        n_dist.total_cmp(&f) != Ordering::Less
                    });
                !worse_than_worst
            };

            if should_add {
                candidates.push(Reverse((OrdS(n_dist), n_id)));
                if passes(n_id) {
                    result.push((OrdS(n_dist), n_id));
                    if result.len() > ef {
                        result.pop();
                    }
                }
            }
        }
    }

    let mut out: Vec<(S, u64)> =
        result.into_iter().map(|(OrdS(d), id)| (d, id)).collect();
    out.sort_by(|a, b| a.0.total_cmp(&b.0));
    out
}

// ---- Neighbor selection ------------------------------------------------

/// Select the M nearest neighbors from candidates (simple heuristic).
#[cfg(test)]
pub(crate) fn select_neighbors_simple<S: Scalar>(
    candidates: &[(S, u64)],
    m: usize,
) -> Vec<u64> {
    let mut sorted: Vec<_> = candidates.to_vec();
    sorted.sort_by(|a, b| a.0.total_cmp(&b.0));
    sorted.iter().take(m).map(|&(_, id)| id).collect()
}

/// Connectivity-aware neighbor selection (HNSW paper Algorithm 4).
///
/// Prefers neighbors that are diverse in direction rather than just closest.
/// For each candidate (in distance order), it is selected only if it is
/// closer to the query than to any already-selected neighbor.
pub(crate) fn select_neighbors_heuristic<S: Scalar, D: DistanceMetric<S>>(
    candidates: &[(S, u64)],
    m: usize,
    get_vector: &dyn Fn(u64) -> Option<Rc<Vec<S>>>,
) -> Vec<u64> {
    let mut sorted: Vec<_> = candidates.to_vec();
    sorted.sort_by(|a, b| a.0.total_cmp(&b.0));

    let mut selected: Vec<(S, u64)> = Vec::with_capacity(m);
    let mut selected_vecs: Vec<Rc<Vec<S>>> = Vec::with_capacity(m);

    for &(dist_to_query, cand_id) in &sorted {
        if selected.len() >= m {
            break;
        }
        let Some(cand_vec) = get_vector(cand_id) else {
            continue;
        };

        // Check: is this candidate closer to query than to any selected neighbor?
        let mut is_diverse = true;
        for sel_vec in &selected_vecs {
            let dist_to_sel = D::distance(&cand_vec, sel_vec);
            if dist_to_sel.total_cmp(&dist_to_query) == Ordering::Less {
                is_diverse = false;
                break;
            }
        }

        if is_diverse {
            selected_vecs.push(cand_vec);
            selected.push((dist_to_query, cand_id));
        }
    }

    // If heuristic didn't fill m slots, pad with closest remaining.
    if selected.len() < m {
        let mut selected_ids: HashSet<u64> =
            selected.iter().map(|&(_, id)| id).collect();
        for &(_, cand_id) in &sorted {
            if selected.len() >= m {
                break;
            }
            if selected_ids.insert(cand_id) {
                selected.push((S::zero(), cand_id));
            }
        }
    }

    selected.iter().map(|&(_, id)| id).collect()
}

/// Prune a neighbor list to at most `m_max` entries using the diversity
/// heuristic (Algorithm 4), matching the selection strategy used during
/// insert.
///
/// Pure computation: returns `None` when nothing needs pruning, else
/// `(pruned_list, evicted_ids)` for the caller to write back.
pub(crate) fn prune_selection<S: Scalar, D: DistanceMetric<S>, A: AdjRead + ?Sized>(
    node_id: u64,
    layer: u8,
    m_max: usize,
    adj: &A,
    get_vector: &dyn Fn(u64) -> Option<Rc<Vec<S>>>,
) -> Option<(Vec<u64>, Vec<u64>)> {
    let neighbors = get_neighbors(adj, layer, node_id);
    if neighbors.len() <= m_max {
        return None;
    }
    let node_vec = get_vector(node_id)?;
    let scored: Vec<(S, u64)> = neighbors
        .iter()
        .filter_map(|&n| get_vector(n).map(|v| (D::distance(&node_vec, &v), n)))
        .collect();
    let pruned = select_neighbors_heuristic::<S, D>(&scored, m_max, get_vector);
    let evicted = neighbors
        .into_iter()
        .filter(|n| !pruned.contains(n))
        .collect();
    Some((pruned, evicted))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adj_key_roundtrip() {
        let key = adj_key(3, 0xDEAD_BEEF_CAFE_BABE);
        assert_eq!(key[0], super::super::TAG_ADJ);
        assert_eq!(key[1], 3);
        let id = u64::from_be_bytes(key[2..10].try_into().unwrap());
        assert_eq!(id, 0xDEAD_BEEF_CAFE_BABE);
    }

    #[test]
    fn neighbor_encoding_roundtrip() {
        let neighbors = vec![1, 2, 3, u64::MAX, 0];
        let encoded = encode_neighbors(&neighbors);
        let decoded = decode_neighbors(&encoded);
        assert_eq!(neighbors, decoded);
    }

    #[test]
    fn select_neighbors_simple_picks_closest() {
        let candidates = vec![(0.5f32, 1), (0.1, 2), (0.9, 3), (0.3, 4)];
        let selected = select_neighbors_simple(&candidates, 2);
        assert_eq!(selected, vec![2, 4]);
    }

    #[test]
    fn random_layer_distribution() {
        let mut counts = [0u32; 33];
        for _ in 0..10000 {
            let l = random_layer(16) as usize;
            counts[l] += 1;
        }
        assert!(counts[0] > counts[1]);
        assert!(counts[1] >= counts[2]);
    }
}
