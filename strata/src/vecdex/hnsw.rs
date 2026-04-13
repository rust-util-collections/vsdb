//! Core HNSW algorithm: layer assignment, graph search, neighbor selection.

use super::distance::{DistanceMetric, Scalar};
use std::{cmp::Ordering, collections::BinaryHeap};
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

// ---- Adjacency key encoding --------------------------------------------

/// Compound key: `[layer: u8][node_id: u64 BE]` = 9 bytes.
#[inline]
pub(crate) fn adj_key(layer: u8, node_id: u64) -> [u8; 9] {
    let mut buf = [0u8; 9];
    buf[0] = layer;
    buf[1..9].copy_from_slice(&node_id.to_be_bytes());
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
pub(crate) fn get_neighbors(adjacency: &MapxRaw, layer: u8, node_id: u64) -> Vec<u64> {
    let key = adj_key(layer, node_id);
    adjacency
        .get(&key)
        .map(|v| decode_neighbors(&v))
        .unwrap_or_default()
}

/// Set neighbors of a node at a given layer.
pub(crate) fn set_neighbors(
    adjacency: &mut MapxRaw,
    layer: u8,
    node_id: u64,
    neighbors: &[u64],
) {
    let key = adj_key(layer, node_id);
    if neighbors.is_empty() {
        adjacency.remove(&key);
    } else {
        adjacency.insert(&key, &encode_neighbors(neighbors));
    }
}

/// Remove a node's adjacency entry at a given layer.
pub(crate) fn remove_adjacency(adjacency: &mut MapxRaw, layer: u8, node_id: u64) {
    adjacency.remove(&adj_key(layer, node_id));
}

// ---- Graph search ------------------------------------------------------

/// Greedy search within a single layer, returning the `ef` closest nodes.
///
/// When `filter` is `Some`, only nodes accepted by the predicate are counted
/// toward the result set.  Rejected nodes still participate in graph traversal.
pub(crate) fn search_layer<S: Scalar, D: DistanceMetric<S>>(
    query: &[S],
    entry_points: &[u64],
    ef: usize,
    layer: u8,
    get_vector: &dyn Fn(u64) -> Option<Vec<S>>,
    adjacency: &MapxRaw,
    filter: Option<&dyn Fn(u64) -> bool>,
) -> Vec<(S, u64)> {
    let mut candidates: BinaryHeap<std::cmp::Reverse<(OrdS<S>, u64)>> =
        BinaryHeap::new();
    let mut result: BinaryHeap<(OrdS<S>, u64)> = BinaryHeap::new();
    let mut visited = std::collections::HashSet::new();

    let passes = |id: u64| -> bool { filter.map_or(true, |f| f(id)) };

    for &ep in entry_points {
        if let Some(vec) = get_vector(ep) {
            let dist = D::distance(query, &vec);
            candidates.push(std::cmp::Reverse((OrdS(dist), ep)));
            if passes(ep) {
                result.push((OrdS(dist), ep));
            }
            visited.insert(ep);
        }
    }

    while let Some(std::cmp::Reverse((OrdS(c_dist), c_id))) = candidates.pop() {
        if let Some(&(OrdS(f_dist), _)) = result.peek() {
            if c_dist.total_cmp(&f_dist) == Ordering::Greater && result.len() >= ef {
                break;
            }
        }

        let neighbors = get_neighbors(adjacency, layer, c_id);
        for &n_id in &neighbors {
            if !visited.insert(n_id) {
                continue;
            }
            let Some(n_vec) = get_vector(n_id) else {
                continue;
            };
            let n_dist = D::distance(query, &n_vec);

            let should_add = result.len() < ef
                || result
                    .peek()
                    .is_some_and(|&(OrdS(f), _)| n_dist.total_cmp(&f) == Ordering::Less);

            if should_add {
                candidates.push(std::cmp::Reverse((OrdS(n_dist), n_id)));
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
    get_vector: &dyn Fn(u64) -> Option<Vec<S>>,
) -> Vec<u64> {
    let mut sorted: Vec<_> = candidates.to_vec();
    sorted.sort_by(|a, b| a.0.total_cmp(&b.0));

    let mut selected: Vec<(S, u64)> = Vec::with_capacity(m);
    let mut selected_vecs: Vec<Vec<S>> = Vec::with_capacity(m);

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
        let selected_ids: std::collections::HashSet<u64> =
            selected.iter().map(|&(_, id)| id).collect();
        for &(_, cand_id) in &sorted {
            if selected.len() >= m {
                break;
            }
            if !selected_ids.contains(&cand_id) {
                selected.push((S::zero(), cand_id));
            }
        }
    }

    selected.iter().map(|&(_, id)| id).collect()
}

/// Prune a neighbor list to at most `m_max` entries, keeping the closest.
pub(crate) fn prune_neighbors<S: Scalar, D: DistanceMetric<S>>(
    node_id: u64,
    layer: u8,
    m_max: usize,
    adjacency: &mut MapxRaw,
    get_vector: &dyn Fn(u64) -> Option<Vec<S>>,
) {
    let neighbors = get_neighbors(adjacency, layer, node_id);
    if neighbors.len() <= m_max {
        return;
    }
    let Some(node_vec) = get_vector(node_id) else {
        return;
    };
    let mut scored: Vec<(S, u64)> = neighbors
        .iter()
        .filter_map(|&n| get_vector(n).map(|v| (D::distance(&node_vec, &v), n)))
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0));
    let pruned: Vec<u64> = scored.iter().take(m_max).map(|&(_, id)| id).collect();
    set_neighbors(adjacency, layer, node_id, &pruned);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adj_key_roundtrip() {
        let key = adj_key(3, 0xDEAD_BEEF_CAFE_BABE);
        assert_eq!(key[0], 3);
        let id = u64::from_be_bytes(key[1..9].try_into().unwrap());
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
