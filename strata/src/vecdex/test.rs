use super::*;
use distance::{Cosine, InnerProduct, L2, MetricKind};
use std::collections::{HashSet, VecDeque};

fn assert_bidirectional<K, D, S>(idx: &VecDex<K, D, S>)
where
    K: KeyEnDe + ValueEnDe + Clone + Eq,
    D: DistanceMetric<S>,
    S: Scalar,
{
    for (node, node_max) in idx.node_layers() {
        for layer in 0..=node_max {
            for neighbor in hnsw::get_neighbors(&idx.store, layer, node) {
                let reverse = hnsw::get_neighbors(&idx.store, layer, neighbor);
                assert!(
                    reverse.contains(&node),
                    "missing reverse edge layer {layer}: {neighbor} -> {node}"
                );
            }
        }
    }
}

#[test]
fn basic_insert_search_l2() {
    let cfg = HnswConfig {
        dim: 3,
        ..Default::default()
    };
    let mut idx: VecDex<String, L2> = VecDex::new(cfg);

    idx.insert(&"a".into(), &[1.0, 0.0, 0.0]).unwrap();
    idx.insert(&"b".into(), &[0.0, 1.0, 0.0]).unwrap();
    idx.insert(&"c".into(), &[1.0, 0.1, 0.0]).unwrap();

    let results = idx.search(&[1.0, 0.0, 0.0], 2).unwrap();
    assert_eq!(results.len(), 2);
    // "a" should be closest (distance 0), then "c".
    assert_eq!(results[0].0, "a");
    assert!(results[0].1 < f32::EPSILON);
    assert_eq!(results[1].0, "c");
}

#[test]
fn basic_insert_search_cosine() {
    let cfg = HnswConfig {
        dim: 3,
        ..Default::default()
    };
    let mut idx: VecDex<String, Cosine> = VecDex::new(cfg);

    idx.insert(&"a".into(), &[1.0, 0.0, 0.0]).unwrap();
    idx.insert(&"b".into(), &[0.0, 1.0, 0.0]).unwrap();
    idx.insert(&"c".into(), &[0.9, 0.1, 0.0]).unwrap();

    let results = idx.search(&[1.0, 0.0, 0.0], 1).unwrap();
    assert_eq!(results[0].0, "a");
}

#[test]
fn basic_insert_search_inner_product() {
    let cfg = HnswConfig {
        dim: 3,
        ..Default::default()
    };
    let mut idx: VecDex<String, InnerProduct> = VecDex::new(cfg);

    idx.insert(&"a".into(), &[1.0, 0.0, 0.0]).unwrap();
    idx.insert(&"b".into(), &[0.0, 1.0, 0.0]).unwrap();
    idx.insert(&"c".into(), &[0.5, 0.5, 0.0]).unwrap();

    let results = idx.search(&[1.0, 0.0, 0.0], 1).unwrap();
    // "a" has max inner product of 1.0 with query.
    assert_eq!(results[0].0, "a");
}

#[test]
fn search_empty_index() {
    let cfg = HnswConfig {
        dim: 4,
        ..Default::default()
    };
    let idx: VecDex<u64, L2> = VecDex::new(cfg);
    let results = idx.search(&[0.0; 4], 5).unwrap();
    assert!(results.is_empty());
}

#[test]
fn dimension_mismatch() {
    let cfg = HnswConfig {
        dim: 3,
        ..Default::default()
    };
    let mut idx: VecDex<u64, L2> = VecDex::new(cfg);
    assert!(idx.insert(&1, &[0.0, 0.0]).is_err());
    idx.insert(&1, &[0.0, 0.0, 0.0]).unwrap();
    assert!(idx.search(&[0.0, 0.0], 1).is_err());
}

#[test]
fn remove_and_search() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<String, L2> = VecDex::new(cfg);

    idx.insert(&"a".into(), &[0.0, 0.0]).unwrap();
    idx.insert(&"b".into(), &[1.0, 1.0]).unwrap();
    idx.insert(&"c".into(), &[2.0, 2.0]).unwrap();
    assert_eq!(idx.len(), 3);

    let removed = idx.remove(&"a".into()).unwrap();
    assert!(removed);
    assert_eq!(idx.len(), 2);

    let results = idx.search(&[0.0, 0.0], 3).unwrap();
    assert_eq!(results.len(), 2);
    // "a" should not appear.
    assert!(results.iter().all(|(k, _)| k != "a"));
}

#[test]
fn remove_nonexistent() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<String, L2> = VecDex::new(cfg);
    let removed = idx.remove(&"nope".into()).unwrap();
    assert!(!removed);
}

#[test]
fn duplicate_key_update() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<String, L2> = VecDex::new(cfg);

    idx.insert(&"a".into(), &[0.0, 0.0]).unwrap();
    idx.insert(&"b".into(), &[10.0, 10.0]).unwrap();

    // Update "a" to be near "b".
    idx.insert(&"a".into(), &[10.0, 10.0]).unwrap();
    assert_eq!(idx.len(), 2);

    let results = idx.search(&[10.0, 10.0], 1).unwrap();
    // Either "a" or "b" are at distance 0.
    assert!(results[0].1 < f32::EPSILON);
}

#[test]
fn save_meta_restore() {
    let cfg = HnswConfig {
        dim: 3,
        ..Default::default()
    };
    let mut idx: VecDex<String, L2> = VecDex::new(cfg);
    idx.insert(&"a".into(), &[1.0, 2.0, 3.0]).unwrap();
    idx.insert(&"b".into(), &[4.0, 5.0, 6.0]).unwrap();

    let id = idx.save_meta().unwrap();

    let restored: VecDex<String, L2> = VecDex::from_meta(id).unwrap();
    assert_eq!(restored.len(), 2);

    let results = restored.search(&[1.0, 2.0, 3.0], 1).unwrap();
    assert_eq!(results[0].0, "a");
}

/// The distance metric occurs in no field type, so this exercises the
/// typed-handle envelope: restoring an L2-built graph as Cosine would
/// silently return wrong neighbors — it must fail loudly instead.
#[test]
fn from_meta_rejects_wrong_metric_or_key() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    idx.insert(&1, &[1.0, 2.0]).unwrap();

    let id = idx.save_meta().unwrap();
    assert!(VecDex::<u32, Cosine>::from_meta(id).is_err());
    assert!(VecDex::<String, L2>::from_meta(id).is_err());
    assert!(VecDex::<u32, L2, f64>::from_meta(id).is_err());
}

#[test]
fn clear_resets_everything() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    idx.insert(&1, &[0.0, 0.0]).unwrap();
    idx.insert(&2, &[1.0, 1.0]).unwrap();
    assert_eq!(idx.len(), 2);

    idx.clear();
    assert_eq!(idx.len(), 0);
    assert!(idx.is_empty());
    assert!(idx.search(&[0.0, 0.0], 1).unwrap().is_empty());

    // Can re-use after clear.
    idx.insert(&3, &[0.0, 0.0]).unwrap();
    assert_eq!(idx.len(), 1);
}

#[test]
fn clear_preserves_ef_search_across_restore() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    let id = idx.save_meta().unwrap();

    for i in 0..20u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }
    idx.set_ef_search(123);
    idx.clear();
    assert!(idx.is_empty());
    assert_eq!(idx.state.ef_search, 123);
    drop(idx);

    // The cleared state row (including the live ef_search) is part of
    // the same atomic batch as the wipe, so a restore observes it.
    let mut restored: VecDex<u32, L2> = VecDex::from_meta(id).unwrap();
    assert!(restored.is_empty());
    assert_eq!(restored.state.ef_search, 123);
    assert!(restored.search(&[0.0, 0.0], 1).unwrap().is_empty());

    // Fully usable after clear + restore.
    restored.insert(&7, &[7.0, 0.0]).unwrap();
    assert_eq!(restored.len(), 1);
    assert_eq!(restored.search(&[7.0, 0.0], 1).unwrap()[0].0, 7);
}

#[test]
fn recall_random_vectors() {
    let cfg = HnswConfig {
        dim: 32,
        m: 16,
        m_max0: 32,
        ef_construction: 200,
        ef_search: 100,
    };
    let mut idx: VecDex<u64, L2> = VecDex::new(cfg);

    let n = 200;
    let k = 5;
    let mut all_vecs: Vec<(u64, Vec<f32>)> = Vec::with_capacity(n);

    for i in 0..n as u64 {
        let v: Vec<f32> = (0..32).map(|_| rand::random::<f32>()).collect();
        idx.insert(&i, &v).unwrap();
        all_vecs.push((i, v));
    }

    // Pick a few random query points and check recall.
    let queries = 10;
    let mut total_recall = 0.0f64;
    for q in 0..queries {
        let query: Vec<f32> = (0..32).map(|_| rand::random::<f32>()).collect();

        // Brute-force ground truth.
        let mut dists: Vec<(f32, u64)> = all_vecs
            .iter()
            .map(|(id, v)| (L2::distance(&query, v), *id))
            .collect();
        dists.sort_by(|a, b| a.0.total_cmp(&b.0));
        let gt: HashSet<u64> = dists.iter().take(k).map(|&(_, id)| id).collect();

        // HNSW result.
        let results = idx.search(&query, k).unwrap();
        let found: HashSet<u64> = results.iter().map(|(key, _)| *key).collect();

        let hits = gt.intersection(&found).count();
        total_recall += hits as f64 / k as f64;

        // Verify results are sorted by distance.
        for w in results.windows(2) {
            assert!(
                w[0].1 <= w[1].1 + f32::EPSILON,
                "query {q}: results not sorted: {} > {}",
                w[0].1,
                w[1].1
            );
        }
    }

    let avg_recall = total_recall / queries as f64;
    assert!(
        avg_recall >= 0.7,
        "average recall@{k} = {avg_recall:.2}, expected >= 0.7"
    );
}

#[test]
fn filtered_search_basic() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<String, L2> = VecDex::new(cfg);

    idx.insert(&"cat-a".into(), &[0.0, 0.0]).unwrap();
    idx.insert(&"cat-b".into(), &[0.1, 0.1]).unwrap();
    idx.insert(&"dog-a".into(), &[10.0, 10.0]).unwrap();
    idx.insert(&"dog-b".into(), &[10.1, 10.1]).unwrap();

    // Without filter: closest to origin is cat-a.
    let results = idx.search(&[0.0, 0.0], 1).unwrap();
    assert_eq!(results[0].0, "cat-a");

    // With filter: only "dog-*" keys — dog-a is closest in that subset.
    let results = idx
        .search_with_filter(&[0.0, 0.0], 1, |k: &String| k.starts_with("dog-"))
        .unwrap();
    assert_eq!(results.len(), 1);
    assert!(results[0].0.starts_with("dog-"));
}

#[test]
fn filtered_search_no_match() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<String, L2> = VecDex::new(cfg);

    idx.insert(&"a".into(), &[0.0, 0.0]).unwrap();
    idx.insert(&"b".into(), &[1.0, 1.0]).unwrap();

    // Filter rejects everything.
    let results = idx
        .search_with_filter(&[0.0, 0.0], 5, |_: &String| false)
        .unwrap();
    assert!(results.is_empty());
}

#[test]
fn filtered_search_layer_uses_visit_budget() {
    let mut adjacency = MapxRaw::new();
    for node in 0..99u64 {
        adjacency.insert(hnsw::adj_key(0, node), hnsw::encode_neighbors(&[node + 1]));
    }

    let vectors: Vec<Vec<f32>> = (0..100).map(|i| vec![i as f32]).collect();
    let get_vec = |id: u64| -> Option<std::rc::Rc<Vec<f32>>> {
        vectors.get(id as usize).cloned().map(std::rc::Rc::new)
    };
    let calls = std::cell::Cell::new(0usize);
    let reject_all = |_: u64| {
        calls.set(calls.get() + 1);
        false
    };

    let results = hnsw::search_layer::<f32, L2, _>(
        &[0.0],
        &[0],
        8,
        0,
        &get_vec,
        &adjacency,
        Some(&reject_all),
    );

    assert!(results.is_empty());
    assert!(calls.get() <= 8, "visited {} nodes", calls.get());
}

#[test]
fn filtered_search_respects_k() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);

    // Even keys near origin, odd keys far away.
    for i in 0..20u32 {
        let v = if i % 2 == 0 {
            vec![i as f32 * 0.1, 0.0]
        } else {
            vec![100.0 + i as f32, 100.0]
        };
        idx.insert(&i, &v).unwrap();
    }

    // Search with filter for even keys only, k=3.
    let results = idx
        .search_with_filter(&[0.0, 0.0], 3, |k: &u32| k.is_multiple_of(2))
        .unwrap();
    assert_eq!(results.len(), 3);
    for (k, _) in &results {
        assert_eq!(k % 2, 0);
    }
}

#[test]
fn f64_basic() {
    let cfg = HnswConfig {
        dim: 3,
        ..Default::default()
    };
    let mut idx: VecDex<String, L2, f64> = VecDex::new(cfg);

    idx.insert(&"a".into(), &[1.0_f64, 0.0, 0.0]).unwrap();
    idx.insert(&"b".into(), &[0.0_f64, 1.0, 0.0]).unwrap();
    idx.insert(&"c".into(), &[1.0_f64, 0.1, 0.0]).unwrap();

    let results = idx.search(&[1.0, 0.0, 0.0], 2).unwrap();
    assert_eq!(results[0].0, "a");
    assert!(results[0].1 < f64::EPSILON);
}

#[test]
fn compact_restores_search() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    let id = idx.save_meta().unwrap();

    for i in 0..20u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }

    // Delete half.
    for i in 0..10u32 {
        idx.remove(&i).unwrap();
    }
    assert_eq!(idx.len(), 10);

    // Compact rebuilds graph.
    idx.compact().unwrap();
    assert_eq!(idx.len(), 10);

    // Search still works correctly.
    let results = idx.search(&[15.0, 0.0], 1).unwrap();
    assert_eq!(results[0].0, 15);

    // The rebuilt graph maintains its invariants and survives a restore
    // through the create-time metadata (saved before the compact).
    assert_bidirectional(&idx);
    let expected: Vec<(u32, Vec<f32>)> = idx.iter().collect();
    drop(idx);

    let restored: VecDex<u32, L2> = VecDex::from_meta(id).unwrap();
    assert_eq!(restored.len(), 10);
    let got: Vec<(u32, Vec<f32>)> = restored.iter().collect();
    assert_eq!(got, expected);
    assert_bidirectional(&restored);
    assert_eq!(restored.search(&[15.0, 0.0], 1).unwrap()[0].0, 15);
}

#[test]
fn insert_batch_works() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);

    let items: Vec<(u32, Vec<f32>)> =
        (0..10u32).map(|i| (i, vec![i as f32, 0.0])).collect();
    idx.insert_batch(&items).unwrap();
    assert_eq!(idx.len(), 10);

    let results = idx.search(&[5.0, 0.0], 1).unwrap();
    assert_eq!(results[0].0, 5);
}

#[test]
fn k_larger_than_index_size() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);

    idx.insert(&1, &[0.0, 0.0]).unwrap();
    idx.insert(&2, &[1.0, 1.0]).unwrap();
    idx.insert(&3, &[2.0, 2.0]).unwrap();

    // Request k=100 but only 3 vectors exist.
    let results = idx.search(&[0.0, 0.0], 100).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, 1);
}

#[test]
fn remove_entry_point_preserves_max_layer() {
    let cfg = HnswConfig {
        dim: 2,
        m: 4,
        m_max0: 8,
        ef_construction: 50,
        ef_search: 50,
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);

    // Insert enough nodes that some land on higher layers.
    for i in 0..50u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }

    // Record the entry point and remove it.
    let meta_before = idx.state.clone();
    let ep = meta_before.entry_point.unwrap();
    idx.remove(&(ep as u32)).unwrap();

    // After removal, max_layer should still reflect the true global max.
    let meta_after = idx.state.clone();
    let actual_max = idx.node_layers().map(|(_, l)| l).max().unwrap_or(0);
    assert_eq!(
        meta_after.max_layer, actual_max,
        "max_layer should equal the true global maximum layer"
    );

    // Search should still work — all remaining vectors reachable.
    let results = idx.search(&[25.0, 0.0], 5).unwrap();
    assert_eq!(results.len(), 5);
}

/// Regression: entry-point re-election on `remove()` must never deflate
/// `max_layer` below the TRUE global maximum layer among live nodes,
/// even when the node chosen as the new entry point (which correctly
/// prefers linked candidates, so an isolated node can't hide the graph)
/// sits at a lower layer than an isolated node holding the real maximum.
///
/// Before the fix, the candidate comparison `(linked, layer)` put
/// `linked` first, so ANY linked node beat ANY unlinked node regardless
/// of layer, and the winning candidate's own (lower) layer was written
/// straight into `max_layer` instead of a true max scan. Organic HNSW
/// graphs (random layer assignment) rarely produce this adversarial
/// shape, so the graph below is constructed directly — bypassing
/// `insert()`'s random layer roll — to pin the exact scenario.
#[test]
fn remove_entry_point_does_not_deflate_max_layer_below_isolated_high_layer_node() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);

    // node 0 "hi": true global max layer (4), but ISOLATED — no edges at
    // any layer.
    // node 1 "low": layer 1, linked to node 2 at layer 0.
    // node 2 "helper": layer 0, linked to node 1 at layer 0.
    // node 3 "ep": layer 5, current entry point (isolated; its own
    // connectivity doesn't matter since it's the one being removed).
    let mut txn: Txn<'_, f32> = Txn::new(&idx.store, idx.state.clone());
    for (node_id, key, layer, vec) in [
        (0u64, 0u32, 4u8, [0.0f32, 0.0]),
        (1, 1, 1, [1.0, 0.0]),
        (2, 2, 0, [2.0, 0.0]),
        (3, 3, 5, [3.0, 0.0]),
    ] {
        txn.put_vec(node_id, &vec);
        txn.rows
            .put(node_key(TAG_NODE2KEY, node_id).to_vec(), encode_value(&key));
        txn.rows
            .put(node_key(TAG_INFO, node_id).to_vec(), encode_value(&layer));
        txn.rows.put(
            user_key(&KeyEnDe::encode(&key)),
            node_id.to_le_bytes().to_vec(),
        );
    }
    // Bidirectional edge: low (1) <-> helper (2) at layer 0.
    txn.set_neighbors(0, 1, &[2]);
    txn.set_neighbors(0, 2, &[1]);

    txn.state.entry_point = Some(3);
    txn.state.max_layer = 5;
    txn.state.node_count = 4;
    txn.state.next_node_id = 4;

    let (rows, state) = txn.finish();
    rows.commit(&mut idx.store).unwrap();
    idx.state = state;

    // Sanity: the constructed graph matches the intended shape before
    // removal.
    assert_eq!(idx.node_layers().map(|(_, l)| l).max(), Some(5));

    idx.remove(&3).unwrap();

    let true_max = idx.node_layers().map(|(_, l)| l).max().unwrap_or(0);
    assert_eq!(
        true_max, 4,
        "sanity: node 0 (\"hi\") must survive at layer 4"
    );
    assert_eq!(
        idx.state.max_layer, true_max,
        "max_layer must reflect the true global max even though the \
         re-elected entry point (node 1, layer 1, linked) sits lower"
    );
}

// ---- T-1: Single-node duplicate-key update (regression for stale-metadata fix) ----

#[test]
fn single_node_duplicate_key_update() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<String, L2> = VecDex::new(cfg);

    idx.insert(&"only".into(), &[0.0, 0.0]).unwrap();
    assert_eq!(idx.len(), 1);

    // Re-insert the only node with a different vector.
    idx.insert(&"only".into(), &[5.0, 5.0]).unwrap();
    assert_eq!(idx.len(), 1);

    let results = idx.search(&[5.0, 5.0], 1).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, "only");
    assert!(results[0].1 < f32::EPSILON);
}

// ---- T-2: Update entry point's vector ----

#[test]
fn update_entry_point_vector() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);

    for i in 0..10u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }

    let ep = idx.state.entry_point.unwrap();
    // Re-insert the entry point with a far-away vector.
    idx.insert(&(ep as u32), &[100.0, 100.0]).unwrap();
    assert_eq!(idx.len(), 10);

    // All 10 nodes should still be searchable.
    let results = idx.search(&[5.0, 0.0], 10).unwrap();
    assert_eq!(results.len(), 10);
}

// ---- T-3: Consecutive entry point removals ----

#[test]
fn consecutive_entry_point_removals() {
    let cfg = HnswConfig {
        dim: 2,
        m: 4,
        m_max0: 8,
        ef_construction: 50,
        ef_search: 50,
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);

    for i in 0..20u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }

    // Repeatedly remove the entry point.
    for _ in 0..10 {
        let meta = idx.state.clone();
        if meta.entry_point.is_none() {
            break;
        }
        let ep = meta.entry_point.unwrap();
        idx.remove(&(ep as u32)).unwrap();

        let after = idx.state.clone();
        let actual_max = idx.node_layers().map(|(_, l)| l).max().unwrap_or(0);
        assert_eq!(after.max_layer, actual_max);
    }
}

// ---- T-4: Remove all then re-insert ----

#[test]
fn remove_all_then_reinsert() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);

    for i in 0..10u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }

    for i in 0..10u32 {
        assert!(idx.remove(&i).unwrap());
    }
    assert_eq!(idx.len(), 0);
    assert!(idx.search(&[0.0, 0.0], 1).unwrap().is_empty());

    for i in 100..105u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }
    assert_eq!(idx.len(), 5);
    let results = idx.search(&[102.0, 0.0], 1).unwrap();
    assert_eq!(results[0].0, 102);
}

// ---- T-5: Graph connectivity after deletions ----

#[test]
fn graph_connectivity_after_deletions() {
    let cfg = HnswConfig {
        dim: 2,
        m: 8,
        m_max0: 16,
        ef_construction: 100,
        ef_search: 50,
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);

    for i in 0..50u32 {
        idx.insert(&i, &[i as f32, (i as f32 * 0.3).sin()]).unwrap();
    }
    assert_bidirectional(&idx);

    // Remove 10 nodes that are NOT the entry point.
    let ep = idx.state.entry_point.unwrap();
    let mut removed = 0;
    for i in 0..50u32 {
        if i as u64 == ep {
            continue;
        }
        idx.remove(&i).unwrap();
        assert_bidirectional(&idx);
        removed += 1;
        if removed >= 10 {
            break;
        }
    }
    let remaining = idx.len() as usize;

    // BFS from entry point at layer 0.
    let ep = idx.state.entry_point.unwrap();
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(ep);
    visited.insert(ep);
    while let Some(node) = queue.pop_front() {
        let neighbors = hnsw::get_neighbors(&idx.store, 0, node);
        for n in neighbors {
            if visited.insert(n) {
                queue.push_back(n);
            }
        }
    }
    assert_eq!(
        visited.len(),
        remaining,
        "all {} remaining nodes must be reachable from EP, but only {} found",
        remaining,
        visited.len()
    );
}

// ---- T-6: Compact recall comparison ----

#[test]
fn compact_improves_or_maintains_recall() {
    let dim = 16;
    let cfg = HnswConfig {
        dim,
        m: 8,
        m_max0: 16,
        ef_construction: 100,
        ef_search: 100,
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);

    let mut vecs = Vec::new();
    for i in 0..100u32 {
        let v: Vec<f32> = (0..dim).map(|_| rand::random::<f32>()).collect();
        idx.insert(&i, &v).unwrap();
        vecs.push((i, v));
    }

    // Delete half.
    for i in 0..50u32 {
        idx.remove(&i).unwrap();
    }
    let live: Vec<_> = vecs.iter().filter(|(id, _)| *id >= 50).collect();

    let measure_recall = |index: &VecDex<u32, L2>| -> f64 {
        let queries = 10;
        let k = 5;
        let mut total = 0.0;
        for _q in 0..queries {
            let query: Vec<f32> = (0..dim).map(|_| rand::random::<f32>()).collect();
            let mut dists: Vec<(f32, u32)> = live
                .iter()
                .map(|(id, v)| (L2::distance(&query, v), *id))
                .collect();
            dists.sort_by(|a, b| a.0.total_cmp(&b.0));
            let gt: HashSet<u32> = dists.iter().take(k).map(|&(_, id)| id).collect();

            let results = index.search(&query, k).unwrap();
            let found: HashSet<u32> = results.iter().map(|(key, _)| *key).collect();
            total += gt.intersection(&found).count() as f64 / k as f64;
        }
        total / queries as f64
    };

    let recall_before = measure_recall(&idx);
    idx.compact().unwrap();
    let recall_after = measure_recall(&idx);

    assert!(
        recall_after >= recall_before - 0.1,
        "compact should maintain or improve recall: before={recall_before:.2}, after={recall_after:.2}"
    );
}

// ---- T-7: Large-scale recall ----

#[test]
fn recall_large_scale() {
    let dim = 32;
    let cfg = HnswConfig {
        dim,
        m: 16,
        m_max0: 32,
        ef_construction: 200,
        ef_search: 100,
    };
    let mut idx: VecDex<u64, L2> = VecDex::new(cfg);

    let n = 500;
    let k = 10;
    let mut all_vecs: Vec<(u64, Vec<f32>)> = Vec::with_capacity(n);
    for i in 0..n as u64 {
        let v: Vec<f32> = (0..dim).map(|_| rand::random::<f32>()).collect();
        idx.insert(&i, &v).unwrap();
        all_vecs.push((i, v));
    }

    let queries = 20;
    let mut total_recall = 0.0f64;
    for _ in 0..queries {
        let query: Vec<f32> = (0..dim).map(|_| rand::random::<f32>()).collect();
        let mut dists: Vec<(f32, u64)> = all_vecs
            .iter()
            .map(|(id, v)| (L2::distance(&query, v), *id))
            .collect();
        dists.sort_by(|a, b| a.0.total_cmp(&b.0));
        let gt: HashSet<u64> = dists.iter().take(k).map(|&(_, id)| id).collect();

        let results = idx.search(&query, k).unwrap();
        let found: HashSet<u64> = results.iter().map(|(key, _)| *key).collect();
        total_recall += gt.intersection(&found).count() as f64 / k as f64;
    }

    let avg_recall = total_recall / queries as f64;
    assert!(
        avg_recall >= 0.8,
        "average recall@{k} = {avg_recall:.2}, expected >= 0.8"
    );
}

// ---- T-8: search_ef and search_ef_with_filter ----

#[test]
fn search_ef_variants() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);

    for i in 0..20u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }

    let results = idx.search_ef(&[5.0, 0.0], 3, 100).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, 5);

    let results = idx
        .search_ef_with_filter(&[0.0, 0.0], 3, 100, |k: &u32| k.is_multiple_of(2))
        .unwrap();
    assert_eq!(results.len(), 3);
    for (k, _) in &results {
        assert_eq!(k % 2, 0);
    }
}

// ---- T-9: Cosine distance with zero vector ----

#[test]
fn cosine_zero_vector() {
    let cfg = HnswConfig {
        dim: 3,
        ..Default::default()
    };
    let mut idx: VecDex<String, Cosine> = VecDex::new(cfg);

    idx.insert(&"zero".into(), &[0.0, 0.0, 0.0]).unwrap();
    idx.insert(&"one".into(), &[1.0, 0.0, 0.0]).unwrap();

    // Searching with a zero vector should not panic.
    let results = idx.search(&[0.0, 0.0, 0.0], 2).unwrap();
    assert_eq!(results.len(), 2);
}

// ---- T-10: Compact empty index ----

#[test]
fn compact_empty_noop() {
    let cfg = HnswConfig {
        dim: 3,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    idx.compact().unwrap();
    assert_eq!(idx.len(), 0);
}

// ---- T-11: Minimum m=2 ----

#[test]
fn minimum_m_config() {
    let cfg = HnswConfig {
        dim: 2,
        m: 2,
        m_max0: 4,
        ef_construction: 50,
        ef_search: 50,
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);

    for i in 0..10u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }
    assert_eq!(idx.len(), 10);

    let results = idx.search(&[5.0, 0.0], 3).unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
#[should_panic(expected = "m must be >= 2")]
fn m_one_panics() {
    let cfg = HnswConfig {
        dim: 2,
        m: 1,
        m_max0: 2,
        ef_construction: 50,
        ef_search: 50,
    };
    let _: VecDex<u32, L2> = VecDex::new(cfg);
}

// ---- T-12: Serde roundtrip ----

#[test]
fn serde_roundtrip() {
    let cfg = HnswConfig {
        dim: 3,
        ..Default::default()
    };
    let mut idx: VecDex<String, L2> = VecDex::new(cfg);
    idx.insert(&"a".into(), &[1.0, 2.0, 3.0]).unwrap();
    idx.insert(&"b".into(), &[4.0, 5.0, 6.0]).unwrap();

    let bytes = postcard::to_allocvec(&idx).unwrap();
    let restored: VecDex<String, L2> = postcard::from_bytes(&bytes).unwrap();

    assert_eq!(restored.len(), 2);
    let results = restored.search(&[1.0, 2.0, 3.0], 1).unwrap();
    assert_eq!(results[0].0, "a");
}

// ---- New API tests ----

#[test]
fn get_and_contains_key() {
    let cfg = HnswConfig {
        dim: 3,
        ..Default::default()
    };
    let mut idx: VecDex<String, L2> = VecDex::new(cfg);
    idx.insert(&"a".into(), &[1.0, 2.0, 3.0]).unwrap();

    assert!(idx.contains_key(&"a".into()));
    assert!(!idx.contains_key(&"b".into()));

    let v = idx.get(&"a".into()).unwrap();
    assert_eq!(v, vec![1.0, 2.0, 3.0]);
    assert!(idx.get(&"b".into()).is_none());
}

#[test]
fn keys_and_iter() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    idx.insert(&1, &[1.0, 0.0]).unwrap();
    idx.insert(&2, &[0.0, 1.0]).unwrap();

    let mut keys: Vec<u32> = idx.keys().collect();
    keys.sort();
    assert_eq!(keys, vec![1, 2]);

    let mut pairs: Vec<(u32, Vec<f32>)> = idx.iter().collect();
    pairs.sort_by_key(|(k, _)| *k);
    assert_eq!(pairs.len(), 2);
    assert_eq!(pairs[0].1, vec![1.0, 0.0]);
}

#[test]
fn set_ef_search_works() {
    let cfg = HnswConfig {
        dim: 2,
        ef_search: 50,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    idx.insert(&1, &[0.0, 0.0]).unwrap();
    idx.set_ef_search(200);

    let results = idx.search(&[0.0, 0.0], 1).unwrap();
    assert_eq!(results.len(), 1);
}
// ---- Restore consistency tests (atomic single-handle model) ----

#[test]
fn restore_after_save_meta() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    for i in 0..5u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }

    let id = idx.save_meta().unwrap();
    let restored: VecDex<u32, L2> = VecDex::from_meta(id).unwrap();
    assert_eq!(restored.len(), 5);
}

#[test]
fn restore_without_explicit_save_is_consistent() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    for i in 0..7u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }

    // No clean-shutdown protocol exists: persisting the (constant)
    // metadata at any point yields a fully consistent restore, because
    // every mutation was committed atomically.
    let id = idx.instance_id();
    crate::common::save_instance_meta(id, &idx).unwrap();

    let restored: VecDex<u32, L2> = VecDex::from_meta(id).unwrap();
    assert_eq!(restored.len(), 7);
    assert_eq!(restored.state.next_node_id, idx.state.next_node_id);
    assert_eq!(restored.state.entry_point, idx.state.entry_point);
    let results = restored.search(&[3.0, 0.0], 3).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, 3);
}

#[test]
fn hdr_meta_is_create_time_constant() {
    let cfg = HnswConfig {
        dim: 4,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    let at_creation = postcard::to_allocvec(&idx).unwrap();

    for i in 0..64u32 {
        let v: Vec<f32> = (0..4).map(|d| (i * 4 + d) as f32).collect();
        idx.insert(&i, &v).unwrap();
    }
    assert_eq!(at_creation, postcard::to_allocvec(&idx).unwrap());

    for i in 0..32u32 {
        idx.remove(&i).unwrap();
    }
    assert_eq!(at_creation, postcard::to_allocvec(&idx).unwrap());

    idx.clear();
    assert_eq!(at_creation, postcard::to_allocvec(&idx).unwrap());
}

#[test]
fn serde_roundtrip_preserves_graph_state() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    for i in 0..12u32 {
        idx.insert(&i, &[i as f32, 1.0]).unwrap();
    }
    idx.remove(&3).unwrap();

    let bytes = postcard::to_allocvec(&idx).unwrap();
    let restored: VecDex<u32, L2> = postcard::from_bytes(&bytes).unwrap();

    assert_eq!(restored.len(), 11);
    assert_eq!(restored.state.entry_point, idx.state.entry_point);
    assert_eq!(restored.state.max_layer, idx.state.max_layer);
    assert_eq!(restored.state.next_node_id, idx.state.next_node_id);
    assert_bidirectional(&restored);
    let results = restored.search(&[7.0, 1.0], 2).unwrap();
    assert_eq!(results[0].0, 7);
}

#[test]
fn node_ids_are_never_reused_across_restores() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    for i in 0..6u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }
    let next_before = idx.state.next_node_id;
    idx.remove(&5).unwrap();

    let bytes = postcard::to_allocvec(&idx).unwrap();
    let mut restored: VecDex<u32, L2> = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(restored.state.next_node_id, next_before);

    restored.insert(&100, &[100.0, 0.0]).unwrap();
    assert!(restored.state.next_node_id > next_before);
}

// =========================================================================
// VecDexDyn — runtime metric selection
// =========================================================================

#[test]
fn dyn_metric_semantics_match_static() {
    let cfg = || HnswConfig {
        dim: 3,
        ..Default::default()
    };

    // L2: nearest by squared euclidean distance.
    let mut idx = VecDexDyn::<String>::new(MetricKind::L2, cfg());
    assert_eq!(idx.metric(), MetricKind::L2);
    idx.insert(&"near".into(), &[1.0, 0.0, 0.0]).unwrap();
    idx.insert(&"far".into(), &[3.0, 0.0, 0.0]).unwrap();
    let r = idx.search(&[0.0, 0.0, 0.0], 2).unwrap();
    assert_eq!(r[0].0, "near");
    assert_eq!(r[0].1, 1.0);
    assert_eq!(r[1].1, 9.0);

    // Cosine: alignment beats magnitude.
    let mut idx = VecDexDyn::<String>::new(MetricKind::Cosine, cfg());
    assert_eq!(idx.metric(), MetricKind::Cosine);
    idx.insert(&"aligned".into(), &[5.0, 0.0, 0.0]).unwrap();
    idx.insert(&"orthogonal".into(), &[0.0, 1.0, 0.0]).unwrap();
    let r = idx.search(&[1.0, 0.0, 0.0], 2).unwrap();
    assert_eq!(r[0].0, "aligned");
    assert!(r[0].1.abs() < 1e-6);

    // InnerProduct: larger dot product ranks first.
    let mut idx = VecDexDyn::<String>::new(MetricKind::InnerProduct, cfg());
    assert_eq!(idx.metric(), MetricKind::InnerProduct);
    idx.insert(&"big".into(), &[5.0, 0.0, 0.0]).unwrap();
    idx.insert(&"small".into(), &[1.0, 0.0, 0.0]).unwrap();
    let r = idx.search(&[1.0, 0.0, 0.0], 2).unwrap();
    assert_eq!(r[0].0, "big");
    assert_eq!(r[0].1, -5.0);
}

#[test]
fn dyn_full_api_delegation() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDexDyn<u32> = VecDexDyn::new(MetricKind::L2, cfg);
    assert!(idx.is_empty());

    idx.insert_batch(&[
        (1, vec![1.0, 0.0]),
        (2, vec![2.0, 0.0]),
        (3, vec![3.0, 0.0]),
    ])
    .unwrap();
    assert_eq!(idx.len(), 3);
    assert!(idx.contains_key(&2));
    assert_eq!(idx.get(&2).unwrap(), vec![2.0, 0.0]);
    assert_eq!(idx.keys().collect::<HashSet<_>>(), HashSet::from([1, 2, 3]));
    assert_eq!(idx.iter().count(), 3);

    // Dimension mismatch surfaces through the dispatch layer.
    assert!(idx.insert(&9, &[1.0]).is_err());
    assert!(idx.search(&[1.0], 1).is_err());

    idx.set_ef_search(64);
    assert_eq!(idx.search_ef(&[1.0, 0.0], 1, 32).unwrap()[0].0, 1);
    let filtered = idx.search_with_filter(&[1.0, 0.0], 3, |k| *k != 1).unwrap();
    assert!(filtered.iter().all(|(k, _)| *k != 1));
    let filtered = idx
        .search_ef_with_filter(&[1.0, 0.0], 3, 32, |k| *k == 3)
        .unwrap();
    assert_eq!(filtered[0].0, 3);

    assert!(idx.remove(&3).unwrap());
    assert!(!idx.remove(&3).unwrap());
    assert_eq!(idx.len(), 2);

    idx.compact().unwrap();
    assert_eq!(idx.search(&[1.0, 0.0], 1).unwrap()[0].0, 1);

    idx.clear();
    assert!(idx.is_empty());
    idx.insert(&7, &[7.0, 0.0]).unwrap();
    assert_eq!(idx.len(), 1);
}

#[test]
fn dyn_save_meta_restores_metric() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDexDyn<u32> = VecDexDyn::new(MetricKind::InnerProduct, cfg);
    idx.insert(&1, &[1.0, 2.0]).unwrap();
    idx.insert(&2, &[3.0, 4.0]).unwrap();
    let id = idx.save_meta().unwrap();
    drop(idx);

    // The metric survives the round-trip without being re-stated.
    let restored: VecDexDyn<u32> = VecDexDyn::from_meta(id).unwrap();
    assert_eq!(restored.metric(), MetricKind::InnerProduct);
    assert_eq!(restored.len(), 2);
    assert_eq!(restored.search(&[1.0, 1.0], 1).unwrap()[0].0, 2);

    // The formats are deliberately distinct: dyn metas do not load as
    // static handles (any metric), nor under another key type.
    assert!(VecDex::<u32, InnerProduct>::from_meta(id).is_err());
    assert!(VecDex::<u32, L2>::from_meta(id).is_err());
    assert!(VecDexDyn::<String>::from_meta(id).is_err());
}

#[test]
fn dyn_rejects_static_meta() {
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    idx.insert(&1, &[1.0, 2.0]).unwrap();
    let id = idx.save_meta().unwrap();

    // A static VecDex meta must not silently load as VecDexDyn.
    assert!(VecDexDyn::<u32>::from_meta(id).is_err());
}

#[test]
fn dyn_wire_tags_are_frozen() {
    // The first meta byte is the metric's frozen wire tag: the on-disk
    // discriminant is pinned by explicit constants, never by enum
    // source order. Reordering variants must fail THIS test, not
    // silently re-interpret persisted metas.
    let cfg = || HnswConfig {
        dim: 2,
        ..Default::default()
    };
    for (metric, tag) in [
        (MetricKind::L2, 0u8),
        (MetricKind::Cosine, 1),
        (MetricKind::InnerProduct, 2),
    ] {
        let idx = VecDexDyn::<u32>::new(metric, cfg());
        let bytes = postcard::to_allocvec(&idx).unwrap();
        assert_eq!(bytes[0], tag, "wire tag drifted for {metric:?}");
        let restored: VecDexDyn<u32> = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(restored.metric(), metric);
    }

    // An out-of-range tag is refused outright (e.g. a meta written by
    // a newer version with more metrics) instead of being
    // mis-decoded as some existing variant's payload.
    assert!(postcard::from_bytes::<VecDexDyn<u32>>(&[9]).is_err());
}

#[test]
fn dyn_f64_end_to_end() {
    let cfg = HnswConfig {
        dim: 3,
        ..Default::default()
    };
    // Cosine on f64 exercises the norm computations through the
    // dispatch layer with the non-default scalar.
    let mut idx: VecDexDyn<String, f64> = VecDexDyn::new(MetricKind::Cosine, cfg);
    idx.insert(&"aligned".into(), &[10.0_f64, 0.0, 0.0])
        .unwrap();
    idx.insert(&"orthogonal".into(), &[0.0_f64, 3.0, 0.0])
        .unwrap();
    idx.insert(&"diagonal".into(), &[1.0_f64, 1.0, 0.0])
        .unwrap();

    let r = idx.search(&[1.0_f64, 0.0, 0.0], 3).unwrap();
    assert_eq!(r.len(), 3);
    assert_eq!(r[0].0, "aligned");
    assert!(r[0].1.abs() < 1e-12);

    let id = idx.save_meta().unwrap();
    drop(idx);

    let restored: VecDexDyn<String, f64> = VecDexDyn::from_meta(id).unwrap();
    assert_eq!(restored.metric(), MetricKind::Cosine);
    assert_eq!(restored.len(), 3);
    assert_eq!(
        restored.get(&"diagonal".into()).unwrap(),
        vec![1.0, 1.0, 0.0]
    );
    assert_eq!(
        restored.search(&[0.0_f64, 1.0, 0.0], 1).unwrap()[0].0,
        "orthogonal"
    );

    // The scalar width is part of the typed-handle tag: an f64 dyn
    // meta must not load under f32, nor as any static handle.
    assert!(VecDexDyn::<String>::from_meta(id).is_err());
    assert!(VecDex::<String, Cosine, f64>::from_meta(id).is_err());
}
