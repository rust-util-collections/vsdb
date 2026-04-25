use super::*;
use distance::{Cosine, InnerProduct, L2};

fn setup() {
    let dir = format!("/tmp/vsdb_vecdex_test/{}", rand::random::<u128>());
    let _ = vsdb_core::vsdb_set_base_dir(&dir);
}

fn assert_bidirectional<K, D, S>(idx: &VecDex<K, D, S>)
where
    K: KeyEnDe + ValueEnDe + Clone + Eq,
    D: DistanceMetric<S>,
    S: Scalar,
{
    for (node, info) in idx.node_info.iter() {
        for layer in 0..=info.max_layer {
            for neighbor in hnsw::get_neighbors(&idx.adjacency, layer, node) {
                let reverse = hnsw::get_neighbors(&idx.adjacency, layer, neighbor);
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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

#[test]
fn clear_resets_everything() {
    setup();
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
fn recall_random_vectors() {
    setup();
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
        let gt: std::collections::HashSet<u64> =
            dists.iter().take(k).map(|&(_, id)| id).collect();

        // HNSW result.
        let results = idx.search(&query, k).unwrap();
        let found: std::collections::HashSet<u64> =
            results.iter().map(|(key, _)| *key).collect();

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
    setup();
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
    setup();
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
fn filtered_search_respects_k() {
    setup();
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
    setup();
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
    setup();
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);

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
}

#[test]
fn insert_batch_works() {
    setup();
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
    setup();
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
    setup();
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
    let meta_before = idx.meta.get_value().clone();
    let ep = meta_before.entry_point.unwrap();
    idx.remove(&(ep as u32)).unwrap();

    // After removal, max_layer should still reflect the true global max.
    let meta_after = idx.meta.get_value().clone();
    let actual_max = idx
        .node_info
        .iter()
        .map(|(_, info)| info.max_layer)
        .max()
        .unwrap_or(0);
    assert_eq!(
        meta_after.max_layer, actual_max,
        "max_layer should equal the true global maximum layer"
    );

    // Search should still work — all remaining vectors reachable.
    let results = idx.search(&[25.0, 0.0], 5).unwrap();
    assert_eq!(results.len(), 5);
}

// ---- T-1: Single-node duplicate-key update (regression for stale-metadata fix) ----

#[test]
fn single_node_duplicate_key_update() {
    setup();
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
    setup();
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);

    for i in 0..10u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }

    let ep = idx.meta.get_value().entry_point.unwrap();
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
    setup();
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
        let meta = idx.meta.get_value().clone();
        if meta.entry_point.is_none() {
            break;
        }
        let ep = meta.entry_point.unwrap();
        idx.remove(&(ep as u32)).unwrap();

        let after = idx.meta.get_value().clone();
        let actual_max = idx
            .node_info
            .iter()
            .map(|(_, info)| info.max_layer)
            .max()
            .unwrap_or(0);
        assert_eq!(after.max_layer, actual_max);
    }
}

// ---- T-4: Remove all then re-insert ----

#[test]
fn remove_all_then_reinsert() {
    setup();
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
    setup();
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
    let ep = idx.meta.get_value().entry_point.unwrap();
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
    let ep = idx.meta.get_value().entry_point.unwrap();
    let mut visited = std::collections::HashSet::new();
    let mut queue = std::collections::VecDeque::new();
    queue.push_back(ep);
    visited.insert(ep);
    while let Some(node) = queue.pop_front() {
        let neighbors = hnsw::get_neighbors(&idx.adjacency, 0, node);
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
    setup();
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
            let gt: std::collections::HashSet<u32> =
                dists.iter().take(k).map(|&(_, id)| id).collect();

            let results = index.search(&query, k).unwrap();
            let found: std::collections::HashSet<u32> =
                results.iter().map(|(key, _)| *key).collect();
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
    setup();
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
        let gt: std::collections::HashSet<u64> =
            dists.iter().take(k).map(|&(_, id)| id).collect();

        let results = idx.search(&query, k).unwrap();
        let found: std::collections::HashSet<u64> =
            results.iter().map(|(key, _)| *key).collect();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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
    setup();
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

// ---- Dirty-flag crash recovery tests ----

#[test]
fn clean_shutdown_skips_rebuild() {
    setup();
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    for i in 0..5u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }

    // Clean shutdown: save_meta clears the dirty bit.
    let id = idx.save_meta().unwrap();
    let raw = idx.meta.get_value().node_count;
    assert!(
        !crate::common::dirty_count::is_dirty(raw),
        "dirty bit should be cleared after save_meta"
    );

    // Restore — count should be correct without rebuild.
    let restored: VecDex<u32, L2> = VecDex::from_meta(id).unwrap();
    assert_eq!(restored.len(), 5);
}

#[test]
fn crash_recovery_rebuilds_count() {
    setup();
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    for i in 0..7u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }

    // Simulate crash: persist without calling save_meta (dirty bit stays set).
    let id = idx.instance_id();
    crate::common::save_instance_meta(id, &idx).unwrap();
    let raw = idx.meta.get_value().node_count;
    assert!(
        crate::common::dirty_count::is_dirty(raw),
        "dirty bit should be set during operation"
    );

    // Restore — ensure_count should detect dirty and rebuild.
    let restored: VecDex<u32, L2> = VecDex::from_meta(id).unwrap();
    assert_eq!(restored.len(), 7);
}

#[test]
fn serde_roundtrip_triggers_ensure_count() {
    setup();
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    idx.insert(&1, &[1.0, 0.0]).unwrap();
    idx.insert(&2, &[2.0, 0.0]).unwrap();

    // Serialize while dirty (no save_meta).
    let bytes = postcard::to_allocvec(&idx).unwrap();

    // Deserialize — should trigger ensure_count via hand-written Deserialize.
    let restored: VecDex<u32, L2> = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(restored.len(), 2);
}

#[test]
fn crash_with_corrupted_count_is_corrected() {
    setup();
    let cfg = HnswConfig {
        dim: 2,
        ..Default::default()
    };
    let mut idx: VecDex<u32, L2> = VecDex::new(cfg);
    for i in 0..10u32 {
        idx.insert(&i, &[i as f32, 0.0]).unwrap();
    }

    // Corrupt count: set dirty + wrong count.
    idx.meta.get_mut().node_count = crate::common::dirty_count::set_dirty(999);

    // Persist the corrupted state.
    let id = idx.instance_id();
    crate::common::save_instance_meta(id, &idx).unwrap();

    // Restore — should rebuild to actual count of 10.
    let restored: VecDex<u32, L2> = VecDex::from_meta(id).unwrap();
    assert_eq!(restored.len(), 10);
}
