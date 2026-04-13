use super::*;
use distance::{Cosine, InnerProduct, L2};

fn setup() {
    let dir = format!("/tmp/vsdb_vecdex_test/{}", rand::random::<u128>());
    let _ = vsdb_core::vsdb_set_base_dir(&dir);
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
        .search_with_filter(&[0.0, 0.0], 3, |k: &u32| k % 2 == 0)
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
