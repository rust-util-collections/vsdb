use super::*;

// Ensure a fresh, isolated DB dir for each test.
fn setup() {
    let dir = format!("/tmp/vsdb_btree_test/{}", rand::random::<u128>());
    let _ = crate::vsdb_set_base_dir(&dir);
}

// =====================================================================
// Basic operations
// =====================================================================

#[test]
fn empty_tree() {
    setup();
    let tree = PersistentBTree::new();
    assert!(tree.get(EMPTY_ROOT, b"x").is_none());
    assert!(!tree.contains_key(EMPTY_ROOT, b"x"));
    assert_eq!(tree.iter(EMPTY_ROOT).count(), 0);
}

#[test]
fn insert_and_get() {
    setup();
    let mut tree = PersistentBTree::new();
    let r = tree.insert(EMPTY_ROOT, b"key1", b"val1");
    assert_eq!(tree.get(r, b"key1").unwrap(), b"val1");
    assert!(tree.get(r, b"key2").is_none());
}

#[test]
fn insert_overwrite() {
    setup();
    let mut tree = PersistentBTree::new();
    let r1 = tree.insert(EMPTY_ROOT, b"k", b"v1");
    let r2 = tree.insert(r1, b"k", b"v2");
    assert_eq!(tree.get(r1, b"k").unwrap(), b"v1");
    assert_eq!(tree.get(r2, b"k").unwrap(), b"v2");
}

#[test]
fn insert_multiple_keys() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    r = tree.insert(r, b"charlie", b"3");
    r = tree.insert(r, b"alice", b"1");
    r = tree.insert(r, b"bob", b"2");

    assert_eq!(tree.get(r, b"alice").unwrap(), b"1");
    assert_eq!(tree.get(r, b"bob").unwrap(), b"2");
    assert_eq!(tree.get(r, b"charlie").unwrap(), b"3");
    assert_eq!(tree.iter(r).count(), 3);
}

#[test]
fn insert_overwrite_preserves_other_keys() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    r = tree.insert(r, b"a", b"1");
    r = tree.insert(r, b"b", b"2");
    r = tree.insert(r, b"c", b"3");

    // Overwrite middle key.
    let r2 = tree.insert(r, b"b", b"99");
    assert_eq!(tree.get(r2, b"a").unwrap(), b"1");
    assert_eq!(tree.get(r2, b"b").unwrap(), b"99");
    assert_eq!(tree.get(r2, b"c").unwrap(), b"3");
    assert_eq!(tree.iter(r2).count(), 3);

    // Original untouched.
    assert_eq!(tree.get(r, b"b").unwrap(), b"2");
}

#[test]
fn insert_same_key_many_times() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    let mut versions = Vec::new();
    for i in 0u32..100 {
        r = tree.insert(r, b"key", &i.to_be_bytes());
        versions.push(r);
    }
    // Each version has exactly one key with the correct value.
    for (i, &v) in versions.iter().enumerate() {
        assert_eq!(tree.get(v, b"key").unwrap(), (i as u32).to_be_bytes());
        assert_eq!(tree.iter(v).count(), 1);
    }
}

#[test]
fn remove_basic() {
    setup();
    let mut tree = PersistentBTree::new();
    let r = tree.insert(EMPTY_ROOT, b"a", b"1");
    let r = tree.insert(r, b"b", b"2");

    let r2 = tree.remove(r, b"a");
    assert!(tree.get(r2, b"a").is_none());
    assert_eq!(tree.get(r2, b"b").unwrap(), b"2");
    // Old version untouched.
    assert_eq!(tree.get(r, b"a").unwrap(), b"1");
}

#[test]
fn remove_nonexistent_key() {
    setup();
    let mut tree = PersistentBTree::new();
    let r = tree.insert(EMPTY_ROOT, b"a", b"1");
    let r2 = tree.remove(r, b"zzz");
    assert_eq!(r, r2); // no change
}

#[test]
fn remove_from_empty_tree() {
    setup();
    let mut tree = PersistentBTree::new();
    let r = tree.remove(EMPTY_ROOT, b"anything");
    assert_eq!(r, EMPTY_ROOT);
}

#[test]
fn remove_until_empty() {
    setup();
    let mut tree = PersistentBTree::new();
    let r = tree.insert(EMPTY_ROOT, b"a", b"1");
    let r = tree.remove(r, b"a");
    assert_eq!(r, EMPTY_ROOT);
}

#[test]
fn remove_multiple_until_empty() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    r = tree.insert(r, b"a", b"1");
    r = tree.insert(r, b"b", b"2");
    r = tree.insert(r, b"c", b"3");

    r = tree.remove(r, b"b");
    assert_eq!(tree.iter(r).count(), 2);
    r = tree.remove(r, b"a");
    assert_eq!(tree.iter(r).count(), 1);
    r = tree.remove(r, b"c");
    assert_eq!(r, EMPTY_ROOT);
}

#[test]
fn remove_first_key() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    for i in 0u32..100 {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }
    // Remove the smallest key.
    r = tree.remove(r, &0u32.to_be_bytes());
    assert!(!tree.contains_key(r, &0u32.to_be_bytes()));
    assert!(tree.contains_key(r, &1u32.to_be_bytes()));
    assert_eq!(tree.iter(r).count(), 99);
}

#[test]
fn remove_last_key() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    for i in 0u32..100 {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }
    // Remove the largest key.
    r = tree.remove(r, &99u32.to_be_bytes());
    assert!(!tree.contains_key(r, &99u32.to_be_bytes()));
    assert!(tree.contains_key(r, &98u32.to_be_bytes()));
    assert_eq!(tree.iter(r).count(), 99);
}

#[test]
fn remove_in_reverse_order() {
    setup();
    let mut tree = PersistentBTree::new();
    let n = 500u32;
    let mut r = EMPTY_ROOT;
    for i in 0..n {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }
    // Remove from largest to smallest — exercises different rebalancing paths.
    for i in (0..n).rev() {
        r = tree.remove(r, &i.to_be_bytes());
    }
    assert_eq!(r, EMPTY_ROOT);
}

#[test]
fn remove_in_random_order() {
    setup();
    let mut tree = PersistentBTree::new();
    let n = 300u32;
    let mut r = EMPTY_ROOT;
    for i in 0..n {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }

    // Pseudo-random removal order using a simple LCG.
    let mut order: Vec<u32> = (0..n).collect();
    let mut seed: u64 = 42;
    for i in (1..order.len()).rev() {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let j = (seed >> 33) as usize % (i + 1);
        order.swap(i, j);
    }

    for key in &order {
        r = tree.remove(r, &key.to_be_bytes());
    }
    assert_eq!(r, EMPTY_ROOT);
}

#[test]
fn interleaved_insert_remove() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;

    // Insert 0..100
    for i in 0u32..100 {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }
    // Remove 0..50, insert 100..150
    for i in 0u32..50 {
        r = tree.remove(r, &i.to_be_bytes());
        r = tree.insert(r, &(i + 100).to_be_bytes(), &(i + 100).to_be_bytes());
    }

    assert_eq!(tree.iter(r).count(), 100);
    for i in 0u32..50 {
        assert!(!tree.contains_key(r, &i.to_be_bytes()));
    }
    for i in 50u32..150 {
        assert!(tree.contains_key(r, &i.to_be_bytes()));
    }
}

// =====================================================================
// Structural sharing (version isolation)
// =====================================================================

#[test]
fn fork_versions() {
    setup();
    let mut tree = PersistentBTree::new();
    let base = tree.insert(EMPTY_ROOT, b"x", b"0");

    let v1 = tree.insert(base, b"x", b"1");
    let v2 = tree.insert(base, b"x", b"2");

    assert_eq!(tree.get(base, b"x").unwrap(), b"0");
    assert_eq!(tree.get(v1, b"x").unwrap(), b"1");
    assert_eq!(tree.get(v2, b"x").unwrap(), b"2");
}

#[test]
fn fork_many_versions() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut base = EMPTY_ROOT;
    for i in 0u32..50 {
        base = tree.insert(base, &i.to_be_bytes(), &i.to_be_bytes());
    }

    // Fork 10 versions from the same base.
    let mut forks = Vec::new();
    for f in 0u32..10 {
        let v = tree.insert(base, &0u32.to_be_bytes(), &(f * 1000).to_be_bytes());
        forks.push(v);
    }

    // Base is untouched.
    assert_eq!(
        tree.get(base, &0u32.to_be_bytes()).unwrap(),
        0u32.to_be_bytes()
    );
    // Each fork has its own value for key 0.
    for (f, &v) in forks.iter().enumerate() {
        assert_eq!(
            tree.get(v, &0u32.to_be_bytes()).unwrap(),
            ((f as u32) * 1000).to_be_bytes()
        );
        // Other keys untouched.
        assert_eq!(
            tree.get(v, &49u32.to_be_bytes()).unwrap(),
            49u32.to_be_bytes()
        );
    }
}

#[test]
fn deep_version_chain() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut versions = vec![EMPTY_ROOT];

    // Build a chain of 100 versions, each adding one key.
    for i in 0u32..100 {
        let prev = *versions.last().unwrap();
        let next = tree.insert(prev, &i.to_be_bytes(), &i.to_be_bytes());
        versions.push(next);
    }

    // Version k should have exactly k keys (0..k-1).
    for k in 0..=100usize {
        assert_eq!(tree.iter(versions[k]).count(), k);
    }
}

// =====================================================================
// Large keys and values
// =====================================================================

#[test]
fn large_keys_and_values() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;

    // Insert entries with large keys (256 bytes) and large values (1KB).
    for i in 0u32..50 {
        let mut key = vec![0u8; 256];
        key[..4].copy_from_slice(&i.to_be_bytes());
        let value = vec![i as u8; 1024];
        r = tree.insert(r, &key, &value);
    }

    assert_eq!(tree.iter(r).count(), 50);

    for i in 0u32..50 {
        let mut key = vec![0u8; 256];
        key[..4].copy_from_slice(&i.to_be_bytes());
        let val = tree.get(r, &key).unwrap();
        assert_eq!(val.len(), 1024);
        assert!(val.iter().all(|&b| b == i as u8));
    }
}

#[test]
fn empty_key_and_value() {
    setup();
    let mut tree = PersistentBTree::new();
    let r = tree.insert(EMPTY_ROOT, b"", b"");
    assert_eq!(tree.get(r, b"").unwrap(), b"");
    assert_eq!(tree.iter(r).count(), 1);
}

#[test]
fn single_byte_keys_full_range() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    // Insert all 256 possible single-byte keys.
    for b in 0u8..=255 {
        r = tree.insert(r, &[b], &[b]);
    }
    assert_eq!(tree.iter(r).count(), 256);
    // Should be sorted by byte value.
    let items: Vec<_> = tree.iter(r).collect();
    for i in 0..256usize {
        assert_eq!(items[i].0, vec![i as u8]);
    }
}

// =====================================================================
// Iteration
// =====================================================================

#[test]
fn iter_order() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    // Insert in reverse order to exercise sorting.
    for i in (0u32..100).rev() {
        r = tree.insert(r, &i.to_be_bytes(), &(i * 10).to_be_bytes());
    }
    let items: Vec<_> = tree.iter(r).collect();
    assert_eq!(items.len(), 100);
    for (idx, (k, v)) in items.iter().enumerate() {
        let expected_key = (idx as u32).to_be_bytes();
        let expected_val = (idx as u32 * 10).to_be_bytes();
        assert_eq!(k.as_slice(), &expected_key);
        assert_eq!(v.as_slice(), &expected_val);
    }
}

#[test]
fn iter_single_element() {
    setup();
    let mut tree = PersistentBTree::new();
    let r = tree.insert(EMPTY_ROOT, b"only", b"one");
    let items: Vec<_> = tree.iter(r).collect();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0], (b"only".to_vec(), b"one".to_vec()));
}

#[test]
fn iter_after_removes() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    for i in 0u32..10 {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }
    // Remove even keys.
    for i in (0u32..10).filter(|i| i % 2 == 0) {
        r = tree.remove(r, &i.to_be_bytes());
    }
    let items: Vec<_> = tree.iter(r).collect();
    assert_eq!(items.len(), 5);
    for (idx, (k, _)) in items.iter().enumerate() {
        let expected = (idx as u32 * 2 + 1).to_be_bytes();
        assert_eq!(k.as_slice(), &expected);
    }
}

// =====================================================================
// Range iteration — edge cases
// =====================================================================

#[test]
fn range_iteration() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    for i in 0u32..50 {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }

    let lo = 10u32.to_be_bytes();
    let hi = 20u32.to_be_bytes();
    let items: Vec<_> = tree
        .range(r, Bound::Included(&lo), Bound::Excluded(&hi))
        .collect();
    assert_eq!(items.len(), 10);
    assert_eq!(items[0].0, 10u32.to_be_bytes());
    assert_eq!(items[9].0, 19u32.to_be_bytes());
}

#[test]
fn range_included_both() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    for i in 0u32..50 {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }
    let lo = 10u32.to_be_bytes();
    let hi = 20u32.to_be_bytes();
    let items: Vec<_> = tree
        .range(r, Bound::Included(&lo), Bound::Included(&hi))
        .collect();
    assert_eq!(items.len(), 11); // 10..=20
}

#[test]
fn range_excluded_both() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    for i in 0u32..50 {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }
    let lo = 10u32.to_be_bytes();
    let hi = 20u32.to_be_bytes();
    let items: Vec<_> = tree
        .range(r, Bound::Excluded(&lo), Bound::Excluded(&hi))
        .collect();
    assert_eq!(items.len(), 9); // 11..20
}

#[test]
fn range_unbounded_lo() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    for i in 0u32..50 {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }
    let hi = 5u32.to_be_bytes();
    let items: Vec<_> = tree
        .range(r, Bound::Unbounded, Bound::Excluded(&hi))
        .collect();
    assert_eq!(items.len(), 5); // 0..5
}

#[test]
fn range_unbounded_hi() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    for i in 0u32..50 {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }
    let lo = 45u32.to_be_bytes();
    let items: Vec<_> = tree
        .range(r, Bound::Included(&lo), Bound::Unbounded)
        .collect();
    assert_eq!(items.len(), 5); // 45..50
}

#[test]
fn range_empty_result() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    for i in 0u32..10 {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }
    // Range beyond all keys.
    let lo = 100u32.to_be_bytes();
    let hi = 200u32.to_be_bytes();
    let items: Vec<_> = tree
        .range(r, Bound::Included(&lo), Bound::Included(&hi))
        .collect();
    assert_eq!(items.len(), 0);
}

#[test]
fn range_single_key_match() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    for i in 0u32..50 {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }
    let k = 25u32.to_be_bytes();
    let items: Vec<_> = tree
        .range(r, Bound::Included(&k), Bound::Included(&k))
        .collect();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].0, k.to_vec());
}

#[test]
fn range_on_empty_tree() {
    setup();
    let tree = PersistentBTree::new();
    let items: Vec<_> = tree
        .range(EMPTY_ROOT, Bound::Unbounded, Bound::Unbounded)
        .collect();
    assert_eq!(items.len(), 0);
}

#[test]
fn range_full_unbounded() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    for i in 0u32..100 {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }
    let items: Vec<_> = tree.range(r, Bound::Unbounded, Bound::Unbounded).collect();
    assert_eq!(items.len(), 100);
}

// =====================================================================
// Split triggers — exact boundary testing
// =====================================================================

#[test]
fn insert_triggers_leaf_split() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    // Insert exactly MAX_KEYS + 1 to trigger a split.
    for i in 0u32..=(MAX_KEYS as u32) {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }
    // All keys must still be present and correctly ordered.
    let items: Vec<_> = tree.iter(r).collect();
    assert_eq!(items.len(), MAX_KEYS + 1);
    for (idx, (k, _)) in items.iter().enumerate() {
        assert_eq!(k.as_slice(), &(idx as u32).to_be_bytes());
    }
}

#[test]
fn insert_triggers_multi_level_splits() {
    setup();
    let mut tree = PersistentBTree::new();
    let mut r = EMPTY_ROOT;
    // Enough entries to force at least 3 levels.
    let n = (MAX_KEYS * MAX_KEYS + 1) as u32;
    for i in 0..n {
        r = tree.insert(r, &i.to_be_bytes(), &i.to_be_bytes());
    }
    assert_eq!(tree.iter(r).count(), n as usize);
    // Spot-check first, middle, last.
    assert_eq!(
        tree.get(r, &0u32.to_be_bytes()).unwrap(),
        0u32.to_be_bytes()
    );
    assert_eq!(
        tree.get(r, &(n / 2).to_be_bytes()).unwrap(),
        (n / 2).to_be_bytes()
    );
    assert_eq!(
        tree.get(r, &(n - 1).to_be_bytes()).unwrap(),
        (n - 1).to_be_bytes()
    );
}

// =====================================================================
// Splits & rebalancing (force many entries through the tree)
// =====================================================================

#[test]
fn many_inserts_and_removes() {
    setup();
    let mut tree = PersistentBTree::new();
    let n = 2000u32;
    let mut root = EMPTY_ROOT;

    // Insert 0..n
    for i in 0..n {
        root = tree.insert(root, &i.to_be_bytes(), &i.to_be_bytes());
    }

    // Verify all present.
    for i in 0..n {
        assert!(
            tree.contains_key(root, &i.to_be_bytes()),
            "missing key {i} after insert"
        );
    }
    assert_eq!(tree.iter(root).count(), n as usize);

    // Remove even keys.
    for i in (0..n).filter(|i| i % 2 == 0) {
        root = tree.remove(root, &i.to_be_bytes());
    }

    // Verify.
    for i in 0..n {
        let present = tree.contains_key(root, &i.to_be_bytes());
        if i % 2 == 0 {
            assert!(!present, "key {i} should have been removed");
        } else {
            assert!(present, "key {i} should still be present");
        }
    }
    assert_eq!(tree.iter(root).count(), (n / 2) as usize);

    // Remove odd keys.
    for i in (0..n).filter(|i| i % 2 != 0) {
        root = tree.remove(root, &i.to_be_bytes());
    }
    assert_eq!(root, EMPTY_ROOT);
}

// =====================================================================
// Bulk load
// =====================================================================

#[test]
fn bulk_load_and_query() {
    setup();
    let mut tree = PersistentBTree::new();
    let entries: Vec<_> = (0u32..1000)
        .map(|i| (i.to_be_bytes().to_vec(), (i * 3).to_be_bytes().to_vec()))
        .collect();
    let root = tree.bulk_load(entries.clone());

    for (k, v) in &entries {
        assert_eq!(tree.get(root, k).unwrap(), *v);
    }
    assert_eq!(tree.iter(root).count(), 1000);
}

#[test]
fn bulk_load_empty() {
    setup();
    let mut tree = PersistentBTree::new();
    let root = tree.bulk_load(Vec::<(Vec<u8>, Vec<u8>)>::new());
    assert_eq!(root, EMPTY_ROOT);
}

#[test]
fn bulk_load_single_entry() {
    setup();
    let mut tree = PersistentBTree::new();
    let root = tree.bulk_load(vec![(b"only".to_vec(), b"one".to_vec())]);
    assert_eq!(tree.get(root, b"only").unwrap(), b"one");
    assert_eq!(tree.iter(root).count(), 1);
}

#[test]
fn bulk_load_then_modify() {
    setup();
    let mut tree = PersistentBTree::new();
    let entries: Vec<_> = (0u32..500)
        .map(|i| (i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()))
        .collect();
    let r1 = tree.bulk_load(entries);

    // Insert new keys into the bulk-loaded tree.
    let mut r2 = r1;
    for i in 500u32..600 {
        r2 = tree.insert(r2, &i.to_be_bytes(), &i.to_be_bytes());
    }
    assert_eq!(tree.iter(r2).count(), 600);
    assert_eq!(tree.iter(r1).count(), 500); // Original untouched.

    // Remove keys from the bulk-loaded tree.
    let mut r3 = r1;
    for i in 0u32..100 {
        r3 = tree.remove(r3, &i.to_be_bytes());
    }
    assert_eq!(tree.iter(r3).count(), 400);
    assert_eq!(tree.iter(r1).count(), 500); // Original untouched.
}

#[test]
fn bulk_load_matches_sequential_insert() {
    setup();
    let mut tree = PersistentBTree::new();
    let entries: Vec<_> = (0u32..200)
        .map(|i| (i.to_be_bytes().to_vec(), (i * 7).to_be_bytes().to_vec()))
        .collect();

    // Bulk load.
    let bulk_root = tree.bulk_load(entries.clone());

    // Sequential insert.
    let mut seq_root = EMPTY_ROOT;
    for (k, v) in &entries {
        seq_root = tree.insert(seq_root, k, v);
    }

    // Both should yield identical iteration results.
    let bulk_items: Vec<_> = tree.iter(bulk_root).collect();
    let seq_items: Vec<_> = tree.iter(seq_root).collect();
    assert_eq!(bulk_items, seq_items);
}

#[test]
fn bulk_load_exactly_max_keys() {
    setup();
    let mut tree = PersistentBTree::new();
    let entries: Vec<_> = (0..MAX_KEYS)
        .map(|i| ((i as u32).to_be_bytes().to_vec(), vec![i as u8]))
        .collect();
    let root = tree.bulk_load(entries);
    assert_eq!(tree.iter(root).count(), MAX_KEYS);
}

// =====================================================================
// GC
// =====================================================================

#[test]
fn gc_removes_unreachable() {
    setup();
    let mut tree = PersistentBTree::new();
    let r1 = tree.insert(EMPTY_ROOT, b"a", b"1");
    let _r2 = tree.insert(r1, b"b", b"2");
    let r3 = tree.insert(r1, b"c", b"3");

    // Keep only r3 alive.
    tree.gc(&[r3]);

    // r3's data must still be accessible.
    assert_eq!(tree.get(r3, b"a").unwrap(), b"1");
    assert_eq!(tree.get(r3, b"c").unwrap(), b"3");
}

#[test]
fn gc_multiple_live_roots() {
    setup();
    let mut tree = PersistentBTree::new();
    let base = tree.insert(EMPTY_ROOT, b"shared", b"data");
    let v1 = tree.insert(base, b"v1", b"yes");
    let v2 = tree.insert(base, b"v2", b"yes");
    let _dead = tree.insert(base, b"dead", b"gone");

    // Keep v1 and v2 alive, discard _dead.
    tree.gc(&[v1, v2]);

    assert_eq!(tree.get(v1, b"shared").unwrap(), b"data");
    assert_eq!(tree.get(v1, b"v1").unwrap(), b"yes");
    assert_eq!(tree.get(v2, b"shared").unwrap(), b"data");
    assert_eq!(tree.get(v2, b"v2").unwrap(), b"yes");
}

#[test]
fn gc_with_empty_root_in_live_set() {
    setup();
    let mut tree = PersistentBTree::new();
    let r = tree.insert(EMPTY_ROOT, b"a", b"1");
    // EMPTY_ROOT in the live set should be harmless.
    tree.gc(&[EMPTY_ROOT, r]);
    assert_eq!(tree.get(r, b"a").unwrap(), b"1");
}

#[test]
fn gc_large_tree_with_diverged_versions() {
    setup();
    let mut tree = PersistentBTree::new();

    // Build a base tree with many entries (forces multi-level structure).
    let mut base = EMPTY_ROOT;
    for i in 0u32..500 {
        base = tree.insert(base, &i.to_be_bytes(), &i.to_be_bytes());
    }

    // Fork two versions.
    let v1 = tree.insert(base, &0u32.to_be_bytes(), b"version1");
    let v2 = tree.insert(base, &0u32.to_be_bytes(), b"version2");

    // GC keeping only v1 — shared subtrees for keys 1..500 must survive.
    tree.gc(&[v1]);
    assert_eq!(tree.get(v1, &0u32.to_be_bytes()).unwrap(), b"version1");
    assert_eq!(
        tree.get(v1, &499u32.to_be_bytes()).unwrap(),
        499u32.to_be_bytes()
    );

    // v2 was NOT in the live set, so its unique nodes may have been collected.
    // We only verify v1 is intact. Do not access v2.
    let _ = v2;
}

// =====================================================================
// Node encode/decode roundtrip
// =====================================================================

#[test]
fn node_leaf_encode_decode_roundtrip() {
    let node = Node::Leaf {
        keys: vec![b"hello".to_vec(), b"world".to_vec()],
        values: vec![b"val1".to_vec(), b"val2".to_vec()],
    };
    let encoded = node.encode();
    let decoded = Node::decode(&encoded);
    match decoded {
        Node::Leaf { keys, values } => {
            assert_eq!(keys, vec![b"hello".to_vec(), b"world".to_vec()]);
            assert_eq!(values, vec![b"val1".to_vec(), b"val2".to_vec()]);
        }
        _ => panic!("expected Leaf"),
    }
}

#[test]
fn node_internal_encode_decode_roundtrip() {
    let node = Node::Internal {
        keys: vec![b"mid".to_vec()],
        children: vec![1, 2],
    };
    let encoded = node.encode();
    let decoded = Node::decode(&encoded);
    match decoded {
        Node::Internal { keys, children } => {
            assert_eq!(keys, vec![b"mid".to_vec()]);
            assert_eq!(children, vec![1, 2]);
        }
        _ => panic!("expected Internal"),
    }
}

#[test]
fn node_empty_leaf_encode_decode() {
    let node = Node::Leaf {
        keys: vec![],
        values: vec![],
    };
    let encoded = node.encode();
    let decoded = Node::decode(&encoded);
    assert_eq!(decoded.key_count(), 0);
}

#[test]
fn node_large_encode_decode() {
    let keys: Vec<Vec<u8>> = (0u32..MAX_KEYS as u32)
        .map(|i| i.to_be_bytes().to_vec())
        .collect();
    let values: Vec<Vec<u8>> = (0..MAX_KEYS).map(|i| vec![i as u8; 100]).collect();
    let node = Node::Leaf {
        keys: keys.clone(),
        values: values.clone(),
    };
    let encoded = node.encode();
    let decoded = Node::decode(&encoded);
    match decoded {
        Node::Leaf {
            keys: dk,
            values: dv,
        } => {
            assert_eq!(dk, keys);
            assert_eq!(dv, values);
        }
        _ => panic!("expected Leaf"),
    }
}

// =====================================================================
// Default trait
// =====================================================================

#[test]
fn default_creates_empty_tree() {
    setup();
    let tree = PersistentBTree::default();
    assert!(tree.get(EMPTY_ROOT, b"anything").is_none());
}

// =====================================================================
// Stress: sequential insert order then random-ish access
// =====================================================================

#[test]
fn stress_sequential_insert_random_access() {
    setup();
    let mut tree = PersistentBTree::new();
    let n = 5000u32;
    let mut root = EMPTY_ROOT;
    for i in 0..n {
        root = tree.insert(root, &i.to_be_bytes(), &(i * 2).to_be_bytes());
    }

    // Access in a scattered pattern.
    for i in (0..n).step_by(7) {
        assert_eq!(
            tree.get(root, &i.to_be_bytes()).unwrap(),
            (i * 2).to_be_bytes()
        );
    }
    assert_eq!(tree.iter(root).count(), n as usize);
}

#[test]
fn stress_reverse_insert_order() {
    setup();
    let mut tree = PersistentBTree::new();
    let n = 3000u32;
    let mut root = EMPTY_ROOT;
    for i in (0..n).rev() {
        root = tree.insert(root, &i.to_be_bytes(), &i.to_be_bytes());
    }
    // Should still iterate in ascending order.
    let items: Vec<_> = tree.iter(root).collect();
    assert_eq!(items.len(), n as usize);
    for (idx, (k, _)) in items.iter().enumerate() {
        assert_eq!(k.as_slice(), &(idx as u32).to_be_bytes());
    }
}

// =====================================================================
// Contains_key edge cases
// =====================================================================

#[test]
fn contains_key_empty_tree() {
    setup();
    let tree = PersistentBTree::new();
    assert!(!tree.contains_key(EMPTY_ROOT, b"anything"));
}

#[test]
fn contains_key_after_insert_and_remove() {
    setup();
    let mut tree = PersistentBTree::new();
    let r1 = tree.insert(EMPTY_ROOT, b"key", b"val");
    assert!(tree.contains_key(r1, b"key"));
    let r2 = tree.remove(r1, b"key");
    assert!(!tree.contains_key(r2, b"key"));
    // r1 still has the key.
    assert!(tree.contains_key(r1, b"key"));
}
