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
fn remove_until_empty() {
    setup();
    let mut tree = PersistentBTree::new();
    let r = tree.insert(EMPTY_ROOT, b"a", b"1");
    let r = tree.remove(r, b"a");
    assert_eq!(r, EMPTY_ROOT);
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
