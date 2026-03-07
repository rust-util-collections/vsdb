use super::map::VersionedMap;
use super::*;

fn setup() {
    let dir = format!("/tmp/vsdb_versioned_test/{}", rand::random::<u128>());
    let _ = vsdb_core::vsdb_set_base_dir(&dir);
}

// =====================================================================
// Basic CRUD
// =====================================================================

#[test]
fn basic_insert_get() {
    setup();
    let mut m: VersionedMap<u32, String> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &"hello".into()).unwrap();
    assert_eq!(
        m.get(MAIN_BRANCH, &1).unwrap(),
        Some("hello".to_string())
    );
    assert_eq!(m.get(MAIN_BRANCH, &2).unwrap(), None);
}

#[test]
fn basic_remove() {
    setup();
    let mut m: VersionedMap<u32, String> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &"hello".into()).unwrap();
    m.remove(MAIN_BRANCH, &1).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), None);
}

#[test]
fn contains_key() {
    setup();
    let mut m: VersionedMap<u32, u64> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &42, &999).unwrap();
    assert!(m.contains_key(MAIN_BRANCH, &42).unwrap());
    assert!(!m.contains_key(MAIN_BRANCH, &43).unwrap());
}

// =====================================================================
// Commit / Rollback
// =====================================================================

#[test]
fn commit_and_rollback() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &100).unwrap();
    let c1 = m.commit(MAIN_BRANCH).unwrap();

    m.insert(MAIN_BRANCH, &1, &200).unwrap();
    let _c2 = m.commit(MAIN_BRANCH).unwrap();

    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(200));

    // Rollback to c1.
    m.rollback_to(MAIN_BRANCH, c1).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(100));
}

#[test]
fn discard_uncommitted() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &100).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    m.insert(MAIN_BRANCH, &1, &999).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(999));

    m.discard(MAIN_BRANCH).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(100));
}

#[test]
fn read_historical_commit() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    let c1 = m.commit(MAIN_BRANCH).unwrap();

    m.insert(MAIN_BRANCH, &1, &20).unwrap();
    let c2 = m.commit(MAIN_BRANCH).unwrap();

    assert_eq!(m.get_at_commit(c1, &1).unwrap(), Some(10));
    assert_eq!(m.get_at_commit(c2, &1).unwrap(), Some(20));
}

// =====================================================================
// Branching
// =====================================================================

#[test]
fn branch_isolation() {
    setup();
    let mut m: VersionedMap<u32, String> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &"base".into()).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feature", MAIN_BRANCH).unwrap();

    // Modify on feature.
    m.insert(feat, &1, &"feature_val".into()).unwrap();
    m.commit(feat).unwrap();

    // Main is unchanged.
    assert_eq!(
        m.get(MAIN_BRANCH, &1).unwrap(),
        Some("base".to_string())
    );
    assert_eq!(
        m.get(feat, &1).unwrap(),
        Some("feature_val".to_string())
    );
}

#[test]
fn delete_branch() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    let b = m.create_branch("temp", MAIN_BRANCH).unwrap();
    m.delete_branch(b).unwrap();
    assert!(m.get(b, &1).is_err());
}

#[test]
fn cannot_delete_main() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert!(m.delete_branch(MAIN_BRANCH).is_err());
}

#[test]
fn list_branches() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.create_branch("dev", MAIN_BRANCH).unwrap();
    let branches = m.list_branches();
    assert_eq!(branches.len(), 2);
}

// =====================================================================
// Merge
// =====================================================================

#[test]
fn merge_fast_forward() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();

    m.merge(feat, MAIN_BRANCH).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(10));
    assert_eq!(m.get(MAIN_BRANCH, &2).unwrap(), Some(20));
}

#[test]
fn merge_diverged() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    // Common ancestor: key 1 = 10
    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.insert(MAIN_BRANCH, &2, &20).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();

    // Feature: change key 1, add key 3.
    m.insert(feat, &1, &11).unwrap();
    m.insert(feat, &3, &30).unwrap();
    m.commit(feat).unwrap();

    // Main: change key 2, add key 4.
    m.insert(MAIN_BRANCH, &2, &22).unwrap();
    m.insert(MAIN_BRANCH, &4, &40).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    m.merge(feat, MAIN_BRANCH).unwrap();

    // key 1: changed only in feat → 11
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(11));
    // key 2: changed only in main → 22
    assert_eq!(m.get(MAIN_BRANCH, &2).unwrap(), Some(22));
    // key 3: added only in feat → 30
    assert_eq!(m.get(MAIN_BRANCH, &3).unwrap(), Some(30));
    // key 4: added only in main → 40
    assert_eq!(m.get(MAIN_BRANCH, &4).unwrap(), Some(40));
}

#[test]
fn merge_conflict_source_wins() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();

    m.insert(feat, &1, &99).unwrap();
    m.commit(feat).unwrap();

    m.insert(MAIN_BRANCH, &1, &77).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    // Both changed key 1 → source (feat=99) wins.
    m.merge(feat, MAIN_BRANCH).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(99));
}

// =====================================================================
// Iteration
// =====================================================================

#[test]
fn iter_ordered() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    for i in (0u32..50).rev() {
        m.insert(MAIN_BRANCH, &i, &(i * 10)).unwrap();
    }
    let items: Vec<(u32, u32)> = m.iter(MAIN_BRANCH).unwrap().collect();
    assert_eq!(items.len(), 50);
    for (idx, (k, v)) in items.iter().enumerate() {
        assert_eq!(*k, idx as u32);
        assert_eq!(*v, (idx as u32) * 10);
    }
}

// =====================================================================
// Log
// =====================================================================

#[test]
fn commit_log() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &1).unwrap();
    m.commit(MAIN_BRANCH).unwrap();
    m.insert(MAIN_BRANCH, &2, &2).unwrap();
    m.commit(MAIN_BRANCH).unwrap();
    m.insert(MAIN_BRANCH, &3, &3).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let log = m.log(MAIN_BRANCH).unwrap();
    assert_eq!(log.len(), 3);
    // Most recent first.
    assert!(log[0].id > log[1].id);
    assert!(log[1].id > log[2].id);
}

// =====================================================================
// GC
// =====================================================================

#[test]
fn gc_reclaims_deleted_branch() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &1).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    m.insert(feat, &2, &2).unwrap();
    m.commit(feat).unwrap();
    m.delete_branch(feat).unwrap();

    m.gc();

    // Main data unaffected.
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(1));
}

// =====================================================================
// Many entries (stress test for splits/rebalancing through versioning)
// =====================================================================

#[test]
fn stress_versioned() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("stress");
    let n = 500u32;

    for i in 0..n {
        m.insert(MAIN_BRANCH, &i, &i).unwrap();
    }
    let c1 = m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    for i in 0..n {
        m.insert(feat, &i, &(i + 1000)).unwrap();
    }
    m.commit(feat).unwrap();

    // Main still has original values.
    for i in 0..n {
        assert_eq!(m.get_at_commit(c1, &i).unwrap(), Some(i));
    }
    // Feature has updated values.
    for i in 0..n {
        assert_eq!(m.get(feat, &i).unwrap(), Some(i + 1000));
    }
}
