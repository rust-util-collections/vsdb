use super::map::VersionedMap;
use super::*;
use std::ops::Bound;

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

#[test]
fn insert_overwrite() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &100).unwrap();
    m.insert(MAIN_BRANCH, &1, &200).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(200));
}

#[test]
fn remove_nonexistent_key() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &100).unwrap();
    // Removing a non-existent key should not error.
    m.remove(MAIN_BRANCH, &999).unwrap();
    // Original key unaffected.
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(100));
}

#[test]
fn get_on_empty_map() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), None);
}

#[test]
fn contains_key_on_empty_map() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert!(!m.contains_key(MAIN_BRANCH, &1).unwrap());
}

#[test]
fn get_on_invalid_branch() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert!(m.get(999, &1).is_err());
}

#[test]
fn insert_on_invalid_branch() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert!(m.insert(999, &1, &1).is_err());
}

#[test]
fn many_keys_crud() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    for i in 0..200u32 {
        m.insert(MAIN_BRANCH, &i, &(i * 10)).unwrap();
    }
    for i in 0..200u32 {
        assert_eq!(m.get(MAIN_BRANCH, &i).unwrap(), Some(i * 10));
    }
    // Remove half.
    for i in (0..200u32).filter(|i| i % 2 == 0) {
        m.remove(MAIN_BRANCH, &i).unwrap();
    }
    for i in 0..200u32 {
        if i % 2 == 0 {
            assert_eq!(m.get(MAIN_BRANCH, &i).unwrap(), None);
        } else {
            assert_eq!(m.get(MAIN_BRANCH, &i).unwrap(), Some(i * 10));
        }
    }
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
fn discard_on_fresh_branch_no_commits() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    // Insert without commit.
    m.insert(MAIN_BRANCH, &1, &42).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(42));

    // Discard should reset to empty (no commits exist).
    m.discard(MAIN_BRANCH).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), None);
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

#[test]
fn get_at_commit_invalid_id() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert!(m.get_at_commit(999, &1).is_err());
}

#[test]
fn rollback_then_continue_committing() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    let c1 = m.commit(MAIN_BRANCH).unwrap();

    m.insert(MAIN_BRANCH, &1, &20).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    m.insert(MAIN_BRANCH, &1, &30).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    // Rollback to c1.
    m.rollback_to(MAIN_BRANCH, c1).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(10));

    // Continue with new changes on top of c1.
    m.insert(MAIN_BRANCH, &1, &999).unwrap();
    let c_new = m.commit(MAIN_BRANCH).unwrap();

    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(999));
    assert_eq!(m.get_at_commit(c_new, &1).unwrap(), Some(999));
}

#[test]
fn rollback_to_first_commit() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &1).unwrap();
    let c1 = m.commit(MAIN_BRANCH).unwrap();

    for i in 2..10u32 {
        m.insert(MAIN_BRANCH, &1, &i).unwrap();
        m.commit(MAIN_BRANCH).unwrap();
    }

    m.rollback_to(MAIN_BRANCH, c1).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(1));
}

#[test]
fn multiple_rollbacks() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    let c1 = m.commit(MAIN_BRANCH).unwrap();

    m.insert(MAIN_BRANCH, &1, &20).unwrap();
    let c2 = m.commit(MAIN_BRANCH).unwrap();

    m.insert(MAIN_BRANCH, &1, &30).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    m.rollback_to(MAIN_BRANCH, c2).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(20));

    m.rollback_to(MAIN_BRANCH, c1).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(10));
}

#[test]
fn empty_commit_no_changes() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    let c1 = m.commit(MAIN_BRANCH).unwrap();

    // Commit again without any changes.
    let c2 = m.commit(MAIN_BRANCH).unwrap();

    assert_ne!(c1, c2);
    assert_eq!(m.get_at_commit(c1, &1).unwrap(), Some(10));
    assert_eq!(m.get_at_commit(c2, &1).unwrap(), Some(10));
}

#[test]
fn head_commit_returns_latest() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    assert!(m.head_commit(MAIN_BRANCH).unwrap().is_none());

    m.insert(MAIN_BRANCH, &1, &1).unwrap();
    let c1 = m.commit(MAIN_BRANCH).unwrap();
    assert_eq!(m.head_commit(MAIN_BRANCH).unwrap().unwrap().id, c1);

    m.insert(MAIN_BRANCH, &2, &2).unwrap();
    let c2 = m.commit(MAIN_BRANCH).unwrap();
    assert_eq!(m.head_commit(MAIN_BRANCH).unwrap().unwrap().id, c2);
}

#[test]
fn head_commit_invalid_branch() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert!(m.head_commit(999).is_err());
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
fn branch_inherits_uncommitted_state() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    // Make uncommitted changes on main.
    m.insert(MAIN_BRANCH, &1, &99).unwrap();

    // Branch from main — should see dirty state (99).
    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    assert_eq!(m.get(feat, &1).unwrap(), Some(99));
}

#[test]
fn branch_from_branch() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let b1 = m.create_branch("b1", MAIN_BRANCH).unwrap();
    m.insert(b1, &2, &20).unwrap();
    m.commit(b1).unwrap();

    // Create b2 from b1 (not from main).
    let b2 = m.create_branch("b2", b1).unwrap();
    assert_eq!(m.get(b2, &1).unwrap(), Some(10));
    assert_eq!(m.get(b2, &2).unwrap(), Some(20));

    // Modify b2 — b1 unaffected.
    m.insert(b2, &3, &30).unwrap();
    m.commit(b2).unwrap();
    assert_eq!(m.get(b1, &3).unwrap(), None);
    assert_eq!(m.get(b2, &3).unwrap(), Some(30));
}

#[test]
fn multiple_branches_from_same_point() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let b1 = m.create_branch("b1", MAIN_BRANCH).unwrap();
    let b2 = m.create_branch("b2", MAIN_BRANCH).unwrap();
    let b3 = m.create_branch("b3", MAIN_BRANCH).unwrap();

    m.insert(b1, &1, &100).unwrap();
    m.insert(b2, &1, &200).unwrap();
    m.insert(b3, &1, &300).unwrap();

    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(10));
    assert_eq!(m.get(b1, &1).unwrap(), Some(100));
    assert_eq!(m.get(b2, &1).unwrap(), Some(200));
    assert_eq!(m.get(b3, &1).unwrap(), Some(300));
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
fn duplicate_branch_name_fails() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.create_branch("feat", MAIN_BRANCH).unwrap();
    assert!(m.create_branch("feat", MAIN_BRANCH).is_err());
}

#[test]
fn delete_branch_then_reuse_name() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    let b1 = m.create_branch("reuse", MAIN_BRANCH).unwrap();
    m.insert(b1, &1, &100).unwrap();
    m.commit(b1).unwrap();
    m.delete_branch(b1).unwrap();

    // Should be able to create a new branch with the same name.
    let b2 = m.create_branch("reuse", MAIN_BRANCH).unwrap();
    assert_ne!(b1, b2);
    // New branch doesn't have old data.
    assert_eq!(m.get(b2, &1).unwrap(), None);
}

#[test]
fn create_branch_from_invalid_source() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert!(m.create_branch("bad", 999).is_err());
}

#[test]
fn list_branches() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.create_branch("dev", MAIN_BRANCH).unwrap();
    let branches = m.list_branches();
    assert_eq!(branches.len(), 2);
}

#[test]
fn list_branches_after_delete() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    let b = m.create_branch("temp", MAIN_BRANCH).unwrap();
    m.create_branch("keep", MAIN_BRANCH).unwrap();
    m.delete_branch(b).unwrap();
    let branches = m.list_branches();
    assert_eq!(branches.len(), 2); // main + keep
}

#[test]
fn list_many_branches() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    for i in 0..20 {
        m.create_branch(&format!("branch_{i}"), MAIN_BRANCH).unwrap();
    }
    assert_eq!(m.list_branches().len(), 21); // 20 + main
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

#[test]
fn merge_both_changed_to_same_value() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();

    m.insert(feat, &1, &42).unwrap();
    m.commit(feat).unwrap();

    m.insert(MAIN_BRANCH, &1, &42).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    // Both changed to same value → keep it.
    m.merge(feat, MAIN_BRANCH).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(42));
}

#[test]
fn merge_delete_in_source_unchanged_in_target() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.insert(MAIN_BRANCH, &2, &20).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();

    // Source deletes key 1; target doesn't touch it.
    m.remove(feat, &1).unwrap();
    m.commit(feat).unwrap();

    m.merge(feat, MAIN_BRANCH).unwrap();
    // Key 1 was deleted in source, unchanged in target → delete.
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), None);
    assert_eq!(m.get(MAIN_BRANCH, &2).unwrap(), Some(20));
}

#[test]
fn merge_delete_in_target_unchanged_in_source() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.insert(MAIN_BRANCH, &2, &20).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();

    // Target deletes key 1; source doesn't touch it.
    m.remove(MAIN_BRANCH, &1).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    m.merge(feat, MAIN_BRANCH).unwrap();
    // Key 1 was deleted in target, unchanged in source → delete.
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), None);
    assert_eq!(m.get(MAIN_BRANCH, &2).unwrap(), Some(20));
}

#[test]
fn merge_delete_in_both() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.insert(MAIN_BRANCH, &2, &20).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();

    m.remove(feat, &1).unwrap();
    m.commit(feat).unwrap();

    m.remove(MAIN_BRANCH, &1).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    m.merge(feat, MAIN_BRANCH).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), None);
    assert_eq!(m.get(MAIN_BRANCH, &2).unwrap(), Some(20));
}

#[test]
fn merge_delete_source_changed_target() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();

    // Source deletes, target changes.
    m.remove(feat, &1).unwrap();
    m.commit(feat).unwrap();

    m.insert(MAIN_BRANCH, &1, &99).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    // Delete in one, changed in other → keep the change.
    m.merge(feat, MAIN_BRANCH).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(99));
}

#[test]
fn merge_changed_source_delete_target() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();

    // Source changes, target deletes.
    m.insert(feat, &1, &99).unwrap();
    m.commit(feat).unwrap();

    m.remove(MAIN_BRANCH, &1).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    // Delete in one, changed in other → keep the change (source's value).
    m.merge(feat, MAIN_BRANCH).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(99));
}

#[test]
fn merge_add_in_both_same_value() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();

    // Both add the same new key with the same value.
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();

    m.insert(MAIN_BRANCH, &2, &20).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    m.merge(feat, MAIN_BRANCH).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &2).unwrap(), Some(20));
}

#[test]
fn merge_add_in_both_different_values() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();

    // Both add the same new key with different values.
    m.insert(feat, &2, &42).unwrap();
    m.commit(feat).unwrap();

    m.insert(MAIN_BRANCH, &2, &99).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    // Source (feat) wins.
    m.merge(feat, MAIN_BRANCH).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &2).unwrap(), Some(42));
}

#[test]
fn merge_into_empty_target() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    m.insert(feat, &1, &10).unwrap();
    m.commit(feat).unwrap();

    // Target (main) has no commits.
    m.merge(feat, MAIN_BRANCH).unwrap();
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(10));
}

#[test]
fn merge_empty_source_fails() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    // feat has a commit (inherited from main), so merge should work.
    // But a truly empty source (no commits at all) should fail.
    let empty = m.create_branch("empty_src", MAIN_BRANCH).unwrap();
    // empty inherits main's head, which is a real commit, so it works.

    // To test truly no commit, we need a branch created before any commits.
    let mut m2: VersionedMap<u32, u32> = VersionedMap::new("test2");
    let b = m2.create_branch("empty", MAIN_BRANCH).unwrap();
    assert!(m2.merge(b, MAIN_BRANCH).is_err());

    // Also test: feat -> main should work normally.
    m.merge(feat, MAIN_BRANCH).unwrap();
    let _ = empty;
}

#[test]
fn merge_into_non_main_branch() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let b1 = m.create_branch("b1", MAIN_BRANCH).unwrap();
    let b2 = m.create_branch("b2", MAIN_BRANCH).unwrap();

    m.insert(b1, &2, &20).unwrap();
    m.commit(b1).unwrap();

    // Merge b1 into b2 (not into main).
    m.merge(b1, b2).unwrap();
    assert_eq!(m.get(b2, &1).unwrap(), Some(10));
    assert_eq!(m.get(b2, &2).unwrap(), Some(20));
    // Main unaffected.
    assert_eq!(m.get(MAIN_BRANCH, &2).unwrap(), None);
}

#[test]
fn merge_creates_merge_commit_with_two_parents() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();

    m.insert(MAIN_BRANCH, &3, &30).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let merge_id = m.merge(feat, MAIN_BRANCH).unwrap();
    let commit = m.head_commit(MAIN_BRANCH).unwrap().unwrap();
    assert_eq!(commit.id, merge_id);
    assert_eq!(commit.parents.len(), 2);
}

#[test]
fn sequential_merges() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    // First merge.
    let f1 = m.create_branch("f1", MAIN_BRANCH).unwrap();
    m.insert(f1, &2, &20).unwrap();
    m.commit(f1).unwrap();
    m.merge(f1, MAIN_BRANCH).unwrap();

    // Second merge.
    let f2 = m.create_branch("f2", MAIN_BRANCH).unwrap();
    m.insert(f2, &3, &30).unwrap();
    m.commit(f2).unwrap();
    m.merge(f2, MAIN_BRANCH).unwrap();

    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(10));
    assert_eq!(m.get(MAIN_BRANCH, &2).unwrap(), Some(20));
    assert_eq!(m.get(MAIN_BRANCH, &3).unwrap(), Some(30));
}

#[test]
fn merge_large_diverged() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    // Common ancestor: 100 keys.
    for i in 0..100u32 {
        m.insert(MAIN_BRANCH, &i, &i).unwrap();
    }
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();

    // Feature modifies even keys.
    for i in (0..100u32).filter(|i| i % 2 == 0) {
        m.insert(feat, &i, &(i + 1000)).unwrap();
    }
    m.commit(feat).unwrap();

    // Main modifies odd keys.
    for i in (0..100u32).filter(|i| i % 2 != 0) {
        m.insert(MAIN_BRANCH, &i, &(i + 2000)).unwrap();
    }
    m.commit(MAIN_BRANCH).unwrap();

    m.merge(feat, MAIN_BRANCH).unwrap();

    for i in 0..100u32 {
        if i % 2 == 0 {
            assert_eq!(m.get(MAIN_BRANCH, &i).unwrap(), Some(i + 1000));
        } else {
            assert_eq!(m.get(MAIN_BRANCH, &i).unwrap(), Some(i + 2000));
        }
    }
}

#[test]
fn merge_with_mixed_add_delete_change() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    // Ancestor: keys 1..=5
    for i in 1..=5u32 {
        m.insert(MAIN_BRANCH, &i, &(i * 10)).unwrap();
    }
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();

    // Feature: delete key 1, change key 2, add key 6.
    m.remove(feat, &1).unwrap();
    m.insert(feat, &2, &99).unwrap();
    m.insert(feat, &6, &60).unwrap();
    m.commit(feat).unwrap();

    // Main: delete key 3, change key 4, add key 7.
    m.remove(MAIN_BRANCH, &3).unwrap();
    m.insert(MAIN_BRANCH, &4, &88).unwrap();
    m.insert(MAIN_BRANCH, &7, &70).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    m.merge(feat, MAIN_BRANCH).unwrap();

    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), None);    // deleted in feat, unchanged in main → delete
    assert_eq!(m.get(MAIN_BRANCH, &2).unwrap(), Some(99)); // changed in feat → take feat
    assert_eq!(m.get(MAIN_BRANCH, &3).unwrap(), None);    // deleted in main, unchanged in feat → delete
    assert_eq!(m.get(MAIN_BRANCH, &4).unwrap(), Some(88)); // changed in main → take main
    assert_eq!(m.get(MAIN_BRANCH, &5).unwrap(), Some(50)); // unchanged both → keep
    assert_eq!(m.get(MAIN_BRANCH, &6).unwrap(), Some(60)); // added in feat → take
    assert_eq!(m.get(MAIN_BRANCH, &7).unwrap(), Some(70)); // added in main → take
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

#[test]
fn iter_empty_map() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    let items: Vec<(u32, u32)> = m.iter(MAIN_BRANCH).unwrap().collect();
    assert!(items.is_empty());
}

#[test]
fn iter_after_commit() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.insert(MAIN_BRANCH, &2, &20).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let items: Vec<(u32, u32)> = m.iter(MAIN_BRANCH).unwrap().collect();
    assert_eq!(items, vec![(1, 10), (2, 20)]);
}

#[test]
fn iter_on_branch() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.insert(feat, &3, &30).unwrap();

    let items: Vec<(u32, u32)> = m.iter(feat).unwrap().collect();
    assert_eq!(items, vec![(1, 10), (2, 20), (3, 30)]);

    // Main should only have key 1.
    let main_items: Vec<(u32, u32)> = m.iter(MAIN_BRANCH).unwrap().collect();
    assert_eq!(main_items, vec![(1, 10)]);
}

#[test]
fn iter_invalid_branch() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert!(m.iter(999).is_err());
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

#[test]
fn log_empty_branch() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    let log = m.log(MAIN_BRANCH).unwrap();
    assert!(log.is_empty());
}

#[test]
fn log_invalid_branch() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert!(m.log(999).is_err());
}

#[test]
fn log_single_commit() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &1).unwrap();
    let c = m.commit(MAIN_BRANCH).unwrap();
    let log = m.log(MAIN_BRANCH).unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].id, c);
    assert!(log[0].parents.is_empty());
}

#[test]
fn log_branch_has_own_history() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &1).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    m.insert(feat, &2, &2).unwrap();
    m.commit(feat).unwrap();
    m.insert(feat, &3, &3).unwrap();
    m.commit(feat).unwrap();

    let feat_log = m.log(feat).unwrap();
    let main_log = m.log(MAIN_BRANCH).unwrap();

    // Feature has 3 entries (2 own + 1 inherited from main).
    assert_eq!(feat_log.len(), 3);
    // Main has 1.
    assert_eq!(main_log.len(), 1);
}

#[test]
fn log_after_merge_follows_first_parent() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    let c1 = m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();

    m.insert(MAIN_BRANCH, &3, &30).unwrap();
    let c2 = m.commit(MAIN_BRANCH).unwrap();

    let merge_id = m.merge(feat, MAIN_BRANCH).unwrap();

    let log = m.log(MAIN_BRANCH).unwrap();
    // log follows first parent chain: merge -> c2 -> c1
    assert_eq!(log.len(), 3);
    assert_eq!(log[0].id, merge_id);
    assert_eq!(log[1].id, c2);
    assert_eq!(log[2].id, c1);
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

#[test]
fn gc_preserves_shared_ancestor_commits() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    let c1 = m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();

    m.insert(MAIN_BRANCH, &3, &30).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    m.gc();

    // Both branches' data intact.
    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(10));
    assert_eq!(m.get(MAIN_BRANCH, &3).unwrap(), Some(30));
    assert_eq!(m.get(feat, &1).unwrap(), Some(10));
    assert_eq!(m.get(feat, &2).unwrap(), Some(20));
    // Shared ancestor commit survives.
    assert_eq!(m.get_at_commit(c1, &1).unwrap(), Some(10));
}

#[test]
fn gc_with_uncommitted_dirty_state() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    // Uncommitted changes.
    m.insert(MAIN_BRANCH, &2, &20).unwrap();

    // GC should preserve dirty root.
    m.gc();

    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(10));
    assert_eq!(m.get(MAIN_BRANCH, &2).unwrap(), Some(20));
}

#[test]
fn gc_multiple_times_is_idempotent() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();
    m.delete_branch(feat).unwrap();

    m.gc();
    m.gc();
    m.gc();

    assert_eq!(m.get(MAIN_BRANCH, &1).unwrap(), Some(10));
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

#[test]
fn stress_many_commits() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("stress");
    let mut commits = Vec::new();

    for i in 0..100u32 {
        m.insert(MAIN_BRANCH, &i, &i).unwrap();
        commits.push(m.commit(MAIN_BRANCH).unwrap());
    }

    // Each commit i should have keys 0..=i.
    for (i, &cid) in commits.iter().enumerate() {
        for j in 0..=(i as u32) {
            assert_eq!(m.get_at_commit(cid, &j).unwrap(), Some(j));
        }
        // Key i+1 should not exist at this commit.
        assert_eq!(m.get_at_commit(cid, &(i as u32 + 1)).unwrap(), None);
    }
}

#[test]
fn stress_many_branches_and_gc() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("stress");

    m.insert(MAIN_BRANCH, &0, &0).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    // Create and delete many branches.
    for i in 0..50u32 {
        let b = m.create_branch(&format!("b{i}"), MAIN_BRANCH).unwrap();
        let key = i + 1;
        m.insert(b, &key, &key).unwrap();
        m.commit(b).unwrap();
        m.delete_branch(b).unwrap();
    }

    m.gc();

    // Main unaffected.
    assert_eq!(m.get(MAIN_BRANCH, &0).unwrap(), Some(0));
    // Branch-only keys are gone from main.
    for i in 1..=50u32 {
        assert_eq!(m.get(MAIN_BRANCH, &i).unwrap(), None);
    }
}

// =====================================================================
// Edge cases: type variety
// =====================================================================

// =====================================================================
// New convenience APIs: branch_id, branch_name, has_uncommitted,
//                       range, iter_at_commit, get_commit
// =====================================================================

// --- branch_id ---

#[test]
fn branch_id_main() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert_eq!(m.branch_id("main"), Some(MAIN_BRANCH));
}

#[test]
fn branch_id_custom_branch() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    let feat = m.create_branch("feature", MAIN_BRANCH).unwrap();
    assert_eq!(m.branch_id("feature"), Some(feat));
}

#[test]
fn branch_id_nonexistent() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert_eq!(m.branch_id("no_such_branch"), None);
}

#[test]
fn branch_id_after_delete() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    let b = m.create_branch("temp", MAIN_BRANCH).unwrap();
    assert_eq!(m.branch_id("temp"), Some(b));
    m.delete_branch(b).unwrap();
    assert_eq!(m.branch_id("temp"), None);
}

#[test]
fn branch_id_reused_name() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    let b1 = m.create_branch("reuse", MAIN_BRANCH).unwrap();
    m.delete_branch(b1).unwrap();
    let b2 = m.create_branch("reuse", MAIN_BRANCH).unwrap();
    assert_eq!(m.branch_id("reuse"), Some(b2));
    assert_ne!(b1, b2);
}

// --- branch_name ---

#[test]
fn branch_name_main() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert_eq!(m.branch_name(MAIN_BRANCH), Some("main".to_string()));
}

#[test]
fn branch_name_custom() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    let feat = m.create_branch("my-feature", MAIN_BRANCH).unwrap();
    assert_eq!(m.branch_name(feat), Some("my-feature".to_string()));
}

#[test]
fn branch_name_nonexistent() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert_eq!(m.branch_name(999), None);
}

#[test]
fn branch_name_after_delete() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    let b = m.create_branch("ephemeral", MAIN_BRANCH).unwrap();
    assert_eq!(m.branch_name(b), Some("ephemeral".to_string()));
    m.delete_branch(b).unwrap();
    assert_eq!(m.branch_name(b), None);
}

#[test]
fn branch_id_and_name_roundtrip() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    let names = ["alpha", "beta", "gamma"];
    for name in &names {
        m.create_branch(name, MAIN_BRANCH).unwrap();
    }
    for name in &names {
        let id = m.branch_id(name).unwrap();
        assert_eq!(m.branch_name(id).unwrap(), *name);
    }
}

// --- has_uncommitted ---

#[test]
fn has_uncommitted_fresh_map() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    // No commits and no inserts → no uncommitted changes.
    assert!(!m.has_uncommitted(MAIN_BRANCH).unwrap());
}

#[test]
fn has_uncommitted_after_insert() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    assert!(m.has_uncommitted(MAIN_BRANCH).unwrap());
}

#[test]
fn has_uncommitted_after_commit() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();
    assert!(!m.has_uncommitted(MAIN_BRANCH).unwrap());
}

#[test]
fn has_uncommitted_after_commit_then_modify() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();
    m.insert(MAIN_BRANCH, &2, &20).unwrap();
    assert!(m.has_uncommitted(MAIN_BRANCH).unwrap());
}

#[test]
fn has_uncommitted_after_discard() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();
    m.insert(MAIN_BRANCH, &2, &20).unwrap();
    assert!(m.has_uncommitted(MAIN_BRANCH).unwrap());
    m.discard(MAIN_BRANCH).unwrap();
    assert!(!m.has_uncommitted(MAIN_BRANCH).unwrap());
}

#[test]
fn has_uncommitted_on_new_branch() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();
    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    // New branch starts clean (same root as parent head).
    assert!(!m.has_uncommitted(feat).unwrap());
    m.insert(feat, &2, &20).unwrap();
    assert!(m.has_uncommitted(feat).unwrap());
}

#[test]
fn has_uncommitted_invalid_branch() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert!(m.has_uncommitted(999).is_err());
}

// --- range ---

#[test]
fn range_full_unbounded() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    for i in 1..=5u32 {
        m.insert(MAIN_BRANCH, &i, &(i * 10)).unwrap();
    }
    let items: Vec<_> = m
        .range(MAIN_BRANCH, Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .collect();
    assert_eq!(items.len(), 5);
    assert_eq!(items[0], (1, 10));
    assert_eq!(items[4], (5, 50));
}

#[test]
fn range_included_both() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    for i in 1..=10u32 {
        m.insert(MAIN_BRANCH, &i, &(i * 10)).unwrap();
    }
    let items: Vec<_> = m
        .range(MAIN_BRANCH, Bound::Included(&3), Bound::Included(&7))
        .unwrap()
        .collect();
    assert_eq!(items.len(), 5);
    assert_eq!(items[0].0, 3);
    assert_eq!(items[4].0, 7);
}

#[test]
fn range_excluded_both() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    for i in 1..=10u32 {
        m.insert(MAIN_BRANCH, &i, &(i * 10)).unwrap();
    }
    let items: Vec<_> = m
        .range(MAIN_BRANCH, Bound::Excluded(&3), Bound::Excluded(&7))
        .unwrap()
        .collect();
    assert_eq!(items.len(), 3); // 4, 5, 6
    assert_eq!(items[0].0, 4);
    assert_eq!(items[2].0, 6);
}

#[test]
fn range_included_lo_excluded_hi() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    for i in 1..=10u32 {
        m.insert(MAIN_BRANCH, &i, &(i * 10)).unwrap();
    }
    // [3, 7) → 3, 4, 5, 6
    let items: Vec<_> = m
        .range(MAIN_BRANCH, Bound::Included(&3), Bound::Excluded(&7))
        .unwrap()
        .collect();
    assert_eq!(items.len(), 4);
    assert_eq!(items[0].0, 3);
    assert_eq!(items[3].0, 6);
}

#[test]
fn range_excluded_lo_included_hi() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    for i in 1..=10u32 {
        m.insert(MAIN_BRANCH, &i, &(i * 10)).unwrap();
    }
    // (3, 7] → 4, 5, 6, 7
    let items: Vec<_> = m
        .range(MAIN_BRANCH, Bound::Excluded(&3), Bound::Included(&7))
        .unwrap()
        .collect();
    assert_eq!(items.len(), 4);
    assert_eq!(items[0].0, 4);
    assert_eq!(items[3].0, 7);
}

#[test]
fn range_unbounded_lo() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    for i in 1..=10u32 {
        m.insert(MAIN_BRANCH, &i, &(i * 10)).unwrap();
    }
    // [.., 5] → 1, 2, 3, 4, 5
    let items: Vec<_> = m
        .range(MAIN_BRANCH, Bound::Unbounded, Bound::Included(&5))
        .unwrap()
        .collect();
    assert_eq!(items.len(), 5);
    assert_eq!(items[0].0, 1);
    assert_eq!(items[4].0, 5);
}

#[test]
fn range_unbounded_hi() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    for i in 1..=10u32 {
        m.insert(MAIN_BRANCH, &i, &(i * 10)).unwrap();
    }
    // [6, ..] → 6, 7, 8, 9, 10
    let items: Vec<_> = m
        .range(MAIN_BRANCH, Bound::Included(&6), Bound::Unbounded)
        .unwrap()
        .collect();
    assert_eq!(items.len(), 5);
    assert_eq!(items[0].0, 6);
    assert_eq!(items[4].0, 10);
}

#[test]
fn range_empty_result() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    for i in 1..=5u32 {
        m.insert(MAIN_BRANCH, &i, &(i * 10)).unwrap();
    }
    // Range with no matching keys.
    let items: Vec<_> = m
        .range(MAIN_BRANCH, Bound::Included(&100), Bound::Included(&200))
        .unwrap()
        .collect();
    assert!(items.is_empty());
}

#[test]
fn range_empty_map() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    let items: Vec<_> = m
        .range(MAIN_BRANCH, Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .collect();
    assert!(items.is_empty());
}

#[test]
fn range_on_branch() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    for i in 1..=10u32 {
        m.insert(MAIN_BRANCH, &i, &(i * 10)).unwrap();
    }
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    m.insert(feat, &11, &110).unwrap();
    m.insert(feat, &12, &120).unwrap();

    // Range on feat should see all 12 keys.
    let items: Vec<_> = m
        .range(feat, Bound::Included(&9), Bound::Unbounded)
        .unwrap()
        .collect();
    assert_eq!(items.len(), 4); // 9, 10, 11, 12
    assert_eq!(items[0].0, 9);
    assert_eq!(items[3].0, 12);

    // Range on main should only see 10 keys.
    let main_items: Vec<_> = m
        .range(MAIN_BRANCH, Bound::Included(&9), Bound::Unbounded)
        .unwrap()
        .collect();
    assert_eq!(main_items.len(), 2); // 9, 10
}

#[test]
fn range_invalid_branch() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert!(m
        .range(999, Bound::Unbounded, Bound::Unbounded)
        .is_err());
}

#[test]
fn range_single_key() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    for i in 1..=10u32 {
        m.insert(MAIN_BRANCH, &i, &(i * 10)).unwrap();
    }
    // Exact single key: [5, 5]
    let items: Vec<_> = m
        .range(MAIN_BRANCH, Bound::Included(&5), Bound::Included(&5))
        .unwrap()
        .collect();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0], (5, 50));
}

// --- iter_at_commit ---

#[test]
fn iter_at_commit_basic() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.insert(MAIN_BRANCH, &2, &20).unwrap();
    let c1 = m.commit(MAIN_BRANCH).unwrap();

    m.insert(MAIN_BRANCH, &3, &30).unwrap();
    let c2 = m.commit(MAIN_BRANCH).unwrap();

    // c1 should have 2 entries.
    let items1: Vec<_> = m.iter_at_commit(c1).unwrap().collect();
    assert_eq!(items1.len(), 2);
    assert_eq!(items1, vec![(1, 10), (2, 20)]);

    // c2 should have 3 entries.
    let items2: Vec<_> = m.iter_at_commit(c2).unwrap().collect();
    assert_eq!(items2.len(), 3);
    assert_eq!(items2, vec![(1, 10), (2, 20), (3, 30)]);
}

#[test]
fn iter_at_commit_after_remove() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.insert(MAIN_BRANCH, &2, &20).unwrap();
    let c1 = m.commit(MAIN_BRANCH).unwrap();

    m.remove(MAIN_BRANCH, &1).unwrap();
    let c2 = m.commit(MAIN_BRANCH).unwrap();

    let items1: Vec<_> = m.iter_at_commit(c1).unwrap().collect();
    assert_eq!(items1.len(), 2);

    let items2: Vec<_> = m.iter_at_commit(c2).unwrap().collect();
    assert_eq!(items2.len(), 1);
    assert_eq!(items2[0], (2, 20));
}

#[test]
fn iter_at_commit_on_branch_commit() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    m.insert(feat, &2, &20).unwrap();
    let fc = m.commit(feat).unwrap();

    // iter_at_commit works with any commit, regardless of branch.
    let items: Vec<_> = m.iter_at_commit(fc).unwrap().collect();
    assert_eq!(items, vec![(1, 10), (2, 20)]);
}

#[test]
fn iter_at_commit_invalid() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert!(m.iter_at_commit(999).is_err());
}

#[test]
fn iter_at_commit_ordered() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    // Insert in reverse order.
    for i in (1..=50u32).rev() {
        m.insert(MAIN_BRANCH, &i, &(i * 10)).unwrap();
    }
    let c = m.commit(MAIN_BRANCH).unwrap();

    let items: Vec<_> = m.iter_at_commit(c).unwrap().collect();
    assert_eq!(items.len(), 50);
    for (idx, (k, v)) in items.iter().enumerate() {
        let expected_key = (idx + 1) as u32;
        assert_eq!(*k, expected_key);
        assert_eq!(*v, expected_key * 10);
    }
}

#[test]
fn iter_at_commit_empty_tree() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    // Commit with nothing in it (empty tree).
    // First add and remove so we have a commit with empty state.
    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.remove(MAIN_BRANCH, &1).unwrap();
    let c = m.commit(MAIN_BRANCH).unwrap();

    let items: Vec<_> = m.iter_at_commit(c).unwrap().collect();
    assert!(items.is_empty());
}

// --- get_commit ---

#[test]
fn get_commit_basic() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    let c = m.commit(MAIN_BRANCH).unwrap();

    let commit = m.get_commit(c).unwrap();
    assert_eq!(commit.id, c);
    assert!(commit.parents.is_empty()); // First commit has no parents.
    assert!(commit.timestamp_us > 0);
}

#[test]
fn get_commit_nonexistent() {
    setup();
    let m: VersionedMap<u32, u32> = VersionedMap::new("test");
    assert!(m.get_commit(999).is_none());
}

#[test]
fn get_commit_with_parent() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    let c1 = m.commit(MAIN_BRANCH).unwrap();

    m.insert(MAIN_BRANCH, &2, &20).unwrap();
    let c2 = m.commit(MAIN_BRANCH).unwrap();

    let commit2 = m.get_commit(c2).unwrap();
    assert_eq!(commit2.id, c2);
    assert_eq!(commit2.parents, vec![c1]);
}

#[test]
fn get_commit_merge_commit() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");

    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let feat = m.create_branch("feat", MAIN_BRANCH).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();

    m.insert(MAIN_BRANCH, &3, &30).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    let merge_id = m.merge(feat, MAIN_BRANCH).unwrap();
    let merge_commit = m.get_commit(merge_id).unwrap();
    assert_eq!(merge_commit.id, merge_id);
    assert_eq!(merge_commit.parents.len(), 2);
}

#[test]
fn get_commit_timestamp_uss_increase() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &1).unwrap();
    let c1 = m.commit(MAIN_BRANCH).unwrap();
    m.insert(MAIN_BRANCH, &2, &2).unwrap();
    let c2 = m.commit(MAIN_BRANCH).unwrap();

    let t1 = m.get_commit(c1).unwrap().timestamp_us;
    let t2 = m.get_commit(c2).unwrap().timestamp_us;
    assert!(t2 >= t1);
}

#[test]
fn get_commit_matches_head_commit() {
    setup();
    let mut m: VersionedMap<u32, u32> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &1, &10).unwrap();
    let c = m.commit(MAIN_BRANCH).unwrap();

    let via_get = m.get_commit(c).unwrap();
    let via_head = m.head_commit(MAIN_BRANCH).unwrap().unwrap();
    assert_eq!(via_get.id, via_head.id);
    assert_eq!(via_get.parents, via_head.parents);
    assert_eq!(via_get.timestamp_us, via_head.timestamp_us);
}

// =====================================================================
// Edge cases: type variety
// =====================================================================

#[test]
fn string_keys_and_values() {
    setup();
    let mut m: VersionedMap<String, String> = VersionedMap::new("test");
    m.insert(MAIN_BRANCH, &"hello".to_string(), &"world".to_string())
        .unwrap();
    m.commit(MAIN_BRANCH).unwrap();
    assert_eq!(
        m.get(MAIN_BRANCH, &"hello".to_string()).unwrap(),
        Some("world".to_string())
    );
}

#[test]
fn u64_keys() {
    setup();
    let mut m: VersionedMap<u64, u64> = VersionedMap::new("test");
    let max = u64::MAX;
    let mid = u64::MAX / 2;
    m.insert(MAIN_BRANCH, &max, &42).unwrap();
    m.insert(MAIN_BRANCH, &0, &0).unwrap();
    m.insert(MAIN_BRANCH, &mid, &21).unwrap();
    m.commit(MAIN_BRANCH).unwrap();

    assert_eq!(m.get(MAIN_BRANCH, &max).unwrap(), Some(42));
    assert_eq!(m.get(MAIN_BRANCH, &0).unwrap(), Some(0));

    // Iteration should be in ascending order.
    let items: Vec<(u64, u64)> = m.iter(MAIN_BRANCH).unwrap().collect();
    assert_eq!(items.len(), 3);
    assert!(items[0].0 < items[1].0);
    assert!(items[1].0 < items[2].0);
}
