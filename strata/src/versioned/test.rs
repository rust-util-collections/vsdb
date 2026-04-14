use super::map::VerMap;
use crate::common::error::VsdbError;
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
    let mut m: VerMap<u32, String> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &"hello".into()).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some("hello".to_string()));
    assert_eq!(m.get(main, &2).unwrap(), None);
}

#[test]
fn basic_remove() {
    setup();
    let mut m: VerMap<u32, String> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &"hello".into()).unwrap();
    m.remove(main, &1).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), None);
}

#[test]
fn contains_key() {
    setup();
    let mut m: VerMap<u32, u64> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &42, &999).unwrap();
    assert!(m.contains_key(main, &42).unwrap());
    assert!(!m.contains_key(main, &43).unwrap());
}

#[test]
fn insert_overwrite() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &100).unwrap();
    m.insert(main, &1, &200).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some(200));
}

#[test]
fn remove_nonexistent_key() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &100).unwrap();
    // Removing a non-existent key should not error.
    m.remove(main, &999).unwrap();
    // Original key unaffected.
    assert_eq!(m.get(main, &1).unwrap(), Some(100));
}

#[test]
fn get_on_empty_map() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    assert_eq!(m.get(main, &1).unwrap(), None);
}

#[test]
fn contains_key_on_empty_map() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    assert!(!m.contains_key(main, &1).unwrap());
}

#[test]
fn get_on_invalid_branch() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert!(m.get(999, &1).is_err());
}

#[test]
fn insert_on_invalid_branch() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert!(m.insert(999, &1, &1).is_err());
}

#[test]
fn many_keys_crud() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    for i in 0..200u32 {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    for i in 0..200u32 {
        assert_eq!(m.get(main, &i).unwrap(), Some(i * 10));
    }
    // Remove half.
    for i in (0..200u32).filter(|i| i % 2 == 0) {
        m.remove(main, &i).unwrap();
    }
    for i in 0..200u32 {
        if i % 2 == 0 {
            assert_eq!(m.get(main, &i).unwrap(), None);
        } else {
            assert_eq!(m.get(main, &i).unwrap(), Some(i * 10));
        }
    }
}

// =====================================================================
// Commit / Rollback
// =====================================================================

#[test]
fn commit_and_rollback() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &100).unwrap();
    let c1 = m.commit(main).unwrap();

    m.insert(main, &1, &200).unwrap();
    let _c2 = m.commit(main).unwrap();

    assert_eq!(m.get(main, &1).unwrap(), Some(200));

    // Rollback to c1.
    m.rollback_to(main, c1).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some(100));
}

#[test]
fn discard_uncommitted() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &100).unwrap();
    m.commit(main).unwrap();

    m.insert(main, &1, &999).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some(999));

    m.discard(main).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some(100));
}

#[test]
fn discard_on_fresh_branch_no_commits() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // Insert without commit.
    m.insert(main, &1, &42).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some(42));

    // Discard should reset to empty (no commits exist).
    m.discard(main).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), None);
}

#[test]
fn read_historical_commit() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();

    m.insert(main, &1, &20).unwrap();
    let c2 = m.commit(main).unwrap();

    assert_eq!(m.get_at_commit(c1, &1).unwrap(), Some(10));
    assert_eq!(m.get_at_commit(c2, &1).unwrap(), Some(20));
}

#[test]
fn get_at_commit_invalid_id() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert!(m.get_at_commit(999, &1).is_err());
}

#[test]
fn rollback_then_continue_committing() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();

    m.insert(main, &1, &20).unwrap();
    m.commit(main).unwrap();

    m.insert(main, &1, &30).unwrap();
    m.commit(main).unwrap();

    // Rollback to c1.
    m.rollback_to(main, c1).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some(10));

    // Continue with new changes on top of c1.
    m.insert(main, &1, &999).unwrap();
    let c_new = m.commit(main).unwrap();

    assert_eq!(m.get(main, &1).unwrap(), Some(999));
    assert_eq!(m.get_at_commit(c_new, &1).unwrap(), Some(999));
}

#[test]
fn rollback_to_first_commit() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &1).unwrap();
    let c1 = m.commit(main).unwrap();

    for i in 2..10u32 {
        m.insert(main, &1, &i).unwrap();
        m.commit(main).unwrap();
    }

    m.rollback_to(main, c1).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some(1));
}

#[test]
fn multiple_rollbacks() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();

    m.insert(main, &1, &20).unwrap();
    let c2 = m.commit(main).unwrap();

    m.insert(main, &1, &30).unwrap();
    m.commit(main).unwrap();

    m.rollback_to(main, c2).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some(20));

    m.rollback_to(main, c1).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some(10));
}

#[test]
fn empty_commit_no_changes() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();

    // Commit again without any changes.
    let c2 = m.commit(main).unwrap();

    assert_ne!(c1, c2);
    assert_eq!(m.get_at_commit(c1, &1).unwrap(), Some(10));
    assert_eq!(m.get_at_commit(c2, &1).unwrap(), Some(10));
}

#[test]
fn head_commit_returns_latest() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    assert!(m.head_commit(main).unwrap().is_none());

    m.insert(main, &1, &1).unwrap();
    let c1 = m.commit(main).unwrap();
    assert_eq!(m.head_commit(main).unwrap().unwrap().id, c1);

    m.insert(main, &2, &2).unwrap();
    let c2 = m.commit(main).unwrap();
    assert_eq!(m.head_commit(main).unwrap().unwrap().id, c2);
}

#[test]
fn head_commit_invalid_branch() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert!(m.head_commit(999).is_err());
}

// =====================================================================
// Branching
// =====================================================================

#[test]
fn branch_isolation() {
    setup();
    let mut m: VerMap<u32, String> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &"base".into()).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feature", main).unwrap();

    // Modify on feature.
    m.insert(feat, &1, &"feature_val".into()).unwrap();
    m.commit(feat).unwrap();

    // Main is unchanged.
    assert_eq!(m.get(main, &1).unwrap(), Some("base".to_string()));
    assert_eq!(m.get(feat, &1).unwrap(), Some("feature_val".to_string()));
}

#[test]
fn branch_inherits_uncommitted_state() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    // Make uncommitted changes on main.
    m.insert(main, &1, &99).unwrap();

    // Branch from main — should see dirty state (99).
    let feat = m.create_branch("feat", main).unwrap();
    assert_eq!(m.get(feat, &1).unwrap(), Some(99));
}

#[test]
fn branch_from_branch() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let b1 = m.create_branch("b1", main).unwrap();
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
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let b1 = m.create_branch("b1", main).unwrap();
    let b2 = m.create_branch("b2", main).unwrap();
    let b3 = m.create_branch("b3", main).unwrap();

    m.insert(b1, &1, &100).unwrap();
    m.insert(b2, &1, &200).unwrap();
    m.insert(b3, &1, &300).unwrap();

    assert_eq!(m.get(main, &1).unwrap(), Some(10));
    assert_eq!(m.get(b1, &1).unwrap(), Some(100));
    assert_eq!(m.get(b2, &1).unwrap(), Some(200));
    assert_eq!(m.get(b3, &1).unwrap(), Some(300));
}

#[test]
fn delete_branch() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let b = m.create_branch("temp", main).unwrap();
    m.delete_branch(b).unwrap();
    assert!(m.get(b, &1).is_err());
}

#[test]
fn cannot_delete_main() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    assert!(m.delete_branch(main).is_err());
}

#[test]
fn duplicate_branch_name_fails() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.create_branch("feat", main).unwrap();
    assert!(m.create_branch("feat", main).is_err());
}

#[test]
fn delete_branch_then_reuse_name() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    let b1 = m.create_branch("reuse", main).unwrap();
    m.insert(b1, &1, &100).unwrap();
    m.commit(b1).unwrap();
    m.delete_branch(b1).unwrap();

    // Should be able to create a new branch with the same name.
    let b2 = m.create_branch("reuse", main).unwrap();
    assert_ne!(b1, b2);
    // New branch doesn't have old data.
    assert_eq!(m.get(b2, &1).unwrap(), None);
}

#[test]
fn create_branch_from_invalid_source() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert!(m.create_branch("bad", 999).is_err());
}

#[test]
fn list_branches() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.create_branch("dev", main).unwrap();
    let branches = m.list_branches();
    assert_eq!(branches.len(), 2);
}

#[test]
fn list_branches_after_delete() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let b = m.create_branch("temp", main).unwrap();
    m.create_branch("keep", main).unwrap();
    m.delete_branch(b).unwrap();
    let branches = m.list_branches();
    assert_eq!(branches.len(), 2); // main + keep
}

#[test]
fn list_many_branches() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    for i in 0..20 {
        m.create_branch(&format!("branch_{i}"), main).unwrap();
    }
    assert_eq!(m.list_branches().len(), 21); // 20 + main
}

// =====================================================================
// Merge
// =====================================================================

#[test]
fn merge_fast_forward() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();

    m.merge(feat, main).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some(10));
    assert_eq!(m.get(main, &2).unwrap(), Some(20));
}

#[test]
fn merge_diverged() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // Common ancestor: key 1 = 10
    m.insert(main, &1, &10).unwrap();
    m.insert(main, &2, &20).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();

    // Feature: change key 1, add key 3.
    m.insert(feat, &1, &11).unwrap();
    m.insert(feat, &3, &30).unwrap();
    m.commit(feat).unwrap();

    // Main: change key 2, add key 4.
    m.insert(main, &2, &22).unwrap();
    m.insert(main, &4, &40).unwrap();
    m.commit(main).unwrap();

    m.merge(feat, main).unwrap();

    // key 1: changed only in feat → 11
    assert_eq!(m.get(main, &1).unwrap(), Some(11));
    // key 2: changed only in main → 22
    assert_eq!(m.get(main, &2).unwrap(), Some(22));
    // key 3: added only in feat → 30
    assert_eq!(m.get(main, &3).unwrap(), Some(30));
    // key 4: added only in main → 40
    assert_eq!(m.get(main, &4).unwrap(), Some(40));
}

#[test]
fn merge_conflict_source_wins() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();

    m.insert(feat, &1, &99).unwrap();
    m.commit(feat).unwrap();

    m.insert(main, &1, &77).unwrap();
    m.commit(main).unwrap();

    // Both changed key 1 → source (feat=99) wins.
    m.merge(feat, main).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some(99));
}

#[test]
fn merge_both_changed_to_same_value() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();

    m.insert(feat, &1, &42).unwrap();
    m.commit(feat).unwrap();

    m.insert(main, &1, &42).unwrap();
    m.commit(main).unwrap();

    // Both changed to same value → keep it.
    m.merge(feat, main).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some(42));
}

#[test]
fn merge_delete_in_source_unchanged_in_target() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.insert(main, &2, &20).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();

    // Source deletes key 1; target doesn't touch it.
    m.remove(feat, &1).unwrap();
    m.commit(feat).unwrap();

    m.merge(feat, main).unwrap();
    // Key 1 was deleted in source, unchanged in target → delete.
    assert_eq!(m.get(main, &1).unwrap(), None);
    assert_eq!(m.get(main, &2).unwrap(), Some(20));
}

#[test]
fn merge_delete_in_target_unchanged_in_source() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.insert(main, &2, &20).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();

    // Target deletes key 1; source doesn't touch it.
    m.remove(main, &1).unwrap();
    m.commit(main).unwrap();

    m.merge(feat, main).unwrap();
    // Key 1 was deleted in target, unchanged in source → delete.
    assert_eq!(m.get(main, &1).unwrap(), None);
    assert_eq!(m.get(main, &2).unwrap(), Some(20));
}

#[test]
fn merge_delete_in_both() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.insert(main, &2, &20).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();

    m.remove(feat, &1).unwrap();
    m.commit(feat).unwrap();

    m.remove(main, &1).unwrap();
    m.commit(main).unwrap();

    m.merge(feat, main).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), None);
    assert_eq!(m.get(main, &2).unwrap(), Some(20));
}

#[test]
fn merge_delete_source_changed_target() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();

    // Source deletes, target changes.
    m.remove(feat, &1).unwrap();
    m.commit(feat).unwrap();

    m.insert(main, &1, &99).unwrap();
    m.commit(main).unwrap();

    // Source deletes, target changes → source wins (delete).
    m.merge(feat, main).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), None);
}

#[test]
fn merge_changed_source_delete_target() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();

    // Source changes, target deletes.
    m.insert(feat, &1, &99).unwrap();
    m.commit(feat).unwrap();

    m.remove(main, &1).unwrap();
    m.commit(main).unwrap();

    // Source changed, target deleted → source wins (keep source's value).
    m.merge(feat, main).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some(99));
}

#[test]
fn merge_add_in_both_same_value() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();

    // Both add the same new key with the same value.
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();

    m.insert(main, &2, &20).unwrap();
    m.commit(main).unwrap();

    m.merge(feat, main).unwrap();
    assert_eq!(m.get(main, &2).unwrap(), Some(20));
}

#[test]
fn merge_add_in_both_different_values() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();

    // Both add the same new key with different values.
    m.insert(feat, &2, &42).unwrap();
    m.commit(feat).unwrap();

    m.insert(main, &2, &99).unwrap();
    m.commit(main).unwrap();

    // Source (feat) wins.
    m.merge(feat, main).unwrap();
    assert_eq!(m.get(main, &2).unwrap(), Some(42));
}

#[test]
fn merge_into_empty_target() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &1, &10).unwrap();
    m.commit(feat).unwrap();

    // Target (main) has no commits.
    m.merge(feat, main).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some(10));
}

#[test]
fn merge_empty_source_fails() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    // feat has a commit (inherited from main), so merge should work.
    // But a truly empty source (no commits at all) should fail.
    let empty = m.create_branch("empty_src", main).unwrap();
    // empty inherits main's head, which is a real commit, so it works.

    // To test truly no commit, we need a branch created before any commits.
    let mut m2: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let b = m2.create_branch("empty", main).unwrap();
    assert!(m2.merge(b, main).is_err());

    // Also test: feat -> main should work normally.
    m.merge(feat, main).unwrap();
    let _ = empty;
}

#[test]
fn merge_into_non_main_branch() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let b1 = m.create_branch("b1", main).unwrap();
    let b2 = m.create_branch("b2", main).unwrap();

    m.insert(b1, &2, &20).unwrap();
    m.commit(b1).unwrap();

    // Merge b1 into b2 (not into main).
    m.merge(b1, b2).unwrap();
    assert_eq!(m.get(b2, &1).unwrap(), Some(10));
    assert_eq!(m.get(b2, &2).unwrap(), Some(20));
    // Main unaffected.
    assert_eq!(m.get(main, &2).unwrap(), None);
}

#[test]
fn merge_creates_merge_commit_with_two_parents() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();

    m.insert(main, &3, &30).unwrap();
    m.commit(main).unwrap();

    let merge_id = m.merge(feat, main).unwrap();
    let commit = m.head_commit(main).unwrap().unwrap();
    assert_eq!(commit.id, merge_id);
    assert_eq!(commit.parents.len(), 2);
}

#[test]
fn sequential_merges() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    // First merge.
    let f1 = m.create_branch("f1", main).unwrap();
    m.insert(f1, &2, &20).unwrap();
    m.commit(f1).unwrap();
    m.merge(f1, main).unwrap();

    // Second merge.
    let f2 = m.create_branch("f2", main).unwrap();
    m.insert(f2, &3, &30).unwrap();
    m.commit(f2).unwrap();
    m.merge(f2, main).unwrap();

    assert_eq!(m.get(main, &1).unwrap(), Some(10));
    assert_eq!(m.get(main, &2).unwrap(), Some(20));
    assert_eq!(m.get(main, &3).unwrap(), Some(30));
}

#[test]
fn merge_large_diverged() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // Common ancestor: 100 keys.
    for i in 0..100u32 {
        m.insert(main, &i, &i).unwrap();
    }
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();

    // Feature modifies even keys.
    for i in (0..100u32).filter(|i| i % 2 == 0) {
        m.insert(feat, &i, &(i + 1000)).unwrap();
    }
    m.commit(feat).unwrap();

    // Main modifies odd keys.
    for i in (0..100u32).filter(|i| i % 2 != 0) {
        m.insert(main, &i, &(i + 2000)).unwrap();
    }
    m.commit(main).unwrap();

    m.merge(feat, main).unwrap();

    for i in 0..100u32 {
        if i % 2 == 0 {
            assert_eq!(m.get(main, &i).unwrap(), Some(i + 1000));
        } else {
            assert_eq!(m.get(main, &i).unwrap(), Some(i + 2000));
        }
    }
}

#[test]
fn merge_with_mixed_add_delete_change() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // Ancestor: keys 1..=5
    for i in 1..=5u32 {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();

    // Feature: delete key 1, change key 2, add key 6.
    m.remove(feat, &1).unwrap();
    m.insert(feat, &2, &99).unwrap();
    m.insert(feat, &6, &60).unwrap();
    m.commit(feat).unwrap();

    // Main: delete key 3, change key 4, add key 7.
    m.remove(main, &3).unwrap();
    m.insert(main, &4, &88).unwrap();
    m.insert(main, &7, &70).unwrap();
    m.commit(main).unwrap();

    m.merge(feat, main).unwrap();

    assert_eq!(m.get(main, &1).unwrap(), None); // deleted in feat, unchanged in main → delete
    assert_eq!(m.get(main, &2).unwrap(), Some(99)); // changed in feat → take feat
    assert_eq!(m.get(main, &3).unwrap(), None); // deleted in main, unchanged in feat → delete
    assert_eq!(m.get(main, &4).unwrap(), Some(88)); // changed in main → take main
    assert_eq!(m.get(main, &5).unwrap(), Some(50)); // unchanged both → keep
    assert_eq!(m.get(main, &6).unwrap(), Some(60)); // added in feat → take
    assert_eq!(m.get(main, &7).unwrap(), Some(70)); // added in main → take
}

// =====================================================================
// Iteration
// =====================================================================

#[test]
fn iter_ordered() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    for i in (0u32..50).rev() {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    let items: Vec<(u32, u32)> = m.iter(main).unwrap().collect();
    assert_eq!(items.len(), 50);
    for (idx, (k, v)) in items.iter().enumerate() {
        assert_eq!(*k, idx as u32);
        assert_eq!(*v, (idx as u32) * 10);
    }
}

#[test]
fn iter_empty_map() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let items: Vec<(u32, u32)> = m.iter(main).unwrap().collect();
    assert!(items.is_empty());
}

#[test]
fn iter_after_commit() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &10).unwrap();
    m.insert(main, &2, &20).unwrap();
    m.commit(main).unwrap();

    let items: Vec<(u32, u32)> = m.iter(main).unwrap().collect();
    assert_eq!(items, vec![(1, 10), (2, 20)]);
}

#[test]
fn iter_on_branch() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.insert(feat, &3, &30).unwrap();

    let items: Vec<(u32, u32)> = m.iter(feat).unwrap().collect();
    assert_eq!(items, vec![(1, 10), (2, 20), (3, 30)]);

    // Main should only have key 1.
    let main_items: Vec<(u32, u32)> = m.iter(main).unwrap().collect();
    assert_eq!(main_items, vec![(1, 10)]);
}

#[test]
fn iter_invalid_branch() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert!(m.iter(999).is_err());
}

// =====================================================================
// Log
// =====================================================================

#[test]
fn commit_log() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &1).unwrap();
    m.commit(main).unwrap();
    m.insert(main, &2, &2).unwrap();
    m.commit(main).unwrap();
    m.insert(main, &3, &3).unwrap();
    m.commit(main).unwrap();

    let log = m.log(main).unwrap();
    assert_eq!(log.len(), 3);
    // Most recent first.
    assert!(log[0].id > log[1].id);
    assert!(log[1].id > log[2].id);
}

#[test]
fn log_empty_branch() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let log = m.log(main).unwrap();
    assert!(log.is_empty());
}

#[test]
fn log_invalid_branch() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert!(m.log(999).is_err());
}

#[test]
fn log_single_commit() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &1).unwrap();
    let c = m.commit(main).unwrap();
    let log = m.log(main).unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0].id, c);
    assert!(log[0].parents.is_empty());
}

#[test]
fn log_branch_has_own_history() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &1).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &2, &2).unwrap();
    m.commit(feat).unwrap();
    m.insert(feat, &3, &3).unwrap();
    m.commit(feat).unwrap();

    let feat_log = m.log(feat).unwrap();
    let main_log = m.log(main).unwrap();

    // Feature has 3 entries (2 own + 1 inherited from main).
    assert_eq!(feat_log.len(), 3);
    // Main has 1.
    assert_eq!(main_log.len(), 1);
}

#[test]
fn log_after_merge_follows_first_parent() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();

    m.insert(main, &3, &30).unwrap();
    let c2 = m.commit(main).unwrap();

    let merge_id = m.merge(feat, main).unwrap();

    let log = m.log(main).unwrap();
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
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &1).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &2, &2).unwrap();
    m.commit(feat).unwrap();
    m.delete_branch(feat).unwrap();

    m.gc();

    // Main data unaffected.
    assert_eq!(m.get(main, &1).unwrap(), Some(1));
}

#[test]
fn gc_preserves_shared_ancestor_commits() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();

    m.insert(main, &3, &30).unwrap();
    m.commit(main).unwrap();

    m.gc();

    // Both branches' data intact.
    assert_eq!(m.get(main, &1).unwrap(), Some(10));
    assert_eq!(m.get(main, &3).unwrap(), Some(30));
    assert_eq!(m.get(feat, &1).unwrap(), Some(10));
    assert_eq!(m.get(feat, &2).unwrap(), Some(20));
    // Shared ancestor commit survives.
    assert_eq!(m.get_at_commit(c1, &1).unwrap(), Some(10));
}

#[test]
fn gc_with_uncommitted_dirty_state() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    // Uncommitted changes.
    m.insert(main, &2, &20).unwrap();

    // GC should preserve dirty root.
    m.gc();

    assert_eq!(m.get(main, &1).unwrap(), Some(10));
    assert_eq!(m.get(main, &2).unwrap(), Some(20));
}

#[test]
fn gc_multiple_times_is_idempotent() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();
    m.delete_branch(feat).unwrap();

    m.gc();
    m.gc();
    m.gc();

    assert_eq!(m.get(main, &1).unwrap(), Some(10));
}

// =====================================================================
// Ref-count GC
// =====================================================================

#[test]
fn delete_branch_refcount_cleanup() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &2, &20).unwrap();
    let fc = m.commit(feat).unwrap();

    // delete_branch cascades ref-count to zero → commit removed.
    m.delete_branch(feat).unwrap();
    assert!(m.get_commit(fc).is_none());

    // Main data unaffected.
    assert_eq!(m.get(main, &1).unwrap(), Some(10));
}

#[test]
fn delete_branch_preserves_shared_ancestors() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();

    m.insert(main, &3, &30).unwrap();
    m.commit(main).unwrap();

    // Delete feat — shared ancestor c1 must survive.
    m.delete_branch(feat).unwrap();

    assert_eq!(m.get(main, &1).unwrap(), Some(10));
    assert_eq!(m.get(main, &3).unwrap(), Some(30));
    assert_eq!(m.get_at_commit(c1, &1).unwrap(), Some(10));
}

#[test]
fn delete_branch_multiple_branches_cleanup() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    for i in 0u32..20 {
        let name = format!("feat_{}", i);
        let br = m.create_branch(&name, main).unwrap();
        m.insert(br, &(100 + i), &i).unwrap();
        m.commit(br).unwrap();
        m.delete_branch(br).unwrap();
    }

    assert_eq!(m.get(main, &1).unwrap(), Some(10));
}

#[test]
fn gc_idempotent() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();
    m.delete_branch(feat).unwrap();

    m.gc();
    m.gc();

    assert_eq!(m.get(main, &1).unwrap(), Some(10));
}

#[test]
fn refcount_after_create_branch() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();
    // c1: ref_count = 1 (main HEAD)
    assert_eq!(m.get_commit(c1).unwrap().ref_count, 1);

    let _feat = m.create_branch("feat", main).unwrap();
    // c1: ref_count = 2 (main HEAD + feat HEAD)
    assert_eq!(m.get_commit(c1).unwrap().ref_count, 2);
}

#[test]
fn refcount_after_commit() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();
    // c1 is the branch HEAD → ref_count = 1
    assert_eq!(m.get_commit(c1).unwrap().ref_count, 1);

    m.insert(main, &2, &20).unwrap();
    let c2 = m.commit(main).unwrap();
    // c2: ref_count = 1 (branch HEAD)
    // c1: still ref_count = 1 (lost branch-HEAD, gained parent-link from c2)
    assert_eq!(m.get_commit(c2).unwrap().ref_count, 1);
    assert_eq!(m.get_commit(c1).unwrap().ref_count, 1);
}

#[test]
fn refcount_cascade_long_chain() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    let feat = m.create_branch("feat", main).unwrap();
    let mut ids = Vec::new();
    for i in 0u32..1000 {
        m.insert(feat, &i, &i).unwrap();
        ids.push(m.commit(feat).unwrap());
    }

    // Delete the branch → entire 1000-commit chain should cascade.
    m.delete_branch(feat).unwrap();

    // All commits should be gone.
    for id in &ids {
        assert!(m.get_commit(*id).is_none());
    }
}

#[test]
fn rollback_decrements_abandoned_commits() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();
    m.insert(main, &2, &20).unwrap();
    let c2 = m.commit(main).unwrap();
    m.insert(main, &3, &30).unwrap();
    let c3 = m.commit(main).unwrap();

    // Rollback to c1 — c2 and c3 become orphaned.
    m.rollback_to(main, c1).unwrap();

    // c1 survives (branch HEAD).
    assert!(m.get_commit(c1).is_some());
    // c2 and c3 were exclusively on this branch → deleted.
    assert!(m.get_commit(c2).is_none());
    assert!(m.get_commit(c3).is_none());
}

#[test]
fn gc_after_serialize_roundtrip() {
    // After serialize→deserialize, ref_counts are preserved and gc()
    // is idempotent.  The deserialized map also has ref_counts_ready=false
    // on the B+ tree (serde skip), so gc() rebuilds the node ref map.
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &2, &20).unwrap();
    let c2 = m.commit(feat).unwrap();

    let bytes = postcard::to_allocvec(&m).unwrap();
    let mut m2: VerMap<u32, u32> = postcard::from_bytes(&bytes).unwrap();

    // gc() rebuilds the in-memory node ref counts and leaves data intact.
    m2.gc();

    assert_eq!(m2.get(main, &1).unwrap(), Some(10));
    assert_eq!(m2.get(feat, &2).unwrap(), Some(20));

    // c1: ref_count=2 (main HEAD + feat's parent-link through c1)
    assert_eq!(m2.get_commit(c1).unwrap().ref_count, 2);
    // c2: ref_count=1 (feat HEAD)
    assert_eq!(m2.get_commit(c2).unwrap().ref_count, 1);

    // delete_branch on the deserialized map works correctly.
    m2.delete_branch(feat).unwrap();
    assert!(m2.get_commit(c2).is_none());
    assert_eq!(m2.get_commit(c1).unwrap().ref_count, 1);
    assert_eq!(m2.get(main, &1).unwrap(), Some(10));
}

// =====================================================================
// Many entries (stress test for splits/rebalancing through versioning)
// =====================================================================

#[test]
fn stress_versioned() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let n = 500u32;

    for i in 0..n {
        m.insert(main, &i, &i).unwrap();
    }
    let c1 = m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
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
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let mut commits = Vec::new();

    for i in 0..100u32 {
        m.insert(main, &i, &i).unwrap();
        commits.push(m.commit(main).unwrap());
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
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &0, &0).unwrap();
    m.commit(main).unwrap();

    // Create and delete many branches.
    for i in 0..50u32 {
        let b = m.create_branch(&format!("b{i}"), main).unwrap();
        let key = i + 1;
        m.insert(b, &key, &key).unwrap();
        m.commit(b).unwrap();
        m.delete_branch(b).unwrap();
    }

    m.gc();

    // Main unaffected.
    assert_eq!(m.get(main, &0).unwrap(), Some(0));
    // Branch-only keys are gone from main.
    for i in 1..=50u32 {
        assert_eq!(m.get(main, &i).unwrap(), None);
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
    let m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    assert_eq!(m.branch_id("main"), Some(main));
}

#[test]
fn branch_id_custom_branch() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let feat = m.create_branch("feature", main).unwrap();
    assert_eq!(m.branch_id("feature"), Some(feat));
}

#[test]
fn branch_id_nonexistent() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert_eq!(m.branch_id("no_such_branch"), None);
}

#[test]
fn branch_id_after_delete() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let b = m.create_branch("temp", main).unwrap();
    assert_eq!(m.branch_id("temp"), Some(b));
    m.delete_branch(b).unwrap();
    assert_eq!(m.branch_id("temp"), None);
}

#[test]
fn branch_id_reused_name() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let b1 = m.create_branch("reuse", main).unwrap();
    m.delete_branch(b1).unwrap();
    let b2 = m.create_branch("reuse", main).unwrap();
    assert_eq!(m.branch_id("reuse"), Some(b2));
    assert_ne!(b1, b2);
}

// --- branch_name ---

#[test]
fn branch_name_main() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    assert_eq!(m.branch_name(main), Some("main".to_string()));
}

#[test]
fn branch_name_custom() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let feat = m.create_branch("my-feature", main).unwrap();
    assert_eq!(m.branch_name(feat), Some("my-feature".to_string()));
}

#[test]
fn branch_name_nonexistent() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert_eq!(m.branch_name(999), None);
}

#[test]
fn branch_name_after_delete() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let b = m.create_branch("ephemeral", main).unwrap();
    assert_eq!(m.branch_name(b), Some("ephemeral".to_string()));
    m.delete_branch(b).unwrap();
    assert_eq!(m.branch_name(b), None);
}

#[test]
fn branch_id_and_name_roundtrip() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let names = ["alpha", "beta", "gamma"];
    for name in &names {
        m.create_branch(name, main).unwrap();
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
    let m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    // No commits and no inserts → no uncommitted changes.
    assert!(!m.has_uncommitted(main).unwrap());
}

#[test]
fn has_uncommitted_after_insert() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &10).unwrap();
    assert!(m.has_uncommitted(main).unwrap());
}

#[test]
fn has_uncommitted_after_commit() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();
    assert!(!m.has_uncommitted(main).unwrap());
}

#[test]
fn has_uncommitted_after_commit_then_modify() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();
    m.insert(main, &2, &20).unwrap();
    assert!(m.has_uncommitted(main).unwrap());
}

#[test]
fn has_uncommitted_after_discard() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();
    m.insert(main, &2, &20).unwrap();
    assert!(m.has_uncommitted(main).unwrap());
    m.discard(main).unwrap();
    assert!(!m.has_uncommitted(main).unwrap());
}

#[test]
fn has_uncommitted_on_new_branch() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();
    let feat = m.create_branch("feat", main).unwrap();
    // New branch starts clean (same root as parent head).
    assert!(!m.has_uncommitted(feat).unwrap());
    m.insert(feat, &2, &20).unwrap();
    assert!(m.has_uncommitted(feat).unwrap());
}

#[test]
fn has_uncommitted_invalid_branch() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert!(m.has_uncommitted(999).is_err());
}

// --- range ---

#[test]
fn range_full_unbounded() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    for i in 1..=5u32 {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    let items: Vec<_> = m
        .range(main, Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .collect();
    assert_eq!(items.len(), 5);
    assert_eq!(items[0], (1, 10));
    assert_eq!(items[4], (5, 50));
}

#[test]
fn range_included_both() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    for i in 1..=10u32 {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    let items: Vec<_> = m
        .range(main, Bound::Included(&3), Bound::Included(&7))
        .unwrap()
        .collect();
    assert_eq!(items.len(), 5);
    assert_eq!(items[0].0, 3);
    assert_eq!(items[4].0, 7);
}

#[test]
fn range_excluded_both() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    for i in 1..=10u32 {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    let items: Vec<_> = m
        .range(main, Bound::Excluded(&3), Bound::Excluded(&7))
        .unwrap()
        .collect();
    assert_eq!(items.len(), 3); // 4, 5, 6
    assert_eq!(items[0].0, 4);
    assert_eq!(items[2].0, 6);
}

#[test]
fn range_included_lo_excluded_hi() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    for i in 1..=10u32 {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    // [3, 7) → 3, 4, 5, 6
    let items: Vec<_> = m
        .range(main, Bound::Included(&3), Bound::Excluded(&7))
        .unwrap()
        .collect();
    assert_eq!(items.len(), 4);
    assert_eq!(items[0].0, 3);
    assert_eq!(items[3].0, 6);
}

#[test]
fn range_excluded_lo_included_hi() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    for i in 1..=10u32 {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    // (3, 7] → 4, 5, 6, 7
    let items: Vec<_> = m
        .range(main, Bound::Excluded(&3), Bound::Included(&7))
        .unwrap()
        .collect();
    assert_eq!(items.len(), 4);
    assert_eq!(items[0].0, 4);
    assert_eq!(items[3].0, 7);
}

#[test]
fn range_unbounded_lo() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    for i in 1..=10u32 {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    // [.., 5] → 1, 2, 3, 4, 5
    let items: Vec<_> = m
        .range(main, Bound::Unbounded, Bound::Included(&5))
        .unwrap()
        .collect();
    assert_eq!(items.len(), 5);
    assert_eq!(items[0].0, 1);
    assert_eq!(items[4].0, 5);
}

#[test]
fn range_unbounded_hi() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    for i in 1..=10u32 {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    // [6, ..] → 6, 7, 8, 9, 10
    let items: Vec<_> = m
        .range(main, Bound::Included(&6), Bound::Unbounded)
        .unwrap()
        .collect();
    assert_eq!(items.len(), 5);
    assert_eq!(items[0].0, 6);
    assert_eq!(items[4].0, 10);
}

#[test]
fn range_empty_result() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    for i in 1..=5u32 {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    // Range with no matching keys.
    let items: Vec<_> = m
        .range(main, Bound::Included(&100), Bound::Included(&200))
        .unwrap()
        .collect();
    assert!(items.is_empty());
}

#[test]
fn range_empty_map() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let items: Vec<_> = m
        .range(main, Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .collect();
    assert!(items.is_empty());
}

#[test]
fn range_on_branch() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    for i in 1..=10u32 {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
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
        .range(main, Bound::Included(&9), Bound::Unbounded)
        .unwrap()
        .collect();
    assert_eq!(main_items.len(), 2); // 9, 10
}

#[test]
fn range_invalid_branch() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert!(m.range(999, Bound::Unbounded, Bound::Unbounded).is_err());
}

#[test]
fn range_single_key() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    for i in 1..=10u32 {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    // Exact single key: [5, 5]
    let items: Vec<_> = m
        .range(main, Bound::Included(&5), Bound::Included(&5))
        .unwrap()
        .collect();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0], (5, 50));
}

// --- iter_at_commit ---

#[test]
fn iter_at_commit_basic() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &10).unwrap();
    m.insert(main, &2, &20).unwrap();
    let c1 = m.commit(main).unwrap();

    m.insert(main, &3, &30).unwrap();
    let c2 = m.commit(main).unwrap();

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
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &10).unwrap();
    m.insert(main, &2, &20).unwrap();
    let c1 = m.commit(main).unwrap();

    m.remove(main, &1).unwrap();
    let c2 = m.commit(main).unwrap();

    let items1: Vec<_> = m.iter_at_commit(c1).unwrap().collect();
    assert_eq!(items1.len(), 2);

    let items2: Vec<_> = m.iter_at_commit(c2).unwrap().collect();
    assert_eq!(items2.len(), 1);
    assert_eq!(items2[0], (2, 20));
}

#[test]
fn iter_at_commit_on_branch_commit() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &2, &20).unwrap();
    let fc = m.commit(feat).unwrap();

    // iter_at_commit works with any commit, regardless of branch.
    let items: Vec<_> = m.iter_at_commit(fc).unwrap().collect();
    assert_eq!(items, vec![(1, 10), (2, 20)]);
}

#[test]
fn iter_at_commit_invalid() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert!(m.iter_at_commit(999).is_err());
}

#[test]
fn iter_at_commit_ordered() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    // Insert in reverse order.
    for i in (1..=50u32).rev() {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    let c = m.commit(main).unwrap();

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
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    // Commit with nothing in it (empty tree).
    // First add and remove so we have a commit with empty state.
    m.insert(main, &1, &10).unwrap();
    m.remove(main, &1).unwrap();
    let c = m.commit(main).unwrap();

    let items: Vec<_> = m.iter_at_commit(c).unwrap().collect();
    assert!(items.is_empty());
}

// --- range_at_commit ---

#[test]
fn range_at_commit_basic() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    for i in 0..10u32 {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    let c1 = m.commit(main).unwrap();

    // Mutate after c1.
    m.insert(main, &5, &999).unwrap();
    m.insert(main, &10, &100).unwrap();
    m.commit(main).unwrap();

    // range_at_commit sees c1's state, not the latest.
    let items: Vec<(u32, u32)> = m
        .range_at_commit(c1, Bound::Included(&3), Bound::Excluded(&7))
        .unwrap()
        .collect();
    assert_eq!(items, vec![(3, 30), (4, 40), (5, 50), (6, 60)]);
}

#[test]
fn range_at_commit_empty_range() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let c = m.commit(main).unwrap();

    let items: Vec<(u32, u32)> = m
        .range_at_commit(c, Bound::Included(&100), Bound::Excluded(&200))
        .unwrap()
        .collect();
    assert!(items.is_empty());
}

#[test]
fn range_at_commit_unbounded() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    for i in 1..=5u32 {
        m.insert(main, &i, &i).unwrap();
    }
    let c = m.commit(main).unwrap();

    // Unbounded on both sides = full scan (same as iter_at_commit).
    let items: Vec<(u32, u32)> = m
        .range_at_commit(c, Bound::Unbounded, Bound::Unbounded)
        .unwrap()
        .collect();
    assert_eq!(items.len(), 5);

    // Lower-bounded only.
    let items: Vec<(u32, u32)> = m
        .range_at_commit(c, Bound::Excluded(&3), Bound::Unbounded)
        .unwrap()
        .collect();
    assert_eq!(items, vec![(4, 4), (5, 5)]);
}

#[test]
fn range_at_commit_invalid_commit() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert!(
        m.range_at_commit(999, Bound::Unbounded, Bound::Unbounded)
            .is_err()
    );
}

#[test]
fn range_at_commit_on_branch() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.insert(main, &2, &20).unwrap();
    m.commit(main).unwrap();

    let fork = m.create_branch("fork", main).unwrap();
    m.insert(fork, &3, &30).unwrap();
    m.insert(fork, &4, &40).unwrap();
    let fc = m.commit(fork).unwrap();

    let items: Vec<(u32, u32)> = m
        .range_at_commit(fc, Bound::Included(&2), Bound::Included(&4))
        .unwrap()
        .collect();
    assert_eq!(items, vec![(2, 20), (3, 30), (4, 40)]);
}

// --- contains_key_at_commit ---

#[test]
fn contains_key_at_commit_basic() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();

    m.insert(main, &2, &20).unwrap();
    m.remove(main, &1).unwrap();
    let c2 = m.commit(main).unwrap();

    // At c1: key 1 exists, key 2 does not.
    assert!(m.contains_key_at_commit(c1, &1).unwrap());
    assert!(!m.contains_key_at_commit(c1, &2).unwrap());

    // At c2: key 1 was removed, key 2 was added.
    assert!(!m.contains_key_at_commit(c2, &1).unwrap());
    assert!(m.contains_key_at_commit(c2, &2).unwrap());
}

#[test]
fn contains_key_at_commit_nonexistent_key() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &1).unwrap();
    let c = m.commit(main).unwrap();

    assert!(!m.contains_key_at_commit(c, &999).unwrap());
}

#[test]
fn contains_key_at_commit_invalid_commit() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert!(m.contains_key_at_commit(999, &1).is_err());
}

#[test]
fn contains_key_at_commit_on_branch() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let fork = m.create_branch("fork", main).unwrap();
    m.insert(fork, &2, &20).unwrap();
    let fc = m.commit(fork).unwrap();

    // Fork commit has both keys.
    assert!(m.contains_key_at_commit(fc, &1).unwrap());
    assert!(m.contains_key_at_commit(fc, &2).unwrap());

    // Main's latest commit does not have key 2.
    let mc = m.head_commit(main).unwrap().unwrap().id;
    assert!(m.contains_key_at_commit(mc, &1).unwrap());
    assert!(!m.contains_key_at_commit(mc, &2).unwrap());
}

#[test]
fn get_commit_basic() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &10).unwrap();
    let c = m.commit(main).unwrap();

    let commit = m.get_commit(c).unwrap();
    assert_eq!(commit.id, c);
    assert!(commit.parents.is_empty()); // First commit has no parents.
    assert!(commit.timestamp_us > 0);
}

#[test]
fn get_commit_nonexistent() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert!(m.get_commit(999).is_none());
}

#[test]
fn get_commit_with_parent() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();

    m.insert(main, &2, &20).unwrap();
    let c2 = m.commit(main).unwrap();

    let commit2 = m.get_commit(c2).unwrap();
    assert_eq!(commit2.id, c2);
    assert_eq!(commit2.parents, vec![c1]);
}

#[test]
fn get_commit_merge_commit() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();

    m.insert(main, &3, &30).unwrap();
    m.commit(main).unwrap();

    let merge_id = m.merge(feat, main).unwrap();
    let merge_commit = m.get_commit(merge_id).unwrap();
    assert_eq!(merge_commit.id, merge_id);
    assert_eq!(merge_commit.parents.len(), 2);
}

#[test]
fn get_commit_timestamp_uss_increase() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &1).unwrap();
    let c1 = m.commit(main).unwrap();
    m.insert(main, &2, &2).unwrap();
    let c2 = m.commit(main).unwrap();

    let t1 = m.get_commit(c1).unwrap().timestamp_us;
    let t2 = m.get_commit(c2).unwrap().timestamp_us;
    assert!(t2 >= t1);
}

#[test]
fn get_commit_matches_head_commit() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &10).unwrap();
    let c = m.commit(main).unwrap();

    let via_get = m.get_commit(c).unwrap();
    let via_head = m.head_commit(main).unwrap().unwrap();
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
    let mut m: VerMap<String, String> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &"hello".to_string(), &"world".to_string())
        .unwrap();
    m.commit(main).unwrap();
    assert_eq!(
        m.get(main, &"hello".to_string()).unwrap(),
        Some("world".to_string())
    );
}

#[test]
fn u64_keys() {
    setup();
    let mut m: VerMap<u64, u64> = VerMap::new();
    let main = m.main_branch();
    let max = u64::MAX;
    let mid = u64::MAX / 2;
    m.insert(main, &max, &42).unwrap();
    m.insert(main, &0, &0).unwrap();
    m.insert(main, &mid, &21).unwrap();
    m.commit(main).unwrap();

    assert_eq!(m.get(main, &max).unwrap(), Some(42));
    assert_eq!(m.get(main, &0).unwrap(), Some(0));

    // Iteration should be in ascending order.
    let items: Vec<(u64, u64)> = m.iter(main).unwrap().collect();
    assert_eq!(items.len(), 3);
    assert!(items[0].0 < items[1].0);
    assert!(items[1].0 < items[2].0);
}

// =====================================================================
// main_branch, new_with_main, set_main_branch
// =====================================================================

#[test]
fn main_branch_default_is_one() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    assert_eq!(main, 1);
}

#[test]
fn new_with_main_custom_name() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new_with_main("genesis");
    let main = m.main_branch();

    // The initial branch works normally.
    m.insert(main, &1, &100).unwrap();
    m.commit(main).unwrap();
    assert_eq!(m.get(main, &1).unwrap(), Some(100));

    // Listed branch has the custom name.
    let branches = m.list_branches();
    assert_eq!(branches.len(), 1);
    assert_eq!(branches[0].1, "genesis");
}

#[test]
fn new_with_main_cannot_delete_initial() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new_with_main("canonical");
    let main = m.main_branch();
    // Cannot remove the initial main, regardless of name.
    assert!(m.clone().delete_branch(main).is_err());
}

#[test]
fn set_main_branch_switches_protection() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let old_main = main;

    // Create a fork.
    m.insert(main, &1, &1).unwrap();
    m.commit(main).unwrap();
    let fork = m.create_branch("fork", main).unwrap();

    // Promote fork to main.
    m.set_main_branch(fork).unwrap();
    assert_eq!(m.main_branch(), fork);

    // Old main is now deletable.
    m.delete_branch(old_main).unwrap();

    // New main is protected.
    assert!(m.delete_branch(fork).is_err());
}

#[test]
fn set_main_branch_nonexistent_fails() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let _main = m.main_branch();
    assert!(m.set_main_branch(999).is_err());
}

#[test]
fn set_main_branch_idempotent() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    // Setting main to itself is a no-op.
    m.set_main_branch(main).unwrap();
    assert_eq!(m.main_branch(), main);
}

#[test]
fn set_main_branch_persists_across_operations() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let fork = m.create_branch("fork", main).unwrap();
    m.insert(fork, &2, &20).unwrap();
    m.commit(fork).unwrap();

    m.set_main_branch(fork).unwrap();

    // Subsequent operations respect the new main.
    let another = m.create_branch("another", fork).unwrap();
    m.insert(another, &3, &30).unwrap();
    m.commit(another).unwrap();

    // Can delete old main and non-main branches.
    m.delete_branch(main).unwrap();
    m.delete_branch(another).unwrap();

    // Cannot delete the new main.
    assert!(m.delete_branch(fork).is_err());
    assert_eq!(m.main_branch(), fork);
}

#[test]
fn set_main_branch_divergent_switch() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // Build primary branch: b0 -> b1 -> b2
    m.insert(main, &0, &0).unwrap();
    let b0 = m.commit(main).unwrap();
    m.insert(main, &1, &1).unwrap();
    m.commit(main).unwrap();
    m.insert(main, &2, &2).unwrap();
    let b2 = m.commit(main).unwrap();

    // Competing fork from b0, longer: b0 -> f1 -> f2 -> f3
    let fork = m.create_branch("fork", main).unwrap();
    m.rollback_to(fork, b0).unwrap();
    for i in 10..13u32 {
        m.insert(fork, &i, &i).unwrap();
        m.commit(fork).unwrap();
    }
    let fork_tip = m.head_commit(fork).unwrap().unwrap().id;

    // Fork is longer — switch primary branch.
    let lca = m.fork_point(b2, fork_tip).unwrap();
    let main_len = m.commit_distance(b2, lca).unwrap();
    let fork_len = m.commit_distance(fork_tip, lca).unwrap();
    assert!(fork_len > main_len);

    // Switch primary branch.
    m.set_main_branch(fork).unwrap();
    assert_eq!(m.main_branch(), fork);

    // Old branch is now deletable.
    m.delete_branch(main).unwrap();
}

// =====================================================================
// fork_point & commit_distance
// =====================================================================

#[test]
fn fork_point_basic_diverge() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // Main: c1 -> c2 -> c3
    m.insert(main, &1, &10).unwrap();
    let _c1 = m.commit(main).unwrap();
    m.insert(main, &2, &20).unwrap();
    let c2 = m.commit(main).unwrap();
    m.insert(main, &3, &30).unwrap();
    let c3 = m.commit(main).unwrap();

    // Fork at c2: c2 -> f1 -> f2 -> f3 -> f4
    let fork = m.create_branch("fork", main).unwrap();
    m.rollback_to(fork, c2).unwrap();
    m.insert(fork, &10, &100).unwrap();
    let _f1 = m.commit(fork).unwrap();
    m.insert(fork, &11, &110).unwrap();
    let _f2 = m.commit(fork).unwrap();
    m.insert(fork, &12, &120).unwrap();
    let _f3 = m.commit(fork).unwrap();
    m.insert(fork, &13, &130).unwrap();
    let f4 = m.commit(fork).unwrap();

    let lca = m.fork_point(c3, f4).unwrap();
    assert_eq!(lca, c2);

    // main ahead by 1, fork ahead by 4
    assert_eq!(m.commit_distance(c3, c2), Some(1));
    assert_eq!(m.commit_distance(f4, c2), Some(4));
}

#[test]
fn fork_point_same_commit() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();

    // fork_point of a commit with itself should be itself
    assert_eq!(m.fork_point(c1, c1), Some(c1));
}

#[test]
fn fork_point_linear_ancestor() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // c1 -> c2 -> c3, all on main, no branching
    m.insert(main, &1, &1).unwrap();
    let c1 = m.commit(main).unwrap();
    m.insert(main, &2, &2).unwrap();
    let c2 = m.commit(main).unwrap();
    m.insert(main, &3, &3).unwrap();
    let c3 = m.commit(main).unwrap();

    // LCA of c3 and c1 in a linear chain should be c1
    assert_eq!(m.fork_point(c3, c1), Some(c1));
    assert_eq!(m.fork_point(c1, c3), Some(c1));

    // LCA of c3 and c2 should be c2
    assert_eq!(m.fork_point(c3, c2), Some(c2));
}

#[test]
fn fork_point_after_merge() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // Main: c1 -> c2
    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();
    m.insert(main, &2, &20).unwrap();
    let _c2 = m.commit(main).unwrap();

    // Fork at c1, add one commit
    let fork = m.create_branch("fork", main).unwrap();
    m.rollback_to(fork, c1).unwrap();
    m.insert(fork, &3, &30).unwrap();
    m.commit(fork).unwrap();

    // Merge fork into main -> creates merge commit
    let merge_id = m.merge(fork, main).unwrap();

    // Continue on main after merge
    m.insert(main, &4, &40).unwrap();
    let c_post = m.commit(main).unwrap();

    // fork_point between post-merge commit and merge commit itself
    assert_eq!(m.fork_point(c_post, merge_id), Some(merge_id));
    assert_eq!(m.commit_distance(c_post, merge_id), Some(1));
}

#[test]
fn fork_point_multiple_forks() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // Main: c1 -> c2 -> c3
    m.insert(main, &1, &1).unwrap();
    let c1 = m.commit(main).unwrap();
    m.insert(main, &2, &2).unwrap();
    let c2 = m.commit(main).unwrap();
    m.insert(main, &3, &3).unwrap();
    let c3 = m.commit(main).unwrap();

    // Fork A from c1: c1 -> a1 -> a2
    let fa = m.create_branch("fa", main).unwrap();
    m.rollback_to(fa, c1).unwrap();
    m.insert(fa, &10, &10).unwrap();
    m.commit(fa).unwrap();
    m.insert(fa, &11, &11).unwrap();
    let a2 = m.commit(fa).unwrap();

    // Fork B from c2: c2 -> b1 -> b2 -> b3
    let fb = m.create_branch("fb", main).unwrap();
    m.rollback_to(fb, c2).unwrap();
    m.insert(fb, &20, &20).unwrap();
    m.commit(fb).unwrap();
    m.insert(fb, &21, &21).unwrap();
    m.commit(fb).unwrap();
    m.insert(fb, &22, &22).unwrap();
    let b3 = m.commit(fb).unwrap();

    // fork_point(a2, b3) should be c1 (deepest common ancestor)
    assert_eq!(m.fork_point(a2, b3), Some(c1));

    // fork_point(c3, a2) should be c1
    assert_eq!(m.fork_point(c3, a2), Some(c1));

    // fork_point(c3, b3) should be c2
    assert_eq!(m.fork_point(c3, b3), Some(c2));

    // Compare fork lengths vs main
    let lca_a = m.fork_point(c3, a2).unwrap();
    let lca_b = m.fork_point(c3, b3).unwrap();
    // Main is 2 ahead of c1, fork A is 2 ahead of c1
    assert_eq!(m.commit_distance(c3, lca_a), Some(2));
    assert_eq!(m.commit_distance(a2, lca_a), Some(2));
    // Main is 1 ahead of c2, fork B is 3 ahead of c2
    assert_eq!(m.commit_distance(c3, lca_b), Some(1));
    assert_eq!(m.commit_distance(b3, lca_b), Some(3));
}

#[test]
fn fork_point_symmetric() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &1).unwrap();
    let c1 = m.commit(main).unwrap();

    let fork = m.create_branch("fork", main).unwrap();
    m.insert(fork, &2, &2).unwrap();
    let f1 = m.commit(fork).unwrap();

    m.insert(main, &3, &3).unwrap();
    let c2 = m.commit(main).unwrap();

    // fork_point should be symmetric
    assert_eq!(m.fork_point(c2, f1), m.fork_point(f1, c2));
    assert_eq!(m.fork_point(c2, f1), Some(c1));
}

#[test]
fn fork_point_nonexistent_commit() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &1).unwrap();
    let c1 = m.commit(main).unwrap();

    // Nonexistent commit ID should yield None
    assert_eq!(m.fork_point(c1, 99999), None);
    assert_eq!(m.fork_point(99999, c1), None);
}

#[test]
fn commit_distance_self_is_zero() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &1).unwrap();
    let c1 = m.commit(main).unwrap();
    m.insert(main, &2, &2).unwrap();
    let c2 = m.commit(main).unwrap();

    assert_eq!(m.commit_distance(c1, c1), Some(0));
    assert_eq!(m.commit_distance(c2, c2), Some(0));
}

#[test]
fn commit_distance_linear_chain() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &1).unwrap();
    let c1 = m.commit(main).unwrap();
    m.insert(main, &2, &2).unwrap();
    let c2 = m.commit(main).unwrap();
    m.insert(main, &3, &3).unwrap();
    let c3 = m.commit(main).unwrap();
    m.insert(main, &4, &4).unwrap();
    let c4 = m.commit(main).unwrap();
    m.insert(main, &5, &5).unwrap();
    let c5 = m.commit(main).unwrap();

    assert_eq!(m.commit_distance(c5, c1), Some(4));
    assert_eq!(m.commit_distance(c5, c2), Some(3));
    assert_eq!(m.commit_distance(c5, c3), Some(2));
    assert_eq!(m.commit_distance(c5, c4), Some(1));
    assert_eq!(m.commit_distance(c5, c5), Some(0));
    assert_eq!(m.commit_distance(c3, c1), Some(2));
    assert_eq!(m.commit_distance(c2, c1), Some(1));
}

#[test]
fn commit_distance_not_ancestor_returns_none() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // c1 -> c2 on main
    m.insert(main, &1, &1).unwrap();
    let c1 = m.commit(main).unwrap();
    m.insert(main, &2, &2).unwrap();
    let c2 = m.commit(main).unwrap();

    // Fork at c1: f1
    let fork = m.create_branch("fork", main).unwrap();
    m.rollback_to(fork, c1).unwrap();
    m.insert(fork, &10, &10).unwrap();
    let f1 = m.commit(fork).unwrap();

    // c2 and f1 are siblings, neither is ancestor of the other
    assert_eq!(m.commit_distance(c2, f1), None);
    assert_eq!(m.commit_distance(f1, c2), None);

    // reversed direction: ancestor -> descendant returns None
    assert_eq!(m.commit_distance(c1, c2), None);
}

#[test]
fn commit_distance_nonexistent_commit() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &1).unwrap();
    let c1 = m.commit(main).unwrap();

    // from is nonexistent
    assert_eq!(m.commit_distance(99999, c1), None);

    // ancestor is nonexistent (can never be reached)
    assert_eq!(m.commit_distance(c1, 99999), None);
}

#[test]
fn commit_distance_after_merge_follows_first_parent() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // Main: c1 -> c2 -> c3
    m.insert(main, &1, &1).unwrap();
    let c1 = m.commit(main).unwrap();
    m.insert(main, &2, &2).unwrap();
    let c2 = m.commit(main).unwrap();
    m.insert(main, &3, &3).unwrap();
    let c3 = m.commit(main).unwrap();

    // Fork from c1: f1 -> f2
    let fork = m.create_branch("fork", main).unwrap();
    m.rollback_to(fork, c1).unwrap();
    m.insert(fork, &10, &10).unwrap();
    let _f1 = m.commit(fork).unwrap();
    m.insert(fork, &11, &11).unwrap();
    let _f2 = m.commit(fork).unwrap();

    // Merge fork into main -> merge commit M
    // M.parents = [c3 (target/first-parent), f2 (source)]
    let merge = m.merge(fork, main).unwrap();

    // commit_distance follows first-parent, so M -> c3 -> c2 -> c1
    assert_eq!(m.commit_distance(merge, c3), Some(1));
    assert_eq!(m.commit_distance(merge, c2), Some(2));
    assert_eq!(m.commit_distance(merge, c1), Some(3));

    // The merge parent (f2) is NOT on the first-parent chain
    assert_eq!(m.commit_distance(merge, _f2), None);
}

#[test]
fn longest_branch_scenario() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // Main branch (primary): b0 -> b1 -> b2 -> b3 -> b4
    m.insert(main, &0, &0).unwrap();
    let b0 = m.commit(main).unwrap();
    m.insert(main, &1, &1).unwrap();
    let b1 = m.commit(main).unwrap();
    m.insert(main, &2, &2).unwrap();
    let _b2 = m.commit(main).unwrap();
    m.insert(main, &3, &3).unwrap();
    let _b3 = m.commit(main).unwrap();
    m.insert(main, &4, &4).unwrap();
    let b4 = m.commit(main).unwrap();

    // Fork A diverges at b1: b1 -> a1 -> a2  (2 commits)
    let chain_a = m.create_branch("chain_a", main).unwrap();
    m.rollback_to(chain_a, b1).unwrap();
    m.insert(chain_a, &100, &100).unwrap();
    m.commit(chain_a).unwrap();
    m.insert(chain_a, &101, &101).unwrap();
    let a_tip = m.commit(chain_a).unwrap();

    // Fork B diverges at b0: b0 -> x1 -> x2 -> x3 -> x4 -> x5 -> x6  (6 commits)
    let chain_b = m.create_branch("chain_b", main).unwrap();
    m.rollback_to(chain_b, b0).unwrap();
    for i in 200..206 {
        m.insert(chain_b, &i, &i).unwrap();
        m.commit(chain_b).unwrap();
    }
    let b_tip = m.head_commit(chain_b).unwrap().unwrap().id;

    // Determine which fork is longest (most commits ahead)
    let lca_a = m.fork_point(b4, a_tip).unwrap();
    let lca_b = m.fork_point(b4, b_tip).unwrap();
    assert_eq!(lca_a, b1); // forked at b1
    assert_eq!(lca_b, b0); // forked at b0

    let main_ahead_of_a = m.commit_distance(b4, lca_a).unwrap(); // 3
    let fork_a_ahead = m.commit_distance(a_tip, lca_a).unwrap(); // 2
    let main_ahead_of_b = m.commit_distance(b4, lca_b).unwrap(); // 4
    let fork_b_ahead = m.commit_distance(b_tip, lca_b).unwrap(); // 6

    assert_eq!(main_ahead_of_a, 3);
    assert_eq!(fork_a_ahead, 2);
    assert_eq!(main_ahead_of_b, 4);
    assert_eq!(fork_b_ahead, 6);

    // Main beats fork A (3 > 2), but fork B beats main (6 > 4)
    assert!(main_ahead_of_a > fork_a_ahead);
    assert!(fork_b_ahead > main_ahead_of_b);
}

#[test]
fn fork_point_deep_chain() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // Build a chain of 50 commits on main
    let mut commits = Vec::new();
    for i in 0..50u32 {
        m.insert(main, &i, &i).unwrap();
        commits.push(m.commit(main).unwrap());
    }

    // Fork at commit 10, add 30 commits
    let fork = m.create_branch("fork", main).unwrap();
    m.rollback_to(fork, commits[10]).unwrap();
    for i in 100..130u32 {
        m.insert(fork, &i, &i).unwrap();
        m.commit(fork).unwrap();
    }
    let fork_tip = m.head_commit(fork).unwrap().unwrap().id;
    let main_tip = *commits.last().unwrap();

    let lca = m.fork_point(main_tip, fork_tip).unwrap();
    assert_eq!(lca, commits[10]);

    // main: 39 commits ahead (index 11..49)
    assert_eq!(m.commit_distance(main_tip, lca), Some(39));
    // fork: 30 commits ahead
    assert_eq!(m.commit_distance(fork_tip, lca), Some(30));
}

// =====================================================================
// Diff
// =====================================================================

#[test]
fn diff_commits_basic() {
    setup();
    let mut m: VerMap<u32, String> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &"a".into()).unwrap();
    m.insert(main, &2, &"b".into()).unwrap();
    let c1 = m.commit(main).unwrap();

    m.insert(main, &2, &"B".into()).unwrap(); // modify
    m.insert(main, &3, &"c".into()).unwrap(); // add
    m.remove(main, &1).unwrap(); // remove
    let c2 = m.commit(main).unwrap();

    let diff = m.diff_commits(c1, c2).unwrap();
    assert_eq!(diff.len(), 3);

    use super::diff::DiffEntry;
    // Diff is in ascending key order.
    assert!(
        matches!(&diff[0], DiffEntry::Removed { key, .. } if key == &1u32.to_be_bytes())
    );
    assert!(
        matches!(&diff[1], DiffEntry::Modified { key, .. } if key == &2u32.to_be_bytes())
    );
    assert!(
        matches!(&diff[2], DiffEntry::Added { key, .. } if key == &3u32.to_be_bytes())
    );
}

#[test]
fn diff_commits_identical() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let c1 = m.commit(main).unwrap();

    let diff = m.diff_commits(c1, c1).unwrap();
    assert!(diff.is_empty());
}

#[test]
fn diff_uncommitted_changes() {
    setup();
    let mut m: VerMap<u32, String> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &"a".into()).unwrap();
    m.insert(main, &2, &"b".into()).unwrap();
    let _c1 = m.commit(main).unwrap();

    // Make uncommitted changes.
    m.insert(main, &3, &"c".into()).unwrap();
    m.remove(main, &1).unwrap();

    let diff = m.diff_uncommitted(main).unwrap();
    assert_eq!(diff.len(), 2);

    use super::diff::DiffEntry;
    assert!(
        matches!(&diff[0], DiffEntry::Removed { key, .. } if key == &1u32.to_be_bytes())
    );
    assert!(
        matches!(&diff[1], DiffEntry::Added { key, .. } if key == &3u32.to_be_bytes())
    );
}

#[test]
fn diff_uncommitted_no_changes() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    let _c1 = m.commit(main).unwrap();

    let diff = m.diff_uncommitted(main).unwrap();
    assert!(diff.is_empty());
}

#[test]
fn diff_from_empty() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // First commit with some data.
    m.insert(main, &1, &10).unwrap();
    m.insert(main, &2, &20).unwrap();

    // Diff of uncommitted from empty HEAD.
    let diff = m.diff_uncommitted(main).unwrap();
    assert_eq!(diff.len(), 2);

    use super::diff::DiffEntry;
    assert!(matches!(&diff[0], DiffEntry::Added { .. }));
    assert!(matches!(&diff[1], DiffEntry::Added { .. }));
}

#[test]
fn diff_across_branches() {
    setup();
    let mut m: VerMap<u32, String> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &"a".into()).unwrap();
    m.insert(main, &2, &"b".into()).unwrap();
    let c1 = m.commit(main).unwrap();

    // Fork and diverge.
    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &1, &"A".into()).unwrap();
    m.insert(feat, &3, &"c".into()).unwrap();
    let c2 = m.commit(feat).unwrap();

    // Diff between the two branch heads.
    let diff = m.diff_commits(c1, c2).unwrap();
    assert_eq!(diff.len(), 2);

    use super::diff::DiffEntry;
    assert!(
        matches!(&diff[0], DiffEntry::Modified { key, .. } if key == &1u32.to_be_bytes())
    );
    assert!(
        matches!(&diff[1], DiffEntry::Added { key, .. } if key == &3u32.to_be_bytes())
    );
}

#[test]
fn diff_large_dataset() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // Commit 100 entries.
    for i in 0..100u32 {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    let c1 = m.commit(main).unwrap();

    // Modify some, add some, remove some.
    for i in 0..10u32 {
        m.remove(main, &i).unwrap();
    }
    for i in 50..60u32 {
        m.insert(main, &i, &(i * 100)).unwrap();
    }
    for i in 100..110u32 {
        m.insert(main, &i, &(i * 10)).unwrap();
    }
    let c2 = m.commit(main).unwrap();

    let diff = m.diff_commits(c1, c2).unwrap();

    use super::diff::DiffEntry;
    let removed: Vec<_> = diff
        .iter()
        .filter(|d| matches!(d, DiffEntry::Removed { .. }))
        .collect();
    let modified: Vec<_> = diff
        .iter()
        .filter(|d| matches!(d, DiffEntry::Modified { .. }))
        .collect();
    let added: Vec<_> = diff
        .iter()
        .filter(|d| matches!(d, DiffEntry::Added { .. }))
        .collect();

    assert_eq!(removed.len(), 10);
    assert_eq!(modified.len(), 10);
    assert_eq!(added.len(), 10);
}

#[test]
fn diff_all_removed() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &10).unwrap();
    m.insert(main, &2, &20).unwrap();
    m.insert(main, &3, &30).unwrap();
    let c1 = m.commit(main).unwrap();

    m.remove(main, &1).unwrap();
    m.remove(main, &2).unwrap();
    m.remove(main, &3).unwrap();
    let c2 = m.commit(main).unwrap();

    let diff = m.diff_commits(c1, c2).unwrap();
    assert_eq!(diff.len(), 3);

    use super::diff::DiffEntry;
    assert!(diff.iter().all(|d| matches!(d, DiffEntry::Removed { .. })));
}

#[test]
fn diff_reverse_direction() {
    setup();
    let mut m: VerMap<u32, String> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &"a".into()).unwrap();
    let c1 = m.commit(main).unwrap();

    m.insert(main, &2, &"b".into()).unwrap();
    let c2 = m.commit(main).unwrap();

    // Forward: key 2 was added.
    let fwd = m.diff_commits(c1, c2).unwrap();
    // Reverse: key 2 was removed.
    let rev = m.diff_commits(c2, c1).unwrap();

    use super::diff::DiffEntry;
    assert!(matches!(&fwd[0], DiffEntry::Added { .. }));
    assert!(matches!(&rev[0], DiffEntry::Removed { .. }));
}

// =========================================================================
// VerMapWithProof tests (feature = "merkle")
// =========================================================================

mod proof_tests {
    use crate::trie::{MptCalc, VerMapWithProof};
    use crate::versioned::map::VerMap;

    type Vm = VerMap<u32, String>;
    type Vp = VerMapWithProof<u32, String, MptCalc>;

    fn new_proof() -> Vp {
        Vp::new()
    }

    #[test]
    fn test_basic_merkle_root() {
        let mut vp = new_proof();
        let main = vp.map().main_branch();

        vp.map_mut().insert(main, &1, &"hello".into()).unwrap();
        vp.map_mut().insert(main, &2, &"world".into()).unwrap();
        let _c1 = vp.map_mut().commit(main).unwrap();

        let hash = vp.merkle_root(main).unwrap();
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_merkle_root_deterministic() {
        let mut vp1 = new_proof();
        let main1 = vp1.map().main_branch();
        vp1.map_mut().insert(main1, &1, &"a".into()).unwrap();
        vp1.map_mut().insert(main1, &2, &"b".into()).unwrap();
        let _c1 = vp1.map_mut().commit(main1).unwrap();
        let hash1 = vp1.merkle_root(main1).unwrap();

        let mut vp2 = new_proof();
        let main2 = vp2.map().main_branch();
        vp2.map_mut().insert(main2, &1, &"a".into()).unwrap();
        vp2.map_mut().insert(main2, &2, &"b".into()).unwrap();
        let _c2 = vp2.map_mut().commit(main2).unwrap();
        let hash2 = vp2.merkle_root(main2).unwrap();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_incremental_update() {
        let mut vp = new_proof();
        let main = vp.map().main_branch();

        vp.map_mut().insert(main, &1, &"a".into()).unwrap();
        vp.map_mut().insert(main, &2, &"b".into()).unwrap();
        let _c1 = vp.map_mut().commit(main).unwrap();
        let hash1 = vp.merkle_root(main).unwrap();

        vp.map_mut().insert(main, &2, &"B".into()).unwrap();
        let _c2 = vp.map_mut().commit(main).unwrap();
        let hash2 = vp.merkle_root(main).unwrap();

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_merkle_root_at_commit() {
        let mut vp = new_proof();
        let main = vp.map().main_branch();

        vp.map_mut().insert(main, &1, &"a".into()).unwrap();
        let c1 = vp.map_mut().commit(main).unwrap();

        vp.map_mut().insert(main, &2, &"b".into()).unwrap();
        let c2 = vp.map_mut().commit(main).unwrap();

        let h1 = vp.merkle_root_at_commit(c1).unwrap();
        let h2 = vp.merkle_root_at_commit(c2).unwrap();
        assert_ne!(h1, h2);

        let h1_again = vp.merkle_root_at_commit(c1).unwrap();
        assert_eq!(h1, h1_again);
    }

    #[test]
    fn test_branch_isolation() {
        let mut vp = new_proof();
        let main = vp.map().main_branch();

        vp.map_mut().insert(main, &1, &"a".into()).unwrap();
        vp.map_mut().insert(main, &2, &"b".into()).unwrap();
        let _c1 = vp.map_mut().commit(main).unwrap();

        let feat = vp.map_mut().create_branch("feat", main).unwrap();
        vp.map_mut().insert(feat, &3, &"c".into()).unwrap();
        let _c2 = vp.map_mut().commit(feat).unwrap();

        let hash_main = vp.merkle_root(main).unwrap();
        let hash_feat = vp.merkle_root(feat).unwrap();
        assert_ne!(hash_main, hash_feat);
    }

    #[test]
    fn test_uncommitted_changes() {
        let mut vp = new_proof();
        let main = vp.map().main_branch();

        vp.map_mut().insert(main, &1, &"a".into()).unwrap();
        let _c1 = vp.map_mut().commit(main).unwrap();
        let hash_committed = vp.merkle_root(main).unwrap();

        vp.map_mut().insert(main, &2, &"b".into()).unwrap();
        let hash_dirty = vp.merkle_root(main).unwrap();
        assert_ne!(hash_committed, hash_dirty);
    }

    #[test]
    fn test_from_existing_map() {
        let mut m: Vm = VerMap::new();
        let main = m.main_branch();
        m.insert(main, &1, &"hello".into()).unwrap();
        m.insert(main, &2, &"world".into()).unwrap();
        let _c1 = m.commit(main).unwrap();

        let mut vp = Vp::from_map(m);
        let hash = vp.merkle_root(main).unwrap();
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn test_auto_cache_save_load() {
        let map_bytes;
        let hash1;

        {
            let mut vp = new_proof();
            let main = vp.map().main_branch();
            vp.map_mut().insert(main, &1, &"a".into()).unwrap();
            vp.map_mut().insert(main, &2, &"b".into()).unwrap();
            let _c1 = vp.map_mut().commit(main).unwrap();
            hash1 = vp.merkle_root(main).unwrap();

            // Serialize the VerMap metadata so we can recreate a handle
            // to the same persistent data (simulates process restart).
            map_bytes = postcard::to_allocvec(vp.map()).unwrap();

            // Cache is saved eagerly inside merkle_root → sync_to_commit.
        }

        {
            // Deserialize recreates a handle with the same instance_id,
            // pointing to the same persistent data.
            let map: Vm = postcard::from_bytes(&map_bytes).unwrap();
            let br = map.main_branch();

            // from_map auto-loads the cache saved by the previous sync.
            let mut vp = Vp::from_map(map);

            // merkle_root should produce the same hash — from cache, not
            // a full rebuild.
            let hash_restored = vp.merkle_root(br).unwrap();
            assert_eq!(hash1, hash_restored);
        }
    }

    #[test]
    fn test_auto_cache_dirty_flag() {
        // Read-only usage: cache_dirty should remain false, no re-save.
        let map_bytes;
        let hash1;

        {
            let mut vp = new_proof();
            let main = vp.map().main_branch();
            vp.map_mut().insert(main, &1, &"x".into()).unwrap();
            let _c1 = vp.map_mut().commit(main).unwrap();
            hash1 = vp.merkle_root(main).unwrap();
            map_bytes = postcard::to_allocvec(vp.map()).unwrap();
            // Cache was saved eagerly in sync_to_commit.
        }

        {
            let map: Vm = postcard::from_bytes(&map_bytes).unwrap();
            let br = map.main_branch();
            let mut vp = Vp::from_map(map);

            // Trie was loaded from cache.  Calling merkle_root on the
            // same commit should be a cache hit — no trie mutation, so
            // cache_dirty stays false.
            let h = vp.merkle_root(br).unwrap();
            assert_eq!(hash1, h);

            // Drop here.  cache_dirty should be false → no disk write.
            // (We can't easily observe this, but at least verify no crash.)
        }
    }

    #[test]
    fn test_auto_cache_incremental_catchup() {
        // After loading a stale cache, the trie catches up via diff.
        let map_bytes;
        let hash1;

        {
            let mut vp = new_proof();
            let main = vp.map().main_branch();
            vp.map_mut().insert(main, &1, &"a".into()).unwrap();
            let _c1 = vp.map_mut().commit(main).unwrap();
            hash1 = vp.merkle_root(main).unwrap();
            map_bytes = postcard::to_allocvec(vp.map()).unwrap();
            // Cache was saved eagerly at commit c1.
        }

        // Mutate the map WITHOUT a VerMapWithProof wrapper (simulates
        // external writes between process restarts).
        let map_bytes2;
        let hash2;
        {
            let mut map: Vm = postcard::from_bytes(&map_bytes).unwrap();
            let br = map.main_branch();
            map.insert(br, &3, &"c".into()).unwrap();
            let _c2 = map.commit(br).unwrap();
            map_bytes2 = postcard::to_allocvec(&map).unwrap();

            // Compute expected hash via a fresh (no-cache) VerMapWithProof.
            let mut vp_fresh = Vp::from_map(map);
            hash2 = vp_fresh.merkle_root(br).unwrap();
            assert_ne!(hash1, hash2);
        }

        {
            // Reload map; from_map auto-loads stale cache at c1.
            let map: Vm = postcard::from_bytes(&map_bytes2).unwrap();
            let br = map.main_branch();
            let mut vp = Vp::from_map(map);

            // merkle_root catches up via incremental diff (c1 → c2).
            let h = vp.merkle_root(br).unwrap();
            assert_eq!(hash2, h);
        }
    }

    #[test]
    fn test_merge_determinism() {
        let mut vp = new_proof();
        let main = vp.map().main_branch();

        vp.map_mut().insert(main, &1, &"a".into()).unwrap();
        vp.map_mut().insert(main, &2, &"b".into()).unwrap();
        let _c1 = vp.map_mut().commit(main).unwrap();

        let feat = vp.map_mut().create_branch("feat", main).unwrap();
        vp.map_mut().insert(feat, &3, &"c".into()).unwrap();
        let _c2 = vp.map_mut().commit(feat).unwrap();

        let _merge_commit = vp.map_mut().merge(feat, main).unwrap();

        let hash_main = vp.merkle_root(main).unwrap();
        let hash_feat = vp.merkle_root(feat).unwrap();
        assert_eq!(hash_main, hash_feat);
    }

    #[test]
    fn test_empty_map_merkle_root() {
        let mut vp = new_proof();
        let main = vp.map().main_branch();

        vp.map_mut().commit(main).unwrap();
        let hash = vp.merkle_root(main).unwrap();
        assert_eq!(hash.len(), 32);
    }

    /// Regression test: calling `merkle_root` twice with the same dirty
    /// state must return the same hash (no double-apply of dirty diff).
    #[test]
    fn test_merkle_root_idempotent_with_dirty() {
        let mut vp = new_proof();
        let main = vp.map().main_branch();

        vp.map_mut().insert(main, &1, &"a".into()).unwrap();
        let _c1 = vp.map_mut().commit(main).unwrap();

        // Add uncommitted changes.
        vp.map_mut().insert(main, &2, &"b".into()).unwrap();

        let h1 = vp.merkle_root(main).unwrap();
        let h2 = vp.merkle_root(main).unwrap();
        let h3 = vp.merkle_root(main).unwrap();
        assert_eq!(h1, h2);
        assert_eq!(h2, h3);
    }

    /// After computing merkle_root with dirty changes, committing,
    /// then computing again should reflect the committed state.
    #[test]
    fn test_merkle_root_dirty_then_commit() {
        let mut vp = new_proof();
        let main = vp.map().main_branch();

        vp.map_mut().insert(main, &1, &"a".into()).unwrap();
        let _c1 = vp.map_mut().commit(main).unwrap();

        // Dirty changes.
        vp.map_mut().insert(main, &2, &"b".into()).unwrap();
        let hash_dirty = vp.merkle_root(main).unwrap();

        // Commit the dirty changes.
        let _c2 = vp.map_mut().commit(main).unwrap();
        let hash_committed = vp.merkle_root(main).unwrap();

        // Should be the same: the committed state is what was dirty.
        assert_eq!(hash_dirty, hash_committed);
    }

    /// Dirty changes that are then reverted (discard) should restore
    /// the original hash.
    #[test]
    fn test_merkle_root_dirty_discard() {
        let mut vp = new_proof();
        let main = vp.map().main_branch();

        vp.map_mut().insert(main, &1, &"a".into()).unwrap();
        vp.map_mut().insert(main, &2, &"b".into()).unwrap();
        let _c1 = vp.map_mut().commit(main).unwrap();
        let hash_clean = vp.merkle_root(main).unwrap();

        // Make dirty changes.
        vp.map_mut().insert(main, &3, &"c".into()).unwrap();
        let hash_dirty = vp.merkle_root(main).unwrap();
        assert_ne!(hash_clean, hash_dirty);

        // Discard uncommitted changes.
        vp.map_mut().discard(main).unwrap();
        let hash_after_discard = vp.merkle_root(main).unwrap();
        assert_eq!(hash_clean, hash_after_discard);
    }

    /// Switching between branches should produce correct hashes.
    #[test]
    fn test_merkle_root_branch_switching() {
        let mut vp = new_proof();
        let main = vp.map().main_branch();

        vp.map_mut().insert(main, &1, &"a".into()).unwrap();
        vp.map_mut().insert(main, &2, &"b".into()).unwrap();
        let _c1 = vp.map_mut().commit(main).unwrap();
        let hash_main = vp.merkle_root(main).unwrap();

        let feat = vp.map_mut().create_branch("feat", main).unwrap();
        vp.map_mut().insert(feat, &3, &"c".into()).unwrap();
        let _c2 = vp.map_mut().commit(feat).unwrap();
        let hash_feat = vp.merkle_root(feat).unwrap();

        // Switch back to main — should get the same hash.
        let hash_main_again = vp.merkle_root(main).unwrap();
        assert_eq!(hash_main, hash_main_again);

        // Switch back to feat — should get the same hash.
        let hash_feat_again = vp.merkle_root(feat).unwrap();
        assert_eq!(hash_feat, hash_feat_again);
    }

    /// Multiple commits with incremental merkle root computation.
    #[test]
    fn test_merkle_root_multi_commit_incremental() {
        let mut vp = new_proof();
        let main = vp.map().main_branch();

        vp.map_mut().insert(main, &1, &"a".into()).unwrap();
        let _c1 = vp.map_mut().commit(main).unwrap();
        let h1 = vp.merkle_root(main).unwrap();

        vp.map_mut().insert(main, &2, &"b".into()).unwrap();
        let _c2 = vp.map_mut().commit(main).unwrap();
        let h2 = vp.merkle_root(main).unwrap();

        vp.map_mut().insert(main, &3, &"c".into()).unwrap();
        let _c3 = vp.map_mut().commit(main).unwrap();
        let h3 = vp.merkle_root(main).unwrap();

        // All different.
        assert_ne!(h1, h2);
        assert_ne!(h2, h3);
        assert_ne!(h1, h3);

        // Verify historical hashes are stable.
        let h1_check = vp.merkle_root_at_commit(_c1).unwrap();
        assert_eq!(h1, h1_check);
    }

    /// Full rebuild path: merkle_root_at_commit with no prior sync.
    #[test]
    fn test_merkle_root_at_commit_cold_start() {
        let mut vp = new_proof();
        let main = vp.map().main_branch();

        vp.map_mut().insert(main, &1, &"a".into()).unwrap();
        vp.map_mut().insert(main, &2, &"b".into()).unwrap();
        let c1 = vp.map_mut().commit(main).unwrap();

        // No prior merkle_root call — cold start.
        let mut vp2 = Vp::from_map(vp.map().clone());
        let h = vp2.merkle_root_at_commit(c1).unwrap();
        assert_eq!(h.len(), 32);

        // Compare with the original.
        let h_orig = vp.merkle_root_at_commit(c1).unwrap();
        assert_eq!(h, h_orig);
    }

    // ---- SMT-backed VerMapWithProof with prove/verify ----

    #[test]
    fn test_smt_backed_prove_verify() {
        use crate::trie::SmtCalc;

        type VpSmt = VerMapWithProof<u32, String, SmtCalc>;

        let mut vp = VpSmt::new();
        let main = vp.map().main_branch();

        vp.map_mut().insert(main, &1, &"alice".into()).unwrap();
        vp.map_mut().insert(main, &2, &"bob".into()).unwrap();
        let _c1 = vp.map_mut().commit(main).unwrap();

        let root = vp.merkle_root(main).unwrap();
        assert_eq!(root.len(), 32);

        // Prove membership for key 1
        let proof = vp.prove(&1u32.to_be_bytes()).unwrap();
        assert!(proof.value.is_some());

        let root_arr: [u8; 32] = root.try_into().unwrap();
        let ok = VpSmt::verify_proof(&root_arr, &proof).unwrap();
        assert!(ok);

        // Prove non-membership for absent key
        let proof_absent = vp.prove(&999u32.to_be_bytes()).unwrap();
        assert!(proof_absent.value.is_none());
        let ok2 = VpSmt::verify_proof(&root_arr, &proof_absent).unwrap();
        assert!(ok2);
    }
}

// =====================================================================
// Meta persistence
// =====================================================================

#[test]
fn test_save_and_from_meta() {
    setup();
    let mut m: VerMap<u32, String> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &"hello".into()).unwrap();
    m.insert(main, &2, &"world".into()).unwrap();
    m.commit(main).unwrap();

    let id = m.save_meta().unwrap();
    assert_eq!(id, m.instance_id());

    let restored: VerMap<u32, String> = VerMap::from_meta(id).unwrap();
    let main_r = restored.main_branch();
    assert_eq!(restored.get(main_r, &1).unwrap(), Some("hello".to_string()));
    assert_eq!(restored.get(main_r, &2).unwrap(), Some("world".to_string()));
}

/// Multi-branch VerMap: save meta once, restore, verify all branches
/// and commit history survive — this exercises the full composite
/// internal structure (PersistentBTree + multiple MapxOrd + Orphan).
#[test]
fn test_save_and_from_meta_with_branches() {
    setup();
    let mut m: VerMap<u32, String> = VerMap::new();
    let main = m.main_branch();

    m.insert(main, &1, &"v1".into()).unwrap();
    m.insert(main, &2, &"v2".into()).unwrap();
    let c1 = m.commit(main).unwrap();

    // Fork a feature branch
    let feat = m.create_branch("feature", main).unwrap();
    m.insert(feat, &1, &"v1-feat".into()).unwrap();
    m.insert(feat, &3, &"v3-feat".into()).unwrap();
    let c2 = m.commit(feat).unwrap();

    // Continue on main
    m.insert(main, &1, &"v1-main".into()).unwrap();
    m.commit(main).unwrap();

    // Save + restore
    let id = m.save_meta().unwrap();
    let restored: VerMap<u32, String> = VerMap::from_meta(id).unwrap();

    let main_r = restored.main_branch();
    let feat_r = restored.branch_id("feature").unwrap();

    // main should have latest values
    assert_eq!(restored.get(main_r, &1).unwrap(), Some("v1-main".into()));
    assert_eq!(restored.get(main_r, &2).unwrap(), Some("v2".into()));
    assert_eq!(restored.get(main_r, &3).unwrap(), None);

    // feature branch is isolated
    assert_eq!(restored.get(feat_r, &1).unwrap(), Some("v1-feat".into()));
    assert_eq!(restored.get(feat_r, &2).unwrap(), Some("v2".into()));
    assert_eq!(restored.get(feat_r, &3).unwrap(), Some("v3-feat".into()));

    // Historical snapshot at c1
    assert_eq!(restored.get_at_commit(c1, &1).unwrap(), Some("v1".into()));
    assert_eq!(
        restored.get_at_commit(c2, &1).unwrap(),
        Some("v1-feat".into())
    );
}

/// Postcard serde roundtrip: VerMap with data, branches, commits.
/// This validates the hand-written 8-tuple Serialize/Deserialize impl.
#[test]
fn test_serde_roundtrip_full() {
    setup();
    let mut m: VerMap<u64, String> = VerMap::new();
    let main = m.main_branch();

    for i in 0..20 {
        m.insert(main, &i, &format!("val_{i}")).unwrap();
    }
    let _c1 = m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &100, &"feat_val".into()).unwrap();
    m.commit(feat).unwrap();

    let bytes = postcard::to_allocvec(&m).unwrap();
    let restored: VerMap<u64, String> = postcard::from_bytes(&bytes).unwrap();

    // Verify main data
    for i in 0..20 {
        assert_eq!(restored.get(main, &i).unwrap(), Some(format!("val_{i}")));
    }

    // Verify branch isolation
    let feat_r = restored.branch_id("feat").unwrap();
    assert_eq!(restored.get(feat_r, &100).unwrap(), Some("feat_val".into()));
    assert!(restored.get(main, &100).unwrap().is_none());

    // Verify iteration order
    let keys: Vec<u64> = restored.iter(main).unwrap().map(|(k, _)| k).collect();
    assert_eq!(keys, (0..20).collect::<Vec<u64>>());
}

/// Serialized size: VerMap = 8 handles, each ~8B + overhead → should be compact.
#[test]
fn test_serde_size() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let bytes = postcard::to_allocvec(&m).unwrap();
    // 8 fields × ~9B each ≈ 72B. With postcard varint overhead < 80B.
    assert!(bytes.len() <= 80, "expected ≤80 bytes, got {}", bytes.len());
}

/// from_meta nonexistent.
#[test]
fn test_from_meta_nonexistent() {
    setup();
    assert!(VerMap::<u32, u32>::from_meta(u64::MAX).is_err());
}

/// Restore from meta, continue committing and branching.
#[test]
fn test_meta_restore_then_continue() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();

    let id = m.save_meta().unwrap();
    let mut restored: VerMap<u32, u32> = VerMap::from_meta(id).unwrap();

    // Continue working on the restored handle
    restored.insert(main, &2, &20).unwrap();
    let _c2 = restored.commit(main).unwrap();

    let dev = restored.create_branch("dev", main).unwrap();
    restored.insert(dev, &3, &30).unwrap();
    restored.commit(dev).unwrap();

    // Verify full state
    assert_eq!(restored.get(main, &1).unwrap(), Some(10));
    assert_eq!(restored.get(main, &2).unwrap(), Some(20));
    assert!(restored.get(main, &3).unwrap().is_none());
    assert_eq!(restored.get(dev, &3).unwrap(), Some(30));

    // GC on restored handle
    restored.delete_branch(dev).unwrap();
    restored.gc();
    assert_eq!(restored.get(main, &1).unwrap(), Some(10));
    assert_eq!(restored.get(main, &2).unwrap(), Some(20));
}

/// Double save-restore: save, restore, mutate, save again, restore again.
#[test]
fn test_double_save_restore() {
    setup();
    let mut m: VerMap<u32, String> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &"first".into()).unwrap();
    m.commit(main).unwrap();

    let id1 = m.save_meta().unwrap();
    let mut m2: VerMap<u32, String> = VerMap::from_meta(id1).unwrap();

    m2.insert(main, &2, &"second".into()).unwrap();
    m2.commit(main).unwrap();

    let id2 = m2.save_meta().unwrap();
    assert_eq!(id1, id2); // Same instance, same id

    let m3: VerMap<u32, String> = VerMap::from_meta(id2).unwrap();
    assert_eq!(m3.get(main, &1).unwrap(), Some("first".into()));
    assert_eq!(m3.get(main, &2).unwrap(), Some("second".into()));
}

// =====================================================================
// VsdbError — verify structured error variants
// =====================================================================

#[test]
fn error_branch_not_found() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let err = m.get(9999, &1).unwrap_err();
    assert!(
        matches!(err, VsdbError::BranchNotFound { branch_id: 9999 }),
        "expected BranchNotFound, got: {err:?}"
    );
}

#[test]
fn error_commit_not_found() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let err = m.get_at_commit(9999, &1).unwrap_err();
    assert!(
        matches!(err, VsdbError::CommitNotFound { commit_id: 9999 }),
        "expected CommitNotFound, got: {err:?}"
    );
}

#[test]
fn error_branch_already_exists() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let err = m.create_branch("main", main).unwrap_err();
    assert!(
        matches!(err, VsdbError::BranchAlreadyExists { .. }),
        "expected BranchAlreadyExists, got: {err:?}"
    );
}

#[test]
fn error_cannot_delete_main() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    let err = m.delete_branch(main).unwrap_err();
    assert!(
        matches!(err, VsdbError::CannotDeleteMainBranch),
        "expected CannotDeleteMainBranch, got: {err:?}"
    );
}

#[test]
fn error_uncommitted_changes_on_merge() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &10).unwrap();
    m.commit(main).unwrap();
    let feat = m.create_branch("feat", main).unwrap();
    m.insert(feat, &2, &20).unwrap();
    m.commit(feat).unwrap();
    // Dirty main before merge
    m.insert(main, &3, &30).unwrap();
    let err = m.merge(feat, main).unwrap_err();
    assert!(
        matches!(err, VsdbError::UncommittedChanges { .. }),
        "expected UncommittedChanges, got: {err:?}"
    );
}

#[test]
fn error_display_preserves_context() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let err = m.get(9999, &1).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("9999"),
        "Display should contain branch id: {msg}"
    );
}

// =====================================================================
// Branch / BranchMut handles
// =====================================================================

#[test]
fn handle_main_read() {
    setup();
    let mut m: VerMap<u32, String> = VerMap::new();
    let main_id = m.main_branch();
    m.insert(main_id, &1, &"hello".into()).unwrap();

    let br = m.main();
    assert_eq!(br.id(), main_id);
    assert_eq!(br.name(), Some("main".to_string()));
    assert_eq!(br.get(&1).unwrap(), Some("hello".to_string()));
    assert!(br.contains_key(&1).unwrap());
    assert!(!br.contains_key(&2).unwrap());
}

#[test]
fn handle_main_mut_write() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    {
        let mut main = m.main_mut();
        main.insert(&1, &10).unwrap();
        main.insert(&2, &20).unwrap();
        let c = main.commit().unwrap();
        assert!(c > 0);
    }
    assert_eq!(m.get(m.main_branch(), &1).unwrap(), Some(10));
    assert_eq!(m.get(m.main_branch(), &2).unwrap(), Some(20));
}

#[test]
fn handle_branch_read_write() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &1, &100).unwrap();
    m.commit(main).unwrap();

    let feat = m.create_branch("feat", main).unwrap();
    {
        let mut handle = m.branch_mut(feat).unwrap();
        handle.insert(&2, &200).unwrap();
        handle.commit().unwrap();
    }

    let handle = m.branch(feat).unwrap();
    assert_eq!(handle.get(&1).unwrap(), Some(100));
    assert_eq!(handle.get(&2).unwrap(), Some(200));
}

#[test]
fn handle_iter() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    {
        let mut main = m.main_mut();
        for i in 1..=5 {
            main.insert(&i, &(i * 10)).unwrap();
        }
    }
    let main = m.main();
    let entries: Vec<_> = main.iter().unwrap().collect();
    assert_eq!(entries.len(), 5);
    assert_eq!(entries[0], (1, 10));
    assert_eq!(entries[4], (5, 50));
}

#[test]
fn handle_range() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    {
        let mut main = m.main_mut();
        for i in 1..=10 {
            main.insert(&i, &(i * 10)).unwrap();
        }
    }
    let main = m.main();
    let entries: Vec<_> = main
        .range(Bound::Included(&3), Bound::Excluded(&7))
        .unwrap()
        .collect();
    assert_eq!(entries.len(), 4);
    assert_eq!(entries[0].0, 3);
    assert_eq!(entries[3].0, 6);
}

#[test]
fn handle_has_uncommitted() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    assert!(!m.main().has_uncommitted().unwrap());
    m.main_mut().insert(&1, &10).unwrap();
    assert!(m.main().has_uncommitted().unwrap());
}

#[test]
fn handle_discard() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    {
        let mut main = m.main_mut();
        main.insert(&1, &10).unwrap();
        main.commit().unwrap();
        main.insert(&2, &20).unwrap();
        main.discard().unwrap();
    }
    assert_eq!(m.get(m.main_branch(), &1).unwrap(), Some(10));
    assert_eq!(m.get(m.main_branch(), &2).unwrap(), None);
}

#[test]
fn handle_rollback() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let c1;
    {
        let mut main = m.main_mut();
        main.insert(&1, &10).unwrap();
        c1 = main.commit().unwrap();
        main.insert(&2, &20).unwrap();
        main.commit().unwrap();
        main.rollback_to(c1).unwrap();
    }
    assert_eq!(m.get(m.main_branch(), &1).unwrap(), Some(10));
    assert_eq!(m.get(m.main_branch(), &2).unwrap(), None);
}

#[test]
fn handle_log_and_head_commit() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    {
        let mut main = m.main_mut();
        main.insert(&1, &10).unwrap();
        main.commit().unwrap();
        main.insert(&2, &20).unwrap();
        main.commit().unwrap();
    }
    let main = m.main();
    let log = main.log().unwrap();
    assert_eq!(log.len(), 2);
    let head = main.head_commit().unwrap().unwrap();
    assert_eq!(head.id, log[0].id);
}

#[test]
fn handle_diff_uncommitted() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    {
        let mut main = m.main_mut();
        main.insert(&1, &10).unwrap();
        main.commit().unwrap();
        main.insert(&2, &20).unwrap();
    }
    let diff = m.main().diff_uncommitted().unwrap();
    assert_eq!(diff.len(), 1);
}

#[test]
fn handle_invalid_branch_returns_error() {
    setup();
    let m: VerMap<u32, u32> = VerMap::new();
    let err = m.branch(9999).unwrap_err();
    assert!(matches!(err, VsdbError::BranchNotFound { .. }));
    // main() never errors (main branch always exists)
    let _ = m.main();
}
