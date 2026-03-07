use super::map::VerMap;
use super::NO_COMMIT;
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
    let main = m.main_branch();
    assert!(m.get(999, &1).is_err());
}

#[test]
fn insert_on_invalid_branch() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();
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
    let main = m.main_branch();
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
    let main = m.main_branch();
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
    let main = m.main_branch();
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
        m.create_branch(&format!("branch_{i}"), main)
            .unwrap();
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
    let main = m.main_branch();
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
    let main = m.main_branch();
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
    let main = m.main_branch();
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
    let main = m.main_branch();
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
    let main = m.main_branch();
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
    let main = m.main_branch();
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
    let main = m.main_branch();
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
    assert!(m
        .range_at_commit(999, Bound::Unbounded, Bound::Unbounded)
        .is_err());
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
    let main = m.main_branch();
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
fn set_main_branch_blockchain_reorg() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // Build canonical chain: b0 -> b1 -> b2
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

    // Fork is longer — do a reorg.
    let lca = m.fork_point(b2, fork_tip).unwrap();
    let main_len = m.commit_distance(b2, lca).unwrap();
    let fork_len = m.commit_distance(fork_tip, lca).unwrap();
    assert!(fork_len > main_len);

    // Switch canonical chain.
    m.set_main_branch(fork).unwrap();
    assert_eq!(m.main_branch(), fork);

    // Old chain is now deletable.
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
    let c1 = m.commit(main).unwrap();
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
fn blockchain_longest_chain_scenario() {
    setup();
    let mut m: VerMap<u32, u32> = VerMap::new();
    let main = m.main_branch();

    // Main chain (canonical): block0 -> block1 -> block2 -> block3 -> block4
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

    // Fork A diverges at block1: block1 -> a1 -> a2  (2 blocks)
    let chain_a = m.create_branch("chain_a", main).unwrap();
    m.rollback_to(chain_a, b1).unwrap();
    m.insert(chain_a, &100, &100).unwrap();
    m.commit(chain_a).unwrap();
    m.insert(chain_a, &101, &101).unwrap();
    let a_tip = m.commit(chain_a).unwrap();

    // Fork B diverges at block0: block0 -> x1 -> x2 -> x3 -> x4 -> x5 -> x6  (6 blocks)
    let chain_b = m.create_branch("chain_b", main).unwrap();
    m.rollback_to(chain_b, b0).unwrap();
    for i in 200..206 {
        m.insert(chain_b, &i, &i).unwrap();
        m.commit(chain_b).unwrap();
    }
    let b_tip = m.head_commit(chain_b).unwrap().unwrap().id;

    // Determine which fork is "longest" (most work)
    let lca_a = m.fork_point(b4, a_tip).unwrap();
    let lca_b = m.fork_point(b4, b_tip).unwrap();
    assert_eq!(lca_a, b1); // forked at block1
    assert_eq!(lca_b, b0); // forked at block0

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
