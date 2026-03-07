//!
//! Three-way merge for persistent B+ trees.
//!
//! # Merge semantics: source wins on conflicts
//!
//! Given an **ancestor** (common base), a **source** branch, and a
//! **target** branch, the merge compares each key across all three
//! snapshots and applies these rules:
//!
//! 1. **Non-conflict (single-sided change):** if only one side differs
//!    from the ancestor, keep that side's change.
//! 2. **Conflict (both sides changed the same key differently):**
//!    **source wins**.
//!
//! A deletion is treated as "assigning the empty value ∅".  This means
//! delete-vs-modify is also a regular conflict, so source-side delete
//! wins over target-side modify.
//!
//! ## Complete decision matrix
//!
//! | ancestor | source | target | result          | rationale                     |
//! |----------|--------|--------|-----------------|-------------------------------|
//! | A        | A      | A      | A (keep)        | no change on either side      |
//! | A        | **S**  | A      | **S**           | only source changed           |
//! | A        | A      | **T**  | **T**           | only target changed           |
//! | A        | **S**  | **S**  | **S**           | both changed to same value    |
//! | A        | **S**  | **T**  | **S** ⚠         | conflict → source wins        |
//! | A        | ∅      | A      | ∅ (delete)      | source deleted, target unchanged → source wins |
//! | A        | A      | ∅      | ∅ (delete)      | target deleted, source unchanged → target wins (no conflict) |
//! | A        | ∅      | **T**  | ∅ (delete) ⚠    | conflict → source wins (delete beats modify)  |
//! | A        | **S**  | ∅      | **S** ⚠         | conflict → source wins (modify beats delete)  |
//! | A        | ∅      | ∅      | ∅ (delete)      | both deleted                  |
//! | ∅        | **S**  | ∅      | **S**           | only source added             |
//! | ∅        | ∅      | **T**  | **T**           | only target added             |
//! | ∅        | **S**  | **S**  | **S**           | both added same value         |
//! | ∅        | **S**  | **T**  | **S** ⚠         | conflict → source wins        |
//!
//! Rows marked ⚠ are conflict cases (source wins). Non-⚠ rows are
//! single-sided or non-conflicting updates determined by ancestor
//! comparison. The caller controls priority by choosing which branch
//! to pass as `source` vs `target` in
//! [`VerMap::merge(source, target)`](super::map::VerMap::merge).
//!

use vsdb_core::basic::persistent_btree::{NodeId, PersistentBTree};

/// Performs a three-way merge.
///
/// Deletion is treated as "assigning ∅", so all conflicts — including
/// delete-vs-modify — are resolved uniformly: **source wins**.
///
/// See the [module-level documentation](self) for the full decision
/// matrix.
pub fn three_way_merge(
    tree: &mut PersistentBTree,
    ancestor_root: NodeId,
    source_root: NodeId,
    target_root: NodeId,
) -> NodeId {
    // Fast paths.
    if ancestor_root == source_root {
        // Source made no changes — result is target.
        return target_root;
    }
    if ancestor_root == target_root {
        // Target made no changes — fast-forward to source.
        return source_root;
    }
    if source_root == target_root {
        // Both sides converged to the same state.
        return source_root;
    }

    // Full three-way: iterate all three trees in sorted order.
    let mut iter_a = tree.iter(ancestor_root).peekable();
    let mut iter_s = tree.iter(source_root).peekable();
    let mut iter_t = tree.iter(target_root).peekable();

    let mut merged: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    loop {
        // Pick the smallest key across the three iterators.
        let ka = iter_a.peek().map(|(k, _)| k.as_slice());
        let ks = iter_s.peek().map(|(k, _)| k.as_slice());
        let kt = iter_t.peek().map(|(k, _)| k.as_slice());

        // All exhausted?
        if ka.is_none() && ks.is_none() && kt.is_none() {
            break;
        }

        let min_key = [ka, ks, kt].into_iter().flatten().min().unwrap().to_vec();

        let a_val = if ka == Some(min_key.as_slice()) {
            let (_, v) = iter_a.next().unwrap();
            Some(v)
        } else {
            None
        };
        let s_val = if ks == Some(min_key.as_slice()) {
            let (_, v) = iter_s.next().unwrap();
            Some(v)
        } else {
            None
        };
        let t_val = if kt == Some(min_key.as_slice()) {
            let (_, v) = iter_t.next().unwrap();
            Some(v)
        } else {
            None
        };

        // Three-way decision matrix.
        let result = match (a_val.as_deref(), s_val.as_deref(), t_val.as_deref()) {
            // Unchanged in both → keep.
            (Some(a), Some(s), Some(t)) if a == s && a == t => Some(a),
            // Changed only in source → take source.
            (Some(a), Some(s), Some(t)) if a == t => Some(s),
            // Changed only in target → take target.
            (Some(a), Some(s), Some(t)) if a == s => Some(t),
            // Changed in both to same value → keep.
            (Some(_), Some(s), Some(t)) if s == t => Some(s),
            // Changed in both to different values → source wins.
            (Some(_), Some(s), Some(_)) => Some(s),

            // Deleted in source, unchanged in target → delete.
            (Some(a), None, Some(t)) if a == t => None,
            // Deleted in target, unchanged in source → delete.
            (Some(a), Some(s), None) if a == s => None,
            // Deleted in source, changed in target → source wins (delete).
            (Some(_), None, Some(_)) => None,
            // Changed in source, deleted in target → source wins (keep change).
            (Some(_), Some(s), None) => Some(s),
            // Deleted in both → delete.
            (Some(_), None, None) => None,

            // Added only in source → take.
            (None, Some(s), None) => Some(s),
            // Added only in target → take.
            (None, None, Some(t)) => Some(t),
            // Added in both to same value → keep.
            (None, Some(s), Some(t)) if s == t => Some(s),
            // Added in both to different values → source wins.
            (None, Some(s), Some(_)) => Some(s),

            // All absent (shouldn't happen due to loop guard, but be safe).
            (None, None, None) => None,
        };

        if let Some(val) = result {
            merged.push((min_key, val.to_vec()));
        }
    }

    // Build the merged tree via bulk load (O(n), optimally packed).
    tree.bulk_load(merged)
}
