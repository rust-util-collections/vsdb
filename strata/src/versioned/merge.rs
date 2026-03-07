//!
//! Three-way merge for persistent B+ trees.
//!
//! Given an ancestor, "ours" and "theirs" tree roots, produces a merged tree:
//!
//! * Keys changed only in ours → take ours.
//! * Keys changed only in theirs → take theirs.
//! * Keys changed in both to the same value → keep.
//! * Keys changed in both to different values → **source wins** (last-writer-wins).
//! * Keys added only in ours → take ours.
//! * Keys added only in theirs → take theirs.
//! * Keys deleted only in ours → delete.
//! * Keys deleted only in theirs → delete.
//! * Key deleted in one side, changed in other → **keep the change** (safer default).
//!

use vsdb_core::basic::persistent_btree::{NodeId, PersistentBTree};

/// Performs a three-way merge.
///
/// `source_root` is given priority over `target_root` when both sides
/// modify the same key to different values (last-writer-wins with source
/// winning).
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

        let min_key = [ka, ks, kt]
            .into_iter()
            .flatten()
            .min()
            .unwrap()
            .to_vec();

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
            // Deleted in one, changed in other → keep the change.
            (Some(_), None, Some(t)) => Some(t),
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
