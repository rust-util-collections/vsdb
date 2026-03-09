//!
//! Two-way diff for persistent B+ tree snapshots.
//!
//! Given two B+ tree roots (`old` and `new`), produces a list of
//! [`DiffEntry`] describing every key that was added, removed, or
//! modified between the two snapshots.
//!
//! Uses the same sorted-merge-join technique as
//! [`three_way_merge`](super::merge::three_way_merge).
//!

use std::cmp::Ordering;
use vsdb_core::basic::persistent_btree::{NodeId, PersistentBTree};

/// A single difference between two snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiffEntry {
    /// Key was added (exists in `new` but not `old`).
    Added { key: Vec<u8>, value: Vec<u8> },
    /// Key was removed (exists in `old` but not `new`).
    Removed { key: Vec<u8>, value: Vec<u8> },
    /// Key exists in both but with different values.
    Modified {
        key: Vec<u8>,
        old_value: Vec<u8>,
        new_value: Vec<u8>,
    },
}

/// Computes the diff between two B+ tree snapshots.
///
/// Returns a list of [`DiffEntry`] in ascending key order, describing
/// every key that was added, removed, or modified between `old_root`
/// and `new_root`.
pub fn diff_roots(
    tree: &PersistentBTree,
    old_root: NodeId,
    new_root: NodeId,
) -> Vec<DiffEntry> {
    if old_root == new_root {
        return Vec::new();
    }

    let mut iter_old = tree.iter(old_root).peekable();
    let mut iter_new = tree.iter(new_root).peekable();
    let mut result = Vec::new();

    loop {
        match (iter_old.peek(), iter_new.peek()) {
            (None, None) => break,
            (Some(_), None) => {
                let (k, v) = iter_old.next().unwrap();
                result.push(DiffEntry::Removed { key: k, value: v });
            }
            (None, Some(_)) => {
                let (k, v) = iter_new.next().unwrap();
                result.push(DiffEntry::Added { key: k, value: v });
            }
            (Some((ok, _)), Some((nk, _))) => match ok.cmp(nk) {
                Ordering::Less => {
                    let (k, v) = iter_old.next().unwrap();
                    result.push(DiffEntry::Removed { key: k, value: v });
                }
                Ordering::Greater => {
                    let (k, v) = iter_new.next().unwrap();
                    result.push(DiffEntry::Added { key: k, value: v });
                }
                Ordering::Equal => {
                    let (k, ov) = iter_old.next().unwrap();
                    let (_, nv) = iter_new.next().unwrap();
                    if ov != nv {
                        result.push(DiffEntry::Modified {
                            key: k,
                            old_value: ov,
                            new_value: nv,
                        });
                    }
                }
            },
        }
    }

    result
}
