//! Staged single-handle mutation support.
//!
//! A [`StagedRows`] is a read-your-writes overlay over one [`MapxRaw`]
//! handle: mutations stage rows into an in-memory map (last write wins)
//! and the whole set is drained into a **single atomic engine write
//! batch** on commit.  This is the building block behind the
//! crash-atomic single-handle structures (SlotDex, VecDex): because
//! every mutation commits all of its rows atomically, on-disk state is
//! always internally consistent and no dirty-flag / rebuild-on-recovery
//! protocol is needed.

use std::collections::BTreeMap;
use std::ops::Bound;

use vsdb_core::basic::mapx_raw::MapxRaw;

use super::error::Result;

/// Row staging overlay for one `MapxRaw` handle.
///
/// `Some(bytes)` = pending insert/overwrite, `None` = pending delete.
#[derive(Default)]
pub(crate) struct StagedRows {
    rows: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl StagedRows {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Stages an insert/overwrite.
    pub(crate) fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.rows.insert(key, Some(value));
    }

    /// Stages a delete.
    pub(crate) fn del(&mut self, key: Vec<u8>) {
        self.rows.insert(key, None);
    }

    /// Read-your-writes point lookup: the overlay wins over `store`.
    pub(crate) fn get_over(&self, store: &MapxRaw, key: &[u8]) -> Option<Vec<u8>> {
        match self.rows.get(key) {
            Some(Some(v)) => Some(v.clone()),
            Some(None) => None,
            None => store.get(key),
        }
    }

    /// Read-your-writes prefix scan: merges the committed rows with the
    /// overlay (staged inserts appear, staged deletes disappear), in key
    /// order.
    pub(crate) fn scan_prefix<'a>(
        &'a self,
        store: &'a MapxRaw,
        prefix: &[u8],
    ) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a {
        let lo = prefix.to_vec();
        let hi = prefix_successor(prefix);
        let disk_hi = match &hi {
            Some(h) => Bound::Excluded(std::borrow::Cow::Owned(h.clone())),
            None => Bound::Unbounded,
        };
        let disk = store.range((
            Bound::Included(std::borrow::Cow::Owned(lo.clone())),
            disk_hi,
        ));
        let over_hi = match &hi {
            Some(h) => Bound::Excluded(h.clone()),
            None => Bound::Unbounded,
        };
        let over = self
            .rows
            .range((Bound::Included(lo), over_hi))
            .map(|(k, v)| (k.clone(), v.clone()));
        MergeScan {
            disk: disk.peekable(),
            over: over.peekable(),
        }
    }

    /// Commits every staged row through a single engine write batch.
    ///
    /// On success the staged set has been applied atomically; on error
    /// nothing was applied (the batch is all-or-nothing).
    pub(crate) fn commit(self, store: &mut MapxRaw) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let mut batch = store.batch_entry();
        for (k, v) in &self.rows {
            match v {
                Some(v) => batch.insert(k, v),
                None => batch.remove(k),
            }
        }
        batch.commit()
    }
}

/// Byte-string successor of `prefix` (exclusive upper bound for a prefix
/// scan): increment the last non-0xFF byte and truncate.  `None` means
/// the prefix is all-0xFF and the scan is unbounded above.
pub(crate) fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut s = prefix.to_vec();
    for i in (0..s.len()).rev() {
        if s[i] < u8::MAX {
            s[i] += 1;
            s.truncate(i + 1);
            return Some(s);
        }
    }
    None
}

/// Ordered merge of the committed stream with the overlay stream.
struct MergeScan<D, O>
where
    D: Iterator<Item = (Vec<u8>, Vec<u8>)>,
    O: Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
{
    disk: std::iter::Peekable<D>,
    over: std::iter::Peekable<O>,
}

impl<D, O> Iterator for MergeScan<D, O>
where
    D: Iterator<Item = (Vec<u8>, Vec<u8>)>,
    O: Iterator<Item = (Vec<u8>, Option<Vec<u8>>)>,
{
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let take_over = match (self.disk.peek(), self.over.peek()) {
                (None, None) => return None,
                (Some(_), None) => false,
                (None, Some(_)) => true,
                (Some((dk, _)), Some((ok, _))) => ok <= dk,
            };
            if take_over {
                let (ok, ov) = self.over.next().expect("peeked");
                // The overlay shadows the committed row with the same key.
                if self.disk.peek().is_some_and(|(dk, _)| *dk == ok) {
                    self.disk.next();
                }
                match ov {
                    Some(v) => return Some((ok, v)),
                    None => continue, // staged delete
                }
            } else {
                return self.disk.next();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_scan_overlay_semantics() {
        // The base dir may already be set (and frozen) by another test
        // in the same process; any writable location works here.
        let _ = vsdb_core::vsdb_set_base_dir(format!(
            "/tmp/vsdb_testing/{}",
            rand::random::<u128>()
        ));
        let mut store = MapxRaw::new();
        store.insert([1u8, 1], [10u8]);
        store.insert([1u8, 2], [20u8]);
        store.insert([1u8, 4], [40u8]);
        store.insert([2u8, 0], [99u8]);

        let mut staged = StagedRows::new();
        staged.del(vec![1, 2]); // delete committed
        staged.put(vec![1, 3], vec![30]); // add new
        staged.put(vec![1, 4], vec![41]); // overwrite committed

        assert_eq!(staged.get_over(&store, &[1, 1]), Some(vec![10]));
        assert_eq!(staged.get_over(&store, &[1, 2]), None);
        assert_eq!(staged.get_over(&store, &[1, 3]), Some(vec![30]));
        assert_eq!(staged.get_over(&store, &[1, 4]), Some(vec![41]));

        let scanned: Vec<_> = staged.scan_prefix(&store, &[1]).collect();
        assert_eq!(
            scanned,
            vec![
                (vec![1, 1], vec![10]),
                (vec![1, 3], vec![30]),
                (vec![1, 4], vec![41]),
            ]
        );

        staged.commit(&mut store).unwrap();
        assert_eq!(store.get([1u8, 2]), None);
        assert_eq!(store.get([1u8, 3]), Some(vec![30]));
        assert_eq!(store.get([1u8, 4]), Some(vec![41]));
    }
}
