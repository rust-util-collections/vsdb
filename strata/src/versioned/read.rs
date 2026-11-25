//! Read operations for VerMap: branch reads and [`Snapshot`] views.
//!
//! Capturing views retains runtime roots; reads do not change persisted map state.

use std::{marker::PhantomData, ops::Bound, sync::Arc};

use crate::{
    basic::persistent_btree::RootLease,
    common::{
        ende::{KeyEnDeOrdered, OrderedKeyLookup, OrderedRangeBounds, ValueEnDe},
        error::Result,
    },
};

use super::{BranchId, CommitId, map::VerMap};

/// A read-only view of one immutable map state: a historical commit
/// ([`VerMap::at`]) or a branch's working state as of when the view was
/// taken ([`VerMap::snapshot`]).
///
/// The view retains the state's tree root, so its reads cannot fail on a
/// missing branch or commit and never see later writes, including mutations
/// through a restored alias. Derived iterators share that retention even
/// after the view is dropped. Nodes remain live until the view and all its
/// iterators are dropped.
///
/// # Panics
///
/// Reads panic if stored bytes cannot be decoded back into `K`/`V` —
/// only possible on data corruption or a type mismatch between the
/// writing and reading code (see the
/// [encode/decode trust model](crate::common::ende)).
pub struct Snapshot<K, V> {
    _types: PhantomData<fn() -> (K, V)>,
    lease: Arc<RootLease>,
}

impl<K, V> Clone for Snapshot<K, V> {
    fn clone(&self) -> Self {
        Self {
            _types: PhantomData,
            lease: Arc::clone(&self.lease),
        }
    }
}

impl<K, V> Snapshot<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Reads the value stored under `key` (any borrowed form of `K`, e.g.
    /// `&str` for `String` keys).
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: OrderedKeyLookup<Q>,
        Q: ?Sized,
    {
        K::with_ordered_lookup(key, |bytes| self.lease.get(bytes))
            .flatten()
            .map(|v| V::decode_value(&v).unwrap())
    }

    /// Whether `key` is present (borrowed forms as in [`get`](Self::get)).
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: OrderedKeyLookup<Q>,
        Q: ?Sized,
    {
        K::with_ordered_lookup(key, |bytes| self.lease.contains_key(bytes))
            .unwrap_or(false)
    }

    /// Iterates all entries in ascending key order.
    pub fn iter(&self) -> impl Iterator<Item = (K, V)> + use<K, V> {
        self.lease
            .range(Bound::Unbounded, Bound::Unbounded)
            .map(decode_entry)
    }

    /// Iterates the entries within `range` in ascending key order.
    pub fn range<R: OrderedRangeBounds<K>>(
        &self,
        range: R,
    ) -> impl Iterator<Item = (K, V)> + use<K, V, R> {
        let (lo, hi) = range.ordered_bounds();
        self.lease.range(lo, hi).map(decode_entry)
    }

    /// Iterates the stored `(key, value)` bytes in ascending key order,
    /// without decoding — e.g. to feed an external hasher or exporter.
    ///
    /// Keys are the [`KeyEnDeOrdered`] encoding of `K`, values the
    /// [`ValueEnDe`] encoding of `V`; the same state always yields the
    /// same bytes.
    pub fn raw_iter(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + use<K, V> {
        self.lease.range(Bound::Unbounded, Bound::Unbounded)
    }
}

impl<K, V> VerMap<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// A view of the immutable state recorded by `commit`.
    ///
    /// # Errors
    ///
    /// [`VsdbError::CommitNotFound`](crate::VsdbError::CommitNotFound) if the
    /// commit does not exist (it may have been reclaimed).
    pub fn at(&self, commit: CommitId) -> Result<Snapshot<K, V>> {
        let lease = self.commit_root(commit)?;
        Ok(Snapshot {
            _types: PhantomData,
            lease,
        })
    }

    /// A view of `branch`'s working state (including uncommitted changes)
    /// as of now.
    ///
    /// # Errors
    ///
    /// [`VsdbError::BranchNotFound`](crate::VsdbError::BranchNotFound) if the
    /// branch does not exist.
    pub fn snapshot(&self, branch: BranchId) -> Result<Snapshot<K, V>> {
        let lease = self.tree.lease_root(|refs| {
            Ok(refs
                .resolve_branch_raw(&self.tree.nodes, branch)?
                .dirty_root)
        })?;
        Ok(Snapshot {
            _types: PhantomData,
            lease,
        })
    }

    /// Reads a value from the working state of `branch`.
    ///
    /// # Panics
    ///
    /// Panics if the stored bytes cannot be decoded back into `V` — see
    /// [`Snapshot`].
    pub fn get<Q>(&self, branch: BranchId, key: &Q) -> Result<Option<V>>
    where
        K: OrderedKeyLookup<Q>,
        Q: ?Sized,
    {
        self.tree.with_pinned_branch(branch, |root| {
            K::with_ordered_lookup(key, |bytes| self.tree.get_committed(root, bytes))
                .flatten()
                .map(|v| V::decode_value(&v).unwrap())
        })
    }

    /// Checks if `key` exists in the working state of `branch`.
    pub fn contains_key<Q>(&self, branch: BranchId, key: &Q) -> Result<bool>
    where
        K: OrderedKeyLookup<Q>,
        Q: ?Sized,
    {
        self.tree.with_pinned_branch(branch, |root| {
            K::with_ordered_lookup(key, |bytes| {
                self.tree.contains_committed(root, bytes)
            })
            .unwrap_or(false)
        })
    }

    /// Iterates all entries on `branch` in ascending key order.
    ///
    /// # Panics
    ///
    /// The returned iterator panics if a stored entry cannot be decoded —
    /// see [`Snapshot`].
    pub fn iter(
        &self,
        branch: BranchId,
    ) -> Result<impl Iterator<Item = (K, V)> + use<K, V>> {
        Ok(self.snapshot(branch)?.iter())
    }

    /// Iterates the entries of `branch` within `range` in ascending key
    /// order.
    ///
    /// # Panics
    ///
    /// The returned iterator panics on decode failure — see [`Snapshot`].
    pub fn range<R: OrderedRangeBounds<K>>(
        &self,
        branch: BranchId,
        range: R,
    ) -> Result<impl Iterator<Item = (K, V)> + use<K, V, R>> {
        Ok(self.snapshot(branch)?.range(range))
    }
}

fn decode_entry<K: KeyEnDeOrdered, V: ValueEnDe>((k, v): (Vec<u8>, Vec<u8>)) -> (K, V) {
    (K::from_slice(&k).unwrap(), V::decode_value(&v).unwrap())
}

/// Immutable version-store handle. Snapshots retain the selected root across
/// later branch changes and can outlive both the reader and its source map.
pub type VerMapReader<K, V> = crate::MapReader<VerMap<K, V>>;

impl<K, V> crate::common::ReadableMap for VerMap<K, V> {
    type Config = crate::basic::persistent_btree::TreeReadState;
}
impl<K, V> VerMap<K, V> {
    /// Shares this store for snapshot acquisition on other threads.
    pub fn reader(&self) -> VerMapReader<K, V> {
        crate::MapReader::new(self.tree.nodes.reader(), self.tree.read_state())
    }
}
impl<K, V> VerMapReader<K, V> {
    /// Captures the branch's current working root.
    pub fn snapshot(&self, id: BranchId) -> Result<Snapshot<K, V>> {
        let [lease] = self.config.lease(&self.raw, |refs| {
            Ok([refs.resolve_branch_reader(&self.raw, id)?.dirty_root])
        })?;
        Ok(Snapshot {
            lease,
            _types: PhantomData,
        })
    }

    /// Captures the immutable root of a retained commit.
    pub fn at(&self, id: CommitId) -> Result<Snapshot<K, V>> {
        use super::store::{COMMIT, row};
        let [lease] = self.config.lease(&self.raw, |_| {
            let raw = self
                .raw
                .get(row(COMMIT, id.raw()))
                .ok_or(crate::VsdbError::CommitNotFound { commit_id: id })?;
            Ok([super::Commit::parse(id, &raw)?.root])
        })?;
        Ok(Snapshot {
            lease,
            _types: PhantomData,
        })
    }
}
