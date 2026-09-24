//! Read operations for VerMap: branch reads and [`Snapshot`] views.
//!
//! Capturing views retains runtime roots; reads do not change persisted map state.

use std::{
    borrow::Borrow,
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use crate::{
    basic::persistent_btree::RootLease,
    common::{
        ende::{KeyEnDeOrdered, OrderedKeyRef, ValueEnDe},
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
pub struct Snapshot<'a, K, V> {
    map: &'a VerMap<K, V>,
    lease: Arc<RootLease>,
}

// The lease owns its node-pool handle; retaining it does not borrow the view.
// An iterator can outlive the temporary Snapshot that created it.
struct PinnedIter<I> {
    inner: I,
    _lease: Arc<RootLease>,
}

impl<I: Iterator> Iterator for PinnedIter<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a, K, V> Snapshot<'a, K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Reads the value stored under `key` (any borrowed form of `K`, e.g.
    /// `&str` for `String` keys).
    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: OrderedKeyRef + ?Sized,
    {
        self.map
            .tree
            .get(self.lease.root, &key.ordered_key_bytes())
            .map(|v| V::decode(&v).unwrap())
    }

    /// Whether `key` is present (borrowed forms as in [`get`](Self::get)).
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: OrderedKeyRef + ?Sized,
    {
        self.map
            .tree
            .contains_key(self.lease.root, &key.ordered_key_bytes())
    }

    /// Iterates all entries in ascending key order.
    pub fn iter(&self) -> impl Iterator<Item = (K, V)> + use<'a, K, V> {
        PinnedIter {
            inner: self.map.tree.iter(self.lease.root),
            _lease: Arc::clone(&self.lease),
        }
        .map(decode_entry)
    }

    /// Iterates the entries within `range` in ascending key order.
    pub fn range<R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> impl Iterator<Item = (K, V)> + use<'a, K, V, R> {
        let lo = encode_bound(range.start_bound());
        let hi = encode_bound(range.end_bound());
        PinnedIter {
            inner: self
                .map
                .tree
                .range(self.lease.root, as_slice(&lo), as_slice(&hi)),
            _lease: Arc::clone(&self.lease),
        }
        .map(decode_entry)
    }

    /// Iterates the stored `(key, value)` bytes in ascending key order,
    /// without decoding — e.g. to feed an external hasher or exporter.
    ///
    /// Keys are the [`KeyEnDeOrdered`] encoding of `K`, values the
    /// [`ValueEnDe`] encoding of `V`; the same state always yields the
    /// same bytes.
    pub fn raw_iter(&self) -> impl Iterator<Item = (Vec<u8>, Vec<u8>)> + use<'a, K, V> {
        PinnedIter {
            inner: self.map.tree.iter(self.lease.root),
            _lease: Arc::clone(&self.lease),
        }
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
    pub fn at(&self, commit: CommitId) -> Result<Snapshot<'_, K, V>> {
        let lease = self
            .tree
            .lease_root(|| Ok(self.get_commit_inner(commit)?.root))?;
        Ok(Snapshot { map: self, lease })
    }

    /// A view of `branch`'s working state (including uncommitted changes)
    /// as of now.
    ///
    /// # Errors
    ///
    /// [`VsdbError::BranchNotFound`](crate::VsdbError::BranchNotFound) if the
    /// branch does not exist.
    pub fn snapshot(&self, branch: BranchId) -> Result<Snapshot<'_, K, V>> {
        let lease = self
            .tree
            .lease_root(|| Ok(self.get_branch(branch)?.dirty_root))?;
        Ok(Snapshot { map: self, lease })
    }

    /// Reads a value from the working state of `branch`.
    ///
    /// # Panics
    ///
    /// Panics if the stored bytes cannot be decoded back into `V` — see
    /// [`Snapshot`].
    pub fn get<Q>(&self, branch: BranchId, key: &Q) -> Result<Option<V>>
    where
        K: Borrow<Q>,
        Q: OrderedKeyRef + ?Sized,
    {
        Ok(self.snapshot(branch)?.get(key))
    }

    /// Checks if `key` exists in the working state of `branch`.
    pub fn contains_key<Q>(&self, branch: BranchId, key: &Q) -> Result<bool>
    where
        K: Borrow<Q>,
        Q: OrderedKeyRef + ?Sized,
    {
        Ok(self.snapshot(branch)?.contains_key(key))
    }

    /// Iterates all entries on `branch` in ascending key order.
    ///
    /// # Panics
    ///
    /// The returned iterator panics if a stored entry cannot be decoded —
    /// see [`Snapshot`].
    pub fn iter(&self, branch: BranchId) -> Result<impl Iterator<Item = (K, V)> + '_> {
        Ok(self.snapshot(branch)?.iter())
    }

    /// Iterates the entries of `branch` within `range` in ascending key
    /// order.
    ///
    /// # Panics
    ///
    /// The returned iterator panics on decode failure — see [`Snapshot`].
    pub fn range<R: RangeBounds<K>>(
        &self,
        branch: BranchId,
        range: R,
    ) -> Result<impl Iterator<Item = (K, V)> + use<'_, K, V, R>> {
        Ok(self.snapshot(branch)?.range(range))
    }
}

fn decode_entry<K: KeyEnDeOrdered, V: ValueEnDe>((k, v): (Vec<u8>, Vec<u8>)) -> (K, V) {
    (K::from_slice(&k).unwrap(), V::decode(&v).unwrap())
}

fn encode_bound<K: KeyEnDeOrdered>(b: Bound<&K>) -> Bound<Vec<u8>> {
    match b {
        Bound::Included(k) => Bound::Included(k.to_bytes()),
        Bound::Excluded(k) => Bound::Excluded(k.to_bytes()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn as_slice(b: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    b.as_ref().map(Vec::as_slice)
}
