//!
//! [`VerMap`] — a typed, versioned key-value map with branch / commit /
//! merge support, modelled after Git semantics.
//!

use super::store::{
    BranchHead, COMMIT, Edits, HEAD, HEADER, Header, NAME, by_name, corrupt, row,
    typed_meta_path,
};
use super::{BranchId, Commit, CommitId, NO_COMMIT, diff::DiffEntry};
use crate::{
    MapxRaw,
    basic::persistent_btree::{
        EMPTY_ROOT, FailedPublish, NodeId, PersistentBTree, RootLease, TreeDiff,
    },
    common::{
        InstanceId, PrefixFrame,
        ende::{KeyEnDeOrdered, OrderedKeyLookup, ValueEnDe},
        error::{Result, VsdbError},
    },
};
use serde::{Deserialize, Serialize};
use smallvec::smallvec;
use std::{
    collections::{BTreeMap, BinaryHeap, HashMap, HashSet},
    fs,
    io::ErrorKind,
    marker::PhantomData,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

/// Both sides of a branch's working diff, retained from the same branch row.
pub(crate) struct BranchRoots {
    pub(crate) head: Option<CommitId>,
    pub(crate) committed: Arc<RootLease>,
    pub(crate) working: Arc<RootLease>,
}

// =========================================================================
// VerMap
// =========================================================================

/// A persistent, versioned, ordered key-value map.
///
/// `VerMap` provides Git-style version control for a typed key-value store:
/// branching, committing, three-way merge, rollback, and garbage collection,
/// all backed by a persistent B+ tree with copy-on-write structural sharing.
///
/// # Lifecycle
///
/// A typical workflow mirrors the Git mental model:
///
/// 1. **Create** — `VerMap::new()` gives you a map with a single `main`
///    branch and an empty working state.
/// 2. **Write** — `insert` / `remove` mutate the *working state* of a
///    branch (analogous to editing files in a Git working directory).
/// 3. **Commit** — `commit` snapshots the current working state into an
///    immutable [`Commit`].  Each commit records the B+ tree root, parent
///    linkage, and a wall-clock timestamp.
/// 4. **Branch** — `create_branch` forks a lightweight branch from any
///    existing branch.  The new branch shares all history via structural
///    sharing — no data is copied.
/// 5. **Merge** — `merge(source, target)` performs a three-way merge.
///    Deletion is treated as "assigning ∅", so all conflicts are resolved
///    uniformly: **source wins** — whether source wrote a new value or
///    deleted the key.
/// 6. **Rollback** — `rollback_to` rewinds a branch to an earlier commit;
///    `discard` throws away uncommitted changes.
/// 7. **History** — `log`, and [`at`](Self::at) snapshots of any commit, let you
///    inspect any historical snapshot.
/// 8. **GC** — garbage collection is automatic: orphaned commits cascade
///    through reference counting and unpinned dead B+ tree objects are deleted
///    via engine batch tombstones. [`gc`](Self::gc) /
///    [`verify_and_gc`](Self::verify_and_gc) are only needed for crash
///    recovery or deep integrity verification.
///
/// # Durability and recovery
///
/// Every component of a `VerMap` — header, branch heads/names, commit DAG,
/// `[0x10]` slotted B+ tree pages, `[0x12]` large-value blobs, `[0x05]`/`[0x06]`
/// crash-recovery anchors, and the `[0x07]` reference checkpoint — lives under
/// **one** [`MapxRaw`] prefix on a single engine shard WAL:
///
/// - `insert`, `remove`, `write_batch` and `discard` publish working-root
///   changes in an ordinary atomic batch, without forcing a WAL sync.
/// - `bulk_load` and history changes (`commit`, `merge`, branch create/delete,
///   `rollback_to` and `set_main_branch`) publish the root or metadata in a
///   synchronous batch. A no-op can return without issuing a batch.
/// - Large merges and bulk loads write unpublished pages with `[0x06]`
///   staging markers before the final synchronous publication.
/// - With readers or restored handles sharing the runtime, publication records
///   `[0x05]` markers for retired roots. Snapshot pins keep pages live.
///   Unreachable pages can be removed in a later publication or checkpoint.
/// - On a writable store, [`flush`](Self::flush) and [`close`](Self::close)
///   save a checksummed, generation-bound `[0x07]` reference checkpoint.
///   Ordinary reopen checks the commit graph, root page headers and ID
///   ceilings. A valid checkpoint without retirement or staging markers can
///   supply the object reference counts. Otherwise reopen recounts them.
///   Use [`from_meta_verified`](Self::from_meta_verified) for deep validation.
///
/// # Read-only mode
///
/// A restored `VerMap` supports branch lookup, snapshots, diffs and logs.
/// Writes and explicit `flush` return [`VsdbError::ReadOnly`]. [`gc`](Self::gc)
/// skips maintenance, [`verify_and_gc`](Self::verify_and_gc) validates in memory
/// and [`close`](Self::close) consumes the handle without a checkpoint.
///
/// # Quick start
///
/// ```
/// use vsdb::versioned::map::VerMap;
/// use vsdb::{VsdbOptions, vsdb_configure, vsdb_get_base_dir};
/// use std::fs;
///
/// let dir = std::env::temp_dir().join(format!("vsdb-{}", rand::random::<u128>()));
/// vsdb_configure(VsdbOptions::new(&dir)).unwrap();
///
/// let mut m: VerMap<u32, String> = VerMap::new();
/// let main = m.main_branch();
///
/// // 1. Write on the default "main" branch.
/// m.insert(main, &1, &"hello".into()).unwrap();
/// m.insert(main, &2, &"world".into()).unwrap();
/// let c1 = m.commit(main).unwrap();
///
/// // 2. Fork a feature branch from main.
/// let feat = m.create_branch("feature", main).unwrap();
/// m.insert(feat, &1, &"hi".into()).unwrap();
/// let c2 = m.commit(feat).unwrap();
///
/// // 3. Branches are isolated — main is unchanged.
/// assert_eq!(m.get(main, &1).unwrap(), Some("hello".into()));
/// assert_eq!(m.get(feat, &1).unwrap(), Some("hi".into()));
///
/// // 4. Merge feature → main (source wins on conflict).
/// m.merge(feat, main).unwrap();
/// assert_eq!(m.get(main, &1).unwrap(), Some("hi".into()));
///
/// // 5. Clean up: delete the feature branch.
/// //    Dead commits and B+ tree nodes are reclaimed automatically.
/// m.delete_branch(feat).unwrap();
///
/// fs::remove_dir_all(&dir).unwrap();
/// ```
#[derive(Debug)]
pub struct VerMap<K, V> {
    /// The underlying persistent B+ tree (shared node pool).
    pub(crate) tree: PersistentBTree,

    _phantom: PhantomData<(K, V)>,
}

/// A bounded read-your-writes overlay supplied by [`VerMap::edit_working`].
pub struct WorkingEdit<'a, K, V> {
    base: &'a RootLease,
    changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    used: usize,
    limit: usize,
    types: PhantomData<fn() -> (K, V)>,
}

impl<K: KeyEnDeOrdered, V: ValueEnDe> WorkingEdit<'_, K, V> {
    /// Reads the most recent staged change or the captured working root.
    pub fn get(&self, key: &K) -> Result<Option<V>> {
        key.with_ordered_bytes(|bytes| match self.changes.get(bytes) {
            Some(value) => value.as_deref().map(V::decode_value).transpose(),
            None => self.base.try_get_with(bytes, V::decode_value),
        })
    }

    /// Stages an insertion or replacement. A rejected edit consumes no budget.
    pub fn insert(&mut self, key: &K, value: &V) -> Result<()> {
        self.stage(
            key,
            Some(encode_value(value)?.into_boxed_slice().into_vec()),
        )
    }

    /// Stages a deletion of the key.
    pub fn remove(&mut self, key: &K) -> Result<()> {
        self.stage(key, None)
    }

    fn stage(&mut self, key: &K, value: Option<Vec<u8>>) -> Result<()> {
        key.with_ordered_bytes(|bytes| {
            let size = value.as_ref().map_or(0, Vec::len);
            let charge = match self.changes.get(bytes) {
                Some(old) => {
                    (self.used - old.as_ref().map_or(0, Vec::len)).checked_add(size)
                }
                None => self
                    .used
                    .checked_add(64)
                    .and_then(|total| total.checked_add(bytes.len()))
                    .and_then(|total| total.checked_add(size)),
            };
            self.used = charge.filter(|&n| n <= self.limit).ok_or_else(|| {
                VsdbError::InvalidConfig {
                    detail: "working edit exceeds its encoded byte budget".into(),
                }
            })?;
            self.changes.insert(bytes.to_vec(), value);
            Ok(())
        })
    }
}

impl<K, V> Clone for VerMap<K, V> {
    /// Deep-copies all branches, working trees, and retained commits into a
    /// fresh single-prefix store in the same namespace.
    fn clone(&self) -> Self {
        self.clone_in(&self.namespace())
            .expect("vsdb: VerMap clone failed")
    }
}

impl<K, V> VerMap<K, V> {
    /// Deep-copies the complete working state and retained history into `ns`.
    pub fn clone_in(&self, ns: &crate::common::Namespace) -> Result<Self> {
        Self::open_or_wipe(self.tree.nodes.clone_in(ns)?, true)
    }

    fn open_or_wipe(mut store: MapxRaw, deep: bool) -> Result<Self> {
        let header = match Header::get(&store) {
            Ok(header) => header,
            Err(err) => {
                let _ = store.batch_wiped().commit();
                return Err(err);
            }
        };
        let mut m = Self {
            tree: PersistentBTree::version_store(store, header.next_id),
            _phantom: PhantomData,
        };
        if let Err(err) = m.recover(deep) {
            let _ = m.tree.nodes.batch_wiped().commit();
            return Err(err);
        }
        Ok(m)
    }

    pub(crate) fn open(store: MapxRaw, deep: bool) -> Result<Self> {
        let header = Header::get(&store)?;
        let tree = PersistentBTree::version_store(store, header.next_id);
        let mut m = Self {
            tree,
            _phantom: PhantomData,
        };
        m.recover(deep)?;
        Ok(m)
    }
}

impl<K, V> Serialize for VerMap<K, V> {
    fn serialize<S: serde::Serializer>(
        &self,
        s: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        crate::common::serialize_prefix_handle::<Self, _>(&self.tree.nodes, &[], s)
    }
}

impl<'de, K, V> Deserialize<'de> for VerMap<K, V> {
    fn deserialize<D: serde::Deserializer<'de>>(
        d: D,
    ) -> std::result::Result<Self, D::Error> {
        crate::common::deserialize_prefix_handle(d, |frame| match frame {
            PrefixFrame(raw, []) => Self::open(raw, false),
            PrefixFrame(_, _) => Err(corrupt("invalid version store handle")),
        })
    }
}
// Separate impl block without K/V trait bounds so that the
// Deserialize visitor (which has no trait bounds on K/V) can call it.

impl<K, V> Default for VerMap<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> VerMap<K, V> {
    /// Returns the unique instance ID of this `VerMap`.
    pub fn instance_id(&self) -> InstanceId {
        self.tree.instance_id()
    }

    /// The namespace this map lives in.
    pub fn namespace(&self) -> crate::common::Namespace {
        self.tree.namespace()
    }

    pub(super) fn ensure_writable(&self, operation: &'static str) -> Result<()> {
        crate::common::ensure_writable(&self.namespace(), operation)
    }

    pub(super) fn get_branch(&self, id: BranchId) -> Result<BranchHead> {
        self.tree.get_branch_head(id)
    }

    pub(crate) fn get_commit_inner(&self, id: CommitId) -> Result<Commit> {
        self.tree
            .nodes
            .try_get_with(row(COMMIT, id.raw()), |raw| Commit::parse(id, raw))?
            .ok_or(VsdbError::CommitNotFound { commit_id: id })
    }

    fn get_history_commit(&self, id: CommitId) -> Result<Commit> {
        match self.get_commit_inner(id) {
            Ok(commit) => Ok(commit),
            Err(err @ VsdbError::CommitNotFound { .. }) => Err(corrupt(err.to_string())),
            Err(err) => Err(err),
        }
    }

    pub(super) fn publish_edits(&mut self, edits: Edits, sync: bool) -> Result<()> {
        let hdr = match Header::get(&self.tree.nodes) {
            Ok(hdr) => hdr,
            Err(err) => {
                self.tree.abandon_staged();
                self.tree.mark_refs_poisoned();
                let _ = self.restore_visible_refs();
                return Err(err);
            }
        };
        self.publish(hdr, edits, sync)
    }

    pub(super) fn publish(
        &mut self,
        mut hdr: Header,
        mut edits: Edits,
        sync: bool,
    ) -> Result<()> {
        self.tree.ensure_healthy_refs()?;
        let Some(generation) = hdr.generation.checked_add(1) else {
            self.tree.abandon_staged();
            self.tree.mark_refs_poisoned();
            let _ = self.restore_visible_refs();
            return Err(corrupt("version store generation exhausted"));
        };
        hdr.generation = generation;
        hdr.next_id = self.tree.next_id();
        edits
            .rows
            .insert(HEADER.to_vec(), Some(hdr.bytes().to_vec()));
        if let Err((err, cleanup)) = self.tree.publish(
            edits.rows,
            &edits.heads,
            &edits.acquire,
            &edits.retire,
            sync.then_some(generation),
        ) {
            if let FailedPublish::AmbiguousStaged { staged, start, end } = cleanup {
                self.tree.defer_ambiguous_staged(staged, start, end);
            }
            let _ = self.restore_visible_refs();
            return Err(err);
        }
        Ok(())
    }

    fn restore_visible_refs(&self) -> Result<()> {
        self.tree.restore_visible_refs()
    }

    fn build_root(
        &mut self,
        build: impl FnOnce(&mut PersistentBTree) -> Result<NodeId>,
    ) -> Result<NodeId> {
        use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
        self.tree.ensure_healthy_refs()?;
        self.tree.begin_staged_build();
        match catch_unwind(AssertUnwindSafe(|| build(&mut self.tree))) {
            Ok(Ok(root)) => Ok(root),
            result => {
                self.tree.abandon_staged();
                self.tree.mark_refs_poisoned();
                let _ = self.restore_visible_refs();
                match result {
                    Ok(result) => result,
                    Err(payload) => resume_unwind(payload),
                }
            }
        }
    }

    fn adjust_commit(
        &self,
        edits: &mut Edits,
        id: CommitId,
        increment: bool,
    ) -> Result<()> {
        let mut work = vec![(id, increment)];
        while let Some((id, inc)) = work.pop() {
            if id == NO_COMMIT {
                continue;
            }
            let key = row(COMMIT, id.raw());
            let mut c = match edits.rows.get(key.as_slice()) {
                Some(Some(raw)) => Commit::parse(id, raw)?,
                Some(None) => return Err(corrupt("inconsistent commit ownership")),
                None => self.get_commit_inner(id)?,
            };
            c.ref_count = if inc {
                c.ref_count.checked_add(1)
            } else {
                c.ref_count.checked_sub(1)
            }
            .ok_or_else(|| corrupt("commit refcount overflow or underflow"))?;
            if c.ref_count == 0 {
                edits.rows.insert(key.to_vec(), None);
                edits.retire.push(c.root);
                work.extend(c.parents.into_iter().map(|p| (p, false)));
            } else {
                edits.write_commit(&c);
            }
        }
        Ok(())
    }

    /// [`diff_commits`](VerMap::diff_commits) as stored bytes (keys in
    /// their `KeyEnDeOrdered` encoding, values in their `ValueEnDe`
    /// encoding) — e.g. to update an external hash structure incrementally.
    /// Both captured states remain retained until comparison finishes.
    pub fn raw_diff_commits(
        &self,
        from: CommitId,
        to: CommitId,
    ) -> Result<Vec<TreeDiff>> {
        let [from_root, to_root] = self.tree.lease_roots(|_| {
            Ok([
                self.get_commit_inner(from)?.root,
                self.get_commit_inner(to)?.root,
            ])
        })?;
        self.diff_roots(&from_root, &to_root)
    }

    /// [`diff_uncommitted`](VerMap::diff_uncommitted) as stored bytes (see
    /// [`raw_diff_commits`](VerMap::raw_diff_commits)).
    pub fn raw_diff_uncommitted(&self, branch: BranchId) -> Result<Vec<TreeDiff>> {
        let roots = self.branch_roots(branch)?;
        self.diff_roots(&roots.committed, &roots.working)
    }

    pub(crate) fn diff_roots(
        &self,
        from: &RootLease,
        to: &RootLease,
    ) -> Result<Vec<TreeDiff>> {
        let mut diff = self.diff_roots_iter(from, to)?;
        let mut entries = Vec::new();
        while let Some(entry) = diff.try_next()? {
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Step the returned stream with `try_next` to surface page errors.
    pub(crate) fn diff_roots_iter(
        &self,
        from: &RootLease,
        to: &RootLease,
    ) -> Result<crate::basic::persistent_btree::BTreeDiffIter<'_>> {
        #[cfg(test)]
        super::test::before_read_traversal();
        self.tree.try_diff_iter(from.root, to.root)
    }

    pub(crate) fn iter_leased_root(
        &self,
        root: &RootLease,
    ) -> crate::basic::persistent_btree::BTreeIter<'_> {
        #[cfg(test)]
        super::test::before_read_traversal();
        self.tree.iter(root.root)
    }

    pub(crate) fn commit_root(&self, id: CommitId) -> Result<Arc<RootLease>> {
        self.tree
            .lease_root(|_| Ok(self.get_commit_inner(id)?.root))
    }

    pub(crate) fn branch_roots(&self, branch: BranchId) -> Result<BranchRoots> {
        let mut head = None;
        let [committed, working] = self.tree.lease_roots(|refs| {
            loop {
                let state = refs.resolve_branch_raw(&self.tree.nodes, branch)?;
                if state.head == NO_COMMIT {
                    return Ok([EMPTY_ROOT, state.dirty_root]);
                }
                match self.get_commit_inner(state.head) {
                    Ok(commit) => {
                        head = Some(state.head);
                        return Ok([commit.root, state.dirty_root]);
                    }
                    Err(VsdbError::CommitNotFound { .. }) => {
                        // Evict any cached BranchHead in case a concurrent
                        // rollback deleted old commits in mmdb just before
                        // updating RefState.heads.
                        refs.heads.remove(&branch);
                        if refs.resolve_branch_raw(&self.tree.nodes, branch)?.head
                            == state.head
                        {
                            return Err(VsdbError::CommitNotFound {
                                commit_id: state.head,
                            });
                        }
                    }
                    Err(err) => return Err(err),
                }
            }
        })?;
        Ok(BranchRoots {
            head,
            committed,
            working,
        })
    }
}

impl<K, V> VerMap<K, V>
where
    K: KeyEnDeOrdered,
    V: ValueEnDe,
{
    /// Creates a new, empty versioned map with a default `main` branch.
    ///
    /// Equivalent to `new_with_main("main")`.
    pub fn new() -> Self {
        Self::new_with_main("main")
    }

    /// [`new`](Self::new) placed in `ns` — every internal component of
    /// the map lands in the same namespace (a composite never spans
    /// namespaces).
    pub fn new_in(ns: &crate::common::Namespace) -> Self {
        ns.scope(Self::new)
    }

    /// Persists this instance's metadata to disk so that it can be
    /// recovered later via [`from_meta`](Self::from_meta).
    ///
    /// Returns the `instance_id` that should be passed to `from_meta`.
    pub fn save_meta(&self) -> Result<InstanceId> {
        self.save_handle_meta(self)
    }

    pub(crate) fn save_handle_meta<T: Serialize>(
        &self,
        handle: &T,
    ) -> Result<InstanceId> {
        self.ensure_writable("version store metadata save")?;
        let id = self.instance_id();
        let primary = self.namespace().meta_path(id.map_id);
        let bytes = postcard::to_allocvec(handle)?;
        let _guard = self.tree.lock_meta_save();
        let path = match fs::read(&primary) {
            Ok(saved) if saved != bytes => typed_meta_path::<T>(&primary),
            Ok(_) => primary,
            Err(err) if err.kind() == ErrorKind::NotFound => primary,
            Err(err) => return Err(err.into()),
        };
        crate::common::create_dir_all_durable(path.parent().expect("has parent"))?;
        crate::common::atomic_write_file(&path, &bytes)?;
        Ok(id)
    }

    /// Recovers a `VerMap` instance from previously saved metadata.
    ///
    /// The caller must ensure that the underlying VSDB database still
    /// contains the data referenced by this instance ID.
    /// Writable recovery can repair indexes and reclaim unpublished pages.
    /// Serialize recovery with other writers to this instance. Use an
    /// independent [`reader`](Self::reader) for concurrent snapshot access.
    pub fn from_meta(instance_id: impl Into<InstanceId>) -> Result<Self> {
        Self::open_meta(instance_id.into(), false)
    }

    /// Creates a new, empty versioned map whose initial branch has the
    /// given `name` (e.g. `"genesis"`, `"canonical"`).
    ///
    /// The initial branch is automatically designated as the *main* branch
    /// and cannot be deleted until another branch is promoted via
    /// [`set_main_branch`](Self::set_main_branch).
    pub fn new_with_main(name: &str) -> Self {
        let mut tree = PersistentBTree::version_store(MapxRaw::new(), 1);
        let hdr = Header::initial();
        let mut batch = tree.nodes.batch();
        batch.insert(HEADER, hdr.bytes());
        batch.insert(
            row(HEAD, 1),
            BranchHead {
                head: NO_COMMIT,
                dirty_root: EMPTY_ROOT,
            }
            .bytes(),
        );
        batch.insert(row(NAME, 1), name.as_bytes());
        batch.insert(by_name(name), 1u64.to_le_bytes());
        batch.commit_sync().expect("failed to create VerMap");
        Self {
            tree,
            _phantom: PhantomData,
        }
    }

    // Main branch
    // =================================================================

    /// Returns the [`BranchId`] of the current main branch.
    pub fn main_branch(&self) -> BranchId {
        Header::get(&self.tree.nodes)
            .expect("invalid version store header")
            .main_branch
    }

    /// Designates `branch` as the new main branch.
    ///
    /// The previous main branch becomes an ordinary branch (deletable).
    /// The new main branch is protected from deletion.
    pub fn set_main_branch(&mut self, branch: BranchId) -> Result<()> {
        self.ensure_writable("main branch update")?;
        self.get_branch(branch)?;
        let mut hdr = Header::get(&self.tree.nodes)?;
        hdr.main_branch = branch;
        self.publish(hdr, Edits::default(), true)
    }

    // =================================================================
    // Branch management
    // =================================================================

    /// Creates a new branch forked from `source_branch`.
    ///
    /// The new branch inherits both the committed head and the current
    /// working state (uncommitted changes, if any) of the source branch.
    pub fn create_branch(
        &mut self,
        name: &str,
        source_branch: BranchId,
    ) -> Result<BranchId> {
        self.ensure_writable("branch creation")?;
        let exists = self
            .tree
            .nodes
            .try_get_with(by_name(name), |raw| {
                if raw.len() != 8 {
                    return Err(corrupt("invalid reverse branch row"));
                }
                Ok(())
            })?
            .is_some();
        if exists {
            return Err(VsdbError::BranchAlreadyExists {
                name: name.to_owned(),
            });
        }
        let src = self.get_branch(source_branch)?;
        let mut hdr = Header::get(&self.tree.nodes)?;
        let id = BranchId::from_raw(hdr.next_branch);
        hdr.next_branch = id
            .raw()
            .checked_add(1)
            .ok_or_else(|| corrupt("branch IDs exhausted"))?;
        let mut edits = Edits::default();
        edits.write_head(id, src);
        edits
            .rows
            .insert(row(NAME, id.raw()).to_vec(), Some(name.as_bytes().to_vec()));
        edits
            .rows
            .insert(by_name(name), Some(id.raw().to_le_bytes().to_vec()));
        edits.acquire.push(src.dirty_root);
        self.adjust_commit(&mut edits, src.head, true)?;
        self.publish(hdr, edits, true)?;
        Ok(id)
    }

    /// Deletes a branch and automatically cleans up orphaned commits.
    ///
    /// Decrements the ref-count on the branch's HEAD commit. If it
    /// reaches zero, the commit is hard-deleted and the decrement
    /// cascades to its parents.
    ///
    /// Unreachable tree objects are reclaimed at publication or queued until
    /// their last reader releases them and a later batch drains the queue.
    pub fn delete_branch(&mut self, branch: BranchId) -> Result<()> {
        self.ensure_writable("branch deletion")?;
        let hdr = Header::get(&self.tree.nodes)?;
        if branch == hdr.main_branch {
            return Err(VsdbError::CannotDeleteMainBranch);
        }
        let state = self.get_branch(branch)?;
        let name = self
            .tree
            .nodes
            .try_get_with(row(NAME, branch.raw()), |raw| {
                std::str::from_utf8(raw)
                    .map(str::to_owned)
                    .map_err(|_| corrupt("invalid branch name"))
            })?
            .ok_or_else(|| corrupt("missing branch name"))?;
        let mut edits = Edits::default();
        edits.delete_head(branch);
        edits.rows.insert(row(NAME, branch.raw()).to_vec(), None);
        edits.rows.insert(by_name(&name), None);
        edits.retire.push(state.dirty_root);
        self.adjust_commit(&mut edits, state.head, false)?;
        self.publish(hdr, edits, true)
    }

    /// Lists all branches as `(BranchId, name)`.
    pub fn list_branches(&self) -> Vec<(BranchId, String)> {
        self.tree
            .nodes
            .range([NAME].as_slice()..[NAME + 1].as_slice())
            .map(|(k, v)| {
                (
                    BranchId::from_raw(u64::from_be_bytes(
                        k[1..].try_into().expect("invalid branch name key"),
                    )),
                    String::from_utf8(v).expect("invalid branch name"),
                )
            })
            .collect()
    }

    /// Looks up a branch by name, returning its ID if it exists.
    pub fn branch_id(&self, name: &str) -> Option<BranchId> {
        self.tree.nodes.get(by_name(name)).map(|v| {
            BranchId::from_raw(u64::from_le_bytes(
                v.try_into().expect("invalid reverse branch row"),
            ))
        })
    }

    /// Returns the name of a branch given its ID.
    pub fn branch_name(&self, branch: BranchId) -> Option<String> {
        self.tree
            .nodes
            .get(row(NAME, branch.raw()))
            .map(|v| String::from_utf8(v).expect("invalid branch name"))
    }

    /// Returns `true` if the branch has uncommitted changes (dirty state
    /// differs from the head commit's snapshot in its stored keys or values).
    /// Reverting all edits to their committed bytes leaves the branch clean.
    pub fn has_uncommitted(&self, branch: BranchId) -> Result<bool> {
        let roots = self.branch_roots(branch)?;
        self.tree.differs(roots.committed.root, roots.working.root)
    }

    // Write (working state)
    // =================================================================

    /// Inserts a key-value pair into the working state of `branch`.
    /// An unreadable or malformed tree page returns an error before the
    /// replacement working root is published.
    pub fn insert(&mut self, branch: BranchId, key: &K, value: &V) -> Result<()> {
        self.ensure_writable("versioned map insert")?;
        let state = self.get_branch(branch)?;
        let bytes = encode_value(value)?;
        let root = self.build_root(|tree| {
            key.with_ordered_bytes(|k| {
                tree.try_insert_buffered(state.dirty_root, k, &bytes)
            })
        })?;
        self.publish_working(branch, state, root)
    }

    /// Replaces the working state with entries in ascending encoded-key order.
    /// Duplicate keys keep the final value. Large inputs publish their root
    /// after the unpublished page batches are durable.
    pub fn bulk_load(
        &mut self,
        branch: BranchId,
        entries: impl IntoIterator<Item = (K, V)>,
    ) -> Result<()> {
        self.ensure_writable("version store bulk load")?;
        let state = self.get_branch(branch)?;
        let root = self.build_root(|tree| {
            tree.try_bulk_load(
                entries
                    .into_iter()
                    .map(|(k, v)| Ok((k.into_bytes(), encode_value(&v)?))),
            )
        })?;
        let mut edits = Edits::default();
        edits.replace_root(state.dirty_root, root);
        edits.write_head(
            branch,
            BranchHead {
                dirty_root: root,
                ..state
            },
        );
        self.publish_edits(edits, true)
    }
    /// Applies a group of working-state edits with one branch-root publication.
    /// A `Some` value inserts or replaces and `None` removes the key.
    /// Duplicate keys keep their last supplied edit.
    pub fn write_batch(
        &mut self,
        branch: BranchId,
        edits: impl IntoIterator<Item = (K, Option<V>)>,
    ) -> Result<()> {
        self.ensure_writable("versioned map batch")?;
        let state = self.get_branch(branch)?;
        let changes = edits
            .into_iter()
            .map(|(key, value)| {
                Ok((
                    key.into_bytes(),
                    value.as_ref().map(encode_value).transpose()?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        self.apply_working_changes(branch, state, changes)
    }

    fn apply_working_changes(
        &mut self,
        branch: BranchId,
        state: BranchHead,
        changes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> Result<()> {
        let root = self.build_root(|tree| {
            tree.apply_changes(state.dirty_root, changes.into_iter().map(Ok), false)
        })?;
        self.publish_working(branch, state, root)
    }

    /// Reads and stages dependent edits on one working branch. A successful
    /// callback publishes all edits together. Returning an error or unwinding
    /// discards the overlay. A subsequent history commit retains its ordinary
    /// durability. Writers to aliases of the branch still require exclusion.
    ///
    /// The budget charges the encoded key and value plus 64 bytes per distinct
    /// edited key, including deletions. Replacements release their previous
    /// value's charge. Temporary encoding and the tree's publication buffers
    /// have independent lifetimes.
    pub fn edit_working<R>(
        &mut self,
        branch: BranchId,
        max_bytes: usize,
        edit: impl FnOnce(&mut WorkingEdit<'_, K, V>) -> Result<R>,
    ) -> Result<R> {
        self.ensure_writable("versioned working edit")?;
        let lease = self.tree.lease_root(|refs| {
            Ok(refs
                .resolve_branch_raw(&self.tree.nodes, branch)?
                .dirty_root)
        })?;
        let mut session = WorkingEdit {
            base: &lease,
            changes: BTreeMap::new(),
            used: 0,
            limit: max_bytes,
            types: PhantomData,
        };
        let result = edit(&mut session)?;
        if !session.changes.is_empty() {
            let state = self.get_branch(branch)?;
            if state.dirty_root != lease.root {
                return Err(VsdbError::WorkingRootChanged { branch_id: branch });
            }
            let changes = session.changes;
            drop(lease);
            self.apply_working_changes(branch, state, changes)?;
        }
        Ok(result)
    }

    /// Removes a key from the working state of `branch`.
    /// An unreadable or malformed tree page returns an error before the
    /// replacement working root is published.
    pub fn remove<Q>(&mut self, branch: BranchId, key: &Q) -> Result<()>
    where
        K: OrderedKeyLookup<Q>,
        Q: ?Sized,
    {
        self.ensure_writable("versioned map remove")?;
        let state = self.get_branch(branch)?;
        let root = self.build_root(|tree| {
            K::with_ordered_lookup(key, |bytes| {
                tree.try_remove_buffered(state.dirty_root, bytes)
            })
            .unwrap_or(Ok(state.dirty_root))
        })?;
        self.publish_working(branch, state, root)
    }

    fn publish_working(
        &mut self,
        branch: BranchId,
        state: BranchHead,
        root: NodeId,
    ) -> Result<()> {
        if state.dirty_root == root {
            self.tree.abandon_staged();
            return Ok(());
        }
        let mut edits = Edits::default();
        edits.replace_root(state.dirty_root, root);
        edits.write_head(
            branch,
            BranchHead {
                dirty_root: root,
                ..state
            },
        );
        self.publish_edits(edits, false)
    }

    // =================================================================
    // Commit / Rollback
    // =================================================================

    /// Commits the current working state of `branch`, creating a new
    /// immutable [`Commit`].  Returns the commit ID.
    pub fn commit(&mut self, branch: BranchId) -> Result<CommitId> {
        self.ensure_writable("versioned map commit")?;
        let state = self.get_branch(branch)?;

        let mut hdr = Header::get(&self.tree.nodes)?;
        let id = CommitId::from_raw(hdr.next_commit);
        hdr.next_commit = id
            .raw()
            .checked_add(1)
            .ok_or_else(|| corrupt("commit IDs exhausted"))?;
        let parents = if state.head == NO_COMMIT {
            smallvec![]
        } else {
            smallvec![state.head]
        };

        // ref_count accounting for this operation:
        //   - new commit = 1: the branch HEAD moves onto it below.
        //   - old HEAD   = net 0: it loses this branch's HEAD (-1) but
        //     gains a parent-link from the new commit (+1), so its
        //     stored count is deliberately left untouched.  This pairing
        //     is load-bearing — if commit() ever stops listing the old
        //     HEAD in `parents` (or the HEAD moves anywhere other than
        //     onto a child of the old HEAD), the old HEAD's count must
        //     be adjusted explicitly.  INV-V1 is pinned mechanically by
        //     the `ref_counts_match_ground_truth_recount` test.
        let commit = Commit {
            id,
            root: state.dirty_root,
            parents,
            timestamp_us: now_us(),
            ref_count: 1,
        };
        let mut edits = Edits::default();
        edits.write_commit(&commit);
        edits.acquire.push(state.dirty_root);
        edits.write_head(branch, BranchHead { head: id, ..state });
        self.publish(hdr, edits, true)?;
        Ok(id)
    }

    /// Discards uncommitted changes, resetting the working state to the
    /// branch head.
    pub fn discard(&mut self, branch: BranchId) -> Result<()> {
        self.ensure_writable("working-state discard")?;
        let state = self.get_branch(branch)?;
        let root = if state.head == NO_COMMIT {
            EMPTY_ROOT
        } else {
            self.get_commit_inner(state.head)?.root
        };
        self.publish_working(branch, state, root)
    }

    /// Rolls back `branch` to a previous commit, discarding all commits
    /// after `target` on this branch.
    ///
    /// `target` must be an ancestor of the branch's current head.
    /// Commits between `target` and the previous head that are exclusively
    /// reachable from this branch are immediately deleted via ref-count
    /// cascade.  Commits still referenced by other branches are preserved.
    /// Call [`gc`](Self::gc) only to recover from a crash or force a full
    /// B+ tree sweep.
    pub fn rollback_to(&mut self, branch: BranchId, target: CommitId) -> Result<()> {
        self.ensure_writable("branch rollback")?;
        let state = self.get_branch(branch)?;
        let _ = self.get_commit_inner(target)?;

        // A branch with no commits has no ancestors — rolling it back to
        // an arbitrary commit would silently attach it to another
        // branch's history.
        if state.head == NO_COMMIT {
            return Err(VsdbError::NotAncestor {
                commit_id: target,
                branch_id: branch,
            });
        }

        // Rolling back — whether to the current HEAD or to a strict
        // ancestor — always overwrites `dirty_root`, silently losing any
        // uncommitted changes.  Reject unconditionally (matching
        // `merge()`'s unconditional guard) and tell the caller to use
        // `discard()` instead. This check must run before the
        // target-vs-head branching below, not just the `target ==
        // state.head` arm — an ancestor target must not bypass it.
        if self.has_uncommitted(branch)? {
            return Err(VsdbError::UncommittedChanges { branch_id: branch });
        }

        if target == state.head {
            // target == head with a clean working state (uncommitted
            // changes are already ruled out above): a complete no-op.
            // Avoid writing an identical branch head.
            return Ok(());
        }

        // Verify target is reachable from the branch head.
        {
            let mut queue = vec![state.head];
            let mut visited = HashSet::new();
            let mut found = false;
            while let Some(cur) = queue.pop() {
                if cur == NO_COMMIT || !visited.insert(cur) {
                    continue;
                }
                if cur == target {
                    found = true;
                    break;
                }
                let c = self.get_history_commit(cur)?;
                queue.extend_from_slice(&c.parents);
            }
            if !found {
                return Err(VsdbError::NotAncestor {
                    commit_id: target,
                    branch_id: branch,
                });
            }
        }

        let commit = self.get_commit_inner(target)?;
        let mut edits = Edits::default();
        edits.write_head(
            branch,
            BranchHead {
                head: target,
                dirty_root: commit.root,
            },
        );
        edits.replace_root(state.dirty_root, commit.root);
        self.adjust_commit(&mut edits, target, true)?;
        self.adjust_commit(&mut edits, state.head, false)?;
        self.publish(Header::get(&self.tree.nodes)?, edits, true)
    }

    // =================================================================
    // Merge
    // =================================================================

    /// Merges `source` branch into `target` branch using three-way merge.
    ///
    /// Both branches must be committed (no uncommitted changes).
    ///
    /// # Conflict resolution: source wins on conflicts
    ///
    /// First, non-conflicting single-sided changes are preserved using the
    /// ancestor snapshot. If both sides changed the same key differently,
    /// **source wins**. A deletion is treated as "assigning ∅", so
    /// delete-vs-modify is also resolved by source priority.
    ///
    /// | source | target | result |
    /// |--------|--------|--------|
    /// | unchanged (A) | changed to T | **T** (target-only change preserved) |
    /// | changed to S | unchanged (A) | **S** (source-only change preserved) |
    /// | changed to S | changed to T | **S** (conflict → source wins) |
    /// | deleted (∅) | changed to T | **∅** (conflict → source wins → delete) |
    /// | changed to S | deleted (∅) | **S** (conflict → source wins → keep) |
    ///
    /// The caller controls priority by choosing which branch to pass as
    /// `source` vs `target`.
    ///
    /// If `target` has no commits, performs a fast-forward (no merge commit
    /// is created). Equal source/target heads return the existing commit.
    /// Other successful merges create a two-parent commit on `target`.
    ///
    /// # Cost
    ///
    /// The merge replays the source-side delta (keys changed since the
    /// merge bases) onto the target tree by copy-on-write. Replay work and
    /// buffering follow changed paths; diff traversal skips shared NodeIds
    /// but can scan the full tree when versions share no nodes. Finding
    /// merge bases also walks commit history. Unaffected target subtrees
    /// remain shared where COW paths permit it.
    pub fn merge(&mut self, source: BranchId, target: BranchId) -> Result<CommitId> {
        self.ensure_writable("branch merge")?;
        if source == target {
            return Err(VsdbError::SelfMerge { branch_id: source });
        }

        // Reject if either branch has uncommitted changes.
        if self.has_uncommitted(source)? {
            return Err(VsdbError::UncommittedChanges { branch_id: source });
        }
        if self.has_uncommitted(target)? {
            return Err(VsdbError::UncommittedChanges { branch_id: target });
        }

        let src = self.get_branch(source)?;
        let tgt = self.get_branch(target)?;

        if src.head == NO_COMMIT {
            return Err(VsdbError::NoCommits { branch_id: source });
        }

        // Both branches point to the same commit — nothing to merge.
        // Creating a merge commit with duplicate parents would permanently
        // overcount the commit's ref_count (GC leak).  Return the existing
        // commit as a successful no-op instead.
        if src.head == tgt.head {
            return Ok(tgt.head);
        }

        let mut hdr = Header::get(&self.tree.nodes)?;
        let mut edits = Edits::default();
        self.adjust_commit(&mut edits, src.head, true)?;
        if tgt.head == NO_COMMIT {
            let commit = self.get_commit_inner(src.head)?;
            edits.replace_root(tgt.dirty_root, commit.root);
            edits.write_head(
                target,
                BranchHead {
                    head: src.head,
                    dirty_root: commit.root,
                },
            );
            self.publish(hdr, edits, true)?;
            return Ok(src.head);
        }
        let src_commit = self.get_commit_inner(src.head)?;
        let tgt_commit = self.get_commit_inner(tgt.head)?;

        let ancestor_roots: Vec<NodeId> = self
            .try_find_merge_bases(src.head, tgt.head)?
            .into_iter()
            .map(|aid| self.get_history_commit(aid).map(|c| c.root))
            .collect::<Result<_>>()?;
        let ancestor_roots = if ancestor_roots.is_empty() {
            vec![EMPTY_ROOT]
        } else {
            ancestor_roots
        };

        let id = CommitId::from_raw(hdr.next_commit);
        hdr.next_commit = id
            .raw()
            .checked_add(1)
            .ok_or_else(|| corrupt("commit IDs exhausted"))?;
        let merged_root = self.build_root(|tree| {
            tree.try_merge(&ancestor_roots, src_commit.root, tgt_commit.root)
        })?;
        edits.write_commit(&Commit {
            id,
            root: merged_root,
            parents: smallvec![tgt.head, src.head],
            timestamp_us: now_us(),
            ref_count: 1,
        });
        edits.acquire.push(merged_root);
        edits.replace_root(tgt.dirty_root, merged_root);
        edits.write_head(
            target,
            BranchHead {
                head: id,
                dirty_root: merged_root,
            },
        );
        self.publish(hdr, edits, true)?;
        Ok(id)
    }

    /// Finds all lowest common ancestors (merge bases) of two commits.
    ///
    /// A criss-cross DAG can have multiple incomparable merge bases.  Feeding
    /// only the highest-id base into a three-way merge can classify a true
    /// conflict as a target-only change and violate the source-wins policy.
    ///
    /// Implementation: git-style "paint down to common".  The DAG is walked
    /// in descending `CommitId` order using a max-heap (`CommitId`s are
    /// monotonic — a parent is always created before its child), flagging
    /// each commit with the side(s) it is reachable from.  A commit popped
    /// with both flags and no `STALE` bit is a merge base; its ancestry is
    /// then painted `STALE`, so dominated common ancestors are never
    /// reported.  The walk ends when the frontier holds only stale entries,
    /// bounding the cost to the fork region instead of the full history.
    fn find_merge_bases(&self, a: CommitId, b: CommitId) -> Vec<CommitId> {
        self.try_find_merge_bases(a, b)
            .expect("vsdb: commit history read failed")
    }

    fn try_find_merge_bases(&self, a: CommitId, b: CommitId) -> Result<Vec<CommitId>> {
        const FROM_A: u8 = 0b001;
        const FROM_B: u8 = 0b010;
        const BOTH: u8 = FROM_A | FROM_B;
        const STALE: u8 = 0b100;

        // Nonexistent IDs are not DAG nodes; without this guard,
        // `find_merge_bases(x, x)` would report a nonexistent `x`
        // as its own merge base.
        match (self.get_commit_inner(a), self.get_commit_inner(b)) {
            (Ok(_), Ok(_)) => {}
            (Err(VsdbError::CommitNotFound { .. }), Ok(_))
            | (Ok(_), Err(VsdbError::CommitNotFound { .. }))
            | (
                Err(VsdbError::CommitNotFound { .. }),
                Err(VsdbError::CommitNotFound { .. }),
            ) => return Ok(vec![]),
            (Err(err), _) | (_, Err(err)) => return Err(err),
        }

        fn mark(
            flags: &mut HashMap<CommitId, u8>,
            heap: &mut BinaryHeap<CommitId>,
            non_stale_count: &mut usize,
            id: CommitId,
            flag: u8,
        ) {
            if id == NO_COMMIT {
                return;
            }
            let slot = flags.entry(id).or_insert(0);
            if *slot == 0 {
                heap.push(id);
                if flag & STALE == 0 {
                    *non_stale_count += 1;
                }
            } else if *slot & STALE == 0 && flag & STALE != 0 {
                // A parent always has a lower id than its child, so it is
                // still queued when a newly popped child marks it stale.
                *non_stale_count -= 1;
            }
            *slot |= flag;
        }

        let mut flags: HashMap<CommitId, u8> = HashMap::new();
        let mut heap: BinaryHeap<CommitId> = BinaryHeap::new();
        let mut non_stale_count = 0;
        let mut bases = Vec::new();

        mark(&mut flags, &mut heap, &mut non_stale_count, a, FROM_A);
        mark(&mut flags, &mut heap, &mut non_stale_count, b, FROM_B);

        while non_stale_count != 0 {
            // The loop guard ensures the heap is nonempty.
            let id = heap.pop().unwrap();
            // Every id pushed onto the heap was first inserted into `flags`.
            let mut f = flags[&id];
            if f & STALE == 0 {
                non_stale_count -= 1;
            }
            if f & BOTH == BOTH {
                if f & STALE == 0 {
                    bases.push(id);
                }
                // Everything below a common commit is dominated.
                f |= STALE;
            }
            let c = self.get_history_commit(id)?;
            for &parent in &c.parents {
                mark(&mut flags, &mut heap, &mut non_stale_count, parent, f);
            }
        }

        Ok(bases)
    }

    /// Finds one lowest (most-recent) common ancestor of two commits.
    fn find_common_ancestor(&self, a: CommitId, b: CommitId) -> Option<CommitId> {
        self.find_merge_bases(a, b).into_iter().max()
    }

    // =================================================================
    // History
    // =================================================================

    /// Returns one lowest common ancestor (fork point) of two commits.
    /// If there are several merge bases, selects the one with the greatest ID;
    /// merge itself considers all lowest common ancestors.
    ///
    /// Useful for branching scenarios: given two divergent tips, this finds
    /// the commit where they diverged.  Returns `None` only if the two
    /// commits share no common history.
    ///
    /// # Panics
    ///
    /// Panics if a stored commit record is corrupt or unreadable.
    pub fn fork_point(&self, a: CommitId, b: CommitId) -> Option<CommitId> {
        self.find_common_ancestor(a, b)
    }

    /// Counts the number of first-parent commits between `from` and
    /// `ancestor` (exclusive).
    ///
    /// Walks the first-parent chain starting at `from` until `ancestor`
    /// is reached.  Returns `None` if `ancestor` is not a first-parent
    /// ancestor of `from`.
    ///
    /// # Panics
    ///
    /// Panics if a stored commit record is corrupt or unreadable.
    ///
    /// # Example — comparing fork lengths
    ///
    /// ```ignore
    /// let lca = map.fork_point(tip_a, tip_b).unwrap();
    /// let ahead_a = map.commit_distance(tip_a, lca).unwrap();
    /// let ahead_b = map.commit_distance(tip_b, lca).unwrap();
    /// // The longer fork wins.
    /// ```
    pub fn commit_distance(&self, from: CommitId, ancestor: CommitId) -> Option<u64> {
        // Both endpoints must be real commits; otherwise
        // `commit_distance(x, x)` would report distance 0 for a
        // nonexistent `x`.
        self.get_commit(from)?;
        self.get_commit(ancestor)?;
        let mut cur = from;
        let mut count = 0u64;
        while cur != ancestor {
            if cur == NO_COMMIT {
                return None;
            }
            let c = self
                .get_history_commit(cur)
                .expect("vsdb: commit history read failed");
            cur = c.parents.first().copied().unwrap_or(NO_COMMIT);
            count += 1;
        }
        Some(count)
    }

    /// Retrieves a commit by its ID.
    ///
    /// # Panics
    ///
    /// Panics if the stored commit record is corrupt or unreadable.
    pub fn get_commit(&self, commit_id: CommitId) -> Option<Commit> {
        match self.get_commit_inner(commit_id) {
            Ok(commit) => Some(commit),
            Err(VsdbError::CommitNotFound { .. }) => None,
            Err(err) => panic!("vsdb: commit read failed: {err}"),
        }
    }

    /// Use ordinary point reads while a history writer may be waiting for fsync.
    /// The header generation and all changed history rows publish atomically.
    /// A snapshot bounds retries when writes keep advancing during a long log.
    #[cold]
    fn overlapping_history<R>(
        &self,
        branch: BranchId,
        read: impl Fn(CommitId, &dyn Fn(CommitId) -> Result<Commit>) -> Result<R>,
    ) -> Result<R> {
        let nodes = &self.tree.nodes;
        let head_key = row(HEAD, branch.raw());
        for _ in 0..2 {
            let generation = Header::get(nodes)?.generation;
            // The branch cache may still contain the head from before the
            // write, so select it directly from the same store as the commits.
            let result = nodes
                .try_get_with(head_key, |raw| Ok(BranchHead::decode(raw)?.head))
                .and_then(|opt| {
                    opt.ok_or(VsdbError::BranchNotFound { branch_id: branch })
                })
                .and_then(|tip| read(tip, &|id| self.get_commit_inner(id)));
            if Header::get(nodes)?.generation == generation {
                return result;
            }
        }
        let view = nodes.read_view();
        let tip = view
            .try_get_with(head_key, |raw| Ok(BranchHead::decode(raw)?.head))?
            .ok_or(VsdbError::BranchNotFound { branch_id: branch })?;
        read(tip, &|id| {
            view.try_get_with(row(COMMIT, id.raw()), |raw| Commit::parse(id, raw))?
                .ok_or(VsdbError::CommitNotFound { commit_id: id })
        })
    }

    fn branch_tip(
        &self,
        branch: BranchId,
        sequence: u64,
        admit: &mut bool,
    ) -> Result<Option<Commit>> {
        self.tree.branch_tip(branch, sequence, admit, |head| {
            read_tip(head, |id| self.get_commit_inner(id))
        })
    }

    /// Returns the commit at the head of `branch` in one consistent state.
    pub fn head_commit(&self, branch: BranchId) -> Result<Option<Commit>> {
        let seq = self.tree.history_sequence();
        let mut admit = false;
        let result = self.branch_tip(branch, seq, &mut admit);
        if self.tree.history_unchanged(seq) {
            let tip = result?;
            if admit {
                self.tree.remember_tip(seq, tip.as_ref());
            }
            return Ok(tip);
        }
        self.overlapping_history(branch, |head, fetch| read_tip(head, fetch))
    }

    /// Walks the first-parent commit history of `branch` from head to root
    /// in one consistent state.
    ///
    /// For merge commits, only the first parent (the target branch at merge
    /// time) is followed — analogous to `git log --first-parent`.
    pub fn log(&self, branch: BranchId) -> Result<Vec<Commit>> {
        let seq = self.tree.history_sequence();
        let mut admit = false;
        let result = self
            .branch_tip(branch, seq, &mut admit)
            .and_then(|tip| read_log(tip, |id| self.get_commit_inner(id)));
        if self.tree.history_unchanged(seq) {
            let log = result?;
            if admit {
                self.tree.remember_tip(seq, log.first());
            }
            return Ok(log);
        }
        self.overlapping_history(branch, |head, fetch| {
            read_log(read_tip(head, fetch)?, fetch)
        })
    }

    // =================================================================
    // Diff
    // =================================================================

    /// Computes the diff between two commits.
    ///
    /// Returns every key that was added, removed, or modified between
    /// `from` and `to`, in ascending key order. Subtrees the two commits
    /// share are skipped, so the cost follows the size of the change.
    ///
    /// # Panics
    ///
    /// Panics if a changed entry cannot be decoded — see [`Snapshot`](super::Snapshot).
    pub fn diff_commits(
        &self,
        from: CommitId,
        to: CommitId,
    ) -> Result<Vec<DiffEntry<K, V>>> {
        Ok(self
            .raw_diff_commits(from, to)?
            .into_iter()
            .map(DiffEntry::decode)
            .collect())
    }

    /// Computes the diff of uncommitted (working) changes on `branch`,
    /// relative to its head commit — analogous to `git diff`.
    ///
    /// # Panics
    ///
    /// Panics if a changed entry cannot be decoded — see [`Snapshot`](super::Snapshot).
    pub fn diff_uncommitted(&self, branch: BranchId) -> Result<Vec<DiffEntry<K, V>>> {
        Ok(self
            .raw_diff_uncommitted(branch)?
            .into_iter()
            .map(DiffEntry::decode)
            .collect())
    }

    // =================================================================
    // GC
    // =================================================================

    /// Validates the commit graph and all reachable objects, then sweeps
    /// unowned pages. Read-only handles skip this maintenance operation.
    /// Use [`verify_and_gc`](Self::verify_and_gc) for read-only validation
    /// and detailed counts.
    pub fn gc(&mut self) -> Result<()> {
        if self.namespace().is_read_only() {
            return Ok(());
        }
        self.verify_and_gc().map(|_| ())
    }
}

#[inline]
fn read_tip(
    head: CommitId,
    fetch: impl FnOnce(CommitId) -> Result<Commit>,
) -> Result<Option<Commit>> {
    #[cfg(test)]
    super::test::before_read_traversal();
    if head == NO_COMMIT {
        Ok(None)
    } else {
        fetch(head).map(Some)
    }
}

fn read_log(
    mut tip: Option<Commit>,
    fetch: impl Fn(CommitId) -> Result<Commit>,
) -> Result<Vec<Commit>> {
    let mut result = Vec::new();
    while let Some(commit) = tip {
        let parent = commit
            .parents
            .first()
            .copied()
            .filter(|&id| id != NO_COMMIT);
        result.push(commit);
        tip = match parent.map(&fetch).transpose() {
            Ok(next) => next,
            Err(err @ VsdbError::CommitNotFound { .. }) => {
                return Err(corrupt(err.to_string()));
            }
            Err(err) => return Err(err),
        };
    }
    Ok(result)
}

fn encode_value(value: &impl ValueEnDe) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    value.encode_value_into(&mut bytes)?;
    Ok(bytes)
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
#[test]
fn merge_bases_match_full_walk_on_a_wide_dag() {
    use std::collections::HashSet;

    let mut map = VerMap::<u32, u32>::new();
    let width = 8u64;
    let count = width * 7;
    let mut ancestors = vec![HashSet::<u64>::new()];
    for id in 1..=count {
        let parents = if id <= width {
            Vec::new()
        } else {
            let lane = (id - 1) % width;
            let previous_layer = ((id - 1) / width - 1) * width;
            vec![
                CommitId::from_raw(previous_layer + lane + 1),
                CommitId::from_raw(previous_layer + (lane + 1) % width + 1),
            ]
        };
        let mut reachable = HashSet::from([id]);
        for parent in &parents {
            reachable.extend(&ancestors[parent.raw() as usize]);
        }
        ancestors.push(reachable);
        map.tree.nodes.insert(
            row(COMMIT, id),
            Commit {
                id: CommitId::from_raw(id),
                root: EMPTY_ROOT,
                parents: parents.into(),
                timestamp_us: 0,
                ref_count: 1,
            }
            .bytes(),
        );
    }

    let tips: Vec<_> = (count - width + 1..=count)
        .chain([1, 8, 9, 16, 25])
        .collect();
    for &a in &tips {
        for &b in &tips {
            let common: HashSet<_> = ancestors[a as usize]
                .intersection(&ancestors[b as usize])
                .copied()
                .collect();
            let mut expected: Vec<_> = common
                .iter()
                .filter(|&&id| {
                    !common.iter().any(|&other| {
                        other != id && ancestors[other as usize].contains(&id)
                    })
                })
                .copied()
                .map(CommitId::from_raw)
                .collect();
            expected.sort_unstable();
            let mut actual =
                map.find_merge_bases(CommitId::from_raw(a), CommitId::from_raw(b));
            actual.sort_unstable();
            assert_eq!(actual, expected, "tips {a}, {b}");
        }
    }
}
