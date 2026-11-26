//!
//! A raw-key, disk-based, directed acyclic graph (DAG) map.
//!
//! `DagMapRawKey` provides a map-like interface where each instance can have a parent
//! and multiple children, forming a directed acyclic graph. Keys are stored as raw
//! bytes, while values are typed and encoded.
//!
//! # Examples
//!
//! ```
//! use vsdb::{DagMapRaw, DagMapRawKey};
//! use vsdb::{VsdbOptions, vsdb_configure, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = std::env::temp_dir().join(format!("vsdb-{}", rand::random::<u128>()));
//! vsdb_configure(VsdbOptions::new(&dir)).unwrap();
//!
//! let mut dag: DagMapRawKey<String> = DagMapRawKey::new(None);
//!
//! // Insert a value
//! dag.insert(&[1], &"hello".to_string());
//! assert_eq!(dag.get(&[1]), Some("hello".to_string()));
//!
//! // Create a child
//! let child = DagMapRawKey::new(Some(&mut dag));
//! assert_eq!(child.get(&[1]), Some("hello".to_string()));
//!
//! // Clean up the directory
//! fs::remove_dir_all(vsdb_get_base_dir()).unwrap();
//! ```

#[cfg(test)]
mod test;

use crate::{
    DagMapId, DagMapRaw, ValueEnDe,
    common::{InstanceId, error::Result},
    dagmap::raw,
};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use vsdb_core::common::RawBytes;

type DagHead<V> = DagMapRawKey<V>;

/// A raw-key, disk-based, directed acyclic graph (DAG) map.
///
/// Deliberately does **not** implement [`Default`] — see
/// [`DagMapRaw`]'s documentation for why: construction always performs
/// real disk I/O, so a `Default` impl would let generic code
/// (`mem::take`, `.or_default()`, `.unwrap_or_default()`) create
/// orphaned on-disk state invisibly. Use [`Self::new`] explicitly.
/// [`Clone`] copies the complete connected graph independently, as described
/// by [`DagMapRaw`].
#[derive(Debug)]
pub struct DagMapRawKey<V> {
    pub(super) inner: DagMapRaw,
    pub(super) _p: PhantomData<V>,
}

impl<V> Clone for DagMapRawKey<V> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _p: PhantomData,
        }
    }
}

impl<V> Serialize for DagMapRawKey<V> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // `V` is phantom-only, so the typed-handle envelope (tagged with
        // `DagMapRawKey<V>`) is the only guard against restoring the map
        // under a different value type.
        self.inner.serialize_as::<Self, _>(serializer)
    }
}

impl<'de, V> Deserialize<'de> for DagMapRawKey<V> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        crate::common::deserialize_prefix_handle::<Self, _, _>(deserializer, |frame| {
            Ok(Self {
                inner: DagMapRaw::from_frame(frame)?,
                _p: PhantomData,
            })
        })
    }
}

impl<V> DagMapRawKey<V>
where
    V: ValueEnDe,
{
    /// Places a root in `ns`. A child inherits its parent's namespace and shard.
    pub fn new_in(ns: &crate::common::Namespace, parent: Option<&mut Self>) -> Self {
        Self {
            inner: DagMapRaw::new_in(ns, parent.map(|p| &mut p.inner)),
            _p: PhantomData,
        }
    }

    /// The namespace this structure lives in.
    pub fn namespace(&self) -> crate::common::Namespace {
        self.inner.namespace()
    }

    /// Creates a new `DagMapRawKey`, optionally attached under `parent`.
    ///
    /// See [`DagMapRaw::new`] for the parent-linking semantics
    /// (parented construction inherits the parent's namespace).
    #[inline(always)]
    pub fn new(parent: Option<&mut Self>) -> Self {
        Self {
            inner: DagMapRaw::new(parent.map(|p| &mut p.inner)),
            _p: PhantomData,
        }
    }

    /// Consumes the `DagMapRawKey` and returns the inner `DagMapRaw`.
    #[inline(always)]
    pub fn into_inner(self) -> DagMapRaw {
        self.inner
    }

    /// Deep-copies the connected graph into the target namespace.
    pub fn clone_in(&self, ns: &crate::Namespace) -> Result<Self> {
        Ok(Self {
            inner: self.inner.clone_in(ns)?,
            _p: PhantomData,
        })
    }

    /// Returns the unique instance ID of this `DagMapRawKey`.
    #[inline(always)]
    pub fn instance_id(&self) -> InstanceId {
        self.inner.instance_id()
    }

    /// Persists this instance's metadata to disk so that it can be
    /// recovered later via [`from_meta`](Self::from_meta).
    ///
    /// Returns the `instance_id` that should be passed to `from_meta`.
    pub fn save_meta(&self) -> Result<InstanceId> {
        let id = self.instance_id();
        crate::common::save_instance_meta(id, self)?;
        Ok(id)
    }

    /// Recovers a `DagMapRawKey` instance from previously saved metadata.
    ///
    /// The caller must ensure that the underlying VSDB database still
    /// contains the data referenced by this instance ID.
    pub fn from_meta(instance_id: impl Into<InstanceId>) -> Result<Self> {
        let id = instance_id.into();
        crate::common::load_instance_meta_checked(id, Self::instance_id)
    }

    /// Checks if the DAG map is dead.
    #[inline(always)]
    pub fn is_dead(&self) -> bool {
        self.inner.is_dead()
    }

    /// Checks if the DAG map has no children.
    #[inline(always)]
    pub fn no_children(&self) -> bool {
        self.inner.no_children()
    }

    /// Returns the registry IDs accepted by the selective child-prune APIs.
    pub fn child_ids(&self) -> Vec<RawBytes> {
        self.inner.child_ids()
    }

    /// Returns this parent's registry ID for `child`, if present.
    pub fn child_id(&self, child: &Self) -> Option<RawBytes> {
        self.inner.child_id(&child.inner)
    }

    /// Retrieves a value from the DAG map.
    #[inline(always)]
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<V> {
        self.inner.get(key).map(|v| V::decode_value(&v).unwrap())
    }

    /// Retrieves a mutable reference to a value in the DAG map.
    ///
    /// Tombstones are excluded. Empty encodings are valid values.
    /// Mutable dereferences mark the value for write-back.
    pub fn get_mut(&mut self, key: impl AsRef<[u8]>) -> Option<ValueMut<'_, V>> {
        self.inner.get_mut(key).map(ValueMut::occupied)
    }
    /// Edits a present local override and reports encoding or storage errors.
    pub fn update<R>(
        &mut self,
        key: impl AsRef<[u8]>,
        edit: impl FnOnce(&mut V) -> R,
    ) -> Result<Option<R>> {
        crate::common::ensure_writable(&self.namespace(), "value update")?;
        let Some(mut guard) = self.get_mut(key) else {
            return Ok(None);
        };
        let result = edit(&mut guard);
        guard.commit()?;
        Ok(Some(result))
    }

    /// Inserts a key-value pair into the DAG map.
    ///
    /// Does not return the old value for performance reasons.
    ///
    #[inline(always)]
    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: &V) {
        self.inner.insert(key, value.encode_value())
    }

    /// Removes a key-value pair from the DAG map.
    ///
    /// Does not return the old value for performance reasons.
    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) {
        self.inner.remove(key)
    }

    /// Prunes the DAG, merging all nodes in the mainline into the genesis node.
    ///
    /// All phases share one shard WAL. See [`DagMapRaw::prune`] for
    /// the full contract.
    #[inline(always)]
    pub fn prune(self) -> Result<DagHead<V>> {
        self.inner.prune().map(|inner| Self {
            inner,
            _p: PhantomData,
        })
    }

    /// Prunes children that are in the `include_targets` list.
    #[inline(always)]
    pub fn prune_children_include(&mut self, include_targets: &[impl AsRef<DagMapId>]) {
        self.inner.prune_children_include(include_targets);
    }

    /// Prunes children that are not in the `exclude_targets` list.
    #[inline(always)]
    pub fn prune_children_exclude(&mut self, exclude_targets: &[impl AsRef<DagMapId>]) {
        self.inner.prune_children_exclude(exclude_targets);
    }

    /// Destroys the DAG map and all its children, unlinking it from its parent.
    ///
    /// Restored handles observe the persisted unlink. See
    /// [`DagMapRaw::destroy`] for the full semantics.
    #[inline(always)]
    pub fn destroy(&mut self) {
        self.inner.destroy();
    }

    /// Checks if this `DagMapRawKey` instance is the same as another.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A mutable reference to a value in a `DagMapRawKey`.
pub type ValueMut<'a, V> = crate::common::guard::ValueGuard<V, raw::ValueMut<'a>>;
