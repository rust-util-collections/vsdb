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
//! use vsdb::{vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut dag: DagMapRawKey<String> = DagMapRawKey::new(None);
//!
//! // Insert a value
//! dag.insert(&[1], &"hello".to_string());
//! assert_eq!(dag.get(&[1]), Some("hello".to_string()));
//!
//! // Create a child
//! let mut raw = dag.into_inner();
//! let child: DagMapRawKey<String> = DagMapRawKey::new(Some(&mut raw));
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
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

type DagHead<V> = DagMapRawKey<V>;

/// A raw-key, disk-based, directed acyclic graph (DAG) map.
///
/// Deliberately does **not** implement [`Default`] — see
/// [`DagMapRaw`]'s documentation for why: construction always performs
/// real disk I/O, so a `Default` impl would let generic code
/// (`mem::take`, `.or_default()`, `.unwrap_or_default()`) create
/// orphaned on-disk state invisibly. Use [`Self::new`] explicitly.
#[derive(Clone, Debug)]
pub struct DagMapRawKey<V> {
    inner: DagMapRaw,
    _p: PhantomData<V>,
}

impl<V> Serialize for DagMapRawKey<V> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // `V` is phantom-only, so the typed-handle envelope (tagged with
        // `DagMapRawKey<V>`) is the only guard against restoring the map
        // under a different value type.
        crate::common::serialize_typed_handle_meta::<Self, S>(&self.inner, serializer)
    }
}

impl<'de, V> Deserialize<'de> for DagMapRawKey<V> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        crate::common::deserialize_typed_handle_meta::<Self, DagMapRaw, D>(deserializer)
            .map(|inner| Self {
                inner,
                _p: PhantomData,
            })
    }
}

impl<V> DagMapRawKey<V>
where
    V: ValueEnDe,
{
    /// [`new`](Self::new) placed in `ns` — every internal component
    /// lands in the same namespace (a composite never spans namespaces).
    ///
    /// With a parent, the child ALWAYS inherits the parent's namespace
    /// (see [`DagMapRaw::new`]); a mismatched `ns` is a caller bug
    /// (`debug_assert`ed, ignored in release).
    pub fn new_in(
        ns: &crate::common::Namespace,
        raw_parent: Option<&mut DagMapRaw>,
    ) -> Self {
        if let Some(p) = &raw_parent {
            debug_assert_eq!(
                ns.id(),
                p.namespace().id(),
                "a DAG never spans namespaces: child must live in its \
                 parent's namespace"
            );
        }
        ns.scope(|| Self::new(raw_parent))
    }

    /// The namespace this structure lives in.
    pub fn namespace(&self) -> crate::common::Namespace {
        self.inner.namespace()
    }

    /// Creates a new `DagMapRawKey`, optionally attached under `raw_parent`.
    ///
    /// See [`DagMapRaw::new`] for the parent-linking semantics
    /// (parented construction inherits the parent's namespace).
    #[inline(always)]
    pub fn new(raw_parent: Option<&mut DagMapRaw>) -> Self {
        Self {
            inner: DagMapRaw::new(raw_parent),
            _p: PhantomData,
        }
    }

    /// Consumes the `DagMapRawKey` and returns the inner `DagMapRaw`.
    #[inline(always)]
    pub fn into_inner(self) -> DagMapRaw {
        self.inner
    }

    /// Creates a "shadow" copy of the inner `DagMapRaw`.
    ///
    /// # Safety
    ///
    /// This API breaks Rust's semantic safety guarantees. The caller must
    /// ensure no concurrent writes to the same key through any handle.
    #[inline(always)]
    pub unsafe fn shadow_inner(&self) -> DagMapRaw {
        // SAFETY: forwards this fn's `unsafe` contract — the caller
        // guarantees no concurrent writes to the same key.
        unsafe { self.inner.shadow() }
    }

    /// Creates a "shadow" copy of the `DagMapRawKey` instance.
    ///
    /// # Safety
    ///
    /// This API breaks Rust's semantic safety guarantees. The caller must
    /// ensure no concurrent writes to the same key through any handle.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> DagMapRawKey<V> {
        // SAFETY: forwards this fn's `unsafe` contract — the caller
        // guarantees no concurrent writes to the same key.
        unsafe {
            Self {
                inner: self.shadow_inner(),
                _p: PhantomData,
            }
        }
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

    /// Retrieves a value from the DAG map.
    #[inline(always)]
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<V> {
        self.inner.get(key).map(|v| V::decode(&v).unwrap())
    }

    /// Retrieves a mutable reference to a value in the DAG map.
    ///
    /// Tombstone (empty-value) entries are already filtered by
    /// [`DagMapRaw::get_mut`], so a returned handle always wraps a
    /// real, non-empty encoded value.
    #[inline(always)]
    pub fn get_mut(&mut self, key: impl AsRef<[u8]>) -> Option<ValueMut<'_, V>> {
        self.inner.get_mut(key.as_ref()).map(|inner| ValueMut {
            value: <V as ValueEnDe>::decode(&inner).unwrap(),
            inner,
            dirty: false,
        })
    }

    /// Inserts a key-value pair into the DAG map.
    ///
    /// Does not return the old value for performance reasons.
    ///
    /// # Panics
    ///
    /// Panics if `value` encodes to an empty byte slice, which is reserved
    /// internally as the deletion tombstone.
    #[inline(always)]
    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: &V) {
        let encoded = value.encode();
        assert!(
            !encoded.is_empty(),
            "empty encoded value is a tombstone; call remove() instead"
        );
        self.inner.insert(key, encoded)
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
    /// Crash-safe by phase ordering (merge → flush → re-parent → flush →
    /// clear) — see [`DagMapRaw::prune`] for the full contract.
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
    /// The unlink is persisted in this node's parent slot and is visible to
    /// pre-existing clones, shadows, and handles restored from metadata — see
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
#[derive(Debug)]
pub struct ValueMut<'a, V>
where
    V: ValueEnDe,
{
    value: V,
    inner: raw::ValueMut<'a>,
    dirty: bool,
}

impl<V> Drop for ValueMut<'_, V>
where
    V: ValueEnDe,
{
    fn drop(&mut self) {
        if self.dirty {
            *self.inner = self.value.encode();
        }
    }
}

impl<V> Deref for ValueMut<'_, V>
where
    V: ValueEnDe,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<V> DerefMut for ValueMut<'_, V>
where
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.dirty = true;
        &mut self.value
    }
}
