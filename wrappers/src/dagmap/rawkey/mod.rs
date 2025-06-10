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
//! use vsdb::{DagMapRaw, DagMapRawKey, Orphan};
//! use vsdb::{vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut parent = Orphan::new(None);
//! let mut dag: DagMapRawKey<String> = DagMapRawKey::new(&mut parent).unwrap();
//!
//! // Insert a value
//! dag.insert(&[1], &"hello".to_string());
//! assert_eq!(dag.get(&[1]), Some("hello".to_string()));
//!
//! // Create a child
//! let mut child = DagMapRawKey::new(&mut Orphan::new(Some(dag.into_inner()))).unwrap();
//! assert_eq!(child.get(&[1]), Some("hello".to_string()));
//!
//! // Clean up the directory
//! fs::remove_dir_all(vsdb_get_base_dir()).unwrap();
//! ```

#[cfg(test)]
mod test;

use crate::{DagMapId, DagMapRaw, Orphan, ValueEnDe, dagmap::raw};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

type DagHead<V> = DagMapRawKey<V>;

/// A raw-key, disk-based, directed acyclic graph (DAG) map.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct DagMapRawKey<V> {
    inner: DagMapRaw,
    _p: PhantomData<V>,
}

impl<V> DagMapRawKey<V>
where
    V: ValueEnDe,
{
    /// Creates a new `DagMapRawKey`.
    #[inline(always)]
    pub fn new(raw_parent: &mut Orphan<Option<DagMapRaw>>) -> Result<Self> {
        DagMapRaw::new(raw_parent).c(d!()).map(|inner| Self {
            inner,
            _p: PhantomData,
        })
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
    /// This API breaks Rust's semantic safety guarantees. Use only in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow_inner(&self) -> DagMapRaw {
        unsafe { self.inner.shadow() }
    }

    /// Creates a "shadow" copy of the `DagMapRawKey` instance.
    ///
    /// # Safety
    ///
    /// This API breaks Rust's semantic safety guarantees. Use only in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> DagMapRawKey<V> {
        unsafe {
            Self {
                inner: self.shadow_inner(),
                _p: PhantomData,
            }
        }
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
    #[inline(always)]
    pub fn get_mut(&mut self, key: impl AsRef<[u8]>) -> Option<ValueMut<'_, V>> {
        self.inner.get_mut(key.as_ref()).map(|inner| ValueMut {
            value: <V as ValueEnDe>::decode(&inner).unwrap(),
            inner,
        })
    }

    /// Inserts a key-value pair into the DAG map.
    ///
    /// Does not return the old value for performance reasons.
    #[inline(always)]
    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: &V) {
        self.inner.insert(key, value.encode())
    }

    /// Removes a key-value pair from the DAG map.
    ///
    /// Does not return the old value for performance reasons.
    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) {
        self.inner.remove(key)
    }

    /// Prunes the DAG, merging all nodes in the mainline into the genesis node.
    #[inline(always)]
    pub fn prune(self) -> Result<DagHead<V>> {
        self.inner.prune().c(d!()).map(|inner| Self {
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

    /// Destroys the DAG map and all its children.
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
}

impl<V> Drop for ValueMut<'_, V>
where
    V: ValueEnDe,
{
    fn drop(&mut self) {
        *self.inner = self.value.encode();
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
        &mut self.value
    }
}
