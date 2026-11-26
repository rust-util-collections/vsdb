//! Typed keys and values over one authenticated DAG node.
use super::{
    DagMapId,
    raw::DagMapRaw,
    rawkey::{DagMapRawKey, ValueMut},
};
use crate::{InstanceId, KeyEnDe, Namespace, Result, ValueEnDe};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{marker::PhantomData, result::Result as StdResult};

/// A typed overlay map. The key codec must obey the same canonical-byte law
/// as `Mapx`. Children inherit values from their parent until overridden.
#[derive(Debug)]
pub struct DagMap<K, V> {
    inner: DagMapRawKey<V>,
    key: PhantomData<K>,
}

impl<K, V> Serialize for DagMap<K, V> {
    fn serialize<S: Serializer>(&self, s: S) -> StdResult<S::Ok, S::Error> {
        self.inner.inner.serialize_as::<Self, _>(s)
    }
}
impl<'de, K, V> Deserialize<'de> for DagMap<K, V> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> StdResult<Self, D::Error> {
        crate::common::deserialize_prefix_handle::<Self, _, _>(d, |frame| {
            Ok(Self {
                inner: DagMapRawKey {
                    inner: DagMapRaw::from_frame(frame)?,
                    _p: PhantomData,
                },
                key: PhantomData,
            })
        })
    }
}
impl<K: KeyEnDe, V: ValueEnDe> DagMap<K, V> {
    fn wrap(inner: DagMapRawKey<V>) -> Self {
        Self {
            inner,
            key: PhantomData,
        }
    }
    /// Creates a root or a child sharing its parent's shard.
    pub fn new(parent: Option<&mut Self>) -> Self {
        Self::wrap(DagMapRawKey::new(parent.map(|p| &mut p.inner)))
    }
    /// Creates the root in a specified namespace.
    /// With a parent, the child always inherits its namespace and shard.
    pub fn new_in(ns: &Namespace, parent: Option<&mut Self>) -> Self {
        Self::wrap(DagMapRawKey::new_in(ns, parent.map(|p| &mut p.inner)))
    }
    /// Reads the closest visible value.
    ///
    /// # Panics
    ///
    /// Panics on a storage read failure, metadata that does not decode, or a
    /// value that cannot be decoded.
    pub fn get(&self, key: &K) -> Option<V> {
        key.with_encoded_key(|k| self.inner.get(k))
    }
    /// Edits a local override.
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, V>> {
        key.with_encoded_key(|k| self.inner.get_mut(k))
    }
    /// Edits a local override and reports encoding or storage errors.
    pub fn update<R>(
        &mut self,
        key: &K,
        edit: impl FnOnce(&mut V) -> R,
    ) -> Result<Option<R>> {
        key.try_with_encoded_key(|k| self.inner.update(k, edit))
    }
    /// Stores an override on this node.
    pub fn insert(&mut self, key: &K, v: &V) {
        key.with_encoded_key(|k| self.inner.insert(k, v))
    }
    /// Masks this key on this node.
    pub fn remove(&mut self, key: &K) {
        key.with_encoded_key(|k| self.inner.remove(k))
    }
    /// Owning namespace.
    pub fn namespace(&self) -> Namespace {
        self.inner.namespace()
    }
    /// Stable prefix identity.
    pub fn instance_id(&self) -> InstanceId {
        self.inner.instance_id()
    }
    /// Saves a typed handle naming this node.
    pub fn save_meta(&self) -> Result<InstanceId> {
        let id = self.instance_id();
        crate::common::save_instance_meta(id, self)?;
        Ok(id)
    }
    /// Opens a saved typed handle.
    pub fn from_meta(id: impl Into<InstanceId>) -> Result<Self> {
        crate::common::load_instance_meta_checked(
            id.into(),
            |frame| {
                DagMapRaw::from_frame(frame).map(|inner| {
                    Self::wrap(DagMapRawKey {
                        inner,
                        _p: PhantomData,
                    })
                })
            },
            Self::instance_id,
        )
    }
    /// Copies the connected graph to fresh storage in another namespace.
    pub fn clone_in(&self, ns: &Namespace) -> Result<Self> {
        self.inner.clone_in(ns).map(Self::wrap)
    }
    /// Folds this mainline into its root.
    pub fn prune(self) -> Result<Self> {
        self.inner.prune().map(Self::wrap)
    }
    /// Removes the owned subtree.
    pub fn destroy(&mut self) {
        self.inner.destroy()
    }
    /// True if no parent, children or live local values remain.
    pub fn is_dead(&self) -> bool {
        self.inner.is_dead()
    }
    /// True when this node has no children.
    pub fn no_children(&self) -> bool {
        self.inner.no_children()
    }
    /// Registry tokens of direct children.
    pub fn child_ids(&self) -> Vec<Vec<u8>> {
        self.inner.child_ids()
    }
    /// Registry token for one child.
    pub fn child_id(&self, child: &Self) -> Option<Vec<u8>> {
        self.inner.child_id(&child.inner)
    }
    /// Removes listed children.
    pub fn prune_children_include(&mut self, targets: &[impl AsRef<DagMapId>]) {
        self.inner.prune_children_include(targets)
    }
    /// Removes children absent from the list.
    pub fn prune_children_exclude(&mut self, targets: &[impl AsRef<DagMapId>]) {
        self.inner.prune_children_exclude(targets)
    }
    /// Whether both handles name the same node.
    pub fn is_the_same_instance(&self, other: &Self) -> bool {
        self.inner.is_the_same_instance(&other.inner)
    }
}
impl<K: KeyEnDe, V: ValueEnDe> Clone for DagMap<K, V> {
    fn clone(&self) -> Self {
        self.clone_in(&self.namespace()).expect("DAG deep clone")
    }
}
