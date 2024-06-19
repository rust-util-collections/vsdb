#[cfg(test)]
mod test;

use crate::{dagmap::raw, DagMapId, DagMapRaw, Orphan, ValueEnDe};
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

type DagHead<V> = DagMapRawKey<V>;

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
    #[inline(always)]
    pub fn new(raw_parent: &mut Orphan<Option<DagMapRaw>>) -> Result<Self> {
        DagMapRaw::new(raw_parent).c(d!()).map(|inner| Self {
            inner,
            _p: PhantomData,
        })
    }

    #[inline(always)]
    pub fn into_inner(self) -> DagMapRaw {
        self.inner
    }

    /// # Safety
    ///
    /// This API breaks the semantic safety guarantees,
    /// but it is safe to use in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow_inner(&self) -> DagMapRaw {
        self.inner.shadow()
    }

    /// # Safety
    ///
    /// This API breaks the semantic safety guarantees,
    /// but it is safe to use in a race-free environment.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> DagMapRawKey<V> {
        Self {
            inner: self.shadow_inner(),
            _p: PhantomData,
        }
    }

    #[inline(always)]
    pub fn is_dead(&self) -> bool {
        self.inner.is_dead()
    }

    #[inline(always)]
    pub fn no_children(&self) -> bool {
        self.inner.no_children()
    }

    #[inline(always)]
    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<V> {
        self.inner.get(key).map(|v| V::decode(&v).unwrap())
    }

    #[inline(always)]
    pub fn get_mut(&mut self, key: impl AsRef<[u8]>) -> Option<ValueMut<'_, V>> {
        self.inner.get_mut(key.as_ref()).map(|inner| ValueMut {
            value: <V as ValueEnDe>::decode(&inner).unwrap(),
            inner,
        })
    }

    #[inline(always)]
    pub fn insert(&mut self, key: impl AsRef<[u8]>, value: &V) -> Option<V> {
        self.inner
            .insert(key, value.encode())
            .map(|v| V::decode(&v).unwrap())
    }

    #[inline(always)]
    pub fn remove(&mut self, key: impl AsRef<[u8]>) -> Option<V> {
        self.inner.remove(key).map(|v| V::decode(&v).unwrap())
    }

    /// Return the new head of mainline,
    /// all instances should have been committed!
    #[inline(always)]
    pub fn prune(self) -> Result<DagHead<V>> {
        self.inner.prune().c(d!()).map(|inner| Self {
            inner,
            _p: PhantomData,
        })
    }

    /// Drop children that are in the `targets` list
    #[inline(always)]
    pub fn prune_children_include(&mut self, include_targets: &[impl AsRef<DagMapId>]) {
        self.inner.prune_children_include(include_targets);
    }

    /// Drop children that are not in the `exclude_targets` list
    #[inline(always)]
    pub fn prune_children_exclude(&mut self, exclude_targets: &[impl AsRef<DagMapId>]) {
        self.inner.prune_children_exclude(exclude_targets);
    }

    #[inline(always)]
    pub fn destroy(&mut self) {
        self.inner.destroy();
    }

    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ValueMut<'a, V>
where
    V: ValueEnDe,
{
    value: V,
    inner: raw::ValueMut<'a>,
}

impl<'a, V> Drop for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    fn drop(&mut self) {
        *self.inner = self.value.encode();
    }
}

impl<'a, V> Deref for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    type Target = V;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, V> DerefMut for ValueMut<'a, V>
where
    V: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
