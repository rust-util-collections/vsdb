//!
//! # Wrapper for compatible reasons
//!

#![allow(missing_docs)]

use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{
        btree_map::{Entry, Iter},
        BTreeMap,
    },
    fmt,
};

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Mapi<K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Serialize
        + for<'a> Deserialize<'a>
        + fmt::Debug,
    V: Clone + Serialize + for<'a> Deserialize<'a> + fmt::Debug,
{
    inner: BTreeMap<K, V>,
}

impl<K, V> Mapi<K, V>
where
    K: Clone
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Serialize
        + for<'a> Deserialize<'a>
        + fmt::Debug,
    V: Clone + Serialize + for<'a> Deserialize<'a> + fmt::Debug,
{
    #[inline(always)]
    pub fn new(_path: &str) -> Result<Self> {
        Ok(Mapi {
            inner: BTreeMap::new(),
        })
    }

    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(key).cloned()
    }

    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        self.inner.get_mut(key)
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline(always)]
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.inner.insert(key, value)
    }

    #[inline(always)]
    pub fn set_value(&mut self, key: K, value: V) {
        self.inner.insert(key, value);
    }

    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        self.inner.entry(key)
    }

    #[inline(always)]
    pub fn iter(&self) -> Iter<'_, K, V> {
        self.inner.iter()
    }

    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(key)
    }

    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.inner.remove(key);
    }

    #[inline(always)]
    pub fn flush_data(&mut self) {}
}
