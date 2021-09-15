//!
//! # Wrapper for compatible reasons
//!

#![allow(missing_docs)]

use ruc::*;
use serde::{Deserialize, Serialize};
use std::{fmt, slice::Iter};

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct Veci<T>
where
    T: Clone + Serialize + for<'a> Deserialize<'a> + fmt::Debug,
{
    inner: Vec<T>,
}

impl<T> Veci<T>
where
    T: Clone + Serialize + for<'a> Deserialize<'a> + fmt::Debug,
{
    #[inline(always)]
    pub fn new(_path: &str) -> Result<Self> {
        Ok(Veci { inner: Vec::new() })
    }

    #[inline(always)]
    pub fn get(&self, idx: usize) -> Option<T> {
        self.inner.get(idx).cloned()
    }

    #[inline(always)]
    pub fn get_mut(&mut self, idx: usize) -> Option<&mut T> {
        self.inner.get_mut(idx)
    }

    #[inline(always)]
    pub fn last(&self) -> Option<T> {
        self.inner.last().cloned()
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
    pub fn push(&mut self, b: T) {
        self.inner.push(b);
    }

    #[inline(always)]
    pub fn iter(&self) -> Iter<'_, T> {
        self.inner.iter()
    }

    #[inline(always)]
    pub fn flush_data(&mut self) {}
}
