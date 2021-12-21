//!
//! NOTE: Documents => [MapxRaw](crate::versioned::mapx_raw)
//!

// TODO

use crate::{
    common::ende::ValueEnDe,
    versioned::mapx_ord_rawkey::{MapxOrdRawKeyVs, MapxOrdRawKeyVsIter, ValueMut},
};
use ruc::*;
use serde::{Deserialize, Serialize};
// use std::cmp::Ordering;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(bound = "")]
pub struct VecxVs<T: ValueEnDe> {
    inner: MapxOrdRawKeyVs<T>,
}

impl<T: ValueEnDe> Default for VecxVs<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ValueEnDe> VecxVs<T> {
    #[inline(always)]
    pub fn new() -> Self {
        VecxVs {
            inner: MapxOrdRawKeyVs::new(),
        }
    }

    #[inline(always)]
    pub fn get(&self, idx: usize) -> Option<T> {
        self.inner.get(&(idx as u64).to_be_bytes())
    }

    #[inline(always)]
    pub fn get_mut(&mut self, idx: usize) -> Option<ValueMut<'_, T>> {
        let idx_bytes = (idx as u64).to_be_bytes();
        self.inner.get(&idx_bytes).map(|v| {
            ValueMut::new(&mut self.inner, idx_bytes.to_vec().into_boxed_slice(), v)
        })
    }

    #[inline(always)]
    pub fn last(&self) -> Option<T> {
        alt!(self.is_empty(), return None);
        Some(
            self.inner
                .get(&(self.len() as u64 - 1).to_be_bytes())
                .unwrap(),
        )
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
    pub fn push(&mut self, v: T) {
        self.push_ref(&v)
    }

    #[inline(always)]
    pub fn push_ref(&mut self, v: &T) {
        self.inner
            .insert_ref(&(self.len() as u64).to_be_bytes(), v)
            .unwrap();
    }

    // #[inline(always)]
    // pub fn insert(&mut self, idx: usize, v: T) {
    //     self.insert_ref(idx, &v)
    // }

    // #[inline(always)]
    // pub fn insert_ref(&mut self, idx: usize, v: &T) {
    //     let idx = idx as u64;
    //     match (self.len() as u64).cmp(&idx) {
    //         Ordering::Greater => {
    //             self.inner
    //                 .range_ref(
    //                     &idx.to_be_bytes()[..]..&(self.len() as u64).to_be_bytes()[..],
    //                 )
    //                 .for_each(|(i, iv)| {
    //                     self.inner.insert_ref(
    //                         &(crate::parse_int!(i, u64) + 1).to_be_bytes(),
    //                         &iv,
    //                     );
    //                 });
    //             self.inner.insert_ref(&idx.to_be_bytes(), v);
    //         }
    //         Ordering::Equal => {
    //             self.push_ref(v);
    //         }
    //         Ordering::Less => {
    //             panic!("out of index");
    //         }
    //     }
    // }

    #[inline(always)]
    pub fn pop(&mut self) -> Result<Option<T>> {
        alt!(self.is_empty(), return Ok(None));
        self.inner.remove(&(self.len() - 1).to_be_bytes()).c(d!())
    }

    // #[inline(always)]
    // pub fn remove(&mut self, idx: usize) -> T {
    //     let idx = idx as u64;
    //     if !self.is_empty() && idx < self.len() as u64 {
    //         let last_idx = self.len() as u64 - 1;
    //         let ret = self.inner.remove(&idx.to_be_bytes()).unwrap().unwrap();
    //         self.inner
    //             .range_ref(&(1 + idx).to_be_bytes()[..]..)
    //             .for_each(|(i, v)| {
    //                 self.inner
    //                     .insert_ref(&(crate::parse_int!(i, u64) - 1).to_be_bytes(), &v);
    //             });
    //         pnk!(self.inner.remove(&last_idx.to_be_bytes()));
    //         return ret;
    //     }
    //     panic!("out of index");
    // }

    // #[inline(always)]
    // pub fn swap_remove(&mut self, idx: usize) -> T {
    //     let idx = idx as u64;
    //     if !self.is_empty() && idx < self.len() as u64 {
    //         let last_idx = self.len() as u64 - 1;
    //         let ret = self.inner.remove(&idx.to_be_bytes()).unwrap().unwrap();
    //         if let Some(v) = self.inner.remove(&last_idx.to_be_bytes()).unwrap() {
    //             self.inner.insert_ref(&idx.to_be_bytes(), &v).unwrap();
    //         }
    //         return ret;
    //     }
    //     panic!("out of index");
    // }

    pub fn update(&mut self, idx: usize, v: T) -> Result<Option<T>> {
        self.update_ref(idx, &v).c(d!())
    }

    #[inline(always)]
    pub fn update_ref(&mut self, idx: usize, v: &T) -> Result<Option<T>> {
        if idx < self.len() {
            return self
                .inner
                .insert_ref(&(idx as u64).to_be_bytes(), v)
                .c(d!());
        }
        panic!("out of index");
    }

    #[inline(always)]
    pub fn iter(&self) -> VecxVsIter<'_, T> {
        VecxVsIter {
            iter: self.inner.iter(),
        }
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

pub struct VecxVsIter<'a, T: ValueEnDe> {
    iter: MapxOrdRawKeyVsIter<'a, T>,
}

impl<'a, T: ValueEnDe> Iterator for VecxVsIter<'a, T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| v.1)
    }
}

impl<'a, T: ValueEnDe> DoubleEndedIterator for VecxVsIter<'a, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(|v| v.1)
    }
}
