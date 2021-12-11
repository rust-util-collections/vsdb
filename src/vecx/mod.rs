//!
//! # A disk-storage replacement for the pure in-memory Vec
//!
//! This module is non-invasive to external code except the `new` method.
//!

mod backend;

#[cfg(test)]
mod test;

use crate::{alloc_id, MetaInfo, SimpleVisitor};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    cmp::Ordering,
    fmt,
    iter::{DoubleEndedIterator, Iterator},
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `Vec<_>`.
///
/// - Each time the program is started, a new database is created
/// - Can ONLY be used in append-only scenes like the block storage
#[derive(PartialEq, Debug, Clone)]
pub struct Vecx<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    in_disk: backend::Vecx<T>,
}

impl<T> From<MetaInfo> for Vecx<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    fn from(mi: MetaInfo) -> Self {
        Self {
            in_disk: backend::Vecx::from(mi),
        }
    }
}

impl<T> Default for Vecx<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

///////////////////////////////////////////////
// Begin of the self-implementation for Vecx //
/*********************************************/

impl<T> Vecx<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    /// Create an instance.
    #[inline(always)]
    pub fn new() -> Self {
        Vecx {
            in_disk: backend::Vecx::load_or_create(alloc_id()),
        }
    }

    // Get the meta-storage path
    fn get_meta(&self) -> MetaInfo {
        self.in_disk.get_meta()
    }

    /// Imitate the behavior of 'Vec<_>.get(...)'
    ///
    /// Any faster/better choice other than JSON ?
    #[inline(always)]
    pub fn get(&self, idx: usize) -> Option<T> {
        self.in_disk.get(idx)
    }

    /// Imitate the behavior of 'Vec<_>.get_mut(...)'
    #[inline(always)]
    pub fn get_mut(&mut self, idx: usize) -> Option<ValueMut<'_, T>> {
        self.in_disk
            .get(idx)
            .map(move |v| ValueMut::new(self, idx, v))
    }

    /// Imitate the behavior of 'Vec<_>.last()'
    #[inline(always)]
    pub fn last(&self) -> Option<T> {
        self.in_disk.last()
    }

    /// Imitate the behavior of 'Vec<_>.len()'
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.in_disk.len()
    }

    /// A helper func
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.in_disk.is_empty()
    }

    /// Imitate the behavior of 'Vec<_>.push(...)'
    #[inline(always)]
    pub fn push(&mut self, b: T) {
        self.in_disk.push(b);
    }

    /// Imitate the behavior of 'Vec<_>.insert(idx, value)',
    /// but we do not return the previous value, like `Vecx<_, _>.set_value`.
    #[inline(always)]
    pub fn set_value(&mut self, idx: usize, b: T) {
        self.in_disk.insert(idx, b);
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub fn iter(&self) -> Box<dyn Iterator<Item = T> + '_> {
        Box::new(VecxIter {
            iter: self.in_disk.iter(),
        })
    }
}

/*******************************************/
// End of the self-implementation for Vecx //
/////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////
// Begin of the implementation of ValueMut(returned by `self.get_mut`) for Vecx //
/********************************************************************************/

/// Returned by `<Vecx>.get_mut(...)`
#[derive(Debug)]
pub struct ValueMut<'a, T>
where
    T: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    mapx: &'a mut Vecx<T>,
    idx: usize,
    value: ManuallyDrop<T>,
}

impl<'a, T> ValueMut<'a, T>
where
    T: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn new(mapx: &'a mut Vecx<T>, idx: usize, value: T) -> Self {
        ValueMut {
            mapx,
            idx,
            value: ManuallyDrop::new(value),
        }
    }

    /// Clone the inner value.
    pub fn clone_inner(self) -> T {
        ManuallyDrop::into_inner(self.value.clone())
    }
}

///
/// **NOTE**: VERY IMPORTANT !!!
///
impl<'a, T> Drop for ValueMut<'a, T>
where
    T: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn drop(&mut self) {
        // This operation is safe within a `drop()`.
        // SEE: [**ManuallyDrop::take**](std::mem::ManuallyDrop::take)
        unsafe {
            self.mapx
                .set_value(self.idx, ManuallyDrop::take(&mut self.value));
        };
    }
}

impl<'a, T> Deref for ValueMut<'a, T>
where
    T: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, T> DerefMut for ValueMut<'a, T>
where
    T: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<'a, T> PartialEq for ValueMut<'a, T>
where
    T: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &ValueMut<'a, T>) -> bool {
        self.value == other.value
    }
}

impl<'a, T> PartialEq<T> for ValueMut<'a, T>
where
    T: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &T) -> bool {
        self.value.deref() == other
    }
}

impl<'a, T> PartialOrd<T> for ValueMut<'a, T>
where
    T: Default
        + Clone
        + PartialEq
        + Ord
        + PartialOrd
        + Serialize
        + DeserializeOwned
        + fmt::Debug,
{
    fn partial_cmp(&self, other: &T) -> Option<Ordering> {
        self.value.deref().partial_cmp(other)
    }
}

/******************************************************************************/
// End of the implementation of ValueMut(returned by `self.get_mut`) for Vecx //
////////////////////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////
// Begin of the implementation of Iter for Vecx //
/************************************************/

/// Iter over [Vecx](self::Vecx).
pub struct VecxIter<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    iter: backend::VecxIter<T>,
}

impl<T> Iterator for VecxIter<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|v| v.1)
    }
}

impl<T> DoubleEndedIterator for VecxIter<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back().map(|v| v.1)
    }
}

/**********************************************/
// End of the implementation of Iter for Vecx //
////////////////////////////////////////////////

////////////////////////////////////////////////
// Begin of the implementation of Eq for Vecx //
/**********************************************/

impl<T> Eq for Vecx<T> where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug
{
}

/********************************************/
// End of the implementation of Eq for Vecx //
//////////////////////////////////////////////

///////////////////////////////////////////////////////////////////
// Begin of the implementation of Serialize/Deserialize for Vecx //
/*****************************************************************/

impl<'a, T> serde::Serialize for Vecx<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let v = pnk!(bincode::serialize(&self.get_meta()));
        serializer.serialize_bytes(&v)
    }
}

impl<'de, T> serde::Deserialize<'de> for Vecx<T>
where
    T: PartialEq + Clone + Serialize + DeserializeOwned + fmt::Debug,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(SimpleVisitor).map(|meta| {
            let meta = pnk!(bincode::deserialize::<MetaInfo>(&meta));
            Vecx::from(meta)
        })
    }
}

/***************************************************************/
// End of the implementation of Serialize/Deserialize for Vecx //
/////////////////////////////////////////////////////////////////
