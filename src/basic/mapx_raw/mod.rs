//!
//! # A disk-storage replacement for the pure in-memory BTreeMap
//!
//! This module is non-invasive to external code except the `new` method.
//!

mod backend;
#[cfg(test)]
mod test;

use crate::common::{MetaInfo, SimpleVisitor, VSDB};
use ruc::*;
use sled::IVec;
use std::{
    iter::Iterator,
    mem::ManuallyDrop,
    ops::{Deref, DerefMut, RangeBounds},
};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `BTreeMap<_, _>`.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct MapxRaw {
    in_disk: backend::MapxRaw,
}

impl From<MetaInfo> for MapxRaw {
    fn from(mi: MetaInfo) -> Self {
        Self {
            in_disk: backend::MapxRaw::from(mi),
        }
    }
}

impl Default for MapxRaw {
    fn default() -> Self {
        Self::new()
    }
}

///////////////////////////////////////////////
// Begin of the self-implementation for MapxRaw //
/*********************************************/

impl MapxRaw {
    /// Create an instance.
    #[inline(always)]
    pub fn new() -> Self {
        MapxRaw {
            in_disk: backend::MapxRaw::must_new(VSDB.alloc_id()),
        }
    }

    // Get the database storage path
    pub(crate) fn get_meta(&self) -> MetaInfo {
        self.in_disk.get_meta()
    }

    /// Imitate the behavior of 'BTreeMap<_>.get(...)'
    ///
    /// Any faster/better choice other than JSON ?
    #[inline(always)]
    pub fn get(&self, key: &[u8]) -> Option<IVec> {
        self.in_disk.get(key)
    }

    /// less or equal value
    #[inline(always)]
    pub fn get_le(&self, key: &[u8]) -> Option<(IVec, IVec)> {
        self.in_disk.get_le(key)
    }

    /// great or equal value
    #[inline(always)]
    pub fn get_ge(&self, key: &[u8]) -> Option<(IVec, IVec)> {
        self.in_disk.get_ge(key)
    }

    /// Imitate the behavior of 'BTreeMap<_>.get_mut(...)'
    #[inline(always)]
    pub fn get_mut(&mut self, key: &[u8]) -> Option<ValueMut<'_>> {
        self.in_disk
            .get(key)
            .map(move |v| ValueMut::new(self, IVec::from(key), v))
    }

    /// Imitate the behavior of 'BTreeMap<_>.len()'.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.in_disk.len()
    }

    /// A helper func
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.in_disk.is_empty()
    }

    /// Imitate the behavior of 'BTreeMap<_>.insert(...)'.
    #[inline(always)]
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Option<IVec> {
        self.in_disk.insert(key, value)
    }

    /// Imitate the behavior of '.entry(...).or_insert(...)'
    #[inline(always)]
    pub fn entry<'a>(&'a mut self, key: &'a [u8]) -> Entry<'a> {
        Entry { key, db: self }
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub fn iter(&self) -> MapxRawIter {
        MapxRawIter {
            iter: self.in_disk.iter(),
        }
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range<'a, R: RangeBounds<&'a [u8]>>(&'a self, bounds: R) -> MapxRawIter {
        MapxRawIter {
            iter: self.in_disk.range(bounds),
        }
    }

    /// Check if a key is exists.
    #[inline(always)]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.in_disk.contains_key(key)
    }

    /// Try to remove an entry
    #[inline(always)]
    pub fn remove(&mut self, key: &[u8]) -> Option<IVec> {
        self.in_disk.remove(key)
    }

    /// Clear all data.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.in_disk.clear();
    }
}

/*******************************************/
// End of the self-implementation for MapxRaw //
/////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////
// Begin of the implementation of ValueMut(returned by `self.get_mut`) for MapxRaw //
/********************************************************************************/

/// Returned by `<MapxRaw>.get_mut(...)`
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct ValueMut<'a> {
    hdr: &'a mut MapxRaw,
    key: ManuallyDrop<IVec>,
    value: ManuallyDrop<IVec>,
}

impl<'a> ValueMut<'a> {
    fn new(hdr: &'a mut MapxRaw, key: IVec, value: IVec) -> Self {
        ValueMut {
            hdr,
            key: ManuallyDrop::new(key),
            value: ManuallyDrop::new(value),
        }
    }

    /// Take the inner value.
    pub fn clone_inner(self) -> IVec {
        ManuallyDrop::into_inner(self.value.clone())
    }
}

///
/// **NOTE**: &[u8]ERY IMPORTANT !!!
///
impl<'a> Drop for ValueMut<'a> {
    fn drop(&mut self) {
        // This operation is safe within a `drop()`.
        // SEE: [**ManuallyDrop::take**](std::mem::ManuallyDrop::take)
        unsafe {
            self.hdr.insert(
                &ManuallyDrop::take(&mut self.key),
                &ManuallyDrop::take(&mut self.value),
            );
        };
    }
}

impl<'a> Deref for ValueMut<'a> {
    type Target = IVec;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a> DerefMut for ValueMut<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/******************************************************************************/
// End of the implementation of ValueMut(returned by `self.get_mut`) for MapxRaw //
////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////
// Begin of the implementation of Entry for MapxRaw //
/*************************************************/

/// Imitate the `btree_map/btree_map::Entry`.
pub struct Entry<'a> {
    key: &'a [u8],
    db: &'a mut MapxRaw,
}

impl<'a> Entry<'a> {
    /// Imitate the `btree_map/btree_map::Entry.or_insert(...)`.
    pub fn or_insert(self, default: &'a [u8]) -> ValueMut<'a> {
        if !self.db.contains_key(self.key) {
            self.db.insert(self.key, default);
        }
        pnk!(self.db.get_mut(self.key))
    }

    /// Imitate the `btree_map/btree_map::Entry.or_insert_with(...)`.
    pub fn or_insert_with<F>(self, default: F) -> ValueMut<'a>
    where
        F: FnOnce() -> Vec<u8>,
    {
        if !self.db.contains_key(self.key) {
            self.db.insert(self.key, &default());
        }
        pnk!(self.db.get_mut(self.key))
    }
}

/***********************************************/
// End of the implementation of Entry for MapxRaw //
/////////////////////////////////////////////////

//////////////////////////////////////////////////
// Begin of the implementation of Iter for MapxRaw //
/************************************************/

/// Iter over [MapxRaw](self::MapxRaw).
pub struct MapxRawIter {
    iter: backend::MapxRawIter,
}

impl Iterator for MapxRawIter {
    type Item = (IVec, IVec);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl DoubleEndedIterator for MapxRawIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}

/**********************************************/
// End of the implementation of Iter for MapxRaw //
////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////
// Begin of the implementation of Serialize/Deserialize for MapxRaw //
/*****************************************************************/

impl serde::Serialize for MapxRaw {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let v = pnk!(bincode::serialize(&self.get_meta()));
        serializer.serialize_bytes(&v)
    }
}

impl<'de> serde::Deserialize<'de> for MapxRaw {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(SimpleVisitor).map(|meta| {
            let meta = pnk!(bincode::deserialize::<MetaInfo>(&meta));
            MapxRaw::from(meta)
        })
    }
}

/***************************************************************/
// End of the implementation of Serialize/Deserialize for MapxRaw //
/////////////////////////////////////////////////////////////////
