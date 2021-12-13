//!
//! # A disk-storage replacement for the pure in-memory BTreeMap
//!
//! This module is non-invasive to external code except the `new` method.
//!

mod backend;
#[cfg(test)]
mod test;

use crate::common::{InstanceCfg, SimpleVisitor};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    cmp::Ordering,
    fmt,
    iter::Iterator,
    mem::{size_of, ManuallyDrop},
    ops::{Deref, DerefMut, RangeBounds},
};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `BTreeMap<_, _>`.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub struct MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    in_disk: backend::MapxOC<K, V>,
}

impl<K, V> From<InstanceCfg> for MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn from(cfg: InstanceCfg) -> Self {
        Self {
            in_disk: backend::MapxOC::from(cfg),
        }
    }
}

impl<K, V> Default for MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

///////////////////////////////////////////////
// Begin of the self-implementation for MapxOC //
/*********************************************/

impl<K, V> MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    /// Create an instance.
    #[inline(always)]
    pub fn new() -> Self {
        MapxOC {
            in_disk: backend::MapxOC::must_new(),
        }
    }

    // Get the database storage path
    pub(crate) fn get_instance_cfg(&self) -> InstanceCfg {
        self.in_disk.get_instance_cfg()
    }

    /// Imitate the behavior of 'BTreeMap<_>.get(...)'
    ///
    /// Any faster/better choice other than JSON ?
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.in_disk.get(key)
    }

    /// Get the closest smaller value,
    /// NOTE: include itself!
    #[inline(always)]
    pub fn get_le(&self, key: &K) -> Option<(K, V)> {
        self.in_disk.get_le(key)
    }

    /// Get the closest larger value,
    /// NOTE: include itself!
    #[inline(always)]
    pub fn get_ge(&self, key: &K) -> Option<(K, V)> {
        self.in_disk.get_ge(key)
    }

    /// Imitate the behavior of 'BTreeMap<_>.get_mut(...)'
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, K, V>> {
        self.in_disk
            .get(key)
            .map(move |v| ValueMut::new(self, key.clone(), v))
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
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.in_disk.insert(key, value)
    }

    /// Similar with `insert`, but ignore the old value.
    #[inline(always)]
    pub fn set_value(&mut self, key: K, value: V) {
        self.in_disk.set_value(key, value);
    }

    /// Imitate the behavior of '.entry(...).or_insert(...)'
    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        Entry { key, db: self }
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub fn iter(&self) -> MapxOCIter<K, V> {
        MapxOCIter {
            iter: self.in_disk.iter(),
        }
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range<R: RangeBounds<K>>(&self, bounds: R) -> MapxOCIter<K, V> {
        MapxOCIter {
            iter: self.in_disk.range(bounds),
        }
    }

    /// Check if a key is exists.
    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.in_disk.contains_key(key)
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.in_disk.remove(key)
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.in_disk.unset_value(key);
    }

    /// Clear all data.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.in_disk.clear();
    }
}

/*******************************************/
// End of the self-implementation for MapxOC //
/////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////
// Begin of the implementation of ValueMut(returned by `self.get_mut`) for MapxOC //
/********************************************************************************/

/// Returned by `<MapxOC>.get_mut(...)`
#[derive(Debug)]
pub struct ValueMut<'a, K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    hdr: &'a mut MapxOC<K, V>,
    key: ManuallyDrop<K>,
    value: ManuallyDrop<V>,
}

impl<'a, K, V> ValueMut<'a, K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn new(hdr: &'a mut MapxOC<K, V>, key: K, value: V) -> Self {
        ValueMut {
            hdr,
            key: ManuallyDrop::new(key),
            value: ManuallyDrop::new(value),
        }
    }

    /// Clone the inner value.
    pub fn clone_inner(self) -> V {
        ManuallyDrop::into_inner(self.value.clone())
    }
}

///
/// **NOTE**: VERY IMPORTANT !!!
///
impl<'a, K, V> Drop for ValueMut<'a, K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn drop(&mut self) {
        // This operation is safe within a `drop()`.
        // SEE: [**ManuallyDrop::take**](std::mem::ManuallyDrop::take)
        unsafe {
            self.hdr.set_value(
                ManuallyDrop::take(&mut self.key),
                ManuallyDrop::take(&mut self.value),
            );
        };
    }
}

impl<'a, K, V> Deref for ValueMut<'a, K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K, V> DerefMut for ValueMut<'a, K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<'a, K, V> PartialEq for ValueMut<'a, K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &ValueMut<'a, K, V>) -> bool {
        self.value == other.value
    }
}

impl<'a, K, V> PartialEq<V> for ValueMut<'a, K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn eq(&self, other: &V) -> bool {
        self.value.deref() == other
    }
}

impl<'a, K, V> PartialOrd<V> for ValueMut<'a, K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Ord + PartialOrd + Serialize + DeserializeOwned + fmt::Debug,
{
    fn partial_cmp(&self, other: &V) -> Option<Ordering> {
        self.value.deref().partial_cmp(other)
    }
}

/******************************************************************************/
// End of the implementation of ValueMut(returned by `self.get_mut`) for MapxOC //
////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////
// Begin of the implementation of Entry for MapxOC //
/*************************************************/

/// Imitate the `btree_map/btree_map::Entry`.
pub struct Entry<'a, K, V>
where
    K: OrderConsistKey,
    V: 'a + fmt::Debug + Clone + PartialEq + Serialize + DeserializeOwned,
{
    key: K,
    db: &'a mut MapxOC<K, V>,
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: OrderConsistKey,
    V: 'a + fmt::Debug + Clone + PartialEq + Serialize + DeserializeOwned,
{
    /// Imitate the `btree_map/btree_map::Entry.or_insert(...)`.
    pub fn or_insert(self, default: V) -> ValueMut<'a, K, V> {
        if !self.db.contains_key(&self.key) {
            self.db.set_value(self.key.clone(), default);
        }
        pnk!(self.db.get_mut(&self.key))
    }

    /// Imitate the `btree_map/btree_map::Entry.or_insert_with(...)`.
    pub fn or_insert_with<F>(self, default: F) -> ValueMut<'a, K, V>
    where
        F: FnOnce() -> V,
    {
        if !self.db.contains_key(&self.key) {
            self.db.set_value(self.key.clone(), default());
        }
        pnk!(self.db.get_mut(&self.key))
    }
}

/***********************************************/
// End of the implementation of Entry for MapxOC //
/////////////////////////////////////////////////

//////////////////////////////////////////////////
// Begin of the implementation of Iter for MapxOC //
/************************************************/

/// Iter over [MapxOC](self::MapxOC).
pub struct MapxOCIter<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    iter: backend::MapxOCIter<K, V>,
}

impl<K, V> Iterator for MapxOCIter<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<K, V> DoubleEndedIterator for MapxOCIter<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}

/**********************************************/
// End of the implementation of Iter for MapxOC //
////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////
// Begin of the implementation of Serialize/Deserialize for MapxOC //
/*****************************************************************/

impl<K, V> serde::Serialize for MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let v = pnk!(bcs::to_bytes(&self.get_instance_cfg()));
        serializer.serialize_bytes(&v)
    }
}

impl<'de, K, V> serde::Deserialize<'de> for MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Clone + PartialEq + Serialize + DeserializeOwned + fmt::Debug,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(SimpleVisitor).map(|meta| {
            let meta = pnk!(bcs::from_bytes::<InstanceCfg>(&meta));
            MapxOC::from(meta)
        })
    }
}

/***************************************************************/
// End of the implementation of Serialize/Deserialize for MapxOC //
/////////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////

/// Key's order in bytes format is consistent with the original data format
pub trait OrderConsistKey:
    Clone + PartialEq + Eq + PartialOrd + Ord + fmt::Debug
{
    /// key => bytes
    fn to_bytes(&self) -> Vec<u8>;
    /// bytes => key
    fn from_bytes(b: &[u8]) -> Result<Self>;
}

macro_rules! impl_ock {
    ($int: ty) => {
        impl OrderConsistKey for $int {
            fn to_bytes(&self) -> Vec<u8> {
                self.to_be_bytes().to_vec()
            }
            fn from_bytes(b: &[u8]) -> Result<Self> {
                <[u8; size_of::<$int>()]>::try_from(b)
                    .c(d!())
                    .map(<$int>::from_be_bytes)
            }
        }
    };
    (@$int: ty) => {
        impl OrderConsistKey for Vec<$int> {
            fn to_bytes(&self) -> Vec<u8> {
                self.iter().map(|i| i.to_be_bytes()).flatten().collect()
            }
            fn from_bytes(b: &[u8]) -> Result<Self> {
                if 0 != b.len() % size_of::<$int>() {
                    return Err(eg!("invalid bytes"));
                }
                b.chunks(size_of::<$int>())
                    .map(|i| {
                        <[u8; size_of::<$int>()]>::try_from(i)
                            .c(d!())
                            .map(<$int>::from_be_bytes)
                    })
                    .collect()
            }
        }
        impl OrderConsistKey for Box<[$int]> {
            fn to_bytes(&self) -> Vec<u8> {
                self.iter().map(|i| i.to_be_bytes()).flatten().collect()
            }
            fn from_bytes(b: &[u8]) -> Result<Self> {
                if 0 != b.len() % size_of::<$int>() {
                    return Err(eg!("invalid bytes"));
                }
                b.chunks(size_of::<$int>())
                    .map(|i| {
                        <[u8; size_of::<$int>()]>::try_from(i)
                            .c(d!())
                            .map(<$int>::from_be_bytes)
                    })
                    .collect()
            }
        }
    };
    ($int: ty, $siz: expr) => {
        impl OrderConsistKey for [$int; $siz] {
            fn to_bytes(&self) -> Vec<u8> {
                self.iter().map(|i| i.to_be_bytes()).flatten().collect()
            }
            fn from_bytes(b: &[u8]) -> Result<Self> {
                if 0 != b.len() % size_of::<$int>() {
                    return Err(eg!("invalid bytes"));
                }
                if $siz != b.len() / size_of::<$int>() {
                    return Err(eg!("invalid bytes"));
                }
                let mut res = [0; $siz];
                b.chunks(size_of::<$int>())
                    .enumerate()
                    .for_each(|(idx, i)| {
                        res[idx] = <[u8; size_of::<$int>()]>::try_from(i)
                            .map(<$int>::from_be_bytes)
                            .unwrap();
                    });
                Ok(res)
            }
        }
    };
}

impl_ock!(i8);
impl_ock!(i16);
impl_ock!(i32);
impl_ock!(i64);
impl_ock!(i128);
impl_ock!(isize);
impl_ock!(u8);
impl_ock!(u16);
impl_ock!(u32);
impl_ock!(u64);
impl_ock!(u128);
impl_ock!(usize);

impl_ock!(@i8);
impl_ock!(@i16);
impl_ock!(@i32);
impl_ock!(@i64);
impl_ock!(@i128);
impl_ock!(@isize);
impl_ock!(@u8);
impl_ock!(@u16);
impl_ock!(@u32);
impl_ock!(@u64);
impl_ock!(@u128);
impl_ock!(@usize);

macro_rules! impl_repeat {
    ($i: expr) => {
        impl_ock!(i8, $i);
        impl_ock!(i16, $i);
        impl_ock!(i32, $i);
        impl_ock!(i64, $i);
        impl_ock!(i128, $i);
        impl_ock!(isize, $i);
        impl_ock!(u8, $i);
        impl_ock!(u16, $i);
        impl_ock!(u32, $i);
        impl_ock!(u64, $i);
        impl_ock!(u128, $i);
        impl_ock!(usize, $i);
    };
    ($i: expr, $($ii: expr),+) => {
        impl_ock!(i8, $i);
        impl_ock!(i16, $i);
        impl_ock!(i32, $i);
        impl_ock!(i64, $i);
        impl_ock!(i128, $i);
        impl_ock!(isize, $i);
        impl_ock!(u8, $i);
        impl_ock!(u16, $i);
        impl_ock!(u32, $i);
        impl_ock!(u64, $i);
        impl_ock!(u128, $i);
        impl_ock!(usize, $i);

        impl_repeat!($($ii), +);
    };
}

impl_repeat!(
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
    24, 25, 26, 27, 28, 29, 30, 31, 32
);
