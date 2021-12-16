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
use sled::IVec;
use std::{
    fmt,
    mem::{size_of, transmute, ManuallyDrop},
    ops::{Deref, DerefMut, RangeBounds},
};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `BTreeMap<_, _>`.
#[derive(PartialEq, Eq, Debug)]
pub struct MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    inner: backend::MapxOC<K, V>,
}

impl<K, V> From<InstanceCfg> for MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    fn from(cfg: InstanceCfg) -> Self {
        Self {
            inner: backend::MapxOC::from(cfg),
        }
    }
}

impl<K, V> Default for MapxOC<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
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
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    /// Create an instance.
    #[inline(always)]
    pub fn new() -> Self {
        MapxOC {
            inner: backend::MapxOC::must_new(),
        }
    }

    // Get the database storage path
    pub(crate) fn get_instance_cfg(&self) -> InstanceCfg {
        self.inner.get_instance_cfg()
    }

    /// Imitate the behavior of 'BTreeMap<_>.get(...)'
    #[inline(always)]
    pub fn get(&self, key: &K) -> Option<V> {
        self.inner.get(key)
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn _get(&self, key: &[u8]) -> Option<V> {
        self.inner._get(key)
    }

    /// Get the closest smaller value, include itself.
    #[inline(always)]
    pub fn get_le(&self, key: &K) -> Option<(K, V)> {
        self.inner.get_le(key)
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn _get_le(&self, key: &[u8]) -> Option<(K, V)> {
        self.inner._get_le(key)
    }

    /// Get the closest larger value, include itself.
    #[inline(always)]
    pub fn get_ge(&self, key: &K) -> Option<(K, V)> {
        self.inner.get_ge(key)
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn _get_ge(&self, key: &[u8]) -> Option<(K, V)> {
        self.inner._get_ge(key)
    }

    /// Imitate the behavior of 'BTreeMap<_>.get_mut(...)'
    #[inline(always)]
    pub fn get_mut(&mut self, key: &K) -> Option<ValueMut<'_, K, V>> {
        self.inner
            .get(key)
            .map(move |v| ValueMut::new(self, key.clone(), v))
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn _get_mut(&mut self, key: &[u8]) -> Option<ValueMut<'_, K, V>> {
        self.inner
            ._get(key)
            .and_then(|v| K::from_slice(key).ok().map(|k| ValueMut::new(self, k, v)))
    }

    /// Imitate the behavior of 'BTreeMap<_>.len()'.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// A helper func
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Imitate the behavior of 'BTreeMap<_>.insert(...)'.
    #[inline(always)]
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.inner.insert(key, value)
    }

    /// Similar with `insert`, but ignore the old value.
    #[inline(always)]
    pub fn set_value(&mut self, key: K, value: V) {
        self.inner.set_value(key, value);
    }

    /// Imitate the behavior of '.entry(...).or_insert(...)'
    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        Entry { key, hdr: self }
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub fn iter(&self) -> MapxOCIter<K, V> {
        MapxOCIter {
            iter: self.inner.iter(),
        }
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range<R: RangeBounds<K>>(&self, bounds: R) -> MapxOCIter<K, V> {
        MapxOCIter {
            iter: self.inner.range(bounds),
        }
    }

    /// First item
    #[inline(always)]
    pub fn first(&self) -> Option<(K, V)> {
        self.iter().next()
    }

    /// Last item
    #[inline(always)]
    pub fn last(&self) -> Option<(K, V)> {
        self.iter().next_back()
    }

    /// Check if a key is exists.
    #[inline(always)]
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn _contains_key(&self, key: &[u8]) -> bool {
        self.inner._contains_key(key)
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(key)
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn _remove(&mut self, key: &[u8]) -> Option<V> {
        self.inner._remove(key)
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.inner.unset_value(key);
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn _unset_value(&mut self, key: &[u8]) {
        self.inner._unset_value(key);
    }

    /// Clear all data.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
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
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    hdr: &'a mut MapxOC<K, V>,
    key: ManuallyDrop<K>,
    value: ManuallyDrop<V>,
}

impl<'a, K, V> ValueMut<'a, K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    fn new(hdr: &'a mut MapxOC<K, V>, key: K, value: V) -> Self {
        ValueMut {
            hdr,
            key: ManuallyDrop::new(key),
            value: ManuallyDrop::new(value),
        }
    }
}

/// NOTE: Very Important !!!
impl<'a, K, V> Drop for ValueMut<'a, K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
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
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K, V> DerefMut for ValueMut<'a, K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
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
    V: 'a + fmt::Debug + Serialize + DeserializeOwned,
{
    key: K,
    hdr: &'a mut MapxOC<K, V>,
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: OrderConsistKey,
    V: 'a + fmt::Debug + Serialize + DeserializeOwned,
{
    /// Imitate the `btree_map/btree_map::Entry.or_insert(...)`.
    pub fn or_insert(self, default: V) -> ValueMut<'a, K, V> {
        if !self.hdr.contains_key(&self.key) {
            self.hdr.set_value(self.key.clone(), default);
        }
        pnk!(self.hdr.get_mut(&self.key))
    }

    /// Imitate the `btree_map/btree_map::Entry.or_insert_with(...)`.
    pub fn or_insert_with<F>(self, default: F) -> ValueMut<'a, K, V>
    where
        F: FnOnce() -> V,
    {
        if !self.hdr.contains_key(&self.key) {
            self.hdr.set_value(self.key.clone(), default());
        }
        pnk!(self.hdr.get_mut(&self.key))
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
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    iter: backend::MapxOCIter<K, V>,
}

impl<K, V> Iterator for MapxOCIter<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<K, V> DoubleEndedIterator for MapxOCIter<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
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
    V: Serialize + DeserializeOwned + fmt::Debug,
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
    V: Serialize + DeserializeOwned + fmt::Debug,
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
    /// &key => bytes
    fn to_bytes(&self) -> Vec<u8>;

    /// key => bytes
    fn into_bytes(self) -> Vec<u8> {
        self.to_bytes()
    }

    /// &bytes => key
    fn from_slice(b: &[u8]) -> Result<Self>;

    /// bytes => key
    fn from_bytes(b: Vec<u8>) -> Result<Self> {
        Self::from_slice(&b)
    }
}

impl OrderConsistKey for IVec {
    #[inline(always)]
    fn to_bytes(&self) -> Vec<u8> {
        self.to_vec()
    }

    #[inline(always)]
    fn from_slice(b: &[u8]) -> Result<Self> {
        Ok(IVec::from(b))
    }
}

impl OrderConsistKey for Vec<u8> {
    #[inline(always)]
    fn to_bytes(&self) -> Vec<u8> {
        self.clone()
    }

    #[inline(always)]
    fn into_bytes(self) -> Vec<u8> {
        self
    }

    #[inline(always)]
    fn from_slice(b: &[u8]) -> Result<Self> {
        Ok(b.to_vec())
    }

    #[inline(always)]
    fn from_bytes(b: Vec<u8>) -> Result<Self> {
        Ok(b)
    }
}

impl OrderConsistKey for String {
    #[inline(always)]
    fn to_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    #[inline(always)]
    fn into_bytes(self) -> Vec<u8> {
        self.into_bytes()
    }

    #[inline(always)]
    fn from_slice(b: &[u8]) -> Result<Self> {
        String::from_utf8(b.to_owned()).c(d!())
    }

    #[inline(always)]
    fn from_bytes(b: Vec<u8>) -> Result<Self> {
        String::from_utf8(b).c(d!())
    }
}

macro_rules! impl_ock {
    ($int: ty) => {
        impl OrderConsistKey for $int {
            #[inline(always)]
            fn to_bytes(&self) -> Vec<u8> {
                self.to_be_bytes().to_vec()
            }
            #[inline(always)]
            fn from_slice(b: &[u8]) -> Result<Self> {
                <[u8; size_of::<$int>()]>::try_from(b)
                    .c(d!())
                    .map(<$int>::from_be_bytes)
            }
        }
    };
    (@$int: ty) => {
        #[allow(clippy::unsound_collection_transmute)]
        impl OrderConsistKey for Vec<$int> {
            #[inline(always)]
            fn to_bytes(&self) -> Vec<u8> {
                self.iter().map(|i| i.to_be_bytes()).flatten().collect()
            }
            #[inline(always)]
            fn into_bytes(mut self) -> Vec<u8> {
                for i in 0..self.len() {
                    self[i] = self[i].to_be();
                }
                unsafe {
                    let mut v = transmute::<Vec<$int>, Vec<u8>>(self);
                    v.set_len(v.len() * size_of::<$int>());
                    v
                }
            }
            #[inline(always)]
            fn from_slice(b: &[u8]) -> Result<Self> {
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
            #[inline(always)]
            fn from_bytes(b: Vec<u8>) -> Result<Self> {
                if 0 != b.len() % size_of::<$int>() {
                    return Err(eg!("invalid bytes"));
                }
                let mut ret = unsafe {
                    let mut v = transmute::<Vec<u8>, Vec<$int>>(b);
                    v.set_len(v.len() / size_of::<$int>());
                    v
                };
                for i in 0..ret.len() {
                    ret[i] = <$int>::from_be(ret[i]);
                }
                Ok(ret)
            }
        }
    };
    ($int: ty, $siz: expr) => {
        impl OrderConsistKey for [$int; $siz] {
            #[inline(always)]
            fn to_bytes(&self) -> Vec<u8> {
                self.iter().map(|i| i.to_be_bytes()).flatten().collect()
            }
            #[inline(always)]
            fn from_slice(b: &[u8]) -> Result<Self> {
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
// impl_ock!(@u8);
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
        impl_repeat!($i);
        impl_repeat!($($ii), +);
    };
}

impl_repeat!(
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
    24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44,
    45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65,
    66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86,
    87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105,
    106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122,
    123, 124, 125, 126, 127, 128
);
