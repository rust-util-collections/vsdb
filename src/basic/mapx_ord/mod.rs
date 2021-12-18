//!
//! A disk-storage replacement for the pure in-memory BTreeMap.
//!

mod backend;

#[cfg(test)]
mod test;

use crate::common::{InstanceCfg, SimpleVisitor};
use ruc::*;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fmt,
    mem::{size_of, transmute, ManuallyDrop},
    ops::{Deref, DerefMut, RangeBounds},
};

/// To solve the problem of unlimited memory usage,
/// use this to replace the original in-memory `BTreeMap<_, _>`.
#[derive(PartialEq, Eq, Debug)]
pub struct MapxOrd<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    inner: backend::MapxOrd<K, V>,
}

impl<K, V> From<InstanceCfg> for MapxOrd<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    fn from(cfg: InstanceCfg) -> Self {
        Self {
            inner: backend::MapxOrd::from(cfg),
        }
    }
}

impl<K, V> Default for MapxOrd<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

///////////////////////////////////////////////
// Begin of the self-implementation for MapxOrd //
/*********************************************/

impl<K, V> MapxOrd<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    /// Create an instance.
    #[inline(always)]
    pub fn new() -> Self {
        MapxOrd {
            inner: backend::MapxOrd::must_new(),
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
    pub fn get_ref_bytes_k(&self, key: &[u8]) -> Option<V> {
        self.inner.get_ref_bytes_k(key)
    }

    /// Get the closest smaller value, include itself.
    #[inline(always)]
    pub fn get_le(&self, key: &K) -> Option<(K, V)> {
        self.inner.get_le(key)
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn get_le_ref_bytes_k(&self, key: &[u8]) -> Option<(K, V)> {
        self.inner.get_le_ref_bytes_k(key)
    }

    /// Get the closest larger value, include itself.
    #[inline(always)]
    pub fn get_ge(&self, key: &K) -> Option<(K, V)> {
        self.inner.get_ge(key)
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn get_ge_ref_bytes_k(&self, key: &[u8]) -> Option<(K, V)> {
        self.inner.get_ge_ref_bytes_k(key)
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
    pub fn get_mut_ref_bytes_k(&mut self, key: &[u8]) -> Option<ValueMut<'_, K, V>> {
        self.inner
            .get_ref_bytes_k(key)
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
        self.insert_ref(&key, &value)
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn insert_ref(&mut self, key: &K, value: &V) -> Option<V> {
        self.inner.insert_ref(key, value)
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn insert_ref_bytes_k(&mut self, key: &[u8], value: &V) -> Option<V> {
        self.inner.insert_ref_bytes_k(key, value)
    }

    /// same as the funtion without the '_' prefix, but use bytes key&value
    #[inline(always)]
    pub fn insert_ref_bytes_kv(&mut self, key: &[u8], value: &[u8]) -> Option<V> {
        self.inner.insert_ref_bytes_kv(key, value)
    }

    /// Similar with `insert`, but ignore the old value.
    #[inline(always)]
    pub fn set_value(&mut self, key: K, value: V) {
        self.set_value_ref(&key, &value)
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn set_value_ref(&mut self, key: &K, value: &V) {
        self.inner.set_value_ref(key, value);
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn set_value_ref_bytes_k(&mut self, key: &[u8], value: &V) {
        self.inner.set_value_ref_bytes_k(key, value);
    }

    /// same as the funtion without the '_' prefix, but use bytes key&value
    #[inline(always)]
    pub fn set_value_ref_bytes_kv(&mut self, key: &[u8], value: &[u8]) {
        self.inner.set_value_ref_bytes_kv(key, value);
    }

    /// Imitate the behavior of '.entry(...).or_insert(...)'
    #[inline(always)]
    pub fn entry(&mut self, key: K) -> Entry<'_, K, V> {
        Entry { key, hdr: self }
    }

    #[inline(always)]
    #[allow(missing_docs)]
    pub fn entry_ref<'a>(&'a mut self, key: &'a K) -> EntryRef<'a, K, V> {
        EntryRef { key, hdr: self }
    }

    /// Imitate the behavior of '.iter()'
    #[inline(always)]
    pub fn iter(&self) -> MapxOrdIter<K, V> {
        MapxOrdIter {
            iter: self.inner.iter(),
        }
    }

    /// range(start..end)
    #[inline(always)]
    pub fn range<R: RangeBounds<K>>(&self, bounds: R) -> MapxOrdIter<K, V> {
        MapxOrdIter {
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
    pub fn contains_key_ref_bytes_k(&self, key: &[u8]) -> bool {
        self.inner.contains_key_ref_bytes_k(key)
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(key)
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn remove_ref_bytes_k(&mut self, key: &[u8]) -> Option<V> {
        self.inner.remove_ref_bytes_k(key)
    }

    /// Remove a <K, V> from mem and disk.
    #[inline(always)]
    pub fn unset_value(&mut self, key: &K) {
        self.inner.unset_value(key);
    }

    /// same as the funtion without the '_' prefix, but use bytes key
    #[inline(always)]
    pub fn unset_value_ref_bytes_k(&mut self, key: &[u8]) {
        self.inner.unset_value_ref_bytes_k(key);
    }

    /// Clear all data.
    #[inline(always)]
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

/*******************************************/
// End of the self-implementation for MapxOrd //
/////////////////////////////////////////////

//////////////////////////////////////////////////////////////////////////////////
// Begin of the implementation of ValueMut(returned by `self.get_mut`) for MapxOrd //
/********************************************************************************/

/// Returned by `<MapxOrd>.get_mut(...)`
#[derive(Debug)]
pub struct ValueMut<'a, K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    hdr: &'a mut MapxOrd<K, V>,
    key: ManuallyDrop<K>,
    value: ManuallyDrop<V>,
}

impl<'a, K, V> ValueMut<'a, K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    pub(crate) fn new(hdr: &'a mut MapxOrd<K, V>, key: K, value: V) -> Self {
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
// End of the implementation of ValueMut(returned by `self.get_mut`) for MapxOrd //
////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////
// Begin of the implementation of Entry for MapxOrd //
/*************************************************/

/// Imitate the `btree_map/btree_map::Entry`.
pub struct Entry<'a, K, V>
where
    K: OrderConsistKey,
    V: 'a + fmt::Debug + Serialize + DeserializeOwned,
{
    key: K,
    hdr: &'a mut MapxOrd<K, V>,
}

impl<'a, K, V> Entry<'a, K, V>
where
    K: OrderConsistKey,
    V: fmt::Debug + Serialize + DeserializeOwned,
{
    /// Imitate the `btree_map/btree_map::Entry.or_insert(...)`.
    pub fn or_insert(self, default: V) -> ValueMut<'a, K, V> {
        if !self.hdr.contains_key(&self.key) {
            self.hdr.set_value_ref(&self.key, &default);
        }
        pnk!(self.hdr.get_mut(&self.key))
    }
}

/// Imitate the `btree_map/btree_map::Entry`.
pub struct EntryRef<'a, K, V>
where
    K: OrderConsistKey,
    V: fmt::Debug + Serialize + DeserializeOwned,
{
    key: &'a K,
    hdr: &'a mut MapxOrd<K, V>,
}

impl<'a, K, V> EntryRef<'a, K, V>
where
    K: OrderConsistKey,
    V: fmt::Debug + Serialize + DeserializeOwned,
{
    /// Imitate the `btree_map/btree_map::Entry.or_insert(...)`.
    pub fn or_insert_ref(self, default: &V) -> ValueMut<'a, K, V> {
        if !self.hdr.contains_key(self.key) {
            self.hdr.set_value_ref(self.key, default);
        }
        pnk!(self.hdr.get_mut(self.key))
    }
}

/***********************************************/
// End of the implementation of Entry for MapxOrd //
/////////////////////////////////////////////////

//////////////////////////////////////////////////
// Begin of the implementation of Iter for MapxOrd //
/************************************************/

/// Iter over [MapxOrd](self::MapxOrd).
pub struct MapxOrdIter<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    iter: backend::MapxOrdIter<K, V>,
}

impl<K, V> Iterator for MapxOrdIter<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    type Item = (K, V);
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<K, V> DoubleEndedIterator for MapxOrdIter<K, V>
where
    K: OrderConsistKey,
    V: Serialize + DeserializeOwned + fmt::Debug,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}

/**********************************************/
// End of the implementation of Iter for MapxOrd //
////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////
// Begin of the implementation of Serialize/Deserialize for MapxOrd //
/*****************************************************************/

impl<K, V> serde::Serialize for MapxOrd<K, V>
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

impl<'de, K, V> serde::Deserialize<'de> for MapxOrd<K, V>
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
            MapxOrd::from(meta)
        })
    }
}

/***************************************************************/
// End of the implementation of Serialize/Deserialize for MapxOrd //
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

macro_rules! impl_type {
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

impl_type!(i8);
impl_type!(i16);
impl_type!(i32);
impl_type!(i64);
impl_type!(i128);
impl_type!(isize);
impl_type!(u8);
impl_type!(u16);
impl_type!(u32);
impl_type!(u64);
impl_type!(u128);
impl_type!(usize);

impl_type!(@i8);
impl_type!(@i16);
impl_type!(@i32);
impl_type!(@i64);
impl_type!(@i128);
impl_type!(@isize);
// impl_type!(@u8);
impl_type!(@u16);
impl_type!(@u32);
impl_type!(@u64);
impl_type!(@u128);
impl_type!(@usize);

macro_rules! impl_repeat {
    ($i: expr) => {
        impl_type!(i8, $i);
        impl_type!(i16, $i);
        impl_type!(i32, $i);
        impl_type!(i64, $i);
        impl_type!(i128, $i);
        impl_type!(isize, $i);
        impl_type!(u8, $i);
        impl_type!(u16, $i);
        impl_type!(u32, $i);
        impl_type!(u64, $i);
        impl_type!(u128, $i);
        impl_type!(usize, $i);
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
