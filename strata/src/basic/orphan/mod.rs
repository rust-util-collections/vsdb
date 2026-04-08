//!
//! A storage type for various kinds of single, non-collection values.
//!
//! `Orphan` is designed to store single values, such as integers, enums, or other
//! simple data types, on disk. It provides a convenient way to manage individual
//! pieces of data that are not part of a larger collection.
//!
//! # Examples
//!
//! ```
//! use vsdb::basic::orphan::Orphan;
//! use vsdb::{vsdb_set_base_dir, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_set_base_dir(&dir).unwrap();
//!
//! let mut o = Orphan::new(10);
//! assert_eq!(o.get_value(), 10);
//!
//! *o.get_mut() += 5;
//! assert_eq!(o.get_value(), 15);
//!
//! // Clean up the directory
//! fs::remove_dir_all(vsdb_get_base_dir()).unwrap();
//! ```

#[cfg(test)]
mod test;

use crate::common::error::Result;
use crate::{ValueEnDe, basic::mapx_ord_rawkey::MapxOrdRawKey};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    ops::{
        Add, AddAssign, BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign,
        Deref, DerefMut, Div, DivAssign, Mul, MulAssign, Neg, Not, Rem, RemAssign, Shl,
        ShlAssign, Shr, ShrAssign, Sub, SubAssign,
    },
};

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

/// A container for a single, non-collection value stored on disk.
///
/// `Orphan` is suitable for storing simple data types like integers, enums, etc.
#[derive(Debug)]
pub struct Orphan<T> {
    inner: MapxOrdRawKey<T>,
}

impl<T> Serialize for Orphan<T> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for Orphan<T> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        MapxOrdRawKey::deserialize(deserializer).map(|inner| Self { inner })
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<T> Orphan<T>
where
    T: ValueEnDe,
{
    /// Creates a second handle to the same underlying storage.
    ///
    /// # Safety
    ///
    /// The caller must enforce Single-Writer-Multiple-Readers (SWMR):
    /// no mutation (`set_value`, `get_mut`) may occur on the original
    /// **or** any shadow while any shadow exists.  All shadows must be
    /// dropped before the next write.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        unsafe {
            Self {
                inner: self.inner.shadow(),
            }
        }
    }

    /// Reconstructs an `Orphan` from a byte slice previously produced by
    /// [`as_bytes`](Self::as_bytes) on a valid instance of the same type
    /// and code version.
    ///
    /// # Safety
    ///
    /// Passing any other bytes (corrupted, truncated, or from a different
    /// type / code version) is undefined behavior and may cause panics
    /// or silent data corruption on subsequent operations.
    #[inline(always)]
    pub unsafe fn from_bytes(s: impl AsRef<[u8]>) -> Self {
        unsafe {
            Self {
                inner: MapxOrdRawKey::from_bytes(s),
            }
        }
    }

    /// Returns the byte representation of the `Orphan`.
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        self.inner.as_bytes()
    }

    /// Creates a new `Orphan` with an initial value.
    pub fn new(v: T) -> Self {
        let mut hdr = MapxOrdRawKey::new();
        hdr.insert([], &v);
        Self { inner: hdr }
    }

    /// Retrieves a clone of the inner value.
    pub fn get_value(&self) -> T {
        self.inner.get([]).unwrap()
    }

    /// Sets the inner value.
    pub fn set_value(&mut self, v: &T) {
        self.inner.insert([], v);
    }

    /// Checks if the `Orphan` is uninitialized.
    pub fn is_uninitialized(&self) -> bool {
        self.inner.get([]).is_none()
    }

    /// Initializes the `Orphan` with a value if it is currently empty.
    pub fn initialize_if_empty(&mut self, v: T) {
        if self.is_uninitialized() {
            self.set_value(&v)
        }
    }

    /// Retrieves a mutable handler for the value.
    ///
    /// This is the recommended way to modify the value.
    ///
    /// # Example
    ///
    /// ```
    /// # use vsdb::basic::orphan::Orphan;
    /// # use vsdb::{vsdb_set_base_dir, vsdb_get_base_dir};
    /// # use std::fs;
    /// # let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
    /// # vsdb_set_base_dir(&dir).unwrap();
    /// let mut o = Orphan::new(10);
    /// *o.get_mut() = 20;
    /// assert_eq!(o.get_value(), 20);
    /// # fs::remove_dir_all(vsdb_get_base_dir()).unwrap();
    /// ```
    pub fn get_mut(&mut self) -> ValueMut<'_, T> {
        let value = self.get_value();
        ValueMut { hdr: self, value }
    }

    /// Checks if this `Orphan` instance is the same as another.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }

    /// Returns the unique instance ID of this `Orphan`.
    #[inline(always)]
    pub fn instance_id(&self) -> u64 {
        self.inner.instance_id()
    }

    /// Persists this instance's metadata to disk so that it can be
    /// recovered later via [`from_meta`](Self::from_meta).
    ///
    /// Returns the `instance_id` that should be passed to `from_meta`.
    pub fn save_meta(&self) -> Result<u64> {
        let id = self.instance_id();
        crate::common::save_instance_meta(id, self)?;
        Ok(id)
    }

    /// Recovers an `Orphan` instance from previously saved metadata.
    ///
    /// The caller must ensure that the underlying VSDB database still
    /// contains the data referenced by this instance ID.
    pub fn from_meta(instance_id: u64) -> Result<Self> {
        crate::common::load_instance_meta(instance_id)
    }
}

impl<T> Clone for Orphan<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: Default + ValueEnDe> Default for Orphan<T> {
    fn default() -> Self {
        let mut hdr = MapxOrdRawKey::new();
        hdr.insert([], &T::default());
        Self { inner: hdr }
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<T> Eq for Orphan<T> where T: ValueEnDe + PartialEq {}

impl<T> PartialEq for Orphan<T>
where
    T: ValueEnDe + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.get_value() == other.get_value()
    }
}

impl<T> PartialEq<T> for Orphan<T>
where
    T: ValueEnDe + PartialEq,
{
    fn eq(&self, other: &T) -> bool {
        self.get_value() == *other
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<T> Ord for Orphan<T>
where
    T: ValueEnDe + Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.get_value().cmp(&other.get_value())
    }
}

impl<T> PartialOrd for Orphan<T>
where
    T: ValueEnDe + Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialOrd<T> for Orphan<T>
where
    T: ValueEnDe + Ord,
{
    fn partial_cmp(&self, other: &T) -> Option<Ordering> {
        self.get_value().partial_cmp(other)
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

macro_rules! impl_ops {
    ($ops: tt, $fn: tt, $op: tt) => {
        impl<T> $ops for Orphan<T>
        where
            T: ValueEnDe + Ord + Eq + $ops<Output = T>,
        {
            type Output = T;
            fn $fn(self, other: Self) -> Self::Output {
                self.get_value() $op other.get_value()
            }
        }

        impl<T> $ops<T> for Orphan<T>
        where
            T: ValueEnDe + Ord + Eq + $ops<Output = T>,
        {
            type Output = T;
            fn $fn(self, other: T) -> Self::Output {
                self.get_value() $op other
            }
        }
    };
    ($ops: tt, $fn: tt, $op: tt, $ops_assign: tt, $fn_assign: tt, $op_assign: tt) => {
        impl_ops!($ops, $fn, $op);

        impl<T> $ops_assign for Orphan<T>
        where
            T: ValueEnDe + Ord + Eq + $ops_assign,
        {
            fn $fn_assign(&mut self, other: Self) {
                *self.get_mut() $op_assign other.get_value();
            }
        }

        impl<T> $ops_assign<T> for Orphan<T>
        where
            T: ValueEnDe + Ord + Eq + $ops_assign,
        {
            fn $fn_assign(&mut self, other: T) {
                *self.get_mut() $op_assign other;
            }
        }
    };
    (@$ops: tt, $fn: tt, $op: tt) => {
        impl<T> $ops for Orphan<T>
        where
            T: ValueEnDe + Ord + Eq + $ops<Output = T>,
        {
            type Output = T;
            fn $fn(self) -> Self::Output {
                $op self.get_value()
            }
        }
    };
}

impl_ops!(Add, add, +, AddAssign, add_assign, +=);
impl_ops!(Sub, sub, -, SubAssign, sub_assign, -=);
impl_ops!(Mul, mul, *, MulAssign, mul_assign, *=);
impl_ops!(Div, div, /, DivAssign, div_assign, /=);
impl_ops!(Rem, rem, %, RemAssign, rem_assign, %=);

impl_ops!(BitAnd, bitand, &, BitAndAssign, bitand_assign, &=);
impl_ops!(BitOr, bitor, |, BitOrAssign, bitor_assign, |=);
impl_ops!(BitXor, bitxor, ^, BitXorAssign, bitxor_assign, ^=);
impl_ops!(Shl, shl, <<, ShlAssign, shl_assign, <<=);
impl_ops!(Shr, shr, >>, ShrAssign, shr_assign, >>=);

impl_ops!(@Not, not, !);
impl_ops!(@Neg, neg, -);

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

/// A mutable reference to the value of an `Orphan`.
///
/// This struct is returned by `get_mut()` and ensures that any changes to the
/// value are written back to disk when it is dropped.
pub struct ValueMut<'a, T>
where
    T: ValueEnDe,
{
    hdr: &'a mut Orphan<T>,
    value: T,
}

impl<T> Drop for ValueMut<'_, T>
where
    T: ValueEnDe,
{
    fn drop(&mut self) {
        self.hdr.set_value(&self.value);
    }
}

impl<T> Deref for ValueMut<'_, T>
where
    T: ValueEnDe,
{
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> DerefMut for ValueMut<'_, T>
where
    T: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
