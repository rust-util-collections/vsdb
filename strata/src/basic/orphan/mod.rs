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
//! use vsdb::{VsdbOptions, vsdb_configure, vsdb_get_base_dir};
//! use std::fs;
//!
//! // It's recommended to use a temporary directory for testing
//! let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
//! vsdb_configure(VsdbOptions::new(&dir)).unwrap();
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

use crate::{
    ValueEnDe,
    basic::mapx_ord_rawkey::{self, MapxOrdRawKey},
    common::{Colocate, InstanceId, Namespace, error::Result},
};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    ops::{
        Add, AddAssign, BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign,
        Deref, DerefMut, Div, DivAssign, Mul, MulAssign, Neg, Not, Rem, RemAssign, Shl,
        ShlAssign, Shr, ShrAssign, Sub, SubAssign,
    },
};
use vsdb_core::MapxRaw;

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
        crate::common::serialize_typed_handle_meta::<Self, S>(&self.inner, serializer)
    }
}

impl<'de, T> Deserialize<'de> for Orphan<T> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        crate::common::deserialize_typed_handle_meta::<Self, MapxOrdRawKey<T>, D>(
            deserializer,
        )
        .map(|inner| Self { inner })
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
    /// The caller must ensure no concurrent writes to the same key
    /// through any handle.  Multiple writers on disjoint keys are safe.
    #[inline(always)]
    pub unsafe fn shadow(&self) -> Self {
        Self {
            // SAFETY: forwards this fn's `unsafe` contract — the caller
            // guarantees no concurrent writes to the same key through
            // any handle.
            inner: unsafe { self.inner.shadow() },
        }
    }

    /// [`new`](Self::new) placed in `ns`.
    pub fn new_in(ns: &Namespace, v: T) -> Self {
        ns.scope(|| Self::new(v))
    }

    /// The namespace this value lives in.
    pub fn namespace(&self) -> Namespace {
        self.inner.namespace()
    }

    /// Makes prior successful writes to this value's shard durable without
    /// forcing a memtable flush. Other maps on that shard are synchronized too;
    /// other shards are unaffected. A read-only handle is a no-op.
    ///
    /// # Panics
    ///
    /// Panics on engine synchronization errors. Use
    /// [`try_sync_wal`](Self::try_sync_wal) to handle them.
    pub fn sync_wal(&self) {
        self.inner.sync_wal();
    }

    /// Fallible form of [`sync_wal`](Self::sync_wal).
    pub fn try_sync_wal(&self) -> Result<()> {
        self.inner.try_sync_wal()
    }

    /// Deep-copies this value into a brand-new instance placed in `ns`
    /// — the cross-namespace form of `Clone` (mirroring
    /// [`new`](Self::new) vs [`new_in`](Self::new_in)).
    ///
    /// # Errors
    ///
    /// If an engine-level write fails.
    pub fn clone_in(&self, ns: &Namespace) -> Result<Self> {
        Ok(Self {
            inner: self.inner.clone_in(ns)?,
        })
    }

    /// Creates a new `Orphan` with an initial value.
    pub fn new(v: T) -> Self {
        let mut hdr = MapxOrdRawKey::new();
        hdr.insert([], &v);
        Self { inner: hdr }
    }

    /// [`new`](Self::new) on the same engine shard as `anchor`.
    pub(crate) fn new_colocated(anchor: &MapxRaw, v: T) -> Self {
        let mut hdr: MapxOrdRawKey<T> = Colocate::new_colocated(anchor);
        hdr.insert([], &v);
        Self { inner: hdr }
    }

    /// A deep copy on the same engine shard as `anchor`.
    pub(crate) fn clone_colocated(&self, anchor: &MapxRaw) -> Result<Self> {
        Ok(Self {
            inner: Colocate::clone_colocated(&self.inner, anchor)?,
        })
    }

    /// The underlying raw map.
    pub(crate) fn raw(&self) -> &MapxRaw {
        Colocate::raw(&self.inner)
    }

    /// Retrieves a clone of the inner value.
    pub fn get_value(&self) -> T {
        self.inner.get([]).unwrap()
    }

    /// Sets the inner value.
    pub fn set_value(&mut self, v: &T) {
        self.inner.insert([], v);
    }

    /// Reads a side key. The value slot is `[]`; aux keys must be non-empty
    /// so old readers of [`get_value`](Self::get_value) ignore them.
    pub(crate) fn get_aux(&self, key: &[u8]) -> Option<T> {
        debug_assert!(!key.is_empty());
        self.inner.get(key)
    }

    /// Whether `key` is present. Does not decode the value.
    pub(crate) fn has_aux(&self, key: &[u8]) -> bool {
        debug_assert!(!key.is_empty());
        self.inner.contains_key(key)
    }

    /// Writes a side key. See [`get_aux`](Self::get_aux).
    pub(crate) fn set_aux(&mut self, key: &[u8], value: &T) {
        debug_assert!(!key.is_empty());
        self.inner.insert(key, value);
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
    /// An untouched guard preserves stored bytes, including for values whose
    /// encodings vary between calls. Write-back follows the same rules as
    /// [`MapxOrdRawKey::get_mut`].
    ///
    /// # Example
    ///
    /// ```
    /// # use vsdb::basic::orphan::Orphan;
    /// # use vsdb::{VsdbOptions, vsdb_configure, vsdb_get_base_dir};
    /// # use std::fs;
    /// # let dir = format!("/tmp/vsdb_testing/{}", rand::random::<u128>());
    /// # vsdb_configure(VsdbOptions::new(&dir)).unwrap();
    /// let mut o = Orphan::new(10);
    /// *o.get_mut() = 20;
    /// assert_eq!(o.get_value(), 20);
    /// # fs::remove_dir_all(vsdb_get_base_dir()).unwrap();
    /// ```
    pub fn get_mut(&mut self) -> ValueMut<'_, T> {
        ValueMut {
            inner: self.inner.get_mut([]).unwrap(),
        }
    }

    /// Checks if this `Orphan` instance is the same as another.
    #[inline(always)]
    pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
        self.inner.is_the_same_instance(&other_hdr.inner)
    }

    /// Returns the unique instance ID of this `Orphan`.
    #[inline(always)]
    pub fn instance_id(&self) -> InstanceId {
        self.inner.instance_id()
    }

    /// Persists this instance's metadata to disk so that it can be
    /// recovered later via [`from_meta`](Self::from_meta).
    ///
    /// Returns the `instance_id` that should be passed to `from_meta`.
    pub fn save_meta(&self) -> Result<InstanceId> {
        let id = self.instance_id();
        crate::common::save_instance_meta(id, self)?;
        Ok(id)
    }

    /// Recovers an `Orphan` instance from previously saved metadata.
    ///
    /// The caller must ensure that the underlying VSDB database still
    /// contains the data referenced by this instance ID.
    ///
    /// # Aliasing warning
    ///
    /// The returned handle is a **full alias** of the original instance,
    /// not an independent copy — it addresses the exact same underlying
    /// key range (this is how [`instance_id`](Self::instance_id) is
    /// recovered: it *is* the raw prefix). If the original handle that
    /// produced this `instance_id` (or another `from_meta`/`shadow`
    /// restore of it) is still alive in-process, the same
    /// no-concurrent-writes-to-the-same-key discipline documented on
    /// [`shadow`](Self::shadow) applies across **every** live alias: no
    /// concurrent writes to the same key through any handle. `from_meta` is intended to restore a handle after
    /// the original has gone out of scope (e.g. across a process
    /// restart); calling it while the original is still live requires
    /// the same care as `shadow()`, even though this function is safe
    /// Rust.
    pub fn from_meta(instance_id: impl Into<InstanceId>) -> Result<Self> {
        let id = instance_id.into();
        crate::common::load_instance_meta_checked(id, Self::instance_id)
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

impl<T> Eq for Orphan<T> where T: ValueEnDe + Eq {}

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

// Arithmetic / bit / negation operators only need `ValueEnDe` plus the
// corresponding `std::ops` trait on `T` — requiring `Ord + Eq` here
// would rule out types like `f64` (only `PartialOrd`/`PartialEq`) even
// though the operations themselves are well-defined.  The comparison
// impls above keep their own tighter bounds.
macro_rules! impl_ops {
    ($ops: tt, $fn: tt, $op: tt) => {
        impl<T> $ops for Orphan<T>
        where
            T: ValueEnDe + $ops<Output = T>,
        {
            type Output = T;
            fn $fn(self, other: Self) -> Self::Output {
                self.get_value() $op other.get_value()
            }
        }

        impl<T> $ops<T> for Orphan<T>
        where
            T: ValueEnDe + $ops<Output = T>,
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
            T: ValueEnDe + $ops_assign,
        {
            fn $fn_assign(&mut self, other: Self) {
                *self.get_mut() $op_assign other.get_value();
            }
        }

        impl<T> $ops_assign<T> for Orphan<T>
        where
            T: ValueEnDe + $ops_assign,
        {
            fn $fn_assign(&mut self, other: T) {
                *self.get_mut() $op_assign other;
            }
        }
    };
    (@$ops: tt, $fn: tt, $op: tt) => {
        impl<T> $ops for Orphan<T>
        where
            T: ValueEnDe + $ops<Output = T>,
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
/// value are written back to disk when it is dropped. A panic that starts
/// while the guard is alive discards the edit instead.
pub struct ValueMut<'a, T>
where
    T: ValueEnDe,
{
    inner: mapx_ord_rawkey::ValueMut<'a, T>,
}

impl<T> Deref for ValueMut<'_, T>
where
    T: ValueEnDe,
{
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for ValueMut<'_, T>
where
    T: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////
