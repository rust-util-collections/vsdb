//!
//! Single Value
//!
//! NOTE:
//! - Values will be encoded by some `serde`-like methods
//!

#[cfg(test)]
mod test;

use crate::{ValueEnDe, Vecx};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    mem::ManuallyDrop,
    ops::{
        Add, AddAssign, BitAnd, BitAndAssign, BitOr, BitOrAssign, BitXor, BitXorAssign,
        Deref, DerefMut, Div, DivAssign, Mul, MulAssign, Neg, Not, Rem, RemAssign, Shl,
        ShlAssign, Shr, ShrAssign, Sub, SubAssign,
    },
};

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

/// Used to express some 'non-collection' types,
/// such as any type of integer, an enum value, etc..
#[derive(Serialize, Deserialize, Debug, Eq)]
#[serde(bound = "")]
pub struct SingleValue<T>
where
    T: ValueEnDe,
{
    inner: Vecx<T>,
}

impl<T> Iterator for SingleValue<T>
where
    T: ValueEnDe + Eq,
{
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        Some(self.get_value())
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<T> Drop for SingleValue<T>
where
    T: ValueEnDe,
{
    fn drop(&mut self) {
        self.inner.clear();
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<T> SingleValue<T>
where
    T: ValueEnDe,
{
    #[allow(missing_docs)]
    pub fn new(v: T) -> Self {
        let mut hdr = Vecx::new();
        hdr.insert(0, v);
        Self { inner: hdr }
    }

    /// Clone the inner value.
    pub fn clone_inner(&self) -> T {
        self.get_value()
    }

    fn get_value(&self) -> T {
        self.inner.get(0).unwrap()
    }

    fn set_value(&mut self, v: T) {
        self.inner.update(0, v);
    }

    /// Get the mutable handler of the value.
    ///
    /// NOTE:
    /// - Always use this method to change value
    ///     - `*(<SingleValue>).get_mut() = ...`
    /// - **NEVER** do this:
    ///     - `*(&mut <SingleValue>) = SingleValue::new(...)`
    ///     - OR you will loss the 'versioned' ability of this object
    pub fn get_mut(&mut self) -> SingleValueMut<'_, T> {
        let value = ManuallyDrop::new(self.get_value());
        SingleValueMut { hdr: self, value }
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<T> PartialEq for SingleValue<T>
where
    T: ValueEnDe + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.get_value() == other.get_value()
    }
}

impl<T> PartialEq<T> for SingleValue<T>
where
    T: ValueEnDe + PartialEq,
{
    fn eq(&self, other: &T) -> bool {
        self.get_value() == *other
    }
}

////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////

impl<T> Ord for SingleValue<T>
where
    T: ValueEnDe + Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.get_value().cmp(&other.get_value())
    }
}

impl<T> PartialOrd for SingleValue<T>
where
    T: ValueEnDe + Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.get_value().partial_cmp(&other.get_value())
    }
}

impl<T> PartialOrd<T> for SingleValue<T>
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
        impl<T> $ops for SingleValue<T>
        where
            T: ValueEnDe + Ord + Eq + $ops<Output = T>,
        {
            type Output = T;
            fn $fn(self, other: Self) -> Self::Output {
                self.get_value() $op other.get_value()
            }
        }

        impl<T> $ops<T> for SingleValue<T>
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

        impl<T> $ops_assign for SingleValue<T>
        where
            T: ValueEnDe + Ord + Eq + $ops_assign,
        {
            fn $fn_assign(&mut self, other: Self) {
                *self.get_mut() $op_assign other.get_value();
            }
        }

        impl<T> $ops_assign<T> for SingleValue<T>
        where
            T: ValueEnDe + Ord + Eq + $ops_assign,
        {
            fn $fn_assign(&mut self, other: T) {
                *self.get_mut() $op_assign other;
            }
        }
    };
    (@$ops: tt, $fn: tt, $op: tt) => {
        impl<T> $ops for SingleValue<T>
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

/// A type returned by `get_mut()`.
pub struct SingleValueMut<'a, T>
where
    T: ValueEnDe,
{
    hdr: &'a mut SingleValue<T>,
    value: ManuallyDrop<T>,
}

impl<'a, T> Drop for SingleValueMut<'a, T>
where
    T: ValueEnDe,
{
    fn drop(&mut self) {
        // This operation is safe within a `drop()`.
        // SEE: [**ManuallyDrop::take**](std::mem::ManuallyDrop::take)
        unsafe {
            self.hdr.set_value(ManuallyDrop::take(&mut self.value));
        };
    }
}

impl<'a, T> Deref for SingleValueMut<'a, T>
where
    T: ValueEnDe,
{
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, T> DerefMut for SingleValueMut<'a, T>
where
    T: ValueEnDe,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
