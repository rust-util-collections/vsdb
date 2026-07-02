//! Trait for types usable as a slot key in [`SlotDex`](super::SlotDex).
//!
//! Implemented for the native unsigned integers `u32`, `u64`, and `u128`.

use serde::{Serialize, de};
use std::{fmt, ops::Not};

use crate::KeyEnDeOrdered;

/// Trait for types usable as a slot key in [`SlotDex`](super::SlotDex).
///
/// Implemented for the native unsigned integers `u32`, `u64`, and `u128`.
pub trait SlotType:
    Clone
    + Ord
    + fmt::Debug
    + Not<Output = Self>
    + KeyEnDeOrdered
    + Serialize
    + de::DeserializeOwned
    + 'static
{
    /// The minimum value of this type.
    const MIN: Self;
    /// The maximum value of this type.
    const MAX: Self;

    /// Floor-align `self` to a multiple of `base`: `self / base * base`.
    fn floor_align(&self, base: &Self) -> Self;

    /// `self.checked_pow(exp)`, returning `None` on overflow.
    fn checked_pow(&self, exp: u32) -> Option<Self>;

    /// Returns the larger of `self` and `other`.
    fn max_val(self, other: Self) -> Self;

    /// Saturating addition.
    fn saturating_add(&self, rhs: &Self) -> Self;

    /// Widen to `i128` for distance arithmetic.
    fn as_i128(&self) -> i128;

    /// Widen to `u64` for entry-count arithmetic.
    fn as_u64(&self) -> u64;
}

macro_rules! impl_slot_type {
    ($($t:ty),+) => { $(
        impl SlotType for $t {
            const MIN: Self = <$t>::MIN;
            const MAX: Self = <$t>::MAX;
            #[inline]
            fn floor_align(&self, base: &Self) -> Self { self / base * base }
            #[inline]
            fn checked_pow(&self, exp: u32) -> Option<Self> { <$t>::checked_pow(*self, exp) }
            #[inline]
            fn max_val(self, other: Self) -> Self { Ord::max(self, other) }
            #[inline]
            fn saturating_add(&self, rhs: &Self) -> Self { <$t>::saturating_add(*self, *rhs) }
            #[inline]
            fn as_i128(&self) -> i128 { i128::try_from(*self).unwrap_or(i128::MAX) }
            #[inline]
            fn as_u64(&self) -> u64 { *self as u64 }
        }
    )+ };
}

impl_slot_type!(u32, u64, u128);
