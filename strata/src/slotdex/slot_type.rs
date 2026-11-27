//! Fixed-width ordered coordinates for a slot index.

use crate::common::error::Result;
use serde::{Serialize, de};
use std::{fmt, ops::Not};

pub(super) mod sealed {
    /// The fixed-size byte representation of a slot.
    pub trait SlotBytes: Copy + Eq + Ord + AsRef<[u8]> + AsMut<[u8]> + 'static {
        const LEN: usize;
        fn zeroed() -> Self;
    }

    impl<const N: usize> SlotBytes for [u8; N] {
        const LEN: usize = N;
        fn zeroed() -> Self {
            [0; N]
        }
    }
}

/// A fixed-width ordered coordinate. Encoded bytes must sort in slot order and
/// round-trip exactly. Bitwise complement must reverse that order and be an
/// involution, exchanging the two endpoints.
///
/// Implemented for fixed-width signed and unsigned integer types.
/// A custom slot must also use a fixed-size byte array.
///
/// ```compile_fail
/// use vsdb::SlotType;
/// # use vsdb::VsdbError;
/// # use serde::{Serialize, Deserialize};
/// # #[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
/// struct Coordinate(u32);
/// # impl std::ops::Not for Coordinate {
/// #     type Output = Self;
/// #     fn not(self) -> Self { Self(!self.0) }
/// # }
/// impl SlotType for Coordinate {
///     type Bytes = Vec<u8>; // Slot encodings must have a fixed width.
/// #     const MIN: Self = Self(u32::MIN);
/// #     const MAX: Self = Self(u32::MAX);
/// #     fn to_bytes(self) -> Self::Bytes { self.0.to_be_bytes().to_vec() }
/// #     fn from_bytes(b: Self::Bytes) -> Result<Self, VsdbError> {
/// #         Ok(Self(u32::from_be_bytes(b.try_into().unwrap())))
/// #     }
/// #     fn tier_stride(c: u32, l: u8) -> Option<Self> { c.checked_pow(l.into()).map(Self) }
/// #     fn floor_align(self, stride: Self) -> Self { Self(self.0 / stride.0 * stride.0) }
/// #     fn checked_succ(self) -> Option<Self> { self.0.checked_add(1).map(Self) }
/// #     fn checked_pred(self) -> Option<Self> { self.0.checked_sub(1).map(Self) }
/// }
/// ```
pub trait SlotType:
    Copy
    + Ord
    + fmt::Debug
    + Not<Output = Self>
    + Serialize
    + de::DeserializeOwned
    + Send
    + Sync
    + 'static
{
    /// An array `[u8; N]` with a constant width for every value.
    type Bytes: sealed::SlotBytes;
    /// Smallest slot.
    const MIN: Self;
    /// Largest slot.
    const MAX: Self;
    /// Encodes this slot in lexicographic order.
    fn to_bytes(self) -> Self::Bytes;
    /// Decodes the fixed-width representation.
    fn from_bytes(bytes: Self::Bytes) -> Result<Self>;
    /// Returns a stride token for `capacity.pow(level)`. Overflow means one
    /// whole-domain bucket rooted at `MIN`.
    fn tier_stride(capacity: u32, level: u8) -> Option<Self>;
    /// Aligns this slot's unsigned offset from `MIN` down to a stride.
    fn floor_align(self, stride: Self) -> Self;
    /// Next slot, or `None` at the maximum.
    fn checked_succ(self) -> Option<Self>;
    /// Previous slot, or `None` at the minimum.
    fn checked_pred(self) -> Option<Self>;
}

macro_rules! impl_slot_type {
    ($slot:ty, $rank:ty) => {
        impl SlotType for $slot {
            type Bytes = [u8; size_of::<Self>()];
            const MIN: Self = <$slot>::MIN;
            const MAX: Self = <$slot>::MAX;

            fn to_bytes(self) -> Self::Bytes {
                ((self ^ Self::MIN) as $rank).to_be_bytes()
            }
            fn from_bytes(bytes: Self::Bytes) -> Result<Self> {
                Ok(<$rank>::from_be_bytes(bytes) as Self ^ Self::MIN)
            }
            fn tier_stride(capacity: u32, level: u8) -> Option<Self> {
                if level == 0 {
                    return Some(1);
                }
                <$rank>::try_from(capacity)
                    .ok()?
                    .checked_pow(level.into())
                    .map(|v| v as Self)
            }
            fn floor_align(self, stride: Self) -> Self {
                let rank = (self ^ Self::MIN) as $rank;
                let stride = stride as $rank;
                (rank / stride * stride) as Self ^ Self::MIN
            }
            fn checked_succ(self) -> Option<Self> {
                self.checked_add(1)
            }
            fn checked_pred(self) -> Option<Self> {
                self.checked_sub(1)
            }
        }
    };
}
impl_slot_type!(u8, u8);
impl_slot_type!(u16, u16);
impl_slot_type!(u32, u32);
impl_slot_type!(u64, u64);
impl_slot_type!(u128, u128);
impl_slot_type!(i8, u8);
impl_slot_type!(i16, u16);
impl_slot_type!(i32, u32);
impl_slot_type!(i64, u64);
impl_slot_type!(i128, u128);
