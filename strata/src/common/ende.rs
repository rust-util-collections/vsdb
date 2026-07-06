//!
//! # EnDe (Encode/Decode)
//!
//! This module provides traits for encoding and decoding keys and values.
//! These traits are used by the various data structures in `vsdb` to serialize
//! and deserialize data for storage.
//!
//! # Trust model
//!
//! VSDB operates a **closed data loop**: every byte sequence stored on disk
//! was produced by the same encode path in the same process (or a prior run
//! of the same binary).  This has two implications for error handling:
//!
//! * **Encoding a valid Rust value should never fail.**  The blanket
//!   implementations delegate to `postcard::to_allocvec`, which can only
//!   fail if the `Serialize` impl itself is buggy.  Accordingly,
//!   [`KeyEnDe::encode`](crate::common::ende::KeyEnDe::encode) /
//!   [`ValueEnDe::encode`](crate::common::ende::ValueEnDe::encode)
//!   **panic** on error —
//!   a failure here is a programming bug, not a recoverable runtime
//!   condition.  Use
//!   [`KeyEnDe::try_encode`](crate::common::ende::KeyEnDe::try_encode) /
//!   [`ValueEnDe::try_encode`](crate::common::ende::ValueEnDe::try_encode)
//!   at trust boundaries (e.g. first-time validation of a third-party
//!   type) where you want a `Result` instead.
//!
//! * **Decoding VSDB-written data should never fail.**  VSDB collections
//!   (`Mapx`, `MapxOrd`, `VerMap`, …) use assert-style `.unwrap()` on
//!   `decode` calls for data they wrote themselves.  A decode failure in
//!   this context indicates data corruption or a schema-incompatible code
//!   change — neither is automatically recoverable.  The `decode` method
//!   returns `Result` at the **trait level** because the trait cannot
//!   assume the byte source is trusted; callers at boundaries (e.g.
//!   [`from_meta`](crate::Mapx::from_meta) reading an on-disk file) use
//!   `?` to propagate errors normally.
//!
//! All fallible methods return [`Result`](crate::common::error::Result)
//! with the ecosystem-wide [`VsdbError`](crate::common::error::VsdbError),
//! so implementing these traits for custom types requires no third-party
//! error dependency.
//!

use super::{
    RawBytes,
    error::{Result, VsdbError},
};
use std::{fmt, mem::size_of};

use serde::{Serialize, de::DeserializeOwned};

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A trait for encoding keys.
///
/// # Warning
///
/// The blanket implementation covers **all** types that implement
/// [`serde::Serialize`], so it also covers types whose serialization is
/// **non-deterministic** or **non-canonical**.  Using such a type as a
/// VSDB key causes lookups to silently miss.
///
/// Only use types with **deterministic, canonical** serialization as
/// keys: integer primitives, `Vec`, `BTreeMap`, `BTreeSet`, tuples,
/// `String`, and fixed-size arrays are safe.  The following are **not**
/// safe as key types:
///
/// * `HashMap`, `HashSet`, and any wrapper containing them — iteration
///   order depends on the random `SipHash` seed, so the encoding differs
///   across process restarts.
/// * Floating-point types (`f32` / `f64`) and any type containing them —
///   `+0.0` and `-0.0` are `PartialEq`-equal but encode to different
///   bytes, and `NaN` never compares equal to itself, so a stored entry
///   can become unreachable.
pub trait KeyEn: Sized {
    /// Attempts to encode the key.  Returns `Err` only if the
    /// `Serialize` implementation is broken — see [module-level trust
    /// model](self) for details.
    fn try_encode_key(&self) -> Result<RawBytes>;

    /// Encodes the key, **panicking** on failure.
    ///
    /// This is the normal path inside VSDB collections.  A panic here
    /// means the type's `Serialize` impl has a bug.
    fn encode_key(&self) -> RawBytes {
        self.try_encode_key().unwrap()
    }
}

/// A trait for decoding keys.
pub trait KeyDe: Sized {
    /// Decodes a key from a byte slice.
    ///
    /// Returns `Err` when the bytes are invalid.  VSDB collections use
    /// assert-style unwraps on this internally because data they wrote is always
    /// trusted — see [module-level trust model](self).
    fn decode_key(bytes: &[u8]) -> Result<Self>;
}

/// A trait for both encoding and decoding keys.
///
/// # Warning
///
/// Key types must serialize **deterministically and canonically**; see
/// [`KeyEn`] for the list of unsupported types (`HashMap`, `HashSet`,
/// floats).  For *ordered* collections the key encoding must additionally
/// be order-preserving — use [`KeyEnDeOrdered`] there.
pub trait KeyEnDe: Sized {
    /// Attempts to encode the key.  Prefer [`encode`](Self::encode) for
    /// internal VSDB paths; use this at trust boundaries where a `Result`
    /// is needed.
    fn try_encode(&self) -> Result<RawBytes>;

    /// Encodes the key, **panicking** on failure.
    ///
    /// See [`try_encode`](Self::try_encode) for the fallible variant.
    fn encode(&self) -> RawBytes {
        self.try_encode().unwrap()
    }

    /// Decodes a key from a byte slice.
    ///
    /// Returns `Err` when the bytes are invalid.  VSDB collections use
    /// assert-style unwraps on this internally for data they wrote
    /// themselves.
    fn decode(bytes: &[u8]) -> Result<Self>;
}

/// A trait for encoding values.
pub trait ValueEn: Sized {
    /// Attempts to encode the value.  Returns `Err` only if the
    /// `Serialize` implementation is broken.
    fn try_encode_value(&self) -> Result<RawBytes>;

    /// Encodes the value, **panicking** on failure.
    ///
    /// This is the normal path inside VSDB collections.  A panic here
    /// means the type's `Serialize` impl has a bug.
    fn encode_value(&self) -> RawBytes {
        self.try_encode_value().unwrap()
    }
}

/// A trait for decoding values.
pub trait ValueDe: Sized {
    /// Decodes a value from a byte slice.
    ///
    /// Returns `Err` when the bytes are invalid.  VSDB collections use
    /// assert-style unwraps on this internally because data they wrote is always
    /// trusted.
    fn decode_value(bytes: &[u8]) -> Result<Self>;
}

/// A trait for both encoding and decoding values.
pub trait ValueEnDe: Sized {
    /// Attempts to encode the value.  Prefer [`encode`](Self::encode) for
    /// internal VSDB paths; use this at trust boundaries where a `Result`
    /// is needed.
    fn try_encode(&self) -> Result<RawBytes>;

    /// Encodes the value, **panicking** on failure.
    ///
    /// See [`try_encode`](Self::try_encode) for the fallible variant.
    fn encode(&self) -> RawBytes {
        self.try_encode().unwrap()
    }

    /// Decodes a value from a byte slice.
    ///
    /// Returns `Err` when the bytes are invalid.  VSDB collections use
    /// assert-style unwraps on this internally for data they wrote
    /// themselves.
    fn decode(bytes: &[u8]) -> Result<Self>;
}

impl<T: Serialize> KeyEn for T {
    fn try_encode_key(&self) -> Result<RawBytes> {
        Ok(postcard::to_allocvec(self)?)
    }
}

impl<T: DeserializeOwned> KeyDe for T {
    fn decode_key(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

impl<T: Serialize> ValueEn for T {
    fn try_encode_value(&self) -> Result<RawBytes> {
        Ok(postcard::to_allocvec(self)?)
    }
}

impl<T: DeserializeOwned> ValueDe for T {
    fn decode_value(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

impl<T: KeyEn + KeyDe> KeyEnDe for T {
    fn try_encode(&self) -> Result<RawBytes> {
        <Self as KeyEn>::try_encode_key(self)
    }

    fn encode(&self) -> RawBytes {
        <Self as KeyEn>::encode_key(self)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        <Self as KeyDe>::decode_key(bytes)
    }
}

impl<T: ValueEn + ValueDe> ValueEnDe for T {
    fn try_encode(&self) -> Result<RawBytes> {
        <Self as ValueEn>::try_encode_value(self)
    }

    fn encode(&self) -> RawBytes {
        <Self as ValueEn>::encode_value(self)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        <Self as ValueDe>::decode_value(bytes)
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// A trait for keys that maintain their order when serialized.
///
/// This trait is crucial for ordered data structures like `MapxOrd`, ensuring that
/// operations like range queries work correctly.
///
/// All built-in implementations (`u32`, `i64`, `String`, etc.) also
/// implement [`KeyEnDe`] (via serde blanket), so they can be used
/// as keys in both [`MapxOrd`](crate::MapxOrd) and [`Mapx`](crate::Mapx).
///
/// **Note**: `usize` and `isize` use platform-dependent encoding widths
/// (4 bytes on 32-bit, 8 bytes on 64-bit). Databases written with these
/// key types are not portable across platforms with different pointer sizes.
pub trait KeyEnDeOrdered: Clone + Eq + Ord + fmt::Debug {
    /// Encodes the key into a byte vector.
    fn to_bytes(&self) -> RawBytes;

    /// Consumes the key and encodes it into a byte vector.
    fn into_bytes(self) -> RawBytes {
        self.to_bytes()
    }

    /// Decodes a key from a byte slice.
    fn from_slice(b: &[u8]) -> Result<Self>;

    /// Consumes a byte vector and decodes it into a key.
    fn from_bytes(b: RawBytes) -> Result<Self> {
        Self::from_slice(&b)
    }
}

impl KeyEnDeOrdered for RawBytes {
    #[inline(always)]
    fn to_bytes(&self) -> RawBytes {
        self.clone()
    }

    #[inline(always)]
    fn into_bytes(self) -> RawBytes {
        self
    }

    #[inline(always)]
    fn from_slice(b: &[u8]) -> Result<Self> {
        Ok(b.to_vec())
    }

    #[inline(always)]
    fn from_bytes(b: RawBytes) -> Result<Self> {
        Ok(b)
    }
}

impl KeyEnDeOrdered for Box<[u8]> {
    #[inline(always)]
    fn to_bytes(&self) -> RawBytes {
        self.to_vec()
    }

    #[inline(always)]
    fn into_bytes(self) -> RawBytes {
        self.into_vec()
    }

    #[inline(always)]
    fn from_slice(b: &[u8]) -> Result<Self> {
        Ok(b.to_vec().into())
    }

    #[inline(always)]
    fn from_bytes(b: RawBytes) -> Result<Self> {
        Ok(b.into())
    }
}

impl KeyEnDeOrdered for String {
    #[inline(always)]
    fn to_bytes(&self) -> RawBytes {
        self.as_bytes().to_vec()
    }

    #[inline(always)]
    fn into_bytes(self) -> RawBytes {
        // Qualified call: the inherent `String::into_bytes` is intended
        // here, not this trait method (which would recurse).
        String::into_bytes(self)
    }

    #[inline(always)]
    fn from_slice(b: &[u8]) -> Result<Self> {
        String::from_utf8(b.to_owned()).map_err(|e| VsdbError::Decode {
            detail: e.to_string(),
        })
    }

    #[inline(always)]
    fn from_bytes(b: RawBytes) -> Result<Self> {
        String::from_utf8(b).map_err(|e| VsdbError::Decode {
            detail: e.to_string(),
        })
    }
}

macro_rules! impl_type {
    ($int: ty) => {
        impl KeyEnDeOrdered for $int {
            #[inline(always)]
            fn to_bytes(&self) -> RawBytes {
                self.wrapping_sub(<$int>::MIN).to_be_bytes().to_vec()
            }
            #[inline(always)]
            fn from_slice(b: &[u8]) -> Result<Self> {
                <[u8; size_of::<$int>()]>::try_from(b)
                    .map_err(|e| VsdbError::Decode {
                        detail: e.to_string(),
                    })
                    .map(|bytes| <$int>::from_be_bytes(bytes).wrapping_add(<$int>::MIN))
            }
        }
    };
    (@$int: ty) => {
        impl KeyEnDeOrdered for Vec<$int> {
            #[inline(always)]
            fn to_bytes(&self) -> RawBytes {
                self.iter()
                    .map(|i| i.wrapping_sub(<$int>::MIN).to_be_bytes())
                    .flatten()
                    .collect::<Vec<_>>()
            }
            #[inline(always)]
            fn from_slice(b: &[u8]) -> Result<Self> {
                if 0 != b.len() % size_of::<$int>() {
                    return Err(VsdbError::Decode {
                        detail: "invalid byte length".to_owned(),
                    });
                }
                b.chunks(size_of::<$int>())
                    .map(|i| {
                        <[u8; size_of::<$int>()]>::try_from(i)
                            .map_err(|e| VsdbError::Decode {
                                detail: e.to_string(),
                            })
                            .map(|bytes| {
                                <$int>::from_be_bytes(bytes).wrapping_add(<$int>::MIN)
                            })
                    })
                    .collect()
            }
        }
    };
    (^$int: ty) => {
        impl KeyEnDeOrdered for Box<[$int]> {
            #[inline(always)]
            fn to_bytes(&self) -> RawBytes {
                KeyEnDeOrdered::to_bytes(&self.to_vec())
            }
            #[inline(always)]
            fn from_slice(b: &[u8]) -> Result<Self> {
                <Vec<$int> as KeyEnDeOrdered>::from_slice(b).map(|b| b.into())
            }
        }
    };
    ($int: ty, $siz: expr) => {
        impl KeyEnDeOrdered for [$int; $siz] {
            #[inline(always)]
            fn to_bytes(&self) -> RawBytes {
                self.iter()
                    .map(|i| i.wrapping_sub(<$int>::MIN).to_be_bytes())
                    .flatten()
                    .collect::<Vec<_>>()
            }
            #[inline(always)]
            fn from_slice(b: &[u8]) -> Result<Self> {
                if 0 != b.len() % size_of::<$int>() {
                    return Err(VsdbError::Decode {
                        detail: "invalid byte length".to_owned(),
                    });
                }
                if $siz != b.len() / size_of::<$int>() {
                    return Err(VsdbError::Decode {
                        detail: "invalid element count".to_owned(),
                    });
                }
                let mut res = [0; $siz];
                b.chunks(size_of::<$int>())
                    .enumerate()
                    .for_each(|(idx, i)| {
                        res[idx] = <[u8; size_of::<$int>()]>::try_from(i)
                            .map(|bytes| {
                                <$int>::from_be_bytes(bytes).wrapping_add(<$int>::MIN)
                            })
                            .unwrap();
                    });
                Ok(res)
            }
        }
    };
}

macro_rules! impl_all {
    ($t: ty) => {
        impl_type!($t);
    };
    ($t: ty, $($tt: ty),+) => {
        impl_all!($t);
        impl_all!($($tt), +);
    };
    (@$t: ty) => {
        impl_type!(@$t);
    };
    (@$t: ty, $(@$tt: ty),+) => {
        impl_all!(@$t);
        impl_all!($(@$tt), +);
    };
    (^$t: ty) => {
        impl_type!(^$t);
    };
    (^$t: ty, $(^$tt: ty),+) => {
        impl_all!(^$t);
        impl_all!($(^$tt), +);
    };
}

impl_all!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize
);
impl_all!(
    @i8, @i16, @i32, @i64, @i128, @isize, @u16, @u32, @u64, @u128, @usize
);
impl_all!(
    ^i8, ^i16, ^i32, ^i64, ^i128, ^isize, ^u16, ^u32, ^u64, ^u128, ^usize
);

macro_rules! impl_array {
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
        impl_array!($i);
        impl_array!($($ii), +);
    };
}

// Sizes 1-32: serde provides Serialize/Deserialize, so the blanket
// impl gives them KeyEnDe automatically.
impl_array!(
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
    24, 25, 26, 27, 28, 29, 30, 31, 32
);

impl_array!(
    33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53,
    54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74,
    75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95,
    96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112,
    113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128
);

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
