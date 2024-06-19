//!
//! # ende
//!
//! (en)Encode and (de)Decode
//!

use super::RawBytes;
use ruc::*;
use std::{
    fmt,
    mem::{size_of, transmute},
};

#[cfg(feature = "serde_ende")]
use serde::{de::DeserializeOwned, Serialize};

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// Methods used to encode the KEY.
pub trait KeyEn: Sized {
    /// Encode original key type to bytes.
    fn try_encode_key(&self) -> Result<RawBytes>;

    fn encode_key(&self) -> RawBytes {
        pnk!(self.try_encode_key())
    }
}

/// Methods used to decode the KEY.
pub trait KeyDe: Sized {
    /// Decode from bytes to the original key type.
    fn decode_key(bytes: &[u8]) -> Result<Self>;
}

/// Methods used to encode and decode the KEY.
pub trait KeyEnDe: Sized {
    /// Encode original key type to bytes.
    fn try_encode(&self) -> Result<RawBytes>;

    fn encode(&self) -> RawBytes {
        pnk!(self.try_encode())
    }

    /// Decode from bytes to the original key type.
    fn decode(bytes: &[u8]) -> Result<Self>;
}

/// Methods used to encode the VALUE.
pub trait ValueEn: Sized {
    /// Encode original key type to bytes.
    fn try_encode_value(&self) -> Result<RawBytes>;

    fn encode_value(&self) -> RawBytes {
        pnk!(self.try_encode_value())
    }
}

/// Methods used to decode the VALUE.
pub trait ValueDe: Sized {
    /// Decode from bytes to the original key type.
    fn decode_value(bytes: &[u8]) -> Result<Self>;
}

/// Methods used to encode and decode the VALUE.
pub trait ValueEnDe: Sized {
    /// Encode original key type to bytes.
    fn try_encode(&self) -> Result<RawBytes>;

    fn encode(&self) -> RawBytes {
        pnk!(self.try_encode())
    }

    /// Decode from bytes to the original key type.
    fn decode(bytes: &[u8]) -> Result<Self>;
}

#[cfg(feature = "serde_ende")]
impl<T: Serialize> KeyEn for T {
    #[cfg(feature = "json_codec")]
    fn try_encode_key(&self) -> Result<RawBytes> {
        serde_json::to_vec(self).c(d!())
    }

    #[cfg(feature = "bcs_codec")]
    fn try_encode_key(&self) -> Result<RawBytes> {
        bcs::to_bytes(self).c(d!())
    }

    #[cfg(feature = "msgpack_codec")]
    fn try_encode_key(&self) -> Result<RawBytes> {
        msgpack::to_vec(self).c(d!())
    }
}

#[cfg(feature = "serde_ende")]
impl<T: DeserializeOwned> KeyDe for T {
    #[cfg(feature = "json_codec")]
    fn decode_key(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).c(d!())
    }

    #[cfg(feature = "bcs_codec")]
    fn decode_key(bytes: &[u8]) -> Result<Self> {
        bcs::from_bytes(bytes).c(d!())
    }

    #[cfg(feature = "msgpack_codec")]
    fn decode_key(bytes: &[u8]) -> Result<Self> {
        msgpack::from_slice(bytes).c(d!())
    }
}

#[cfg(feature = "serde_ende")]
impl<T: Serialize> ValueEn for T {
    #[cfg(feature = "json_codec")]
    fn try_encode_value(&self) -> Result<RawBytes> {
        serde_json::to_vec(self).c(d!())
    }

    #[cfg(feature = "bcs_codec")]
    fn try_encode_value(&self) -> Result<RawBytes> {
        bcs::to_bytes(self).c(d!())
    }

    #[cfg(feature = "msgpack_codec")]
    fn try_encode_value(&self) -> Result<RawBytes> {
        msgpack::to_vec(self).c(d!())
    }
}

#[cfg(feature = "serde_ende")]
impl<T: DeserializeOwned> ValueDe for T {
    #[cfg(feature = "json_codec")]
    fn decode_value(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).c(d!())
    }

    #[cfg(feature = "bcs_codec")]
    fn decode_value(bytes: &[u8]) -> Result<Self> {
        bcs::from_bytes(bytes).c(d!())
    }

    #[cfg(feature = "msgpack_codec")]
    fn decode_value(bytes: &[u8]) -> Result<Self> {
        msgpack::from_slice(bytes).c(d!())
    }
}

impl<T: KeyEn + KeyDe> KeyEnDe for T {
    fn try_encode(&self) -> Result<RawBytes> {
        <Self as KeyEn>::try_encode_key(self).c(d!())
    }

    fn encode(&self) -> RawBytes {
        <Self as KeyEn>::encode_key(self)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        <Self as KeyDe>::decode_key(bytes).c(d!())
    }
}

impl<T: ValueEn + ValueDe> ValueEnDe for T {
    fn try_encode(&self) -> Result<RawBytes> {
        <Self as ValueEn>::try_encode_value(self).c(d!())
    }

    fn encode(&self) -> RawBytes {
        <Self as ValueEn>::encode_value(self)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        <Self as ValueDe>::decode_value(bytes).c(d!())
    }
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

#[cfg(not(feature = "serde_ende"))]
impl<T: KeyEnDeOrdered> KeyEn for T {
    fn try_encode_key(&self) -> Result<RawBytes> {
        Ok(self.encode_key())
    }

    fn encode_key(&self) -> RawBytes {
        <T as KeyEnDeOrdered>::to_bytes(self)
    }
}

#[cfg(not(feature = "serde_ende"))]
impl<T: KeyEnDeOrdered> KeyDe for T {
    fn decode_key(bytes: &[u8]) -> Result<Self> {
        <T as KeyEnDeOrdered>::from_slice(bytes).c(d!())
    }
}

macro_rules! impl_v_ende {
    ($t: ty) => {
        #[cfg(not(feature = "serde_ende"))]
        impl ValueEnDe for $t {
            fn try_encode(&self) -> Result<RawBytes> {
                Ok(self.encode())
            }
            fn encode(&self) -> RawBytes {
                self.as_bytes().into()
            }
            fn decode(bytes: &[u8]) -> Result<Self> {
                unsafe { Ok(<$t>::from_bytes(bytes)) }
            }
        }
    };
    (@$t: ty) => {
        #[cfg(not(feature = "serde_ende"))]
        impl<K: KeyEnDeOrdered> ValueEnDe for $t {
            fn try_encode(&self) -> Result<RawBytes> {
                Ok(self.encode())
            }
            fn encode(&self) -> RawBytes {
                self.as_bytes().into()
            }
            fn decode(bytes: &[u8]) -> Result<Self> {
                unsafe { Ok(<$t>::from_bytes(bytes)) }
            }
        }
    };
    (^$t: ty) => {
        #[cfg(not(feature = "serde_ende"))]
        impl<V: ValueEnDe> ValueEnDe for $t {
            fn try_encode(&self) -> Result<RawBytes> {
                Ok(self.encode())
            }
            fn encode(&self) -> RawBytes {
                self.as_bytes().into()
            }
            fn decode(bytes: &[u8]) -> Result<Self> {
                unsafe { Ok(<$t>::from_bytes(bytes)) }
            }
        }
    };
    (~$t: ty) => {
        #[cfg(not(feature = "serde_ende"))]
        impl<K: KeyEnDeOrdered, V: ValueEnDe> ValueEnDe for $t {
            fn try_encode(&self) -> Result<RawBytes> {
                Ok(self.encode())
            }
            fn encode(&self) -> RawBytes {
                self.as_bytes().into()
            }
            fn decode(bytes: &[u8]) -> Result<Self> {
                unsafe { Ok(<$t>::from_bytes(bytes)) }
            }
        }
    };
}

impl_v_ende!(vsdb_core::MapxRaw);
impl_v_ende!(crate::basic::vecx_raw::VecxRaw);
impl_v_ende!(~crate::basic::mapx::Mapx<K, V>);
impl_v_ende!(~crate::basic::mapx_ord::MapxOrd<K, V>);
impl_v_ende!(^crate::basic::vecx::Vecx<V>);
impl_v_ende!(^crate::basic::orphan::Orphan<V>);
impl_v_ende!(^crate::basic::mapx_ord_rawkey::MapxOrdRawKey<V>);
impl_v_ende!(@crate::basic::mapx_ord_rawvalue::MapxOrdRawValue<K>);

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////

/// For keys that their serialized order keep consistent with their original format.
/// When using this kind of keys, we can do some ordered operations, such as: `get_le/get_be ...`
pub trait KeyEnDeOrdered: Clone + Eq + Ord + fmt::Debug {
    /// &key => bytes
    fn to_bytes(&self) -> RawBytes;

    /// key => bytes
    fn into_bytes(self) -> RawBytes {
        self.to_bytes()
    }

    /// &bytes => key
    fn from_slice(b: &[u8]) -> Result<Self>;

    /// bytes => key
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
        Ok(b.to_vec())
    }
}

impl KeyEnDeOrdered for Box<[u8]> {
    #[inline(always)]
    fn to_bytes(&self) -> RawBytes {
        self.clone().to_vec()
    }

    #[inline(always)]
    fn into_bytes(self) -> RawBytes {
        self.to_vec()
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
        self.into_bytes()
    }

    #[inline(always)]
    fn from_slice(b: &[u8]) -> Result<Self> {
        String::from_utf8(b.to_owned()).c(d!())
    }

    #[inline(always)]
    fn from_bytes(b: RawBytes) -> Result<Self> {
        String::from_utf8(b).c(d!())
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
                    .c(d!())
                    .map(|bytes| <$int>::from_be_bytes(bytes).wrapping_add(<$int>::MIN))
            }
        }
    };
    (@$int: ty) => {
        #[allow(clippy::unsound_collection_transmute)]
        impl KeyEnDeOrdered for Vec<$int> {
            #[inline(always)]
            fn to_bytes(&self) -> RawBytes {
                self.iter()
                    .map(|i| i.wrapping_sub(<$int>::MIN).to_be_bytes())
                    .flatten()
                    .collect::<Vec<_>>()
            }
            #[inline(always)]
            fn into_bytes(mut self) -> RawBytes {
                for i in 0..self.len() {
                    self[i] = self[i].wrapping_sub(<$int>::MIN).to_be();
                }
                unsafe {
                    let v = transmute::<Vec<$int>, RawBytes>(self);
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
                        <[u8; size_of::<$int>()]>::try_from(i).c(d!()).map(|bytes| {
                            <$int>::from_be_bytes(bytes).wrapping_add(<$int>::MIN)
                        })
                    })
                    .collect()
            }
            #[inline(always)]
            fn from_bytes(b: RawBytes) -> Result<Self> {
                if 0 != b.len() % size_of::<$int>() {
                    return Err(eg!("invalid bytes"));
                }
                let mut ret = unsafe {
                    let mut v = transmute::<Vec<u8>, Vec<$int>>(b);
                    v.set_len(v.len() / size_of::<$int>());
                    v
                };
                for i in 0..ret.len() {
                    ret[i] = <$int>::from_be(ret[i]).wrapping_add(<$int>::MIN);
                }
                Ok(ret)
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
            fn into_bytes(self) -> RawBytes {
                KeyEnDeOrdered::into_bytes(self.to_vec())
            }
            #[inline(always)]
            fn from_slice(b: &[u8]) -> Result<Self> {
                <Vec<$int> as KeyEnDeOrdered>::from_slice(b).map(|b| b.into())
            }
            #[inline(always)]
            fn from_bytes(b: RawBytes) -> Result<Self> {
                <Vec<$int> as KeyEnDeOrdered>::from_bytes(b).map(|b| b.into())
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
                            .map(|bytes| {
                                <$int>::from_be_bytes(bytes).wrapping_add(<$int>::MIN)
                            })
                            .unwrap();
                    });
                Ok(res)
            }
        }
    };
    (%$hash: ty) => {
        impl KeyEnDeOrdered for $hash {
            #[inline(always)]
            fn to_bytes(&self) -> RawBytes {
                self.as_bytes().to_vec()
            }
            #[inline(always)]
            fn from_slice(b: &[u8]) -> Result<Self> {
                if b.len() != <$hash>::len_bytes() {
                    return Err(eg!("length mismatch"));
                }
                Ok(<$hash>::from_slice(b))
            }
        }
    };
    (~$big_uint: ty) => {
        impl KeyEnDeOrdered for $big_uint {
            #[inline(always)]
            fn to_bytes(&self) -> RawBytes {
                let mut r = vec![];
                self.to_big_endian(&mut r);
                r
            }
            #[inline(always)]
            fn from_slice(b: &[u8]) -> Result<Self> {
                Ok(<$big_uint>::from_big_endian(b))
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
    (%$t: ty) => {
        impl_type!(%$t);
    };
    (%$t: ty, $(%$tt: ty),+) => {
        impl_all!(%$t);
        impl_all!($(%$tt), +);
    };
    (~$t: ty) => {
        impl_type!(~$t);
    };
    (~$t: ty, $(~$tt: ty),+) => {
        impl_all!(~$t);
        impl_all!($(~$tt), +);
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

#[cfg(feature = "extra_types")]
impl_all!(
    %vsdb_core::primitive_types_0_10::H128,
    %vsdb_core::primitive_types_0_10::H160,
    %vsdb_core::primitive_types_0_10::H256,
    %vsdb_core::primitive_types_0_10::H512,
    %vsdb_core::primitive_types_0_11::H128,
    %vsdb_core::primitive_types_0_11::H160,
    %vsdb_core::primitive_types_0_11::H256,
    %vsdb_core::primitive_types_0_11::H512,
    %vsdb_core::primitive_types_0_12::H128,
    %vsdb_core::primitive_types_0_12::H160,
    %vsdb_core::primitive_types_0_12::H256,
    %vsdb_core::primitive_types_0_12::H512
);

#[cfg(feature = "extra_types")]
impl_all!(
    ~vsdb_core::primitive_types_0_10::U128,
    ~vsdb_core::primitive_types_0_10::U256,
    ~vsdb_core::primitive_types_0_10::U512,
    ~vsdb_core::primitive_types_0_11::U128,
    ~vsdb_core::primitive_types_0_11::U256,
    ~vsdb_core::primitive_types_0_11::U512,
    ~vsdb_core::primitive_types_0_12::U128,
    ~vsdb_core::primitive_types_0_12::U256,
    ~vsdb_core::primitive_types_0_12::U512
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

impl_array!(
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
    24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44,
    45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65,
    66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86,
    87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105,
    106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122,
    123, 124, 125, 126, 127, 128
);

/////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////
