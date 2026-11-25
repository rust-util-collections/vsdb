//! Key and value codecs used by the persistent collections.

use super::{
    RawBytes,
    error::{Result, VsdbError},
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    fmt,
    hash::Hash,
    ops::{
        Bound, Range, RangeBounds, RangeFrom, RangeFull, RangeInclusive, RangeTo,
        RangeToInclusive,
    },
};

/// An equality key must encode canonically: two keys compare equal exactly
/// when their encodings are identical. The serde blanket requires `Eq + Hash`.
///
/// ```compile_fail
/// let m = vsdb::Mapx::<f64, String>::new();
/// ```
/// ```compile_fail
/// let m = vsdb::Mapx::<std::collections::HashMap<u32, u32>, String>::new();
/// ```
pub trait KeyEnDe: Eq + Sized {
    /// Appends the key encoding to a reusable destination.
    fn encode_key_into(&self, dst: &mut Vec<u8>) -> Result<()>;
    fn encode_key(&self) -> RawBytes {
        let mut dst = Vec::new();
        self.encode_key_into(&mut dst).expect("vsdb: encode key");
        dst
    }
    fn with_encoded_key<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        f(&self.encode_key())
    }
    fn decode_key(bytes: &[u8]) -> Result<Self>;
}

/// Codec for persisted values. Encoding into a caller-supplied buffer is fallible.
pub trait ValueEnDe: Sized {
    fn encode_value_into(&self, dst: &mut Vec<u8>) -> Result<()>;
    fn encode_value(&self) -> RawBytes {
        let mut dst = Vec::new();
        self.encode_value_into(&mut dst)
            .expect("vsdb: encode value");
        dst
    }
    fn decode_value(bytes: &[u8]) -> Result<Self>;
}

fn postcard_key<T: Serialize + ?Sized, R>(key: &T, f: impl FnOnce(&[u8]) -> R) -> R {
    let mut buf = [0; 128];
    match postcard::to_slice(key, &mut buf) {
        Ok(bytes) => f(bytes),
        Err(postcard::Error::SerializeBufferFull) => {
            f(&postcard::to_allocvec(key).expect("vsdb: encode key"))
        }
        Err(e) => panic!("vsdb: encode key: {e}"),
    }
}
impl<T: Serialize + DeserializeOwned + Eq + Hash> KeyEnDe for T {
    fn encode_key_into(&self, dst: &mut Vec<u8>) -> Result<()> {
        postcard::to_io(self, dst)?;
        Ok(())
    }
    fn with_encoded_key<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        postcard_key(self, f)
    }
    fn decode_key(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }
}
impl<T: Serialize + DeserializeOwned> ValueEnDe for T {
    fn encode_value_into(&self, dst: &mut Vec<u8>) -> Result<()> {
        postcard::to_io(self, dst)?;
        Ok(())
    }
    fn decode_value(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Resolves an exact query using its owning key's encoding. An invalid
/// fixed-array length returns `None` before calling the storage operation.
pub trait KeyLookup<Q: ?Sized>: KeyEnDe {
    fn with_lookup<R>(query: &Q, f: impl FnOnce(&[u8]) -> R) -> Option<R>;
}
pub trait OrderedKeyLookup<Q: ?Sized>: KeyEnDeOrdered {
    fn with_ordered_lookup<R>(query: &Q, f: impl FnOnce(&[u8]) -> R) -> Option<R>;
}
impl<K: KeyEnDe> KeyLookup<K> for K {
    fn with_lookup<R>(query: &K, f: impl FnOnce(&[u8]) -> R) -> Option<R> {
        Some(query.with_encoded_key(f))
    }
}
impl<K: KeyEnDeOrdered> OrderedKeyLookup<K> for K {
    fn with_ordered_lookup<R>(query: &K, f: impl FnOnce(&[u8]) -> R) -> Option<R> {
        Some(query.with_ordered_bytes(f))
    }
}
macro_rules! borrowed_keys {
    ($owner:ty, $query:ty) => {
        impl KeyLookup<$query> for $owner {
            fn with_lookup<R>(query: &$query, f: impl FnOnce(&[u8]) -> R) -> Option<R> {
                Some(postcard_key(query, f))
            }
        }
        impl OrderedKeyLookup<$query> for $owner {
            fn with_ordered_lookup<R>(
                query: &$query,
                f: impl FnOnce(&[u8]) -> R,
            ) -> Option<R> {
                Some(f(query.as_ref()))
            }
        }
        impl OrderedBoundEndpoint<&$query> for $owner {
            fn with_endpoint<R>(e: &&$query, f: impl FnOnce(&[u8]) -> R) -> R {
                f(e.as_ref())
            }
        }
    };
}
borrowed_keys!(String, str);
borrowed_keys!(Box<str>, str);
borrowed_keys!(Vec<u8>, [u8]);
borrowed_keys!(Box<[u8]>, [u8]);

impl<const N: usize> KeyLookup<[u8]> for [u8; N]
where
    Self: KeyEnDe,
{
    fn with_lookup<R>(q: &[u8], f: impl FnOnce(&[u8]) -> R) -> Option<R> {
        (q.len() == N).then(|| f(q))
    }
}
impl<const N: usize> OrderedKeyLookup<[u8]> for [u8; N] {
    fn with_ordered_lookup<R>(q: &[u8], f: impl FnOnce(&[u8]) -> R) -> Option<R> {
        (q.len() == N).then(|| f(q))
    }
}

/// Ordered key bytes compare lexicographically in the same order as the keys.
/// Persist integers with explicit widths.
///
/// ```compile_fail
/// let map = vsdb::MapxOrd::<usize, u64>::new();
/// ```
/// ```compile_fail
/// let map = vsdb::MapxOrd::<Vec<isize>, u64>::new();
/// ```
pub trait KeyEnDeOrdered: Clone + Eq + Ord + fmt::Debug {
    fn with_ordered_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R;
    fn encode_ordered_into(&self, dst: &mut Vec<u8>) {
        self.with_ordered_bytes(|bytes| dst.extend_from_slice(bytes))
    }
    fn to_bytes(&self) -> RawBytes {
        let mut bytes = Vec::new();
        self.encode_ordered_into(&mut bytes);
        bytes
    }
    fn into_bytes(self) -> RawBytes {
        self.to_bytes()
    }
    fn from_slice(b: &[u8]) -> Result<Self>;
    fn from_bytes(b: RawBytes) -> Result<Self> {
        Self::from_slice(&b)
    }
}

macro_rules! contiguous_key {
    ($ty:ty, $decode:expr, $take:expr) => {
        impl KeyEnDeOrdered for $ty {
            fn with_ordered_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
                f(self.as_ref())
            }
            fn from_slice(b: &[u8]) -> Result<Self> {
                Self::from_bytes(b.to_vec())
            }
            fn from_bytes(b: RawBytes) -> Result<Self> {
                $decode(b)
            }
            fn into_bytes(self) -> RawBytes {
                $take(self)
            }
        }
    };
}
contiguous_key!(Vec<u8>, Ok, std::convert::identity);
contiguous_key!(Box<[u8]>, |v: Vec<u8>| Ok(v.into_boxed_slice()), Vec::from);
fn text(bytes: Vec<u8>) -> Result<String> {
    String::from_utf8(bytes).map_err(|e| VsdbError::Decode {
        detail: e.to_string(),
    })
}
contiguous_key!(String, text, String::into_bytes);
impl KeyEnDeOrdered for Box<str> {
    fn with_ordered_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        f(self.as_bytes())
    }
    fn from_slice(bytes: &[u8]) -> Result<Self> {
        Self::from_bytes(bytes.to_vec())
    }
    fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        text(bytes).map(String::into_boxed_str)
    }
    fn into_bytes(self) -> Vec<u8> {
        String::from(self).into_bytes()
    }
}
impl<const N: usize> KeyEnDeOrdered for [u8; N] {
    fn with_ordered_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        f(self)
    }
    fn from_slice(bytes: &[u8]) -> Result<Self> {
        bytes.try_into().map_err(|_| bad_len())
    }
}
fn bad_len() -> VsdbError {
    VsdbError::Decode {
        detail: "invalid ordered key width".into(),
    }
}

macro_rules! scalar_keys {
    ($($int:ty),+) => {$(
        impl KeyEnDeOrdered for $int {
            fn with_ordered_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
                f(&self.wrapping_sub(Self::MIN).to_be_bytes())
            }
            fn from_slice(bytes: &[u8]) -> Result<Self> {
                let raw = bytes.try_into().map_err(|_| bad_len())?;
                Ok(Self::from_be_bytes(raw).wrapping_add(Self::MIN))
            }
        }
    )+};
}
scalar_keys!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);

fn append_items<I: KeyEnDeOrdered>(items: &[I], dst: &mut Vec<u8>) {
    dst.reserve(std::mem::size_of_val(items));
    for item in items {
        item.encode_ordered_into(dst);
    }
}
fn decode_items<I: KeyEnDeOrdered>(bytes: &[u8]) -> Result<Vec<I>> {
    let chunks = bytes.chunks_exact(size_of::<I>());
    if !chunks.remainder().is_empty() {
        return Err(bad_len());
    }
    chunks.map(I::from_slice).collect()
}
// Scalar implementations above emit exactly size_of::<I>() ordered bytes.
fn with_integer_bytes<I: KeyEnDeOrdered, R>(
    items: &[I],
    f: impl FnOnce(&[u8]) -> R,
) -> R {
    let mut stack = [0u8; 128];
    let len = std::mem::size_of_val(items);
    if len > stack.len() {
        let mut bytes = Vec::with_capacity(len);
        append_items(items, &mut bytes);
        return f(&bytes);
    }
    for (item, out) in items
        .iter()
        .zip(stack[..len].chunks_exact_mut(size_of::<I>()))
    {
        item.with_ordered_bytes(|bytes| out.copy_from_slice(bytes));
    }
    f(&stack[..len])
}
macro_rules! sequence_methods {
    () => {
        fn with_ordered_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
            with_integer_bytes(self, f)
        }
        fn encode_ordered_into(&self, dst: &mut Vec<u8>) {
            append_items(self, dst)
        }
    };
}
macro_rules! integer_sequences {
    ($($int:ty),+) => {$(
        impl KeyEnDeOrdered for Vec<$int> {
            sequence_methods!();
            fn from_slice(bytes: &[u8]) -> Result<Self> {
                decode_items(bytes)
            }
        }
        impl KeyEnDeOrdered for Box<[$int]> {
            sequence_methods!();
            fn from_slice(bytes: &[u8]) -> Result<Self> {
                Ok(decode_items(bytes)?.into_boxed_slice())
            }
        }
        impl<const N: usize> KeyEnDeOrdered for [$int; N] {
            sequence_methods!();
            fn from_slice(bytes: &[u8]) -> Result<Self> {
                if bytes.len() != size_of::<Self>() {
                    return Err(bad_len());
                }
                let mut out = [0; N];
                for (value, bytes) in out.iter_mut().zip(bytes.chunks_exact(size_of::<$int>())) {
                    *value = <$int>::from_slice(bytes)?;
                }
                Ok(out)
            }
        }
    )+};
}
integer_sequences!(u16, u32, u64, u128, i8, i16, i32, i64, i128);

/// Encoding of a valid boundary for an ordered key.
pub trait OrderedBoundEndpoint<E>: KeyEnDeOrdered {
    fn with_endpoint<R>(e: &E, f: impl FnOnce(&[u8]) -> R) -> R;
}
impl<K: KeyEnDeOrdered> OrderedBoundEndpoint<K> for K {
    fn with_endpoint<R>(e: &K, f: impl FnOnce(&[u8]) -> R) -> R {
        e.with_ordered_bytes(f)
    }
}
impl<K: KeyEnDeOrdered> OrderedBoundEndpoint<&K> for K {
    fn with_endpoint<R>(e: &&K, f: impl FnOnce(&[u8]) -> R) -> R {
        e.with_ordered_bytes(f)
    }
}
pub trait OrderedRangeBounds<K> {
    fn ordered_bounds(&self) -> (Bound<Vec<u8>>, Bound<Vec<u8>>);
}
impl<K> OrderedRangeBounds<K> for RangeFull {
    fn ordered_bounds(&self) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
        (Bound::Unbounded, Bound::Unbounded)
    }
}
macro_rules! ordered_ranges {
    ($($range:ty),+) => {$(
        impl<E, K: OrderedBoundEndpoint<E>> OrderedRangeBounds<K> for $range {
            fn ordered_bounds(&self) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
                let encode = |endpoint: &E| K::with_endpoint(endpoint, <[u8]>::to_vec);
                let lo = RangeBounds::<E>::start_bound(self).map(encode);
                let hi = RangeBounds::<E>::end_bound(self).map(encode);
                (lo, hi)
            }
        }
    )+};
}
ordered_ranges!(
    Range<E>,
    RangeInclusive<E>,
    RangeFrom<E>,
    RangeTo<E>,
    RangeToInclusive<E>,
    (Bound<E>, Bound<E>)
);
