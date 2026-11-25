//! Byte range endpoints for raw maps and snapshot views.

use std::ops::{
    Bound, Range, RangeBounds, RangeFrom, RangeFull, RangeInclusive, RangeTo,
    RangeToInclusive,
};

/// Bounds in lexicographic byte order. Accepts owned or borrowed byte
/// strings, including arrays, slices, vectors and `Cow<[u8]>`.
pub trait RawRangeBounds {
    fn raw_bounds(&self) -> (Bound<&[u8]>, Bound<&[u8]>);
}

macro_rules! raw_ranges {
    ($($range:ty),+) => {
        $(impl<E: AsRef<[u8]>> RawRangeBounds for $range {
            fn raw_bounds(&self) -> (Bound<&[u8]>, Bound<&[u8]>) {
                let start = RangeBounds::<E>::start_bound(self).map(AsRef::as_ref);
                let end = RangeBounds::<E>::end_bound(self).map(AsRef::as_ref);
                (start, end)
            }
        })+
    };
}

raw_ranges!(
    Range<E>,
    RangeInclusive<E>,
    RangeFrom<E>,
    RangeTo<E>,
    RangeToInclusive<E>,
    (Bound<E>, Bound<E>)
);

impl RawRangeBounds for RangeFull {
    fn raw_bounds(&self) -> (Bound<&[u8]>, Bound<&[u8]>) {
        (Bound::Unbounded, Bound::Unbounded)
    }
}
