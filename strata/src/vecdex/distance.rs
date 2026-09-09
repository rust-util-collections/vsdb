//! Scalar types and distance metrics for vector similarity search.

use serde::{Deserialize, Serialize, de};
use std::{
    cmp::Ordering,
    iter::Sum,
    ops::{Add, Div, Mul, Neg, Sub},
};

/// A floating-point scalar suitable for vector components.
///
/// Implemented for `f32` and `f64`.
pub trait Scalar:
    Copy
    + Default
    + PartialOrd
    + Add<Output = Self>
    + Sub<Output = Self>
    + Mul<Output = Self>
    + Div<Output = Self>
    + Neg<Output = Self>
    + Sum
    + Serialize
    + de::DeserializeOwned
    + Send
    + Sync
    + 'static
{
    fn sqrt(self) -> Self;
    fn epsilon() -> Self;
    fn one() -> Self;
    fn zero() -> Self;
    fn total_cmp(&self, other: &Self) -> Ordering;
}

impl Scalar for f32 {
    #[inline]
    fn sqrt(self) -> Self {
        f32::sqrt(self)
    }
    #[inline]
    fn epsilon() -> Self {
        f32::EPSILON
    }
    #[inline]
    fn one() -> Self {
        1.0
    }
    #[inline]
    fn zero() -> Self {
        0.0
    }
    #[inline]
    fn total_cmp(&self, other: &Self) -> Ordering {
        f32::total_cmp(self, other)
    }
}

impl Scalar for f64 {
    #[inline]
    fn sqrt(self) -> Self {
        f64::sqrt(self)
    }
    #[inline]
    fn epsilon() -> Self {
        f64::EPSILON
    }
    #[inline]
    fn one() -> Self {
        1.0
    }
    #[inline]
    fn zero() -> Self {
        0.0
    }
    #[inline]
    fn total_cmp(&self, other: &Self) -> Ordering {
        f64::total_cmp(self, other)
    }
}

/// Trait for computing distance between two vectors.
///
/// Lower distance means more similar.  Generic over [`Scalar`] so the
/// same metric works for both `f32` and `f64` vectors.
pub trait DistanceMetric<S: Scalar = f32>:
    Clone + Default + Serialize + de::DeserializeOwned + Send + Sync + 'static
{
    /// Compute the distance between two vectors of equal length.
    fn distance(a: &[S], b: &[S]) -> S;
}

/// A runtime-selected distance metric — the value-level counterpart of
/// the compile-time [`DistanceMetric`] types, consumed by
/// [`VecDexDyn`](super::VecDexDyn).
///
/// Selecting through `MetricKind` costs one enum dispatch per public
/// operation; the distance loops themselves stay the statically
/// monomorphized implementations of [`L2`], [`Cosine`], and
/// [`InnerProduct`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MetricKind {
    /// Euclidean (L2) squared distance — [`L2`].
    L2,
    /// Cosine distance — [`Cosine`].
    Cosine,
    /// Negated inner product — [`InnerProduct`].
    InnerProduct,
}

/// Euclidean (L2) squared distance.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct L2;

impl<S: Scalar> DistanceMetric<S> for L2 {
    #[inline]
    fn distance(a: &[S], b: &[S]) -> S {
        debug_assert_eq!(a.len(), b.len());
        let (mut s0, mut s1, mut s2, mut s3) =
            (S::zero(), S::zero(), S::zero(), S::zero());
        let chunks = a.len() / 4;
        for i in 0..chunks {
            let j = i * 4;
            let d0 = a[j] - b[j];
            let d1 = a[j + 1] - b[j + 1];
            let d2 = a[j + 2] - b[j + 2];
            let d3 = a[j + 3] - b[j + 3];
            s0 = s0 + d0 * d0;
            s1 = s1 + d1 * d1;
            s2 = s2 + d2 * d2;
            s3 = s3 + d3 * d3;
        }
        let mut sum = s0 + s1 + s2 + s3;
        for i in (chunks * 4)..a.len() {
            let d = a[i] - b[i];
            sum = sum + d * d;
        }
        sum
    }
}

/// Cosine distance: `1.0 - cosine_similarity`.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Cosine;

impl<S: Scalar> DistanceMetric<S> for Cosine {
    #[inline]
    fn distance(a: &[S], b: &[S]) -> S {
        debug_assert_eq!(a.len(), b.len());
        let (mut d0, mut d1, mut d2, mut d3) =
            (S::zero(), S::zero(), S::zero(), S::zero());
        let (mut a0, mut a1, mut a2, mut a3) =
            (S::zero(), S::zero(), S::zero(), S::zero());
        let (mut b0, mut b1, mut b2, mut b3) =
            (S::zero(), S::zero(), S::zero(), S::zero());
        let chunks = a.len() / 4;
        for i in 0..chunks {
            let j = i * 4;
            let (x0, y0) = (a[j], b[j]);
            let (x1, y1) = (a[j + 1], b[j + 1]);
            let (x2, y2) = (a[j + 2], b[j + 2]);
            let (x3, y3) = (a[j + 3], b[j + 3]);
            d0 = d0 + x0 * y0;
            d1 = d1 + x1 * y1;
            d2 = d2 + x2 * y2;
            d3 = d3 + x3 * y3;
            a0 = a0 + x0 * x0;
            a1 = a1 + x1 * x1;
            a2 = a2 + x2 * x2;
            a3 = a3 + x3 * x3;
            b0 = b0 + y0 * y0;
            b1 = b1 + y1 * y1;
            b2 = b2 + y2 * y2;
            b3 = b3 + y3 * y3;
        }
        let (mut dot, mut na, mut nb) =
            (d0 + d1 + d2 + d3, a0 + a1 + a2 + a3, b0 + b1 + b2 + b3);
        for i in (chunks * 4)..a.len() {
            dot = dot + a[i] * b[i];
            na = na + a[i] * a[i];
            nb = nb + b[i] * b[i];
        }
        // Preserve the existing fast path for ordinary squared norms.
        // For f32/f64 these conservative bounds stay far from underflow
        // and overflow, including the final product of square roots.
        // Epsilon is already part of Scalar; no new trait methods or
        // scalar-width assumptions are needed by the fallback below.
        let norm_min = S::epsilon() * S::epsilon();
        let norm_max = S::one() / norm_min;
        if (na < norm_min || nb < norm_min || na > norm_max || nb > norm_max)
            && let Some(distance) = scaled_cosine_distance(a, b)
        {
            return distance;
        }

        let denom = na.sqrt() * nb.sqrt();
        if denom == S::zero() {
            S::one()
        } else {
            S::one() - dot / denom
        }
    }
}

/// Computes cosine after independently rescaling both finite vectors.
///
/// Every component then has magnitude <= 1 and each nonzero vector has
/// at least one component of magnitude 1. The dot product and squared
/// norms therefore remain representable for any in-memory f32/f64 slice,
/// even when the original squares overflowed or underflowed.
///
/// Nonfinite inputs return None so the caller retains the existing
/// arithmetic behavior for them instead of defining a new input policy.
fn scaled_cosine_distance<S: Scalar>(a: &[S], b: &[S]) -> Option<S> {
    let zero = S::zero();
    let (mut scale_a, mut scale_b) = (zero, zero);
    for (&x, &y) in a.iter().zip(b) {
        // Finite scalars multiplied by zero produce signed zero; NaN
        // and either infinity do not. Scalar has no is_finite/abs API.
        if x * zero != zero || y * zero != zero {
            return None;
        }
        let abs_x = if x < zero { -x } else { x };
        let abs_y = if y < zero { -y } else { y };
        if abs_x > scale_a {
            scale_a = abs_x;
        }
        if abs_y > scale_b {
            scale_b = abs_y;
        }
    }
    if scale_a == zero || scale_b == zero {
        return Some(S::one());
    }

    let (mut dot, mut norm_a, mut norm_b) = (zero, zero, zero);
    for (&x, &y) in a.iter().zip(b) {
        let x = x / scale_a;
        let y = y / scale_b;
        dot = dot + x * y;
        norm_a = norm_a + x * x;
        norm_b = norm_b + y * y;
    }
    Some(S::one() - dot / (norm_a.sqrt() * norm_b.sqrt()))
}

/// Inner product distance: `-(a . b)`.
///
/// Negated so that higher inner product = lower distance.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct InnerProduct;

impl<S: Scalar> DistanceMetric<S> for InnerProduct {
    #[inline]
    fn distance(a: &[S], b: &[S]) -> S {
        debug_assert_eq!(a.len(), b.len());
        let (mut s0, mut s1, mut s2, mut s3) =
            (S::zero(), S::zero(), S::zero(), S::zero());
        let chunks = a.len() / 4;
        for i in 0..chunks {
            let j = i * 4;
            s0 = s0 + a[j] * b[j];
            s1 = s1 + a[j + 1] * b[j + 1];
            s2 = s2 + a[j + 2] * b[j + 2];
            s3 = s3 + a[j + 3] * b[j + 3];
        }
        let mut sum = s0 + s1 + s2 + s3;
        for i in (chunks * 4)..a.len() {
            sum = sum + a[i] * b[i];
        }
        -sum
    }
}
