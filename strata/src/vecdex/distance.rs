//! Scalar types and distance metrics for vector similarity search.

use serde::{Deserialize, Serialize, de};
use std::iter::Sum;
use std::ops::{Add, Div, Mul, Neg, Sub};

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
    fn total_cmp(&self, other: &Self) -> std::cmp::Ordering;
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
    fn total_cmp(&self, other: &Self) -> std::cmp::Ordering {
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
    fn total_cmp(&self, other: &Self) -> std::cmp::Ordering {
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

/// Euclidean (L2) squared distance.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct L2;

impl<S: Scalar> DistanceMetric<S> for L2 {
    #[inline]
    fn distance(a: &[S], b: &[S]) -> S {
        debug_assert_eq!(a.len(), b.len());
        a.iter()
            .zip(b)
            .map(|(x, y)| {
                let d = *x - *y;
                d * d
            })
            .sum()
    }
}

/// Cosine distance: `1.0 - cosine_similarity`.
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Cosine;

impl<S: Scalar> DistanceMetric<S> for Cosine {
    #[inline]
    fn distance(a: &[S], b: &[S]) -> S {
        debug_assert_eq!(a.len(), b.len());
        let (mut dot, mut na, mut nb) = (S::zero(), S::zero(), S::zero());
        for (x, y) in a.iter().zip(b) {
            dot = dot + *x * *y;
            na = na + *x * *x;
            nb = nb + *y * *y;
        }
        let denom = na.sqrt() * nb.sqrt();
        if denom < S::epsilon() {
            S::one()
        } else {
            S::one() - dot / denom
        }
    }
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
        -(a.iter().zip(b).map(|(x, y)| *x * *y).sum::<S>())
    }
}
