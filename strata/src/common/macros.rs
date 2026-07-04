//! Internal helper macros for the typed collection wrappers.
//!
//! None of these are part of the public API — they are `pub(crate)`
//! re-exports usable only inside the `vsdb` crate.

macro_rules! define_map_wrapper {
    (
        $(#[$struct_doc:meta])*
        $vis:vis struct $wrapper_name:ident <$($wrapper_generics:tt),*> {
            $inner_vis:vis inner: $inner_type:ty,
            $phantom_field:ident: $phantom_type:ty,
        }
        where $($trait_bounds:tt)+
    ) => {
        $(#[$struct_doc])*
        #[derive(PartialEq, Eq, Debug)]
        $vis struct $wrapper_name<$($wrapper_generics),*> {
            $inner_vis inner: $inner_type,
            $phantom_field: $phantom_type,
        }

        impl<$($wrapper_generics),*> serde::Serialize for $wrapper_name<$($wrapper_generics),*> {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                $crate::common::serialize_typed_handle_meta::<Self, S>(&self.inner, serializer)
            }
        }

        impl<'de, $($wrapper_generics),*> serde::Deserialize<'de> for $wrapper_name<$($wrapper_generics),*> {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                $crate::common::deserialize_typed_handle_meta::<Self, $inner_type, D>(deserializer)
                    .map(|inner| Self { inner, $phantom_field: std::marker::PhantomData })
            }
        }

        impl<$($wrapper_generics),*> $wrapper_name<$($wrapper_generics),*>
        where
            $($trait_bounds)+
        {
            /// # Safety
            ///
            /// Creates a second handle to the same underlying storage, bypassing
            /// Rust's aliasing guarantees.  The caller **must** enforce
            /// Single-Writer-Multiple-Readers (SWMR) for the entire lifetime
            /// of the shadow:
            ///
            /// - No `insert`, `remove`, `set_value`, or other mutation may occur
            ///   on the original **or** any other shadow while the shadow exists.
            /// - Multiple concurrent *reads* are permitted.
            /// - All shadows must be dropped before the next write.
            #[inline(always)]
            pub unsafe fn shadow(&self) -> Self {
                Self {
                    // SAFETY: forwards this fn's `unsafe` contract — the
                    // caller guarantees the SWMR discipline (no concurrent
                    // writes through the shadow and the original).
                    inner: unsafe { self.inner.shadow() },
                    $phantom_field: std::marker::PhantomData,
                }
            }

            /// # Safety
            ///
            /// Reconstructs a handle from a raw byte slice that was previously
            /// produced by [`as_bytes`](Self::as_bytes) on a valid instance of
            /// the **same type and code version**.  Passing any other bytes
            /// (corrupted, truncated, from a different type, or from an
            /// incompatible code version) is undefined behavior and may cause
            /// panics or silent data corruption on subsequent operations.
            #[inline(always)]
            pub unsafe fn from_bytes(s: impl AsRef<[u8]>) -> Self {
                Self {
                    // SAFETY: forwards this fn's `unsafe` contract — the
                    // caller guarantees `s` was produced by `as_bytes()` on
                    // the same type and code version.
                    inner: unsafe { <$inner_type>::from_bytes(s) },
                    $phantom_field: std::marker::PhantomData,
                }
            }

            #[inline(always)]
            pub fn as_bytes(&self) -> &[u8] {
                self.inner.as_bytes()
            }

            #[inline(always)]
            pub fn new() -> Self {
                Self {
                    inner: <$inner_type>::new(),
                    $phantom_field: std::marker::PhantomData,
                }
            }

            #[inline(always)]
            pub fn clear(&mut self) {
                self.inner.clear();
            }

            #[inline(always)]
            pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
                self.inner.is_the_same_instance(&other_hdr.inner)
            }

            /// Returns the unique instance ID of this data structure.
            #[inline(always)]
            pub fn instance_id(&self) -> u64 {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(self.as_bytes());
                u64::from_le_bytes(bytes)
            }

            /// Persists this instance's metadata to disk so that it can be
            /// recovered later via [`from_meta`](Self::from_meta).
            ///
            /// Returns the `instance_id` that should be passed to `from_meta`.
            pub fn save_meta(&self) -> $crate::common::error::Result<u64> {
                let id = self.instance_id();
                $crate::common::save_instance_meta(id, self)?;
                Ok(id)
            }

            /// Recovers an instance from previously saved metadata.
            ///
            /// The caller must ensure that the underlying VSDB database still
            /// contains the data referenced by this instance ID.
            ///
            /// # Aliasing warning
            ///
            /// The returned handle is a **full alias** of the original
            /// instance, not an independent copy — it addresses the exact
            /// same underlying key range (this is how
            /// [`instance_id`](Self::instance_id) is recovered: it *is*
            /// the raw prefix).  If the original handle that produced
            /// this `instance_id` (or another `from_meta`/`shadow`
            /// restore of it) is still alive in-process, the same
            /// Single-Writer-Multiple-Readers (SWMR) discipline
            /// documented on [`shadow`](Self::shadow) applies across
            /// **every** live alias: no mutation may occur on any one
            /// of them while any other is in use for writing.
            /// `from_meta` is intended to restore a handle after the
            /// original has gone out of scope (e.g. across a process
            /// restart); calling it while the original is still live
            /// requires the same care as `shadow()`, even though this
            /// function is safe Rust.
            pub fn from_meta(instance_id: u64) -> $crate::common::error::Result<Self> {
                $crate::common::load_instance_meta(instance_id)
            }
        }

        impl<$($wrapper_generics),*> Clone for $wrapper_name<$($wrapper_generics),*>
        {
            fn clone(&self) -> Self {
                Self {
                    inner: self.inner.clone(),
                    $phantom_field: std::marker::PhantomData,
                }
            }
        }

        impl<$($wrapper_generics),*> Default for $wrapper_name<$($wrapper_generics),*>
        where
            $($trait_bounds)+
        {
            fn default() -> Self {
                Self::new()
            }
        }
    };
}
pub(crate) use define_map_wrapper;

macro_rules! entry_or_insert_via_mock {
    ($slf:expr, $hdr_ty:ty, $get_mut_call:ident($($get_mut_args:expr),*), $mock_call:ident($($mock_args:expr),*)) => {{
        let hdr = $slf.hdr as *mut $hdr_ty;
        // SAFETY: `hdr` is derived from `$slf.hdr: &'a mut $hdr_ty`.
        // The two dereferences are in mutually exclusive match arms and
        // never coexist; no aliasing occurs.
        match unsafe { &mut *hdr }.$get_mut_call($($get_mut_args),*) {
            Some(v) => v,
            _ => unsafe { &mut *hdr }.$mock_call($($mock_args),*),
        }
    }};
}
pub(crate) use entry_or_insert_via_mock;

macro_rules! cow_bytes_bounds {
    ($bounds:expr) => {{
        use std::{borrow::Cow, ops::Bound};

        // Bind once: evaluating `$bounds` twice could yield inconsistent
        // start/end bounds for non-idempotent expressions.
        let b = &($bounds);

        let l = match b.start_bound() {
            Bound::Included(lo) => Bound::Included(Cow::Owned(
                $crate::common::ende::KeyEnDeOrdered::to_bytes(lo),
            )),
            Bound::Excluded(lo) => Bound::Excluded(Cow::Owned(
                $crate::common::ende::KeyEnDeOrdered::to_bytes(lo),
            )),
            Bound::Unbounded => Bound::Unbounded,
        };

        let h = match b.end_bound() {
            Bound::Included(hi) => Bound::Included(Cow::Owned(
                $crate::common::ende::KeyEnDeOrdered::to_bytes(hi),
            )),
            Bound::Excluded(hi) => Bound::Excluded(Cow::Owned(
                $crate::common::ende::KeyEnDeOrdered::to_bytes(hi),
            )),
            Bound::Unbounded => Bound::Unbounded,
        };

        (l, h)
    }};
}
pub(crate) use cow_bytes_bounds;
