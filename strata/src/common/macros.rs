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
        #[derive(Debug)]
        $vis struct $wrapper_name<$($wrapper_generics),*> {
            $inner_vis inner: $inner_type,
            $phantom_field: $phantom_type,
        }

        impl<$($wrapper_generics),*> serde::Serialize for $wrapper_name<$($wrapper_generics),*> {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                let raw = $crate::common::SingleMap::raw(self);
                $crate::common::serialize_prefix_handle::<Self, S>(raw, &[], serializer)
            }
        }

        impl<'de, $($wrapper_generics),*> serde::Deserialize<'de> for $wrapper_name<$($wrapper_generics),*> {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                $crate::common::deserialize_prefix_handle(deserializer, |frame| {
                    Ok(Self {
                        inner: $crate::common::basic_inner(frame)?,
                        $phantom_field: std::marker::PhantomData,
                    })
                })
            }
        }

        impl<$($wrapper_generics),*> $wrapper_name<$($wrapper_generics),*>
        where
            $($trait_bounds)+
        {
            /// # Safety
            ///
            /// Creates a second handle to the same underlying storage, bypassing
            /// Rust's aliasing guarantees.  The caller **must** ensure no
            /// concurrent writes to the same key through any handle.  Multiple
            /// writers on disjoint keys are safe. Concurrent immutable reads,
            /// including iteration, may run alongside writes. Each iterator
            /// retains the committed view captured when it is created, across
            /// later writes, clears and flushes. Separate reads/iterators can
            /// observe different views. Mutable iteration remains a write and
            /// requires the same exclusion of other writers.
            #[inline(always)]
            pub unsafe fn shadow(&self) -> Self {
                Self {
                    // SAFETY: forwards this fn's `unsafe` contract — the
                    // caller guarantees no concurrent writes to the same
                    // key through any handle.
                    inner: unsafe { self.inner.shadow() },
                    $phantom_field: std::marker::PhantomData,
                }
            }

            /// Creates a new instance in the current ambient namespace
            /// ([`Namespace::current`](vsdb_core::Namespace::current)).
            ///
            /// # Panics
            ///
            /// Panics in read-only mode. Restore an existing handle with
            /// [`from_meta`](Self::from_meta) instead.
            #[inline(always)]
            pub fn new() -> Self {
                Self {
                    inner: <$inner_type>::new(),
                    $phantom_field: std::marker::PhantomData,
                }
            }

            /// Creates a new instance placed in `ns` — the explicit form
            /// of the ambient-scope placement performed by
            /// [`new`](Self::new).
            ///
            /// # Panics
            ///
            /// Panics if `ns` is read-only.
            #[inline(always)]
            pub fn new_in(ns: &$crate::common::Namespace) -> Self {
                Self {
                    inner: <$inner_type>::new_in(ns),
                    $phantom_field: std::marker::PhantomData,
                }
            }

            /// The namespace (engine placement group) this structure lives in:
            /// `new_in(&existing.namespace())` places new data in the same
            /// namespace as `existing`.
            #[inline(always)]
            pub fn namespace(&self) -> $crate::common::Namespace {
                self.inner.namespace()
            }

            /// Makes prior successful writes on this map's shard durable
            /// without forcing a memtable flush. Other maps on that shard
            /// are synchronized too; other shards are unaffected.
            ///
            /// A read-only handle is a no-op. This is not a transaction
            /// across collections. Prefer [`try_sync_wal`](Self::try_sync_wal)
            /// to handle synchronization errors.
            ///
            /// # Panics
            ///
            /// Panics if the engine cannot synchronize its WAL.
            pub fn sync_wal(&self) {
                self.inner.sync_wal();
            }

            /// Fallible form of [`sync_wal`](Self::sync_wal), returning engine
            /// synchronization errors without forcing a memtable flush.
            /// A read-only handle is a no-op.
            pub fn try_sync_wal(&self) -> $crate::common::error::Result<()> {
                self.inner.try_sync_wal()
            }

            /// Deep-copies every entry into a brand-new instance placed
            /// in `ns` — the cross-namespace form of `Clone` (`clone()`
            /// copies into the *source's* namespace; `clone_in` chooses
            /// the target instead, mirroring [`new`](Self::new) vs
            /// [`new_in`](Self::new_in)).
            ///
            /// The copy runs in bounded chunks (never buffering the
            /// whole map in memory).
            ///
            /// # Errors
            ///
            /// Returns [`VsdbError::ReadOnly`](vsdb_core::VsdbError::ReadOnly)
            /// if `ns` is read-only, or an engine-level write error. The
            /// partially-written target is reclaimed with a best-effort O(1) wipe; only
            /// if that wipe also fails is it abandoned as unreferenced,
            /// invisible garbage (the same residue a mid-`clone()`
            /// panic leaves behind).
            pub fn clone_in(
                &self,
                ns: &$crate::common::Namespace,
            ) -> $crate::common::error::Result<Self> {
                Ok(Self {
                    inner: self.inner.clone_in(ns)?,
                    $phantom_field: std::marker::PhantomData,
                })
            }

            /// # Panics
            ///
            /// Panics in read-only mode or if the engine rejects the batch.
            #[inline(always)]
            pub fn clear(&mut self) {
                self.inner.clear();
            }

            #[inline(always)]
            pub fn is_the_same_instance(&self, other_hdr: &Self) -> bool {
                self.inner.is_the_same_instance(&other_hdr.inner)
            }

            /// Returns the complete public identity of this instance:
            /// `{ map_id, ns }` (`ns: None` ⇔ default namespace). A
            /// pre-v16 bare `u64` id equals `InstanceId::from(u64)`.
            #[inline(always)]
            pub fn instance_id(&self) -> $crate::common::InstanceId {
                self.inner.instance_id()
            }

            /// Persists this instance's metadata to disk (in its owning
            /// namespace's meta directory) so that it can be recovered
            /// later via [`from_meta`](Self::from_meta).
            ///
            /// Returns the [`InstanceId`](vsdb_core::InstanceId) that
            /// should be passed to `from_meta`.
            ///
            /// # Errors
            ///
            /// Returns [`VsdbError::ReadOnly`](vsdb_core::VsdbError::ReadOnly)
            /// in read-only mode, or an I/O/encoding error if the metadata
            /// cannot be written.
            pub fn save_meta(&self) -> $crate::common::error::Result<$crate::common::InstanceId> {
                let id = $crate::common::SingleMap::raw(self).try_instance_id()?;
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
            /// no-concurrent-writes-to-the-same-key discipline
            /// documented on [`shadow`](Self::shadow) applies across
            /// **every** live alias: no concurrent writes to the same
            /// key through any handle.
            /// `from_meta` is intended to restore a handle after the
            /// original has gone out of scope (e.g. across a process
            /// restart); calling it while the original is still live
            /// requires the same care as `shadow()`, even though this
            /// function is safe Rust.
            pub fn from_meta(
                instance_id: impl Into<$crate::common::InstanceId>,
            ) -> $crate::common::error::Result<Self> {
                let id = instance_id.into();
                $crate::common::load_instance_meta_checked(
                    id,
                    |frame| {
                        Ok(Self {
                            inner: $crate::common::basic_inner(frame)?,
                            $phantom_field: std::marker::PhantomData,
                        })
                    },
                    Self::instance_id,
                )
            }
        }

        impl<$($wrapper_generics),*> $crate::common::SingleMap for $wrapper_name<$($wrapper_generics),*>
        {
            fn from_raw(raw: vsdb_core::MapxRaw) -> Self {
                Self {
                    inner: $crate::common::SingleMap::from_raw(raw),
                    $phantom_field: std::marker::PhantomData,
                }
            }

            #[inline(always)]
            fn raw(&self) -> &vsdb_core::MapxRaw {
                $crate::common::SingleMap::raw(&self.inner)
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
