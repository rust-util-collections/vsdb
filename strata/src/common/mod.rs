//!
//! # Common Components
//!
//! This module provides common components and utilities used throughout the `vsdb` crate.
//! It re-exports items from `vsdb_core::common` (including the [`error`]
//! module — the unified error vocabulary of the whole ecosystem) and
//! includes the `ende` module for encoding and decoding traits.
//!

/// A module for encoding and decoding traits.
pub mod ende;
mod frame;
pub(crate) mod guard;
mod reader;
#[cfg(test)]
pub(crate) use frame::parse_prefix_handle;
pub(crate) use frame::{
    PrefixFrame, basic_inner, deserialize_prefix_handle, open_raw,
    parse_prefix_handle_checked, serialize_prefix_handle,
};
pub use reader::*;
pub(crate) mod macros;
pub(crate) mod staged;

pub use vsdb_core::common::*;

use vsdb_core::MapxRaw;

use error::{Result, VsdbError};
use serde::{Serialize, de::DeserializeOwned};
use std::{any::type_name, fs, path::Path, thread};

const PREFIX_FRAME_MAGIC: &[u8; 8] = b"VSTYPE04";

/// Whether the thread was already unwinding when a write-back guard was
/// created.
///
/// A panic that begins while a guard is alive means its buffered edit may
/// be half-done: the guard discards it instead of persisting it (which also
/// avoids encoding user values, or a second panic, while unwinding). A guard
/// created during unwinding — e.g. inside a `Drop` — writes normally.
#[derive(Clone, Copy, Debug)]
pub(crate) struct UnwindMark(bool);

impl UnwindMark {
    #[inline(always)]
    pub(crate) fn new() -> Self {
        Self(thread::panicking())
    }

    /// True when a panic began after this mark was taken.
    #[inline(always)]
    pub(crate) fn interrupted(self) -> bool {
        !self.0 && thread::panicking()
    }
}

/// Typed wrappers backed by a single raw map.
pub(crate) trait SingleMap: Sized {
    fn from_raw(raw: MapxRaw) -> Self;
    fn raw(&self) -> &MapxRaw;
}
impl SingleMap for MapxRaw {
    fn from_raw(raw: MapxRaw) -> Self {
        raw
    }
    fn raw(&self) -> &MapxRaw {
        self
    }
}

pub(crate) fn ensure_writable(ns: &Namespace, operation: &'static str) -> Result<()> {
    if ns.is_read_only() {
        Err(error::VsdbError::ReadOnly { operation })
    } else {
        Ok(())
    }
}

pub(crate) fn ensure_process_writable(operation: &'static str) -> Result<()> {
    if vsdb_open_mode() == OpenMode::ReadOnly {
        Err(error::VsdbError::ReadOnly { operation })
    } else {
        Ok(())
    }
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0100_0000_01b3;
    bytes.iter().fold(FNV_OFFSET, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(FNV_PRIME)
    })
}

/// Type tag written by this version: the hash of the full Rust type name.
///
/// The tag exists to reject restoring a handle as the wrong type (a false
/// rejection is safer than silent type confusion). Module paths must stay:
/// unrelated same-named types can have different schemas whose encoded bytes
/// happen to overlap. Moving a persisted type therefore needs an explicit
/// migration. This is an identity check, not a schema fingerprint: changing
/// fields while retaining the same type name still requires schema versioning.
pub(crate) fn type_tag<T: ?Sized>() -> u64 {
    fnv1a64(type_name::<T>().as_bytes())
}

/// Serializes `value` with `postcard` and writes it to the owning
/// namespace's instance-meta directory under `id.map_id`.
///
/// The write is atomic (tmp + fsync + rename), so a crash mid-save can
/// never leave a truncated meta file behind.
///
/// # Errors
///
/// Returns [`error::VsdbError::ReadOnly`] in read-only mode, or an
/// encoding/storage error.
pub fn save_instance_meta(id: InstanceId, value: &impl Serialize) -> Result<()> {
    let ns = Namespace::open(id.ns.unwrap_or(DEFAULT_NS_ID))?;
    ensure_writable(&ns, "metadata save")?;
    let path = ns.meta_path(id.map_id);
    create_dir_all_durable(path.parent().expect("has parent"))?;
    let bytes = postcard::to_allocvec(value)?;
    atomic_write_file(&path, &bytes)?;
    Ok(())
}

/// Reads the meta file for `id` and deserializes it as `T`.
///
/// Resolution is deterministic, never a search: `id.ns` names the meta
/// directory (`None` ⇒ the default namespace's). This function does not
/// itself require the typed-handle magic or type tag. Those gates run
/// when `T` is a collection handle (`from_meta` / `Deserialize`). A
/// non-handle payload written by [`save_instance_meta`] is returned as
/// postcard decodes it.
pub fn load_instance_meta<T: DeserializeOwned>(id: InstanceId) -> Result<T> {
    let ns = Namespace::open(id.ns.unwrap_or(DEFAULT_NS_ID))?;
    load_meta_file(&ns.meta_path(id.map_id))
}

pub(crate) fn load_meta_file<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = fs::read(path)?;
    let (value, rest) = postcard::take_from_bytes(&bytes)?;
    if !rest.is_empty() {
        return Err(VsdbError::Decode {
            detail: "trailing bytes in instance metadata".into(),
        });
    }
    Ok(value)
}

pub(crate) fn load_instance_meta_checked<T>(
    id: InstanceId,
    open: impl FnOnce(PrefixFrame<'_>) -> Result<T>,
    instance_id: impl FnOnce(&T) -> InstanceId,
) -> Result<T> {
    let id = InstanceId::new(id.map_id, id.ns.unwrap_or(DEFAULT_NS_ID));
    let ns = Namespace::open(id.ns.unwrap_or(DEFAULT_NS_ID))?;
    let bytes = fs::read(ns.meta_path(id.map_id))?;
    let (raw_frame, rest): (&[u8], _) = postcard::take_from_bytes(&bytes)?;
    if !rest.is_empty() {
        return Err(VsdbError::Decode {
            detail: "trailing bytes in instance metadata".into(),
        });
    }
    let frame = frame::parse_prefix_handle_checked::<T>(raw_frame, Some(id))?;
    let value = open(frame)?;
    let found = instance_id(&value);
    if found != id {
        return Err(error::VsdbError::Decode {
            detail: format!(
                "metadata identity mismatch: requested {id}, payload names {found}"
            ),
        });
    }
    Ok(value)
}

#[cfg(test)]
mod type_tag_test {
    use super::*;

    mod a {
        #[derive(serde::Serialize, serde::Deserialize)]
        pub struct Row(pub u32);
    }
    mod b {
        #[derive(serde::Serialize, serde::Deserialize)]
        pub struct Row(pub u32);
    }

    #[test]
    fn tags_distinguish_qualified_types_and_parameters() {
        use crate::{Mapx, MapxOrd};
        // Same leaf name and shape are insufficient to establish identity.
        assert_ne!(
            type_tag::<Mapx<u32, a::Row>>(),
            type_tag::<Mapx<u32, b::Row>>()
        );
        // Different parameters and collection wrappers also differ.
        assert_ne!(
            type_tag::<Mapx<u32, a::Row>>(),
            type_tag::<Mapx<u64, a::Row>>()
        );
        assert_ne!(
            type_tag::<Mapx<u32, u32>>(),
            type_tag::<MapxOrd<u32, u32>>()
        );
    }

    #[test]
    fn basic_handles_are_flat() {
        use crate::Mapx;
        let mut m = Mapx::<u8, u8>::new();
        m.insert(&7, &11);
        let frame = postcard::to_allocvec(&m).unwrap();
        assert_eq!(frame.len(), 37);
        assert_eq!(&frame[1..9], PREFIX_FRAME_MAGIC);
        assert_eq!(&frame[17..25], &m.instance_id().map_id.to_le_bytes());
        let restored: Mapx<u8, u8> = postcard::from_bytes(&frame).unwrap();
        assert_eq!(restored.get(&7), Some(11));
        assert!(postcard::from_bytes::<Mapx<u8, u16>>(&frame).is_err());
    }
}
