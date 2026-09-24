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
pub(crate) mod macros;
pub(crate) mod staged;

pub use vsdb_core::common::*;

use vsdb_core::MapxRaw;

use error::Result;
use serde::{Serialize, de::DeserializeOwned};
use std::{any::type_name, fmt, fs, result::Result as StdResult, thread};

const TYPED_HANDLE_META_MAGIC: &[u8; 8] = b"VSTYPE02";
const TYPED_HANDLE_TAG_LEN: usize = 8;

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

/// Storage handles that can be placed on the same engine shard as an
/// anchor map (see [`MapxRaw::new_colocated`]), so a composite built from
/// them shares one WAL and one crash order.
pub(crate) trait Colocate: Sized {
    /// A fresh, empty instance co-located with `anchor`.
    fn new_colocated(anchor: &MapxRaw) -> Self;
    /// The underlying raw map.
    fn raw(&self) -> &MapxRaw;
    /// A deep copy co-located with `anchor`.
    fn clone_colocated(&self, anchor: &MapxRaw) -> Result<Self>;
}

impl Colocate for MapxRaw {
    #[inline(always)]
    fn new_colocated(anchor: &MapxRaw) -> Self {
        anchor.new_colocated()
    }

    #[inline(always)]
    fn raw(&self) -> &MapxRaw {
        self
    }

    fn clone_colocated(&self, anchor: &MapxRaw) -> Result<Self> {
        MapxRaw::clone_colocated(self, anchor)
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

/// FNV-1a 64 hash of `T`'s full type path.
///
/// The tag inherits `std::any::type_name`'s caveats: it is not guaranteed
/// stable across compiler versions, and renaming/moving a type (or any of
/// its generic parameters) changes it. Persisted typed-handle metadata is
/// therefore tied to the writing build's type layout — an intentional
/// property: the tag exists to reject cross-type restores, and a false
/// rejection is always safer than silent type confusion.
fn type_tag<T: ?Sized>() -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0100_0000_01b3;
    type_name::<T>()
        .as_bytes()
        .iter()
        .fold(FNV_OFFSET, |h, &b| {
            (h ^ u64::from(b)).wrapping_mul(FNV_PRIME)
        })
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
    let ns = match id.ns {
        None => Namespace::default_ns(),
        Some(n) => Namespace::open(n)?,
    };
    ensure_writable(&ns, "metadata save")?;
    let path = ns.meta_path(id.map_id);
    fs::create_dir_all(path.parent().expect("has parent"))?;
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
/// postcard decodes it. Typed-handle restore rejects a legacy prefix
/// payload; see `CHANGELOG.md`.
pub fn load_instance_meta<T: DeserializeOwned>(id: InstanceId) -> Result<T> {
    let ns = match id.ns {
        None => Namespace::default_ns(),
        Some(n) => Namespace::open(n)?,
    };
    let path = ns.meta_path(id.map_id);
    let bytes = fs::read(&path)?;
    Ok(postcard::from_bytes(&bytes)?)
}

pub(crate) fn load_instance_meta_checked<T>(
    id: InstanceId,
    instance_id: impl FnOnce(&T) -> InstanceId,
) -> Result<T>
where
    T: DeserializeOwned,
{
    let id = InstanceId::new(id.map_id, id.ns.unwrap_or(DEFAULT_NS_ID));
    let value = load_instance_meta(id)?;
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

pub(crate) fn serialize_typed_handle_meta<T, Ser>(
    inner: &impl Serialize,
    serializer: Ser,
) -> StdResult<Ser::Ok, Ser::Error>
where
    T: ?Sized,
    Ser: serde::Serializer,
{
    let bytes =
        encode_typed_handle_meta::<T>(inner).map_err(serde::ser::Error::custom)?;
    serializer.serialize_bytes(&bytes)
}

pub(crate) fn deserialize_typed_handle_meta<'de, T, Inner, De>(
    deserializer: De,
) -> StdResult<Inner, De::Error>
where
    T: ?Sized,
    Inner: DeserializeOwned,
    De: serde::Deserializer<'de>,
{
    struct BytesVisitor;

    impl<'de> serde::de::Visitor<'de> for BytesVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("typed VSDB handle metadata")
        }

        fn visit_bytes<E>(self, v: &[u8]) -> StdResult<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(v.to_vec())
        }

        fn visit_byte_buf<E>(self, v: Vec<u8>) -> StdResult<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(v)
        }

        fn visit_seq<A>(self, mut seq: A) -> StdResult<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut ret = vec![];
            while let Some(i) = seq.next_element()? {
                ret.push(i);
            }
            Ok(ret)
        }
    }

    deserializer
        .deserialize_byte_buf(BytesVisitor)
        .and_then(|meta| {
            decode_typed_handle_meta::<T, Inner>(&meta).map_err(serde::de::Error::custom)
        })
}

fn encode_typed_handle_meta<T: ?Sized>(inner: &impl Serialize) -> Result<Vec<u8>> {
    let inner = postcard::to_allocvec(inner)?;
    let mut out = Vec::with_capacity(
        TYPED_HANDLE_META_MAGIC.len() + TYPED_HANDLE_TAG_LEN + inner.len(),
    );
    out.extend_from_slice(TYPED_HANDLE_META_MAGIC);
    out.extend_from_slice(&type_tag::<T>().to_le_bytes());
    out.extend_from_slice(&inner);
    Ok(out)
}

fn decode_typed_handle_meta<T, Inner>(meta: &[u8]) -> Result<Inner>
where
    T: ?Sized,
    Inner: DeserializeOwned,
{
    let magic_len = TYPED_HANDLE_META_MAGIC.len();
    let header_len = magic_len + TYPED_HANDLE_TAG_LEN;
    if meta.len() < header_len || &meta[..magic_len] != TYPED_HANDLE_META_MAGIC {
        return Err(error::VsdbError::Decode {
            detail: "invalid typed handle metadata magic".to_owned(),
        });
    }

    let found = u64::from_le_bytes(meta[magic_len..header_len].try_into().unwrap());
    let expected = type_tag::<T>();
    if found != expected {
        return Err(error::VsdbError::Decode {
            detail: format!(
                "typed handle mismatch: expected {} (tag {expected:016x}), found tag {found:016x}",
                type_name::<T>()
            ),
        });
    }

    Ok(postcard::from_bytes(&meta[header_len..])?)
}
