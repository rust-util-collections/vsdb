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

pub use vsdb_core::common::*;

pub mod dirty_count;

use error::Result;
use serde::{Serialize, de::DeserializeOwned};
use std::{any::type_name, fmt, fs, result::Result as StdResult};

const TYPED_HANDLE_META_MAGIC: &[u8; 8] = b"VSTYPE01";

/// Serializes `value` with `postcard` and writes it to the instance-meta
/// directory under the given `instance_id`.
pub fn save_instance_meta(instance_id: u64, value: &impl Serialize) -> Result<()> {
    let path = vsdb_meta_path(instance_id);
    let bytes = postcard::to_allocvec(value)?;
    fs::write(&path, bytes)?;
    Ok(())
}

/// Reads the meta file for `instance_id` and deserializes it back.
///
/// Only the current (magic-tagged) meta format is accepted; metas written
/// by pre-v13.4 code must be re-saved under a v13 release first.
pub fn load_instance_meta<T: DeserializeOwned>(instance_id: u64) -> Result<T> {
    let path = vsdb_meta_path(instance_id);
    let bytes = fs::read(&path)?;
    Ok(postcard::from_bytes(&bytes)?)
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
    let type_name = type_name::<T>().as_bytes();
    let type_len =
        u32::try_from(type_name.len()).map_err(|_| error::VsdbError::Decode {
            detail: "typed handle name is too long".to_owned(),
        })?;
    let inner = postcard::to_allocvec(inner)?;
    let inner_len =
        u32::try_from(inner.len()).map_err(|_| error::VsdbError::Decode {
            detail: "typed handle payload is too large".to_owned(),
        })?;

    let mut out = Vec::with_capacity(
        TYPED_HANDLE_META_MAGIC.len() + 4 + type_name.len() + 4 + inner.len(),
    );
    out.extend_from_slice(TYPED_HANDLE_META_MAGIC);
    out.extend_from_slice(&type_len.to_le_bytes());
    out.extend_from_slice(type_name);
    out.extend_from_slice(&inner_len.to_le_bytes());
    out.extend_from_slice(&inner);
    Ok(out)
}

fn decode_typed_handle_meta<T, Inner>(meta: &[u8]) -> Result<Inner>
where
    T: ?Sized,
    Inner: DeserializeOwned,
{
    let mut off = TYPED_HANDLE_META_MAGIC.len();
    if meta.len() < off || &meta[..off] != TYPED_HANDLE_META_MAGIC {
        return Err(error::VsdbError::Decode {
            detail: "invalid typed handle metadata magic".to_owned(),
        });
    }
    if meta.len() < off + 4 {
        return Err(error::VsdbError::Decode {
            detail: "truncated typed handle metadata type length".to_owned(),
        });
    }

    let type_len = u32::from_le_bytes(meta[off..off + 4].try_into().unwrap()) as usize;
    off += 4;
    if meta.len() < off + type_len + 4 {
        return Err(error::VsdbError::Decode {
            detail: "truncated typed handle metadata type tag".to_owned(),
        });
    }

    let actual = std::str::from_utf8(&meta[off..off + type_len]).map_err(|_| {
        error::VsdbError::Decode {
            detail: "typed handle metadata type tag is not UTF-8".to_owned(),
        }
    })?;
    let expected = type_name::<T>();
    if actual != expected {
        return Err(error::VsdbError::Decode {
            detail: format!(
                "typed handle mismatch: expected {expected}, found {actual}"
            ),
        });
    }
    off += type_len;

    let inner_len = u32::from_le_bytes(meta[off..off + 4].try_into().unwrap()) as usize;
    off += 4;
    if meta.len() != off + inner_len {
        return Err(error::VsdbError::Decode {
            detail: "invalid typed handle metadata payload length".to_owned(),
        });
    }

    Ok(postcard::from_bytes(&meta[off..])?)
}
