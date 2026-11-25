//! Single-prefix handle envelopes. The prefix offset is also used by
//! the engine's missing-ceiling recovery scan.

use super::{PREFIX_FRAME_MAGIC, SingleMap, type_tag};
use crate::{InstanceId, Result, VsdbError};
use serde::{Deserialize, Deserializer, Serializer, de, ser};
use std::{fmt, marker::PhantomData, result::Result as StdResult};
use vsdb_core::MapxRaw;

/// A validated storage handle and its collection-specific metadata.
pub(crate) struct PrefixFrame<'a>(pub MapxRaw, pub &'a [u8]);

pub(crate) fn serialize_prefix_handle<T, S: Serializer>(
    map: &MapxRaw,
    extra: &[u8],
    serializer: S,
) -> StdResult<S::Ok, S::Error> {
    let id = map.instance_id();
    let len = u32::try_from(extra.len()).map_err(ser::Error::custom)?;
    let mut header = [0u8; 36];
    header[..8].copy_from_slice(PREFIX_FRAME_MAGIC);
    header[8..16].copy_from_slice(&type_tag::<T>().to_le_bytes());
    header[16..24].copy_from_slice(&id.map_id.to_le_bytes());
    header[24..32].copy_from_slice(&id.ns.unwrap_or_default().to_le_bytes());
    header[32..].copy_from_slice(&len.to_le_bytes());
    if extra.is_empty() {
        return serializer.serialize_bytes(&header);
    }
    let mut frame = Vec::with_capacity(header.len() + extra.len());
    frame.extend_from_slice(&header);
    frame.extend_from_slice(extra);
    serializer.serialize_bytes(&frame)
}

pub(crate) fn parse_prefix_handle<T>(bytes: &[u8]) -> Result<PrefixFrame<'_>> {
    let invalid = || VsdbError::Decode {
        detail: format!("invalid {} handle", std::any::type_name::<T>()),
    };
    if bytes.len() < 36
        || &bytes[..8] != PREFIX_FRAME_MAGIC
        || bytes[8..16] != type_tag::<T>().to_le_bytes()
    {
        return Err(invalid());
    }
    let len = u32::from_le_bytes(bytes[32..36].try_into().unwrap()) as usize;
    if bytes.len() - 36 != len {
        return Err(invalid());
    }
    let id = InstanceId::new(
        u64::from_le_bytes(bytes[16..24].try_into().unwrap()),
        u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
    );
    Ok(PrefixFrame(open_raw(id)?, &bytes[36..]))
}

/// Restores a validated raw handle inside a collection decoder.
pub(crate) fn open_raw(id: InstanceId) -> Result<MapxRaw> {
    let mut raw = [0; 24];
    raw[..8].copy_from_slice(b"VSMAPX02");
    raw[8..16].copy_from_slice(&id.map_id.to_le_bytes());
    raw[16..].copy_from_slice(&id.ns.unwrap_or_default().to_le_bytes());
    MapxRaw::deserialize(de::value::BytesDeserializer::<de::value::Error>::new(&raw))
        .map_err(|e| VsdbError::Decode {
            detail: e.to_string(),
        })
}

pub(crate) fn deserialize_prefix_handle<'de, T, D, F>(
    deserializer: D,
    f: F,
) -> StdResult<T, D::Error>
where
    D: Deserializer<'de>,
    F: FnOnce(PrefixFrame<'_>) -> Result<T>,
{
    struct Visitor<T, F>(PhantomData<T>, F);
    impl<'de, T, F: FnOnce(PrefixFrame<'_>) -> Result<T>> de::Visitor<'de>
        for Visitor<T, F>
    {
        type Value = T;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a typed VSDB handle")
        }
        fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> StdResult<T, E> {
            parse_prefix_handle::<T>(bytes)
                .and_then(self.1)
                .map_err(E::custom)
        }
        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> StdResult<T, A::Error> {
            let mut bytes = Vec::new();
            while let Some(b) = seq.next_element()? {
                bytes.push(b);
            }
            self.visit_bytes(&bytes)
        }
    }
    deserializer.deserialize_bytes(Visitor(PhantomData, f))
}

pub(crate) fn basic_inner<I: SingleMap>(frame: PrefixFrame<'_>) -> Result<I> {
    match frame {
        PrefixFrame(raw, []) => Ok(I::from_raw(raw)),
        PrefixFrame(_, _) => Err(VsdbError::Decode {
            detail: "unexpected basic map metadata".into(),
        }),
    }
}
