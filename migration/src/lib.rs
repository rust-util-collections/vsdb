//! Offline v17 → v18 conversion. Stop writers, call [`migrate_universe`],
//! configure v18 at that root and convert the saved collection handles.
//! Embedded handles and application sidecars require their application's schema.
#![deny(warnings)]

use serde::{Deserialize, Deserializer, de};
use std::{any::type_name, fs};
use vsdb::{InstanceId, MapxRaw, Namespace, Result, VsdbError};
pub mod dagmap;
pub mod slotdex;
mod universe;
pub mod vecdex;
pub mod versioned;
pub use universe::migrate_universe;

fn corrupt(detail: impl Into<String>) -> VsdbError {
    VsdbError::Corrupt {
        detail: detail.into(),
    }
}
fn tag(name: &str) -> u64 {
    name.bytes().fold(0xcbf29ce484222325, |hash, b| {
        (hash ^ u64::from(b)).wrapping_mul(0x100000001b3)
    })
}

pub(crate) fn legacy_tag_for<T>(vstype02: bool) -> u64 {
    // Renamed library-owned types retain their v16/v17 spellings in legacy tags.
    let mut name = String::new();
    let full = type_name::<T>();
    let mut start = 0;
    let convert = |path: &str| match path {
        p if p == type_name::<vsdb::BranchId>() => {
            if vstype02 {
                "u64"
            } else {
                "vsdb::versioned::BranchId"
            }
        }
        p if p == type_name::<vsdb::CommitId>() => {
            if vstype02 {
                "u64"
            } else {
                "vsdb::versioned::CommitId"
            }
        }
        _ => "",
    };
    for (i, c) in full
        .char_indices()
        .chain(std::iter::once((full.len(), '\0')))
    {
        if c.is_alphanumeric() || matches!(c, '_' | ':') {
            continue;
        }
        let path = &full[start..i];
        let mapped = convert(path);
        name.push_str(if mapped.is_empty() { path } else { mapped });
        if i != full.len() {
            name.push(c);
        }
        start = i + c.len_utf8();
    }
    tag(&name)
}

/// Payload in a v16/v17 typed envelope after checking its nominal tag.
pub fn legacy_payload<T>(frame: &[u8]) -> Result<&[u8]> {
    let vstype02 = match frame.get(..8) {
        Some(b"VSTYPE03") => false,
        Some(b"VSTYPE02") => true,
        _ => {
            return Err(corrupt(format!("invalid v17 {} handle", type_name::<T>())));
        }
    };
    if frame.get(8..16) != Some(&legacy_tag_for::<T>(vstype02).to_le_bytes()) {
        return Err(corrupt(format!("invalid v17 {} handle", type_name::<T>())));
    }
    Ok(&frame[16..])
}

/// Encodes a v18 typed handle as a postcard byte string.
pub fn typed_handle<T>(id: InstanceId, extra: &[u8]) -> Result<Vec<u8>> {
    typed_handle_tag(tag(type_name::<T>()), id, extra)
}

fn typed_handle_tag(type_tag: u64, id: InstanceId, extra: &[u8]) -> Result<Vec<u8>> {
    let len = u32::try_from(extra.len()).map_err(|_| corrupt("metadata too large"))?;
    let mut frame = b"VSTYPE04".to_vec();
    frame.extend_from_slice(&type_tag.to_le_bytes());
    frame.extend_from_slice(&id.map_id.to_le_bytes());
    frame.extend_from_slice(&id.ns.unwrap_or_default().to_le_bytes());
    frame.extend_from_slice(&len.to_le_bytes());
    frame.extend_from_slice(extra);
    Ok(postcard::to_allocvec(&frame)?)
}

fn raw_frame(id: InstanceId) -> [u8; 24] {
    let mut bytes = [0; 24];
    bytes[..8].copy_from_slice(b"VSMAPX02");
    bytes[8..16].copy_from_slice(&id.map_id.to_le_bytes());
    bytes[16..].copy_from_slice(&id.ns.unwrap_or_default().to_le_bytes());
    bytes
}
/// Opens an allocator-issued raw prefix with a current-format envelope.
pub fn raw_map(id: InstanceId) -> Result<MapxRaw> {
    MapxRaw::deserialize(de::value::BytesDeserializer::<de::value::Error>::new(
        &raw_frame(id),
    ))
    .map_err(|e| corrupt(e.to_string()))
}

fn raw_id(bytes: &[u8]) -> Result<InstanceId> {
    if bytes.get(..8) != Some(b"VSMAPX01") || !matches!(bytes.len(), 16 | 24) {
        return Err(corrupt("invalid v17 raw map"));
    }
    let prefix = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    if prefix <= vsdb_core::common::BIGGEST_RESERVED_ID {
        return Err(corrupt("reserved v17 map prefix"));
    }
    let ns = bytes
        .get(16..24)
        .map_or(0, |b| u64::from_le_bytes(b.try_into().unwrap()));
    Ok(InstanceId::new(prefix, ns))
}

/// Converts a postcard-serialized raw map handle embedded in an application
/// record. The enclosing value can be encoded again without opening storage.
pub fn raw_handle(bytes: &[u8]) -> Result<Vec<u8>> {
    let frame: &[u8] = exactly(bytes)?;
    if frame.starts_with(b"VSMAPX02") {
        if frame.len() != 24
            || u64::from_le_bytes(frame[8..16].try_into().unwrap())
                <= vsdb_core::common::BIGGEST_RESERVED_ID
        {
            return Err(corrupt("invalid current raw map handle"));
        }
        return Ok(bytes.to_vec());
    }
    Ok(postcard::to_allocvec(
        &raw_frame(raw_id(frame)?).as_slice(),
    )?)
}

/// A raw v17 map embedded in a composite payload.
pub struct LegacyRaw(pub MapxRaw);
impl<'de> Deserialize<'de> for LegacyRaw {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let bytes = <&[u8]>::deserialize(d)?;
        raw_id(bytes)
            .and_then(raw_map)
            .map(Self)
            .map_err(de::Error::custom)
    }
}

fn exactly<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T> {
    let (value, rest) = postcard::take_from_bytes(bytes)?;
    if !rest.is_empty() {
        return Err(corrupt("trailing bytes in v17 metadata"));
    }
    Ok(value)
}

/// Converts the serialized handle for a basic map or Orphan, including
/// a handle embedded inside a user-defined value. Row contents are preserved.
pub fn basic_handle<T: BasicHandle>(bytes: &[u8]) -> Result<Vec<u8>> {
    let frame: &[u8] = exactly(bytes)?;
    if let Some(payload) = frame.strip_prefix(b"VSTYPE04") {
        if payload.len() != 28
            || payload[..8] != tag(type_name::<T>()).to_le_bytes()
            || u64::from_le_bytes(payload[8..16].try_into().unwrap())
                <= vsdb_core::common::BIGGEST_RESERVED_ID
            || payload[24..] != [0; 4]
        {
            return Err(corrupt("invalid current basic handle"));
        }
        return Ok(bytes.to_vec());
    }
    let id = T::v17_id(frame)?;
    typed_handle::<T>(id, &[])
}

/// v17 metadata shape of a basic typed collection.
pub trait BasicHandle: Sized {
    #[doc(hidden)]
    fn v17_id(frame: &[u8]) -> Result<InstanceId>;
}
impl<V: vsdb::ValueEnDe> BasicHandle for vsdb::MapxOrdRawKey<V> {
    fn v17_id(frame: &[u8]) -> Result<InstanceId> {
        raw_id(exactly(legacy_payload::<Self>(frame)?)?)
    }
}
macro_rules! nested_basic {
    ($ty:ty, $($param:ident:$bound:path),+) => {
        impl<$($param:$bound),+> BasicHandle for $ty {
            fn v17_id(frame:&[u8])->Result<InstanceId>{
                vsdb::MapxOrdRawKey::<V>::v17_id(exactly(legacy_payload::<Self>(frame)?)?)
            }
        }
    }
}
nested_basic!(vsdb::Mapx<K,V>,K:vsdb::KeyEnDe,V:vsdb::ValueEnDe);
nested_basic!(vsdb::MapxOrd<K,V>,K:vsdb::KeyEnDeOrdered,V:vsdb::ValueEnDe);
nested_basic!(vsdb::Orphan<V>, V:vsdb::ValueEnDe);

/// Upgrades the saved meta for a basic map, retaining its InstanceId.
pub fn migrate_basic<T: BasicHandle + de::DeserializeOwned>(id: InstanceId) -> Result<T> {
    let id = InstanceId::new(id.map_id, id.ns.unwrap_or_default());
    let ns = Namespace::open(id.ns.unwrap_or_default())?;
    let path = ns.meta_path(id.map_id);
    let bytes = fs::read(&path)?;
    let frame: &[u8] = exactly(&bytes)?;
    if frame.starts_with(b"VSTYPE04") {
        let expected = typed_handle::<T>(id, &[])?;
        if bytes != expected {
            return Err(corrupt("basic metadata identity mismatch"));
        }
        return exactly(&bytes);
    }
    if T::v17_id(frame)? != id {
        return Err(corrupt("basic metadata identity mismatch"));
    }
    let converted = typed_handle::<T>(id, &[])?;
    let map = exactly(&converted)?;
    vsdb::common::atomic_write_file(&path, &converted)?;
    Ok(map)
}

/// Upgrades a raw map's saved metadata in place.
pub fn migrate_raw(id: InstanceId) -> Result<MapxRaw> {
    let id = InstanceId::new(id.map_id, id.ns.unwrap_or_default());
    let path = Namespace::open(id.ns.unwrap_or_default())?.meta_path(id.map_id);
    let bytes = fs::read(&path)?;
    if bytes == raw_frame(id) {
        return raw_map(id);
    }
    if raw_id(&bytes)? != id {
        return Err(corrupt("raw metadata identity mismatch"));
    }
    let map = raw_map(id)?;
    map.save_meta()?;
    Ok(map)
}

/// Rewrites opaque application values in bounded, crash-atomic batches. The
/// caller selects the schema by map ID. Return `None` for already converted
/// values so an interrupted conversion can be retried from the beginning.
pub fn rewrite_values(
    id: InstanceId,
    mut convert: impl FnMut(&[u8], &[u8]) -> Result<Option<Vec<u8>>>,
) -> Result<u64> {
    let mut map = raw_map(id)?;
    if map.namespace().is_read_only() {
        return Err(VsdbError::ReadOnly {
            operation: "value conversion",
        });
    }
    let mut rows = map.range_detached(..).peekable();
    let mut count = 0;
    while rows.peek().is_some() {
        let mut batch = map.batch();
        let mut staged = 0;
        for (key, value) in rows.by_ref() {
            if let Some(new) = convert(&key, &value)? {
                staged += key.len() + new.len() + 16;
                batch.insert(&key, new);
                count += 1;
                if staged >= 4 << 20 {
                    break;
                }
            }
        }
        batch.commit_sync()?;
    }
    Ok(count)
}
