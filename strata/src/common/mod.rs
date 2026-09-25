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

/// Typed-handle envelope: magic + 8-byte type tag + postcard payload.
///
/// `VSTYPE03` tags hash the fully qualified Rust type name. `VSTYPE02`
/// (v16) is still accepted, with the old versioning-id aliases restored.
const TYPED_HANDLE_META_MAGIC: &[u8; 8] = b"VSTYPE03";
const LEGACY_TYPED_HANDLE_META_MAGIC: &[u8; 8] = b"VSTYPE02";
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

fn fnv1a64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0100_0000_01b3;
    bytes.iter().fold(FNV_OFFSET, |h, &b| {
        (h ^ u64::from(b)).wrapping_mul(FNV_PRIME)
    })
}

fn map_type_paths(full: &str, mut map: impl FnMut(&str) -> &str) -> String {
    let mut out = String::with_capacity(full.len());
    let mut start = 0;
    for (i, c) in full.char_indices() {
        if !(c.is_alphanumeric() || c == '_' || c == ':') {
            out.push_str(map(&full[start..i]));
            out.push(c);
            start = i + c.len_utf8();
        }
    }
    out.push_str(map(&full[start..]));
    out
}

/// Type tag written by this version: the hash of the full Rust type name.
///
/// The tag exists to reject restoring a handle as the wrong type (a false
/// rejection is safer than silent type confusion). Module paths must stay:
/// unrelated same-named types can have different schemas whose encoded bytes
/// happen to overlap. Moving a persisted type therefore needs an explicit
/// migration. This is an identity check, not a schema fingerprint: changing
/// fields while retaining the same type name still requires schema versioning.
fn type_tag<T: ?Sized>() -> u64 {
    fnv1a64(type_name::<T>().as_bytes())
}

/// The full type name as rendered by v16, where the versioning ids were
/// aliases of `u64`. Match complete paths only: unrelated user types and
/// names such as `CommitIdExtra` must keep their original tags.
fn legacy_type_name(full: &str) -> String {
    use crate::versioned::{BranchId, CommitId};

    map_type_paths(full, |path| {
        if path == type_name::<BranchId>() || path == type_name::<CommitId>() {
            "u64"
        } else {
            path
        }
    })
}

/// Type tag of v16's `VSTYPE02` envelope. The transparent id newtypes
/// retain their old alias tags here; `VSTYPE03` keeps them distinct.
fn legacy_type_tag<T: ?Sized>() -> u64 {
    fnv1a64(legacy_type_name(type_name::<T>()).as_bytes())
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
    if meta.len() < header_len {
        return Err(error::VsdbError::Decode {
            detail: "invalid typed handle metadata magic".to_owned(),
        });
    }
    let expected = match &meta[..magic_len] {
        m if m == TYPED_HANDLE_META_MAGIC => type_tag::<T>(),
        m if m == LEGACY_TYPED_HANDLE_META_MAGIC => legacy_type_tag::<T>(),
        _ => {
            return Err(error::VsdbError::Decode {
                detail: "invalid typed handle metadata magic".to_owned(),
            });
        }
    };

    let found = u64::from_le_bytes(meta[magic_len..header_len].try_into().unwrap());
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
        // The legacy tag is the full-path hash.
        assert_ne!(
            legacy_type_tag::<Mapx<u32, a::Row>>(),
            legacy_type_tag::<Mapx<u32, b::Row>>()
        );
    }

    #[test]
    fn legacy_vstype02_envelope_still_restores() {
        use crate::Mapx;
        let mut m: Mapx<u32, String> = Mapx::new();
        m.insert(&1, &"one".to_string());
        let current = postcard::to_allocvec(&m).unwrap();

        // Rebuild the same handle in v16's envelope.
        let meta: Vec<u8> = postcard::from_bytes(&current).unwrap();
        assert_eq!(&meta[..8], TYPED_HANDLE_META_MAGIC);
        let mut legacy = LEGACY_TYPED_HANDLE_META_MAGIC.to_vec();
        legacy.extend_from_slice(&legacy_type_tag::<Mapx<u32, String>>().to_le_bytes());
        legacy.extend_from_slice(&meta[16..]);
        let legacy = postcard::to_allocvec(&legacy).unwrap();

        let restored: Mapx<u32, String> = postcard::from_bytes(&legacy).unwrap();
        assert_eq!(restored.get(&1), Some("one".to_string()));
        // The legacy envelope still rejects a different type.
        assert!(postcard::from_bytes::<Mapx<u64, String>>(&legacy).is_err());
        // Re-serializing upgrades it to the current envelope.
        let upgraded: Vec<u8> =
            postcard::from_bytes(&postcard::to_allocvec(&restored).unwrap()).unwrap();
        assert_eq!(&upgraded[..8], TYPED_HANDLE_META_MAGIC);
    }

    /// v16's encoder, deliberately using the unmodified full type name.
    /// Nest these envelopes to reproduce every layer of a v16 handle.
    fn v16_meta<T>(inner: &impl Serialize) -> Vec<u8> {
        let mut meta = b"VSTYPE02".to_vec();
        meta.extend_from_slice(&fnv1a64(type_name::<T>().as_bytes()).to_le_bytes());
        meta.extend_from_slice(&postcard::to_allocvec(inner).unwrap());
        meta
    }

    #[test]
    fn legacy_handles_restore_versioning_id_aliases() {
        use crate::{
            Mapx, MapxOrdRawKey, Orphan,
            versioned::{BranchId, CommitId},
        };

        // Mapx<String, CommitId> was Mapx<String, u64> in v16,
        // including its inner MapxOrdRawKey's tag.
        let mut raw = MapxRaw::new();
        raw.insert(postcard::to_allocvec("tip").unwrap(), [7]);
        let inner = v16_meta::<MapxOrdRawKey<u64>>(&raw);
        let outer = v16_meta::<Mapx<String, u64>>(&inner);
        let bytes = postcard::to_allocvec(&outer).unwrap();
        let restored: Mapx<String, CommitId> = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(restored.get("tip"), Some(CommitId::from_raw(7)));

        // Aliases can occur in keys, nested parameters, and Orphans too.
        let mut raw = MapxRaw::new();
        raw.insert([3, 5], postcard::to_allocvec(&vec![7u64, 9]).unwrap());
        let inner = v16_meta::<MapxOrdRawKey<Vec<u64>>>(&raw);
        let outer = v16_meta::<Mapx<(u64, u64), Vec<u64>>>(&inner);
        let bytes = postcard::to_allocvec(&outer).unwrap();
        let restored: Mapx<(BranchId, CommitId), Vec<CommitId>> =
            postcard::from_bytes(&bytes).unwrap();
        assert_eq!(
            restored.get(&(BranchId::from_raw(3), CommitId::from_raw(5))),
            Some(vec![CommitId::from_raw(7), CommitId::from_raw(9)])
        );

        let mut raw = MapxRaw::new();
        raw.insert([], [11]);
        let inner = v16_meta::<MapxOrdRawKey<u64>>(&raw);
        let outer = v16_meta::<Orphan<u64>>(&inner);
        let bytes = postcard::to_allocvec(&outer).unwrap();
        let restored: Orphan<BranchId> = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(restored.get_value(), BranchId::from_raw(11));

        // Saving upgrades the envelope without weakening current tags.
        let upgraded = postcard::to_allocvec(&restored).unwrap();
        let reopened: Orphan<BranchId> = postcard::from_bytes(&upgraded).unwrap();
        assert_eq!(reopened.get_value(), BranchId::from_raw(11));
        assert!(postcard::from_bytes::<Orphan<CommitId>>(&upgraded).is_err());
        assert!(postcard::from_bytes::<Orphan<u64>>(&upgraded).is_err());
        assert!(postcard::from_bytes::<Orphan<u32>>(&bytes).is_err());
    }

    #[test]
    fn legacy_ordered_handles_restore_versioning_id_aliases() {
        use crate::{
            MapxOrd, MapxOrdRawKey,
            common::ende::KeyEnDeOrdered,
            versioned::{BranchId, CommitId},
        };

        fn check<Id: KeyEnDeOrdered, Other>(id: fn(u64) -> Id) {
            let mut raw = MapxRaw::new();
            // Encode v16's u64 keys directly, independent of the new ID codec.
            for (key, value) in [(256u64, 3u8), (1, 1), (255, 2)] {
                raw.insert(key.to_be_bytes(), [value]);
            }
            let inner = v16_meta::<MapxOrdRawKey<u8>>(&raw);
            let outer = v16_meta::<MapxOrd<u64, u8>>(&inner);
            let bytes = postcard::to_allocvec(&outer).unwrap();
            let mut restored: MapxOrd<Id, u8> = postcard::from_bytes(&bytes).unwrap();

            assert_eq!(restored.get(&id(256)), Some(3));
            assert_eq!(
                restored.iter().collect::<Vec<_>>(),
                vec![(id(1), 1), (id(255), 2), (id(256), 3)]
            );
            assert_eq!(
                restored.range(id(255)..=id(256)).collect::<Vec<_>>(),
                vec![(id(255), 2), (id(256), 3)]
            );

            restored.insert(&id(257), &4);
            restored.insert(&id(255), &5);
            assert_eq!(raw.get(257u64.to_be_bytes()), Some(vec![4]));
            assert_eq!(raw.get(255u64.to_be_bytes()), Some(vec![5]));
            let upgraded = postcard::to_allocvec(&restored).unwrap();
            let meta: Vec<u8> = postcard::from_bytes(&upgraded).unwrap();
            assert_eq!(&meta[..8], TYPED_HANDLE_META_MAGIC);
            let reopened: MapxOrd<Id, u8> = postcard::from_bytes(&upgraded).unwrap();
            assert_eq!(
                reopened.iter().collect::<Vec<_>>(),
                vec![(id(1), 1), (id(255), 5), (id(256), 3), (id(257), 4)]
            );
            assert!(postcard::from_bytes::<MapxOrd<Other, u8>>(&upgraded).is_err());
            assert!(postcard::from_bytes::<MapxOrd<u64, u8>>(&upgraded).is_err());
        }

        check::<BranchId, CommitId>(BranchId::from_raw);
        check::<CommitId, BranchId>(CommitId::from_raw);
    }

    #[test]
    fn legacy_id_aliases_match_only_complete_library_type_paths() {
        use crate::versioned::CommitId;

        let id = type_name::<CommitId>();
        assert_eq!(
            legacy_type_name(&format!("({id}, [{id}; 2])")),
            "(u64, [u64; 2])"
        );
        for unrelated in [
            format!("{id}Extra"),
            format!("app::{id}"),
            "app::CommitId".into(),
        ] {
            assert_eq!(legacy_type_name(&unrelated), unrelated);
        }
    }
}
