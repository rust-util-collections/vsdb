//! Upgrade fixed-width v17 SlotDex indices without moving their data.
use crate::{LegacyRaw, legacy_payload, typed_handle};
use std::{any::TypeId, fs};
use vsdb::{InstanceId, KeyEnDeOrdered, Namespace, Result, SlotDex, SlotType, VsdbError};
use vsdb_core::{MapxRaw, common::atomic_write_file};

fn invalid(detail: &str) -> VsdbError {
    VsdbError::Decode {
        detail: detail.into(),
    }
}

/// Converts saved v17 metadata and the eight-byte total row. Existing keys,
/// per-slot counts, and upper tiers retain their exact bytes. Run offline.
/// The total upgrade is fenced before publishing the new metadata.
pub fn migrate_v17<S: SlotType, K: KeyEnDeOrdered>(
    id: impl Into<InstanceId>,
) -> Result<SlotDex<S, K>> {
    convert(id.into(), None)
}

/// Use when the application confirms the old codec always used the current
/// fixed width. Converts a custom slot coordinate after checking the caller's capacity
/// against the serialized coordinate and reconciling every entry, slot count
/// and tier floor with the proposed fixed-width codec. Ambiguous datasets
/// require rebuilding from authoritative application records.
pub fn migrate_custom_v17<S: SlotType, K: KeyEnDeOrdered>(
    id: impl Into<InstanceId>,
    capacity: u32,
) -> Result<SlotDex<S, K>> {
    convert(id.into(), Some(capacity))
}

fn convert<S: SlotType, K: KeyEnDeOrdered>(
    id: InstanceId,
    requested: Option<u32>,
) -> Result<SlotDex<S, K>> {
    let id = InstanceId::new(id.map_id, id.ns.unwrap_or_default());
    let ns = Namespace::open(id.ns.unwrap_or_default())?;
    if ns.is_read_only() {
        return Err(VsdbError::ReadOnly {
            operation: "SlotDex migration",
        });
    }
    if let Ok(done) = SlotDex::from_meta(id) {
        return Ok(done);
    }
    if requested.is_none()
        && ![
            TypeId::of::<u32>(),
            TypeId::of::<u64>(),
            TypeId::of::<u128>(),
        ]
        .contains(&TypeId::of::<S>())
    {
        return Err(invalid(
            "custom v17 slots require application verification of their encoding and bucket alignment",
        ));
    }
    let path = ns.meta_path(id.map_id);
    let bytes = fs::read(&path)?;
    let (frame, tail) = postcard::take_from_bytes::<&[u8]>(&bytes)?;
    if !tail.is_empty() {
        return Err(invalid("trailing SlotDex metadata"));
    }
    let payload = legacy_payload::<SlotDex<S, K>>(frame)?;
    let ((version, LegacyRaw(mut store)), rest) =
        postcard::take_from_bytes::<(u8, LegacyRaw)>(payload)?;
    let (capacity, swap, tail) = if let Some(capacity) = requested {
        let ((old, swap), tail) = postcard::take_from_bytes::<(S, bool)>(rest)?;
        if S::tier_stride(capacity, 1) != Some(old) {
            return Err(invalid("custom v17 SlotDex capacity mismatch"));
        }
        (capacity, swap, tail)
    } else {
        let ((cap, swap), tail) = postcard::take_from_bytes::<(u128, bool)>(rest)?;
        let cap = u32::try_from(cap).map_err(|_| invalid("v17 SlotDex capacity exceeds u32"))?;
        (cap, swap, tail)
    };
    if !tail.is_empty() || version != 2 || capacity < 2 || store.instance_id() != id {
        return Err(invalid("invalid v17 SlotDex configuration or identity"));
    }
    let total = match store.get([2]).as_deref() {
        None if store.iter().next().is_none() => 0,
        Some(raw) if raw.len() == 8 => u64::from_le_bytes(raw.try_into().unwrap()),
        Some(raw) if raw.len() == 16 && raw[8..] == [0; 8] => {
            u64::from_le_bytes(raw[..8].try_into().unwrap())
        }
        _ => return Err(invalid("invalid v17 SlotDex total row")),
    };
    if requested.is_some() {
        verify_custom::<S, K>(&store, capacity, total)?;
    }
    let handle =
        typed_handle::<SlotDex<S, K>>(id, &postcard::to_allocvec(&(3u8, capacity, swap))?)?;
    let mut batch = store.batch();
    batch.insert([2], [total.to_le_bytes(), [0; 8]].concat());
    batch.commit_sync()?;
    atomic_write_file(&path, &handle)?;
    SlotDex::from_meta(id)
}

fn coordinate<S: SlotType>(raw: &[u8]) -> Result<S> {
    let mut bytes = S::MIN.to_bytes();
    if bytes.as_ref().len() != raw.len() {
        return Err(invalid("variable-width v17 slot cannot be upgraded"));
    }
    bytes.as_mut().copy_from_slice(raw);
    let slot = S::from_bytes(bytes)?;
    if slot.to_bytes().as_ref() != raw {
        return Err(invalid("v17 slot does not round-trip"));
    }
    Ok(slot)
}
fn count(raw: &[u8]) -> Result<u64> {
    let n = u64::from_le_bytes(
        raw.try_into()
            .map_err(|_| invalid("malformed v17 slot count"))?,
    );
    if n == 0 {
        return Err(invalid("empty v17 slot count"));
    }
    Ok(n)
}
fn verify_custom<S: SlotType, K: KeyEnDeOrdered>(
    store: &MapxRaw,
    capacity: u32,
    total: u64,
) -> Result<()> {
    let mut entries = store.range([0]..[1]);
    let mut seen = 0u64;
    for (key, val) in store.range([1, 0]..[1, 1]) {
        let slot = coordinate::<S>(&key[2..])?;
        let n = count(&val)?;
        seen = seen
            .checked_add(n)
            .ok_or_else(|| invalid("slot count overflow"))?;
        let prefix = slot.to_bytes();
        for _ in 0..n {
            let (row, val) = entries
                .next()
                .ok_or_else(|| invalid("missing v17 slot entry"))?;
            let suffix = row[1..]
                .strip_prefix(prefix.as_ref())
                .ok_or_else(|| invalid("v17 slot entry mismatch"))?;
            if !val.is_empty() || !K::from_slice(suffix)?.with_ordered_bytes(|v| v == suffix) {
                return Err(invalid("noncanonical v17 SlotDex entry"));
            }
        }
    }
    if seen != total || entries.next().is_some() {
        return Err(invalid("v17 SlotDex total mismatch"));
    }
    let mut previous = 0u8;
    let mut rows = store
        .range([1, 1]..[2, 0])
        .take_while(|(k, _)| k[0] == 1)
        .peekable();
    while let Some((k, _)) = rows.peek() {
        let level = *k.get(1).ok_or_else(|| invalid("invalid v17 tier tag"))?;
        if previous.checked_add(1u8) != Some(level) {
            return Err(invalid("missing v17 tier"));
        }
        let stride = S::tier_stride(capacity, level);
        let mut lower = store.range([1, previous]..[1, level]).peekable();
        while let Some((k, _)) = rows.peek()
            && k[1] == level
        {
            let (k, v) = rows.next().expect("peeked");
            let floor = coordinate::<S>(&k[2..])?;
            let mut sum = 0u64;
            while let Some((low, _)) = lower.peek() {
                let slot = coordinate::<S>(&low[2..])?;
                if stride.map_or(S::MIN, |s| slot.floor_align(s)) != floor {
                    break;
                }
                let (_, v) = lower.next().expect("peeked");
                sum = sum
                    .checked_add(count(&v)?)
                    .ok_or_else(|| invalid("tier count overflow"))?;
            }
            if sum != count(&v)? {
                return Err(invalid("incompatible v17 tier alignment or count"));
            }
        }
        if lower.next().is_some() {
            return Err(invalid("incomplete v17 tier"));
        }
        previous = level;
    }
    Ok(())
}
