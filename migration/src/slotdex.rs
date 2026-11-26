//! Upgrade fixed-width v17 SlotDex indices without moving their data.
mod resize;

use crate::{LegacyRaw, legacy_payload, try_get, typed_handle};
use std::{any::TypeId, fs};
use vsdb::{InstanceId, KeyEnDeOrdered, Namespace, Result, SlotDex, SlotType, VsdbError};
use vsdb_core::{MapxRaw, common::atomic_write_file};

fn invalid(detail: &str) -> VsdbError {
    VsdbError::Decode {
        detail: detail.into(),
    }
}

#[derive(Clone, Copy)]
enum Capacity {
    Builtin(Option<u32>),
    Custom(u32),
}

/// Converts saved v17 metadata offline. Oversized unsigned capacities use
/// `u32::MAX` unless an earlier conversion selected a replacement. Entry
/// keys and per-slot counts stay in place. The total and any rebuilt tiers
/// are durable before publishing the handle.
pub fn migrate_v17<S: SlotType, K: KeyEnDeOrdered>(
    id: impl Into<InstanceId>,
) -> Result<SlotDex<S, K>> {
    convert(id.into(), Capacity::Builtin(None))
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
    convert(id.into(), Capacity::Custom(capacity))
}

/// Converts a postcard-serialized v17 `SlotDex` handle (and its underlying
/// eight-byte total row) without requiring an `__instance_meta__` file.
pub fn migrate_handle_v17<S: SlotType, K: KeyEnDeOrdered>(bytes: &[u8]) -> Result<Vec<u8>> {
    convert_bytes::<S, K>(bytes, None, Capacity::Builtin(None)).map(|(_, h)| h)
}

/// [`migrate_custom_v17`] for a postcard-serialized `SlotDex` handle stored
/// outside `__instance_meta__`.
pub fn migrate_custom_handle_v17<S: SlotType, K: KeyEnDeOrdered>(
    bytes: &[u8],
    capacity: u32,
) -> Result<Vec<u8>> {
    convert_bytes::<S, K>(bytes, None, Capacity::Custom(capacity)).map(|(_, h)| h)
}

/// Selects a v18 capacity of at least two for a built-in unsigned index
/// whose v17 capacity exceeds `u32::MAX`. Repeat the same choice for aliases.
pub fn migrate_v17_with_capacity<S: SlotType, K: KeyEnDeOrdered>(
    id: impl Into<InstanceId>,
    capacity: u32,
) -> Result<SlotDex<S, K>> {
    convert(id.into(), Capacity::Builtin(Some(capacity)))
}

/// [`migrate_v17_with_capacity`] for a serialized handle.
pub fn migrate_handle_v17_with_capacity<S: SlotType, K: KeyEnDeOrdered>(
    bytes: &[u8],
    capacity: u32,
) -> Result<Vec<u8>> {
    convert_bytes::<S, K>(bytes, None, Capacity::Builtin(Some(capacity))).map(|(_, h)| h)
}

fn convert<S: SlotType, K: KeyEnDeOrdered>(
    id: InstanceId,
    requested: Capacity,
) -> Result<SlotDex<S, K>> {
    let id = InstanceId::new(id.map_id, id.ns.unwrap_or_default());
    let ns = Namespace::open(id.ns.unwrap_or_default())?;
    if ns.is_read_only() {
        return Err(VsdbError::ReadOnly {
            operation: "SlotDex migration",
        });
    }
    let path = ns.meta_path(id.map_id);
    let bytes = fs::read(&path)?;
    let (dex, handle) = convert_bytes::<S, K>(&bytes, Some(id), requested)?;
    if bytes != handle {
        atomic_write_file(&path, &handle)?;
    } else {
        crate::sync_parent(&path)?;
    }
    Ok(dex)
}

fn convert_bytes<S: SlotType, K: KeyEnDeOrdered>(
    bytes: &[u8],
    expected: Option<InstanceId>,
    requested: Capacity,
) -> Result<(SlotDex<S, K>, Vec<u8>)> {
    let (frame, tail) = postcard::take_from_bytes::<&[u8]>(bytes)?;
    if !tail.is_empty() {
        return Err(invalid("trailing SlotDex metadata"));
    }
    if frame.starts_with(b"VSTYPE04") {
        let dex: SlotDex<S, K> = crate::exactly(bytes)?;
        if dex.namespace().is_read_only() {
            return Err(VsdbError::ReadOnly {
                operation: "SlotDex migration",
            });
        }
        if expected.is_some_and(|e| dex.instance_id() != e) {
            return Err(invalid("invalid v18 SlotDex identity"));
        }
        if let Capacity::Builtin(Some(wanted)) = requested {
            // The runtime decoder above validated the complete prefix frame.
            let (_, stored, _): (u8, u32, bool) = crate::exactly(&frame[36..])?;
            if stored != wanted {
                return Err(invalid("different SlotDex replacement capacity"));
            }
        }
        return Ok((dex, bytes.to_vec()));
    }
    if !matches!(requested, Capacity::Custom(_))
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
    let payload = legacy_payload::<SlotDex<S, K>>(frame)?;
    let ((version, LegacyRaw(mut store)), rest) =
        postcard::take_from_bytes::<(u8, LegacyRaw)>(payload)?;
    if store.namespace().is_read_only() {
        return Err(VsdbError::ReadOnly {
            operation: "SlotDex migration",
        });
    }
    let id = store.instance_id();
    if version != 2 || expected.is_some_and(|e| id != e) {
        return Err(invalid("invalid v17 SlotDex configuration or identity"));
    }
    let (old, swap): (S, bool) = crate::exactly(rest)?;
    let capacity = match requested {
        Capacity::Custom(capacity) => {
            if S::tier_stride(capacity, 1) != Some(old) {
                return Err(invalid("custom v17 SlotDex capacity mismatch"));
            }
            capacity
        }
        Capacity::Builtin(replacement) => {
            // All three built-in unsigned coordinates use postcard's unsigned varint.
            let (old, _): (u128, bool) = crate::exactly(rest)?;
            let Ok(capacity) = u32::try_from(old) else {
                return resize::convert::<S, K>(&mut store, old, swap, replacement);
            };
            if replacement.is_some_and(|r| r != capacity) {
                return Err(invalid(
                    "capacity replacement requires an oversized v17 SlotDex",
                ));
            }
            capacity
        }
    };
    if capacity < 2 {
        return Err(invalid("invalid v17 SlotDex capacity"));
    }
    let (total, generation) = read_total(&store)?;
    if generation.is_some_and(|g| g != 0) {
        return Err(invalid("v18 SlotDex already modified"));
    }
    if matches!(requested, Capacity::Custom(_)) {
        verify_entries::<S, K>(&store, total)?;
        verify_tiers(&store, |level| Ok(S::tier_stride(capacity, level)))?;
    }
    let handle = target_handle::<S, K>(id, capacity, swap)?;
    let mut batch = store.batch();
    batch.insert([2], [total.to_le_bytes(), [0; 8]].concat());
    batch.commit_sync()?;
    let dex: SlotDex<S, K> = crate::exactly(&handle)?;
    Ok((dex, handle))
}

fn target_handle<S: SlotType, K: KeyEnDeOrdered>(
    id: InstanceId,
    capacity: u32,
    swap: bool,
) -> Result<Vec<u8>> {
    typed_handle::<SlotDex<S, K>>(id, &postcard::to_allocvec(&(3u8, capacity, swap))?)
}

fn read_total(store: &MapxRaw) -> Result<(u64, Option<u64>)> {
    match try_get(store, [2])?.as_deref() {
        None if store.try_range(..)?.try_step_next()?.is_none() => Ok((0, None)),
        Some(raw) if raw.len() == 8 => Ok((u64::from_le_bytes(raw.try_into().unwrap()), None)),
        Some(raw) if raw.len() == 16 => Ok((
            u64::from_le_bytes(raw[..8].try_into().unwrap()),
            Some(u64::from_le_bytes(raw[8..].try_into().unwrap())),
        )),
        _ => Err(invalid("invalid v17 SlotDex total row")),
    }
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
fn verify_entries<S: SlotType, K: KeyEnDeOrdered>(store: &MapxRaw, total: u64) -> Result<u64> {
    if store
        .try_range(..[0].as_slice())?
        .try_step_next()?
        .is_some()
        || store.try_contains_key([1])?
        || store
            .try_range([2, 0].as_slice()..)?
            .try_step_next()?
            .is_some()
    {
        return Err(invalid("unexpected v17 SlotDex row"));
    }
    let mut slots = 0;
    let mut entries = store.try_range([0]..[1])?;
    let mut level0 = store.try_range([1, 0]..[1, 1])?;
    let mut seen = 0u64;
    while let Some((key, val)) = level0.try_step_next()? {
        slots += 1;
        let slot = coordinate::<S>(&key[2..])?;
        let n = count(val)?;
        seen = seen
            .checked_add(n)
            .ok_or_else(|| invalid("slot count overflow"))?;
        let prefix = slot.to_bytes();
        for _ in 0..n {
            let (row, val) = entries
                .try_step_next()?
                .ok_or_else(|| invalid("missing v17 slot entry"))?;
            let suffix = row[1..]
                .strip_prefix(prefix.as_ref())
                .ok_or_else(|| invalid("v17 slot entry mismatch"))?;
            if !val.is_empty() || !K::from_slice(suffix)?.with_ordered_bytes(|v| v == suffix) {
                return Err(invalid("noncanonical v17 SlotDex entry"));
            }
        }
    }
    if seen != total || entries.try_step_next()?.is_some() {
        return Err(invalid("v17 SlotDex total mismatch"));
    }
    Ok(slots)
}

fn verify_tiers<S: SlotType>(
    store: &MapxRaw,
    stride: impl Fn(u8) -> Result<Option<S>>,
) -> Result<()> {
    let mut previous = 0u8;
    let mut rows = store.try_range([1, 1].as_slice()..[2].as_slice())?;
    let mut cur_row = rows.try_step_next()?.map(|(k, v)| (k.to_vec(), v.to_vec()));
    while let Some((ref first_k, _)) = cur_row {
        let level = *first_k
            .get(1)
            .ok_or_else(|| invalid("invalid v17 tier tag"))?;
        if previous.checked_add(1u8) != Some(level) {
            return Err(invalid("missing v17 tier"));
        }
        let stride = stride(level)?;
        let mut lower = store.try_range([1, previous]..[1, level])?;
        let mut cur_low = lower
            .try_step_next()?
            .map(|(k, v)| (k.to_vec(), v.to_vec()));
        while cur_row.as_ref().is_some_and(|(k, _)| k[1] == level) {
            let (k, v) = cur_row.take().expect("checked");
            cur_row = rows.try_step_next()?.map(|(k, v)| (k.to_vec(), v.to_vec()));
            let floor = coordinate::<S>(&k[2..])?;
            let mut sum = 0u64;
            while let Some((ref low, _)) = cur_low {
                let slot = coordinate::<S>(&low[2..])?;
                if stride.map_or(S::MIN, |s| slot.floor_align(s)) != floor {
                    break;
                }
                let (_, lv) = cur_low.take().expect("checked");
                cur_low = lower
                    .try_step_next()?
                    .map(|(k, v)| (k.to_vec(), v.to_vec()));
                sum = sum
                    .checked_add(count(&lv)?)
                    .ok_or_else(|| invalid("tier count overflow"))?;
            }
            if sum != count(&v)? {
                return Err(invalid("incompatible v17 tier alignment or count"));
            }
        }
        if cur_low.is_some() {
            return Err(invalid("incomplete v17 tier"));
        }
        previous = level;
    }
    Ok(())
}
