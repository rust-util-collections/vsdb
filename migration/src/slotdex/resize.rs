//! Rebuild upper tiers from immutable level-zero rows in the original prefix.
use super::{coordinate, count, invalid, read_total, target_handle, verify_entries, verify_tiers};
use crate::{exactly, sync_parent};
use std::{fs, io};
use vsdb::{KeyEnDeOrdered, Result, SlotDex, SlotType};
use vsdb_core::{MapxRaw, common::atomic_write_file};

pub(super) fn convert<S: SlotType, K: KeyEnDeOrdered>(
    store: &mut MapxRaw,
    old: u128,
    swap: bool,
    requested: Option<u32>,
) -> Result<(SlotDex<S, K>, Vec<u8>)> {
    let id = store.instance_id();
    let ns = store.namespace();
    // Serialize old aliases choosing a capacity and rebuilding the same rows.
    // The directory inode stays stable across atomic sidecar replacements.
    let guard = fs::File::open(ns.system_dir())?;
    guard.lock()?;
    let path = ns.meta_path(id.map_id).with_extension("slotdex-v17");
    let record = match fs::read(&path) {
        Ok(bytes) => Some(bytes),
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.into()),
    };
    let prior = record
        .as_deref()
        .map(exactly::<(u128, u32, &[u8])>)
        .transpose()?;
    let capacity = requested
        .or(prior.map(|(_, cap, _)| cap))
        .unwrap_or(u32::MAX);
    if capacity < 2 {
        return Err(invalid("SlotDex replacement capacity must be at least two"));
    }
    let handle = target_handle::<S, K>(id, capacity, swap)?;
    let (total, generation) = read_total(store)?;
    if let Some(prior) = prior {
        if prior != (old, capacity, handle.as_slice()) {
            return Err(invalid("conflicting SlotDex capacity conversion"));
        }
        sync_parent(&path)?;
        if generation.is_some() {
            store.try_sync_wal()?;
            return Ok((exactly(&handle)?, handle));
        }
    } else if generation.is_some() {
        return Err(invalid("missing SlotDex capacity conversion record"));
    }

    let slots = verify_entries::<S, K>(store, total)?;
    if prior.is_none() {
        verify_tiers(store, |level| {
            // v17 saturates an overflowing unsigned stride to the coordinate maximum.
            let bytes = old.saturating_pow(u32::from(level)).to_be_bytes();
            let start = bytes.len() - S::MAX.to_bytes().as_ref().len();
            if bytes[..start].iter().any(|&b| b != 0) {
                Ok(Some(S::MAX))
            } else {
                coordinate::<S>(&bytes[start..]).map(Some)
            }
        })?;
        atomic_write_file(&path, &postcard::to_allocvec(&(old, capacity, &handle))?)?;
    }

    // A retry can discard any partial upper tiers. Entries and level zero
    // stay authoritative. Only the final WAL fence publishes the v18 total.
    let mut batch = store.batch();
    batch.delete_range(&[1, 1], &[2]);
    batch.commit()?;
    let mut rows = slots;
    let mut level = 0u8;
    while rows > u64::from(capacity) {
        level = level
            .checked_add(1)
            .ok_or_else(|| invalid("too many SlotDex tiers"))?;
        rows = rebuild_level::<S>(store, level, capacity)?;
    }
    let mut batch = store.batch();
    batch.insert([2], [total.to_le_bytes(), [0; 8]].concat());
    batch.commit_sync()?;
    Ok((exactly(&handle)?, handle))
}

fn rebuild_level<S: SlotType>(store: &mut MapxRaw, level: u8, capacity: u32) -> Result<u64> {
    let reader = store.reader();
    let view = reader.read_view();
    let mut lower = view.try_range([1, level - 1]..[1, level])?;
    let stride = S::tier_stride(capacity, level);
    let mut next = || -> Result<Option<(S, u64)>> {
        lower
            .try_step_next()?
            .map(|(key, raw)| {
                let slot = coordinate::<S>(&key[2..])?;
                Ok((stride.map_or(S::MIN, |s| slot.floor_align(s)), count(raw)?))
            })
            .transpose()
    };
    let mut current = next()?;
    let mut rows = 0u64;
    let mut batch = store.batch();
    while let Some((floor, mut sum)) = current {
        current = next()?;
        while let Some((f, n)) = current {
            if f != floor {
                break;
            }
            sum = sum
                .checked_add(n)
                .ok_or_else(|| invalid("SlotDex tier count overflow"))?;
            current = next()?;
        }
        let key = [&[1, level], floor.to_bytes().as_ref()].concat();
        batch.insert(key, sum.to_le_bytes());
        rows += 1;
        if rows.is_multiple_of(4096) {
            batch.commit()?;
            batch = store.batch();
        }
    }
    batch.commit()?;
    Ok(rows)
}
