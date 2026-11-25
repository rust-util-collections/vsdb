//! Slot-based index for efficient, timestamp-based paged queries.
//!
//! [`SlotDex`] is a skip-list-like data structure ideal for indexing and
//! querying large datasets where entries are associated with a slot
//! (e.g., a timestamp or sequence number).
//!
//! # Storage model (single-handle, crash-atomic)
//!
//! All persistent state lives in **one** [`vsdb_core::MapxRaw`] handle, partitioned
//! by a leading tag byte:
//!
//! ```text
//! [0x00 | slot_be | key_be]      -> []        one row per entry
//! [0x01 | level:u8 | floor_be]   -> u64 LE    bucket counts
//! [0x02]                         -> [total, generation] (two u64 LE)
//! ```
//!
//! Level `0` holds per-slot entry counts; level `l >= 1` coarsens slots
//! by `tier_capacity^l` (the tier acceleration stack). Each mutation,
//! including an entire `insert_batch` call, commits related rows in one
//! atomic engine batch.
//! Recovery hydrates runtime caches; it does not rebuild persistent counts.
//! Callers can split large imports across calls to bound temporary memory;
//! earlier calls remain committed if a later call fails. Atomicity does not
//! imply an fsync per call. Writers must be externally serialized.
//!
//! The serialized form of a `SlotDex` (its typed handle metadata) is the
//! raw prefix of the single handle plus the two creation-time constants
//! `tier_capacity` and `swap_order`. It is **create-time constant**:
//! growing or shrinking the tier stack only writes ordinary data rows,
//! never new handles, so the metadata saved once at creation stays valid
//! for the lifetime of the instance.

mod query;
mod slot_type;
pub use query::SlotDexIter;
use query::{Query, level0_range};
use slot_type::sealed::SlotBytes;

pub use slot_type::SlotType;

use crate::{
    KeyEnDeOrdered,
    common::{
        InstanceId, PrefixFrame, ensure_process_writable, ensure_writable,
        error::{Result, VsdbError},
        staged::{StagedRows, prefix_successor},
    },
};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::BTreeMap,
    fmt,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
    result::Result as StdResult,
};
use vsdb_core::basic::mapx_raw::{MapxRaw, MapxRawReadView};

pub(crate) type EntryCnt = u64;

type PageSize = u16;
type PageIndex = u32;

// Namespace tags (first key byte).
const TAG_ENTRY: u8 = 0x00;
const TAG_LEVEL: u8 = 0x01;
const TAG_TOTAL: u8 = 0x02;

/// Serialized-payload layout version. Guards against positionally
/// decoding metadata written by the pre-single-handle layout (which
/// began with a different field sequence).
const LAYOUT_VERSION: u8 = 3;

/// A skip-list-like data structure for fast, timestamp-based paged queries.
///
/// `SlotDex` organizes data into "slots" (e.g., timestamps or sequence numbers),
/// which are then grouped into tier levels. This hierarchical structure allows
/// for rapid seeking and counting, making it highly efficient for pagination
/// and range queries over large datasets.
///
/// The slot type `S` must implement [`SlotType`]; built-in support covers
/// fixed-width signed and unsigned integers.
///
/// Every mutation is applied through a single atomic engine write batch,
/// so a crash can never leave the index internally inconsistent.
///
/// # Handle ownership
///
/// Reads through independently restored handles capture a consistent snapshot.
/// Serialize all mutations across aliases. An obsolete writer returns an error;
/// restore a fresh handle to resume writing. `Clone` makes a deep independent copy.
pub struct SlotDex<S, K>
where
    S: SlotType,
    K: KeyEnDeOrdered,
{
    store: MapxRaw,
    tier_capacity: u32,
    swap_order: bool,
    /// In-memory mirror of the persisted total row.
    total_cache: EntryCnt,
    generation: u64,
    /// In-memory caches of the tier levels (`levels[i]` = level `i + 1`).
    /// Hydrated from the store on open; level 0 (per-slot counts) is
    /// walked on disk and never cached.
    levels: Vec<Level<S>>,
    /// In-memory count of the committed level-0 rows (distinct populated
    /// slots). Consulted only by the "no tiers yet" growth gate, so it is
    /// kept exact whenever `levels` is empty (seeded on open / re-seeded
    /// when tier truncation empties `levels`; both scans are bounded —
    /// a tier-less committed index never exceeds `tier_capacity + 1`
    /// slot rows) and may go stale while tiers exist.
    slot_rows: EntryCnt,
    _p: PhantomData<K>,
}

/// In-memory cache of one tier level (level index >= 1).
#[derive(Clone)]
struct Level<S> {
    floor_base: Option<S>,
    buckets: BTreeMap<S, EntryCnt>,
}

impl<S: SlotType> Level<S> {
    fn new(level: u8, tier_capacity: u32) -> Self {
        Self {
            floor_base: S::tier_stride(tier_capacity, level),
            buckets: BTreeMap::new(),
        }
    }
}

fn aligned<S: SlotType>(slot: S, stride: Option<S>) -> S {
    stride.map_or(S::MIN, |s| slot.floor_align(s))
}

// =========================================================================
// Key codecs
// =========================================================================

fn entry_key<S: SlotType, K: KeyEnDeOrdered>(slot: &S, k: &K) -> Vec<u8> {
    k.with_ordered_bytes(|bytes| {
        let mut key = Vec::with_capacity(1 + S::Bytes::LEN + bytes.len());
        key.push(TAG_ENTRY);
        key.extend_from_slice(slot.to_bytes().as_ref());
        key.extend_from_slice(bytes);
        key
    })
}

fn level_key<S: SlotType>(level: u8, floor: &S) -> Vec<u8> {
    let f = floor.to_bytes();
    let mut v = Vec::with_capacity(2 + S::Bytes::LEN);
    v.push(TAG_LEVEL);
    v.push(level);
    v.extend_from_slice(f.as_ref());
    v
}

fn level_prefix(level: u8) -> [u8; 2] {
    [TAG_LEVEL, level]
}

const TOTAL_KEY: [u8; 1] = [TAG_TOTAL];

fn encode_cnt(v: EntryCnt) -> [u8; 8] {
    v.to_le_bytes()
}

fn decode_cnt(raw: &[u8]) -> EntryCnt {
    let mut b = [0u8; 8];
    b.copy_from_slice(&raw[..8]);
    EntryCnt::from_le_bytes(b)
}

fn decode_slot<S: SlotType>(raw: &[u8]) -> S {
    let mut bytes = S::Bytes::zeroed();
    bytes.as_mut().copy_from_slice(raw);
    S::from_bytes(bytes).expect("SlotDex: corrupt slot bytes")
}

fn decode_level_row<S: SlotType>(key: &[u8], value: &[u8]) -> (S, EntryCnt) {
    (decode_slot(&key[2..]), decode_cnt(value))
}

fn slot_entry_cnt<S: SlotType>(
    slot: S,
    get: impl FnOnce(&[u8]) -> Option<Vec<u8>>,
) -> EntryCnt {
    let row = if S::Bytes::LEN <= 32 {
        let mut key = [0; 34];
        key[0] = TAG_LEVEL;
        key[2..2 + S::Bytes::LEN].copy_from_slice(slot.to_bytes().as_ref());
        get(&key[..2 + S::Bytes::LEN])
    } else {
        get(&level_key(0, &slot))
    };
    row.map_or(0, |v| decode_cnt(&v))
}

fn total_record(total: u64, generation: u64) -> [u8; 16] {
    let mut record = [0; 16];
    record[..8].copy_from_slice(&total.to_le_bytes());
    record[8..].copy_from_slice(&generation.to_le_bytes());
    record
}

fn read_total(row: Option<Vec<u8>>) -> Result<(u64, u64)> {
    match row.as_deref() {
        Some(raw) if raw.len() == 16 => Ok((decode_cnt(raw), decode_cnt(&raw[8..]))),
        _ => Err(VsdbError::Corrupt {
            detail: "SlotDex: invalid total row length".into(),
        }),
    }
}

// =========================================================================
// Serde: typed handle metadata (create-time constant)
// =========================================================================

impl<S, K> Serialize for SlotDex<S, K>
where
    S: SlotType,
    K: KeyEnDeOrdered,
{
    fn serialize<Ser>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error>
    where
        Ser: serde::Serializer,
    {
        let mut buffer = [0; 7];
        let extra = postcard::to_slice(
            &(LAYOUT_VERSION, self.tier_capacity, self.swap_order),
            &mut buffer,
        )
        .map_err(serde::ser::Error::custom)?;
        crate::common::serialize_prefix_handle::<Self, _>(&self.store, extra, serializer)
    }
}

impl<'de, S, K> Deserialize<'de> for SlotDex<S, K>
where
    S: SlotType,
    K: KeyEnDeOrdered,
{
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        crate::common::deserialize_prefix_handle::<Self, _, _>(deserializer, |frame| {
            let PrefixFrame(store, extra) = frame;
            let ((version, capacity, swap), tail) =
                postcard::take_from_bytes::<(u8, u32, bool)>(extra)?;
            if version != LAYOUT_VERSION || !tail.is_empty() {
                return Err(VsdbError::Decode {
                    detail: format!("unsupported SlotDex layout {version}"),
                });
            }
            Self::check_capacity(capacity)?;
            Self::hydrate(store, capacity, swap)
        })
    }
}

impl<S, K> fmt::Debug for SlotDex<S, K>
where
    S: SlotType,
    K: KeyEnDeOrdered,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlotDex")
            .field("total", &self.total_cache)
            .field("levels", &self.levels.len())
            .field("tier_capacity", &self.tier_capacity)
            .field("swap_order", &self.swap_order)
            .finish()
    }
}

impl<S, K> SlotDex<S, K>
where
    S: SlotType,
    K: KeyEnDeOrdered,
{
    /// [`new`](Self::new) placed in `ns` — every internal component
    /// lands in the same namespace (a composite never spans namespaces).
    pub fn new_in(
        ns: &crate::common::Namespace,
        tier_capacity: u32,
        swap_order: bool,
    ) -> Result<Self> {
        ns.scope(|| Self::new(tier_capacity, swap_order))
    }

    /// The namespace this structure lives in.
    pub fn namespace(&self) -> crate::common::Namespace {
        self.store.namespace()
    }

    /// Creates a new `SlotDex`.
    ///
    /// # Arguments
    ///
    /// * `tier_capacity` - The capacity of each tier, controlling the granularity
    ///   of the index.  Must be at least 2: each tier coarsens slot buckets by
    ///   this factor, so a capacity of 1 would never terminate tier growth.
    /// * `swap_order` - If `true`, reverses the internal slot order. This can improve
    ///   performance for applications that primarily query in reverse chronological order.
    ///
    /// # Errors
    ///
    /// [`VsdbError::InvalidConfig`] if `tier_capacity < 2`, or
    /// [`VsdbError::ReadOnly`] in read-only mode.
    pub fn new(tier_capacity: u32, swap_order: bool) -> Result<Self> {
        // Each level's floor_base is tier_capacity^level; growth only
        // terminates when every new level strictly coarsens the previous
        // one, which requires a capacity of at least 2.
        Self::check_capacity(tier_capacity)?;
        ensure_process_writable("slot index new")?;

        let mut store = MapxRaw::new();
        let mut batch = store.batch();
        batch.insert(TOTAL_KEY, total_record(0, 0));
        batch.commit()?;
        Ok(Self {
            store,
            tier_capacity,
            swap_order,
            total_cache: 0,
            generation: 0,
            levels: Vec::new(),
            slot_rows: 0,
            _p: PhantomData,
        })
    }

    fn check_capacity(tier_capacity: u32) -> Result<()> {
        if tier_capacity < 2 {
            return Err(VsdbError::InvalidConfig {
                detail: "SlotDex tier_capacity must be >= 2".to_owned(),
            });
        }
        Ok(())
    }

    fn hydrate(store: MapxRaw, tier_capacity: u32, swap_order: bool) -> Result<Self> {
        let view = store.read_view();
        let (total_cache, generation) = read_total(view.get(TOTAL_KEY))?;
        let levels = Self::load_levels(&view, tier_capacity);
        let slot_rows = if levels.is_empty() {
            level0_range(&view, S::MIN, S::MAX).count() as u64
        } else {
            0
        };
        drop(view);
        Ok(Self {
            store,
            tier_capacity,
            swap_order,
            total_cache,
            generation,
            levels,
            slot_rows,
            _p: PhantomData,
        })
    }

    fn load_levels(view: &MapxRawReadView<'_>, tier_capacity: u32) -> Vec<Level<S>> {
        // Levels are contiguous from 1 by construction; stop at the first
        // level with no rows.
        let mut levels = vec![];
        for level in 1..=u8::MAX {
            let prefix = level_prefix(level);
            let lo = Bound::Included(prefix.to_vec());
            let hi = Bound::Excluded(
                prefix_successor(&prefix).expect("level prefix below 0xff"),
            );
            let mut cache = Level::new(level, tier_capacity);
            let mut empty = true;
            for (rk, rv) in view.range((lo, hi)) {
                let (floor, cnt) = decode_level_row::<S>(&rk, &rv);
                cache.buckets.insert(floor, cnt);
                empty = false;
            }
            if empty {
                break;
            }
            levels.push(cache);
        }

        levels
    }

    /// Deep-copies the captured state into a fresh prefix in `ns`.
    pub fn clone_in(&self, ns: &crate::common::Namespace) -> Result<Self> {
        let mut store = self.store.clone_in(ns)?;
        let (total, _) = read_total(store.get(TOTAL_KEY))?;
        let mut batch = store.batch();
        batch.insert(TOTAL_KEY, total_record(total, 0));
        batch.commit()?;
        Self::hydrate(store, self.tier_capacity, self.swap_order)
    }

    fn check_generation(&self) -> Result<()> {
        let (_, generation) = read_total(self.store.get(TOTAL_KEY))?;
        if generation != self.generation {
            return Err(VsdbError::Other {
                detail: format!(
                    "stale SlotDex writer: cached generation {}, stored {generation}",
                    self.generation
                ),
            });
        }
        Ok(())
    }

    fn next_generation(&self) -> Result<u64> {
        self.generation
            .checked_add(1)
            .ok_or_else(|| VsdbError::Other {
                detail: "SlotDex generation exhausted".into(),
            })
    }

    /// Returns the unique instance ID of this `SlotDex`.
    #[inline(always)]
    pub fn instance_id(&self) -> InstanceId {
        self.store.instance_id()
    }

    /// Persists this instance's metadata to disk so that it can be
    /// recovered later via [`from_meta`](Self::from_meta).
    ///
    /// The metadata is create-time constant (single handle + two config
    /// values), so calling this once after creation is sufficient for
    /// the lifetime of the instance.
    ///
    /// Returns the `instance_id` that should be passed to `from_meta`.
    pub fn save_meta(&self) -> Result<InstanceId> {
        let id = self.instance_id();
        crate::common::save_instance_meta(id, self)?;
        Ok(id)
    }

    /// Recovers a `SlotDex` instance from previously saved metadata.
    ///
    /// Restores runtime caches from persisted rows and counts. Each mutation
    /// or bulk-insert chunk commits those rows together; recovery does not
    /// rebuild persistent counts.
    ///
    /// Restored aliases can read concurrently. Externally serialize writers;
    /// restoring a fresh handle gives it the latest writer generation.
    pub fn from_meta(instance_id: impl Into<InstanceId>) -> Result<Self> {
        let id = instance_id.into();
        crate::common::load_instance_meta_checked(id, Self::instance_id)
    }

    // =====================================================================
    // Mutations
    // =====================================================================

    /// Inserts a key into a specified slot.
    ///
    /// The entry row, the per-slot count, every tier bucket count, any
    /// tier-growth rows, and the grand total are committed through one
    /// atomic engine batch.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot to insert the key into (e.g., a timestamp).
    /// * `k` - The key to insert.
    ///
    /// # Errors
    ///
    /// Read-only and preflight rejection leave state unchanged. Caches
    /// update only after commit succeeds. A persistence error can leave
    /// recoverable WAL data and must not be retried automatically.
    pub fn insert(&mut self, slot: S, k: K) -> Result<()> {
        ensure_writable(&self.namespace(), "slot index insert")?;
        self.check_generation()?;
        let slot = self.to_storage_slot(slot);

        let ekey = entry_key(&slot, &k);
        if self.store.contains_key(&ekey) {
            return Ok(());
        }

        let mut staged = StagedRows::new();
        let grown = self.stage_level_growth(&mut staged);

        staged.put(ekey, vec![]);
        let slot_cnt = slot_entry_cnt(slot, |k| self.store.get(k));
        staged.put(level_key(0, &slot), encode_cnt(slot_cnt + 1).to_vec());

        // Bucket increments for every level, including a just-staged one.
        let mut bumps: Vec<(usize, S, EntryCnt)> = Vec::with_capacity(self.levels.len());
        for (i, lv) in self.levels.iter().enumerate() {
            let floor = aligned(slot, lv.floor_base);
            let base = lv.buckets.get(&floor).copied().unwrap_or(0);
            staged.put(
                level_key(i as u8 + 1, &floor),
                encode_cnt(base + 1).to_vec(),
            );
            bumps.push((i, floor, base + 1));
        }
        if let Some(grown) = grown.as_ref() {
            let floor = aligned(slot, grown.floor_base);
            let base = grown.buckets.get(&floor).copied().unwrap_or(0);
            staged.put(
                level_key(self.levels.len() as u8 + 1, &floor),
                encode_cnt(base + 1).to_vec(),
            );
        }

        let generation = self.next_generation()?;
        staged.put(
            TOTAL_KEY.to_vec(),
            total_record(self.total_cache + 1, generation).to_vec(),
        );
        staged.commit(&mut self.store)?;
        self.generation = generation;

        // Disk state is committed; now apply the same changes to the caches.
        if let Some(mut grown) = grown {
            let floor = aligned(slot, grown.floor_base);
            *grown.buckets.entry(floor).or_insert(0) += 1;
            self.levels.push(grown);
        }
        for (i, floor, v) in bumps {
            self.levels[i].buckets.insert(floor, v);
        }
        if 0 == slot_cnt {
            self.slot_rows += 1;
        }
        self.total_cache += 1;

        Ok(())
    }

    /// Inserts many `(slot, key)` pairs at once.
    ///
    /// Semantically identical to calling [`insert`](Self::insert) per
    /// pair, but all rows — entries, per-slot counts, tier buckets,
    /// growth rows, and the total — are committed through **one** atomic
    /// engine batch for the whole call.
    ///
    /// Intended for bulk loads (imports, index rebuilds) where the
    /// per-key commit cost of `insert` dominates.
    ///
    /// # Errors
    ///
    /// Read-only and preflight rejection leave state unchanged. Caches
    /// update only after commit succeeds. A persistence error can leave
    /// recoverable WAL data and must not be retried automatically.
    pub fn insert_batch<I>(&mut self, items: I) -> Result<()>
    where
        I: IntoIterator<Item = (S, K)>,
    {
        ensure_writable(&self.namespace(), "slot index batch insert")?;
        self.check_generation()?;
        let mut staged = StagedRows::new();
        // Levels staged for growth during this batch (appended to
        // `self.levels` only after the commit succeeds).
        let mut grown: Vec<Level<S>> = vec![];
        // Cache deltas: (level_idx, floor) -> new count, applied on success.
        let mut bumps: BTreeMap<(usize, S), EntryCnt> = BTreeMap::new();
        // Number of floors introduced by `bumps` beyond each level's cache.
        // Updating this once per new floor keeps the per-key growth gate O(1).
        let mut pending_new_floors = vec![0usize; self.levels.len()];
        // Level-0 rows newly staged by earlier groups of this batch:
        // keeps the no-tiers growth gate in step with serial cadence.
        let mut pending_slot_rows: EntryCnt = 0;
        let mut total_added: EntryCnt = 0;

        for (slot, k) in items {
            let slot = self.to_storage_slot(slot);
            let ekey = entry_key(&slot, &k);
            if staged.get_over(&self.store, &ekey).is_some() {
                continue;
            }

            // Match serial `insert` exactly: each unique key observes the
            // tiers and bucket counts staged by every earlier key.
            if let Some(lv) = self.stage_level_growth_over(
                &mut staged,
                &grown,
                &bumps,
                &pending_new_floors,
                pending_slot_rows,
            ) {
                grown.push(lv);
                pending_new_floors.push(0);
            }

            staged.put(ekey, vec![]);
            let slot_key = level_key(0, &slot);
            let slot_cnt = staged
                .get_over(&self.store, &slot_key)
                .map(|raw| decode_cnt(&raw))
                .unwrap_or(0);
            staged.put(slot_key, encode_cnt(slot_cnt + 1).to_vec());
            if 0 == slot_cnt {
                pending_slot_rows += 1;
            }

            for (i, lv) in self
                .levels
                .iter()
                .map(|l| (&l.floor_base, &l.buckets))
                .chain(grown.iter().map(|l| (&l.floor_base, &l.buckets)))
                .enumerate()
            {
                let (floor_base, buckets) = lv;
                let floor = aligned(slot, *floor_base);
                let v = bumps.entry((i, floor)).or_insert_with(|| {
                    buckets.get(&floor).copied().unwrap_or_else(|| {
                        pending_new_floors[i] += 1;
                        0
                    })
                });
                *v += 1;
                staged.put(level_key(i as u8 + 1, &floor), encode_cnt(*v).to_vec());
            }
            total_added += 1;
        }

        if 0 == total_added {
            return Ok(());
        }

        let generation = self.next_generation()?;
        staged.put(
            TOTAL_KEY.to_vec(),
            total_record(self.total_cache + total_added, generation).to_vec(),
        );
        staged.commit(&mut self.store)?;
        self.generation = generation;

        self.levels.extend(grown);
        for ((i, floor), v) in bumps {
            self.levels[i].buckets.insert(floor, v);
        }
        self.slot_rows += pending_slot_rows;
        self.total_cache += total_added;

        Ok(())
    }

    /// Removes a key from a specified slot.
    ///
    /// All row updates are committed through one atomic engine batch.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot to remove the key from.
    /// * `k` - The key to remove.
    ///
    /// Removing an absent key is a no-op.
    ///
    /// # Errors
    ///
    /// [`VsdbError::ReadOnly`] in read-only mode, or the batch commit
    /// error. Preflight rejection leaves state unchanged; caches update
    /// only after commit succeeds. A persistence error can leave recoverable
    /// WAL data and must not be retried automatically.
    pub fn remove(&mut self, slot: S, k: &K) -> Result<()> {
        ensure_writable(&self.namespace(), "slot index remove")?;
        self.check_generation()?;
        let slot = self.to_storage_slot(slot);

        let ekey = entry_key(&slot, k);
        if !self.store.contains_key(&ekey) {
            return Ok(());
        }

        let mut staged = StagedRows::new();
        staged.del(ekey);

        // Shrink degenerate top levels (structural maintenance), based on
        // the pre-removal state.
        let mut kept = self.levels.len();
        while kept > 0 && self.levels[kept - 1].buckets.len() < 2 {
            // `self.levels[i]` is level `i + 1`, so the top being
            // dropped is level `kept`.
            for floor in self.levels[kept - 1].buckets.keys() {
                staged.del(level_key(kept as u8, floor));
            }
            kept -= 1;
        }

        let slot_cnt = slot_entry_cnt(slot, |k| self.store.get(k));
        if slot_cnt <= 1 {
            staged.del(level_key(0, &slot));
        } else {
            staged.put(level_key(0, &slot), encode_cnt(slot_cnt - 1).to_vec());
        }

        let mut decs: Vec<(usize, S, Option<EntryCnt>)> = Vec::with_capacity(kept);
        for (i, lv) in self.levels.iter().take(kept).enumerate() {
            let floor = aligned(slot, lv.floor_base);
            let cnt = match lv.buckets.get(&floor).copied() {
                Some(n) => n,
                None => continue,
            };
            if cnt <= 1 {
                staged.del(level_key(i as u8 + 1, &floor));
                decs.push((i, floor, None));
            } else {
                staged.put(level_key(i as u8 + 1, &floor), encode_cnt(cnt - 1).to_vec());
                decs.push((i, floor, Some(cnt - 1)));
            }
        }

        let generation = self.next_generation()?;
        staged.put(
            TOTAL_KEY.to_vec(),
            total_record(self.total_cache - 1, generation).to_vec(),
        );
        staged.commit(&mut self.store)?;
        self.generation = generation;

        let had_tiers = !self.levels.is_empty();
        self.levels.truncate(kept);
        for (i, floor, v) in decs {
            match v {
                Some(v) => {
                    self.levels[i].buckets.insert(floor, v);
                }
                None => {
                    self.levels[i].buckets.remove(&floor);
                }
            }
        }
        if 0 == kept && had_tiers {
            // Truncation just re-entered the tier-less state, where the
            // growth gate relies on `slot_rows` being exact — re-seed it
            // from the committed rows. Bounded: dropping level 1 required
            // it to hold at most one bucket, i.e. every populated slot
            // lies within one `tier_capacity`-wide window.
            self.slot_rows = level0_range(&self.store.read_view(), S::MIN, S::MAX)
                .count() as EntryCnt;
        } else if slot_cnt <= 1 {
            // This remove deleted the slot's level-0 row.
            self.slot_rows = self.slot_rows.saturating_sub(1);
        }
        self.total_cache = self.total_cache.saturating_sub(1);
        Ok(())
    }

    /// Clears the `SlotDex`, removing all entries and tier levels.
    ///
    /// The wipe is a **single atomic engine write batch** (one
    /// engine-level range tombstone), so a crash can never expose a
    /// partially-cleared index; the in-memory caches are reset to match.
    ///
    /// # Errors
    ///
    /// [`VsdbError::ReadOnly`] in read-only mode, or the batch commit
    /// error. Preflight rejection leaves state unchanged; caches update
    /// only after commit succeeds. A persistence error can leave recoverable
    /// WAL data and must not be retried automatically.
    pub fn clear(&mut self) -> Result<()> {
        ensure_writable(&self.namespace(), "slot index clear")?;
        self.check_generation()?;
        let generation = self.next_generation()?;
        let mut batch = self.store.batch_wiped();
        batch.insert(TOTAL_KEY, total_record(0, generation));
        batch.commit()?;
        self.generation = generation;
        self.levels.clear();
        self.slot_rows = 0;
        self.total_cache = 0;
        Ok(())
    }

    // =====================================================================
    // Tier growth
    // =====================================================================

    /// If the top level (or level 0 when no tiers exist yet) exceeds
    /// `tier_capacity` buckets, build the next coarser level from it and
    /// stage its rows. Returns the new level's cache; the caller pushes
    /// it onto `self.levels` after the batch commits.
    fn stage_level_growth(&self, staged: &mut StagedRows) -> Option<Level<S>> {
        self.stage_level_growth_over(staged, &[], &BTreeMap::new(), &[], 0)
    }

    /// Growth check that also sees levels grown earlier in the same
    /// (not-yet-committed) bulk operation, plus pending bucket updates.
    /// `pending_new_floors` counts pending floors absent from each cache;
    /// updates to existing floors do not affect the growth threshold.
    /// `pending_slot_rows` is the number of level-0 rows staged earlier
    /// in the same operation (0 outside `insert_batch`), so a single
    /// bulk load promotes tiers mid-batch exactly like serial inserts.
    fn stage_level_growth_over(
        &self,
        staged: &mut StagedRows,
        grown: &[Level<S>],
        bumps: &BTreeMap<(usize, S), EntryCnt>,
        pending_new_floors: &[usize],
        pending_slot_rows: EntryCnt,
    ) -> Option<Level<S>> {
        let n = self.levels.len() + grown.len();
        let top = grown.last().or_else(|| self.levels.last());
        let rows = if let Some(top) = top {
            top.buckets.len() as u64
                + pending_new_floors.get(n - 1).copied().unwrap_or(0) as u64
        } else {
            self.slot_rows + pending_slot_rows
        };
        if rows <= u64::from(self.tier_capacity) {
            return None;
        }

        let new_level_no = u8::try_from(n + 1).ok()?;
        let mut newtop = Level::new(new_level_no, self.tier_capacity);
        if let Some(top) = top {
            let top_idx = n - 1;
            let pending = bumps.range((top_idx, S::MIN)..=(top_idx, S::MAX));
            // Merged view of the top level: committed buckets overlaid
            // with this operation's pending updates.
            let mut view = top.buckets.clone();
            for ((_, floor), v) in pending {
                view.insert(*floor, *v);
            }
            for (slot, cnt) in view {
                let floor = aligned(slot, newtop.floor_base);
                *newtop.buckets.entry(floor).or_insert(0) += cnt;
            }
        } else {
            // No tiers yet: build from the merged (committed ⊕ staged)
            // level-0 stream so earlier keys in this batch are included.
            for (rk, rv) in staged.scan_prefix(&self.store, &level_prefix(0)) {
                let (slot, cnt) = decode_level_row::<S>(&rk, &rv);
                let floor = aligned(slot, newtop.floor_base);
                *newtop.buckets.entry(floor).or_insert(0) += cnt;
            }
        }

        for (floor, cnt) in &newtop.buckets {
            staged.put(level_key(new_level_no, floor), encode_cnt(*cnt).to_vec());
        }
        Some(newtop)
    }

    // =====================================================================
    // Disk walks (level 0 and entry rows)
    // =====================================================================

    fn query(&self) -> Query<'_, S> {
        let view = self.store.read_view();
        let (total, generation) =
            read_total(view.get(TOTAL_KEY)).expect("corrupt SlotDex total");
        let levels = if generation == self.generation {
            Cow::Borrowed(self.levels.as_slice())
        } else {
            Cow::Owned(Self::load_levels(&view, self.tier_capacity))
        };
        Query {
            view,
            total,
            levels,
            capacity: self.tier_capacity,
        }
    }

    /// Number of entries currently stored across all slots.
    pub fn total(&self) -> EntryCnt {
        read_total(self.store.get(TOTAL_KEY))
            .expect("corrupt SlotDex total")
            .0
    }

    /// Retrieves an offset-based page in slot order. Keys within each slot
    /// are always ascending. Each call captures one committed read view.
    pub fn page(
        &self,
        slots: impl RangeBounds<S>,
        page_size: PageSize,
        page_index: PageIndex,
        order: Order,
    ) -> Vec<K> {
        let iter = self.scan(slots, order, u64::from(page_size) * u64::from(page_index));
        iter.take(usize::from(page_size)).collect()
    }

    /// Streams keys from one snapshot. Physical descending traversal groups
    /// small adjacent slots and streams oversized slots individually.
    pub fn iter(
        &self,
        slots: impl RangeBounds<S>,
        order: Order,
    ) -> SlotDexIter<'_, S, K> {
        self.scan(slots, order, 0)
    }

    fn scan(
        &self,
        slots: impl RangeBounds<S>,
        order: Order,
        skip: u64,
    ) -> SlotDexIter<'_, S, K> {
        let bounds =
            inclusive_slots(&slots).map(|(lo, hi)| self.transform_range(lo, hi));
        SlotDexIter::new(
            self.query(),
            bounds,
            (order == Order::Desc) ^ self.swap_order,
            skip,
        )
    }

    /// Counts entries in a slot range at one committed sequence.
    pub fn count(&self, slots: impl RangeBounds<S>) -> EntryCnt {
        inclusive_slots(&slots).map_or(0, |(lo, hi)| {
            let (lo, hi) = self.transform_range(lo, hi);
            self.query().count(lo, hi)
        })
    }

    // --- Private Helper Methods ---

    // Convert a logical slot (user perspective) to a storage slot (internal key).
    #[inline(always)]
    fn to_storage_slot(&self, logical_slot: S) -> S {
        if self.swap_order {
            !logical_slot
        } else {
            logical_slot
        }
    }

    // Transform a logical range [min, max] into a storage range and direction flag.
    // Returns (storage_min, storage_max, storage_is_reversed_relative_to_logical)
    fn transform_range(&self, logical_min: Option<S>, logical_max: Option<S>) -> (S, S) {
        let min = logical_min.unwrap_or(S::MIN);
        let max = logical_max.unwrap_or(S::MAX);

        if self.swap_order {
            // If storage is reversed:
            // logical [10, 20] -> storage [!20, !10]
            // And the storage order is reversed relative to logical order.
            (!max, !min)
        } else {
            (min, max)
        }
    }
}

impl<S: SlotType, K: KeyEnDeOrdered> Clone for SlotDex<S, K> {
    fn clone(&self) -> Self {
        self.clone_in(&self.namespace())
            .expect("SlotDex deep copy failed")
    }
}

/// Convenience alias for `SlotDex<u32, K>`.
pub type SlotDex32<K> = SlotDex<u32, K>;
/// Convenience alias for `SlotDex<u64, K>`.
pub type SlotDex64<K> = SlotDex<u64, K>;

/// Slot order used by [`SlotDex::page`] and [`SlotDex::iter`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Order {
    /// Smallest slot first.
    #[default]
    Asc,
    /// Largest slot first.
    Desc,
}

/// A slot range as inclusive `Option` bounds (`None` = open), or `None`
/// if the range is empty.
fn inclusive_slots<S: SlotType>(
    slots: &impl RangeBounds<S>,
) -> Option<(Option<S>, Option<S>)> {
    let lo = match slots.start_bound() {
        Bound::Included(s) => Some(*s),
        Bound::Excluded(s) => Some(s.checked_succ()?),
        Bound::Unbounded => None,
    };
    let hi = match slots.end_bound() {
        Bound::Included(s) => Some(*s),
        Bound::Excluded(s) => Some(s.checked_pred()?),
        Bound::Unbounded => None,
    };
    match (&lo, &hi) {
        (Some(l), Some(h)) if l > h => None,
        _ => Some((lo, hi)),
    }
}
/// Convenience alias for `SlotDex<u128, K>`.
pub type SlotDex128<K> = SlotDex<u128, K>;

// Compile-time proof that SlotDex is Send + Sync.
fn _assert_send_sync() {
    fn require<T: Send + Sync>() {}
    require::<SlotDex<u64, u64>>();
}

#[cfg(test)]
mod test;
