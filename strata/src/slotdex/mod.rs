//! Slot-based index for efficient, timestamp-based paged queries.
//!
//! [`SlotDex`] is a skip-list-like data structure ideal for indexing and
//! querying large datasets where entries are associated with a slot
//! (e.g., a timestamp or sequence number).
//!
//! # Storage model (single-handle, crash-atomic)
//!
//! All persistent state lives in **one** [`MapxRaw`] handle, partitioned
//! by a leading tag byte:
//!
//! ```text
//! [0x00 | slot_be | key_be]      -> []        one row per entry
//! [0x01 | level:u8 | floor_be]   -> u64 LE    bucket counts
//! [0x02]                         -> u64 LE    grand total
//! ```
//!
//! Level `0` holds per-slot entry counts; level `l >= 1` coarsens slots
//! by `tier_capacity^l` (the tier acceleration stack). Because every
//! mutation stages its rows and commits them through a single engine
//! write batch, on-disk state is always internally consistent — there is
//! no dirty flag and no rebuild-on-recovery path.
//!
//! The serialized form of a `SlotDex` (its typed handle metadata) is the
//! raw prefix of the single handle plus the two creation-time constants
//! `tier_capacity` and `swap_order`. It is **create-time constant**:
//! growing or shrinking the tier stack only writes ordinary data rows,
//! never new handles, so the metadata saved once at creation stays valid
//! for the lifetime of the instance.

mod slot_type;

pub use slot_type::SlotType;

use crate::{
    KeyEnDeOrdered,
    common::{
        InstanceId,
        error::Result,
        staged::{StagedRows, prefix_successor},
    },
};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow, collections::BTreeMap, fmt, marker::PhantomData, ops::Bound,
    result::Result as StdResult,
};
use vsdb_core::basic::mapx_raw::MapxRaw;

pub(crate) type EntryCnt = u64;
type SkipNum = EntryCnt;
type TakeNum = EntryCnt;

// Declare as a signed `int`!
type Distance = i128;

type PageSize = u16;
type PageIndex = u32;

// Namespace tags (first key byte).
const TAG_ENTRY: u8 = 0x00;
const TAG_LEVEL: u8 = 0x01;
const TAG_TOTAL: u8 = 0x02;

/// Serialized-payload layout version. Guards against positionally
/// decoding metadata written by the pre-single-handle layout (which
/// began with a different field sequence).
const LAYOUT_VERSION: u8 = 2;

/// A skip-list-like data structure for fast, timestamp-based paged queries.
///
/// `SlotDex` organizes data into "slots" (e.g., timestamps or sequence numbers),
/// which are then grouped into tier levels. This hierarchical structure allows
/// for rapid seeking and counting, making it highly efficient for pagination
/// and range queries over large datasets.
///
/// The slot type `S` must implement [`SlotType`]; built-in support covers
/// `u32`, `u64`, and `u128`.
///
/// Every mutation is applied through a single atomic engine write batch,
/// so a crash can never leave the index internally inconsistent.
pub struct SlotDex<S, K>
where
    S: SlotType,
    K: Clone + Ord + KeyEnDeOrdered,
{
    store: MapxRaw,
    tier_capacity: S,
    swap_order: bool,
    /// In-memory mirror of the persisted total row.
    total_cache: EntryCnt,
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
struct Level<S> {
    floor_base: S,
    buckets: BTreeMap<S, EntryCnt>,
}

impl<S: SlotType> Level<S> {
    fn new(level: u8, tier_capacity: &S) -> Self {
        Self {
            floor_base: floor_base_of(level, tier_capacity),
            buckets: BTreeMap::new(),
        }
    }
}

/// `tier_capacity^level`, saturating to `S::MAX` on overflow (a tier
/// whose base saturates coarsens everything into a single bucket, which
/// terminates growth naturally).
fn floor_base_of<S: SlotType>(level: u8, tier_capacity: &S) -> S {
    tier_capacity
        .checked_pow(level as u32)
        .filter(|v| *v != S::MIN)
        .unwrap_or(S::MAX)
}

// =========================================================================
// Key codecs
// =========================================================================

fn entry_key<S: SlotType, K: KeyEnDeOrdered>(slot: &S, k: &K) -> Vec<u8> {
    let s = slot.to_bytes();
    let kb = k.to_bytes();
    let mut v = Vec::with_capacity(1 + s.len() + kb.len());
    v.push(TAG_ENTRY);
    v.extend_from_slice(&s);
    v.extend_from_slice(&kb);
    v
}

fn level_key<S: SlotType>(level: u8, floor: &S) -> Vec<u8> {
    let f = floor.to_bytes();
    let mut v = Vec::with_capacity(2 + f.len());
    v.push(TAG_LEVEL);
    v.push(level);
    v.extend_from_slice(&f);
    v
}

fn level_prefix(level: u8) -> Vec<u8> {
    vec![TAG_LEVEL, level]
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

// =========================================================================
// Raw-bound helpers
// =========================================================================

fn bound_to_raw(b: Bound<Vec<u8>>) -> Bound<Cow<'static, [u8]>> {
    match b {
        Bound::Included(v) => Bound::Included(Cow::Owned(v)),
        Bound::Excluded(v) => Bound::Excluded(Cow::Owned(v)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Translate a slot-space bound into a raw lower bound over entry rows.
fn entry_lower_bound<S: SlotType>(b: &Bound<S>) -> Option<Bound<Vec<u8>>> {
    match b {
        Bound::Unbounded => {
            let mut v = vec![TAG_ENTRY];
            v.extend_from_slice(&S::MIN.to_bytes());
            Some(Bound::Included(v))
        }
        Bound::Included(s) => {
            let mut v = vec![TAG_ENTRY];
            v.extend_from_slice(&s.to_bytes());
            Some(Bound::Included(v))
        }
        // Excluding slot `s` means starting at slot `s + 1`; the encoded
        // successor of the slot bytes is exactly that (fixed-width
        // order-preserving encoding). All-0xFF means `s == S::MAX`, so
        // the range is empty.
        Bound::Excluded(s) => {
            let succ = prefix_successor(&s.to_bytes())?;
            let mut v = vec![TAG_ENTRY];
            v.extend_from_slice(&succ);
            Some(Bound::Included(v))
        }
    }
}

/// Translate a slot-space bound into a raw upper bound over entry rows.
fn entry_upper_bound<S: SlotType>(b: &Bound<S>) -> Bound<Vec<u8>> {
    match b {
        Bound::Unbounded => Bound::Excluded(vec![TAG_ENTRY + 1]),
        // Including slot `s` means including every `[TAG|s|k]` row; the
        // exclusive raw bound is the successor of the `[TAG|s]` prefix
        // (all-0xFF cannot happen: the prefix starts with TAG_ENTRY = 0).
        Bound::Included(s) => {
            let mut v = vec![TAG_ENTRY];
            v.extend_from_slice(&s.to_bytes());
            Bound::Excluded(prefix_successor(&v).expect("prefix starts with 0x00"))
        }
        Bound::Excluded(s) => {
            let mut v = vec![TAG_ENTRY];
            v.extend_from_slice(&s.to_bytes());
            Bound::Excluded(v)
        }
    }
}

fn decode_entry_row<S: SlotType, K: KeyEnDeOrdered>(raw_key: &[u8]) -> (S, K) {
    let slot_len = S::MIN.to_bytes().len();
    let slot = S::from_bytes(raw_key[1..1 + slot_len].to_vec())
        .expect("SlotDex: corrupt entry-row slot bytes");
    let k = K::from_bytes(raw_key[1 + slot_len..].to_vec())
        .expect("SlotDex: corrupt entry-row key bytes");
    (slot, k)
}

fn decode_level_row<S: SlotType>(raw_key: &[u8], raw_val: &[u8]) -> (S, EntryCnt) {
    let floor = S::from_bytes(raw_key[2..].to_vec())
        .expect("SlotDex: corrupt level-row floor bytes");
    (floor, decode_cnt(raw_val))
}

// =========================================================================
// Serde: typed handle metadata (create-time constant)
// =========================================================================

impl<S, K> Serialize for SlotDex<S, K>
where
    S: SlotType,
    K: Clone + Ord + KeyEnDeOrdered,
{
    fn serialize<Ser>(&self, serializer: Ser) -> StdResult<Ser::Ok, Ser::Error>
    where
        Ser: serde::Serializer,
    {
        crate::common::serialize_typed_handle_meta::<Self, Ser>(
            &(
                LAYOUT_VERSION,
                &self.store,
                &self.tier_capacity,
                &self.swap_order,
            ),
            serializer,
        )
    }
}

impl<'de, S, K> Deserialize<'de> for SlotDex<S, K>
where
    S: SlotType,
    K: Clone + Ord + KeyEnDeOrdered,
{
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (version, store, tier_capacity, swap_order) =
            crate::common::deserialize_typed_handle_meta::<
                Self,
                (u8, MapxRaw, S, bool),
                D,
            >(deserializer)?;
        if version != LAYOUT_VERSION {
            return Err(serde::de::Error::custom(format!(
                "SlotDex: unsupported layout version {version} (expected {LAYOUT_VERSION})"
            )));
        }
        Ok(Self::hydrate(store, tier_capacity, swap_order))
    }
}

impl<S, K> fmt::Debug for SlotDex<S, K>
where
    S: SlotType,
    K: Clone + Ord + KeyEnDeOrdered,
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
    K: Clone + Ord + KeyEnDeOrdered,
{
    /// [`new`](Self::new) placed in `ns` — every internal component
    /// lands in the same namespace (a composite never spans namespaces).
    pub fn new_in(
        ns: &crate::common::Namespace,
        tier_capacity: S,
        swap_order: bool,
    ) -> Self {
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
    pub fn new(tier_capacity: S, swap_order: bool) -> Self {
        // Each level's floor_base is tier_capacity^level; growth only
        // terminates when every new level strictly coarsens the previous
        // one, which requires a capacity of at least 2.
        assert!(
            tier_capacity.as_i128() >= 2,
            "SlotDex: tier_capacity must be >= 2"
        );

        Self {
            store: MapxRaw::new(),
            tier_capacity,
            swap_order,
            total_cache: 0,
            levels: vec![],
            slot_rows: 0,
            _p: PhantomData,
        }
    }

    /// Reconnects to an existing store and rebuilds the in-memory caches
    /// (total mirror + tier level caches) from its rows.
    fn hydrate(store: MapxRaw, tier_capacity: S, swap_order: bool) -> Self {
        let total_cache = store.get(TOTAL_KEY).map(|v| decode_cnt(&v)).unwrap_or(0);

        // Levels are contiguous from 1 by construction; stop at the first
        // level with no rows.
        let mut levels = vec![];
        for level in 1..=u8::MAX {
            let prefix = level_prefix(level);
            let lo = Bound::Included(Cow::Owned(prefix.clone()));
            let hi = bound_to_raw(
                prefix_successor(&prefix)
                    .map(Bound::Excluded)
                    .expect("prefix starts with 0x01"),
            );
            let mut cache = Level::new(level, &tier_capacity);
            let mut empty = true;
            for (rk, rv) in store.range((lo, hi)) {
                let (floor, cnt) = decode_level_row::<S>(&rk, &rv);
                cache.buckets.insert(floor, cnt);
                empty = false;
            }
            if empty {
                break;
            }
            levels.push(cache);
        }

        // The growth gate reads `slot_rows` only while `levels` is empty;
        // seed it exactly for that state (bounded: a tier-less committed
        // index holds at most `tier_capacity + 1` slot rows). With tiers
        // present the mirror is unused until a truncation re-seeds it.
        let slot_rows = if levels.is_empty() {
            let lo = bound_to_raw(Bound::Included(level_key(0, &S::MIN)));
            let hi = bound_to_raw(Bound::Excluded(level_prefix(1)));
            store.range((lo, hi)).count() as EntryCnt
        } else {
            0
        };

        Self {
            store,
            tier_capacity,
            swap_order,
            total_cache,
            levels,
            slot_rows,
            _p: PhantomData,
        }
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
    /// Every mutation is applied atomically, so the recovered state is
    /// always internally consistent — there is no rebuild path.
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
    /// If the batch commit fails, neither the on-disk state nor the
    /// in-memory caches are modified.
    pub fn insert(&mut self, slot: S, k: K) -> Result<()> {
        let slot = self.to_storage_slot(slot);

        let ekey = entry_key(&slot, &k);
        if self.store.contains_key(&ekey) {
            return Ok(());
        }

        let mut staged = StagedRows::new();
        let grown = self.stage_level_growth(&mut staged);

        staged.put(ekey, vec![]);
        let slot_cnt = self.slot_entry_cnt(&slot);
        staged.put(level_key(0, &slot), encode_cnt(slot_cnt + 1).to_vec());

        // Bucket increments for every level, including a just-staged one.
        let mut bumps: Vec<(usize, S, EntryCnt)> = Vec::with_capacity(self.levels.len());
        for (i, lv) in self.levels.iter().enumerate() {
            let floor = slot.floor_align(&lv.floor_base);
            let base = lv.buckets.get(&floor).copied().unwrap_or(0);
            staged.put(
                level_key(i as u8 + 1, &floor),
                encode_cnt(base + 1).to_vec(),
            );
            bumps.push((i, floor, base + 1));
        }
        if let Some(grown) = grown.as_ref() {
            let floor = slot.floor_align(&grown.floor_base);
            let base = grown.buckets.get(&floor).copied().unwrap_or(0);
            staged.put(
                level_key(self.levels.len() as u8 + 1, &floor),
                encode_cnt(base + 1).to_vec(),
            );
        }

        staged.put(
            TOTAL_KEY.to_vec(),
            encode_cnt(self.total_cache + 1).to_vec(),
        );
        staged.commit(&mut self.store)?;

        // Disk state is committed; now apply the same changes to the caches.
        if let Some(mut grown) = grown {
            let floor = slot.floor_align(&grown.floor_base);
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
    /// If the batch commit fails, neither the on-disk state nor the
    /// in-memory caches are modified.
    pub fn insert_batch<I>(&mut self, items: I) -> Result<()>
    where
        I: IntoIterator<Item = (S, K)>,
    {
        let mut staged = StagedRows::new();
        // Levels staged for growth during this batch (appended to
        // `self.levels` only after the commit succeeds).
        let mut grown: Vec<Level<S>> = vec![];
        // Cache deltas: (level_idx, floor) -> new count, applied on success.
        let mut bumps: BTreeMap<(usize, S), EntryCnt> = BTreeMap::new();
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
                pending_slot_rows,
            ) {
                grown.push(lv);
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
                let floor = slot.floor_align(floor_base);
                let v = bumps
                    .get(&(i, floor.clone()))
                    .copied()
                    .unwrap_or_else(|| buckets.get(&floor).copied().unwrap_or(0))
                    + 1;
                staged.put(level_key(i as u8 + 1, &floor), encode_cnt(v).to_vec());
                bumps.insert((i, floor), v);
            }
            total_added += 1;
        }

        if 0 == total_added {
            return Ok(());
        }

        staged.put(
            TOTAL_KEY.to_vec(),
            encode_cnt(self.total_cache + total_added).to_vec(),
        );
        staged.commit(&mut self.store)?;

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
    /// # Panics
    ///
    /// Panics if the engine-level batch commit fails (matching the
    /// behavior of the plain collection types on engine write failure);
    /// nothing is applied in that case.
    pub fn remove(&mut self, slot: S, k: &K) {
        let slot = self.to_storage_slot(slot);

        let ekey = entry_key(&slot, k);
        if !self.store.contains_key(&ekey) {
            return;
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

        let slot_cnt = self.slot_entry_cnt(&slot);
        if slot_cnt <= 1 {
            staged.del(level_key(0, &slot));
        } else {
            staged.put(level_key(0, &slot), encode_cnt(slot_cnt - 1).to_vec());
        }

        let mut decs: Vec<(usize, S, Option<EntryCnt>)> = Vec::with_capacity(kept);
        for (i, lv) in self.levels.iter().take(kept).enumerate() {
            let floor = slot.floor_align(&lv.floor_base);
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

        staged.put(
            TOTAL_KEY.to_vec(),
            encode_cnt(self.total_cache.saturating_sub(1)).to_vec(),
        );
        staged
            .commit(&mut self.store)
            .expect("vsdb: SlotDex remove batch commit failed");

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
            self.slot_rows = self
                .level0_range(Bound::Unbounded, Bound::Unbounded)
                .count() as EntryCnt;
        } else if slot_cnt <= 1 {
            // This remove deleted the slot's level-0 row.
            self.slot_rows = self.slot_rows.saturating_sub(1);
        }
        self.total_cache = self.total_cache.saturating_sub(1);
    }

    /// Clears the `SlotDex`, removing all entries and tier levels.
    ///
    /// The wipe is a **single atomic engine write batch** (one
    /// engine-level range tombstone), so a crash can never expose a
    /// partially-cleared index; the in-memory caches are reset to match.
    pub fn clear(&mut self) {
        self.store.clear();
        self.levels.clear();
        self.slot_rows = 0;
        self.total_cache = 0;
    }

    // =====================================================================
    // Tier growth
    // =====================================================================

    /// If the top level (or level 0 when no tiers exist yet) exceeds
    /// `tier_capacity` buckets, build the next coarser level from it and
    /// stage its rows. Returns the new level's cache; the caller pushes
    /// it onto `self.levels` after the batch commits.
    fn stage_level_growth(&self, staged: &mut StagedRows) -> Option<Level<S>> {
        self.stage_level_growth_over(staged, &[], &BTreeMap::new(), 0)
    }

    /// Growth check that also sees levels grown earlier in the same
    /// (not-yet-committed) bulk operation, plus pending bucket updates.
    /// `pending_slot_rows` is the number of level-0 rows staged earlier
    /// in the same operation (0 outside `insert_batch`), so a single
    /// bulk load promotes tiers mid-batch exactly like serial inserts.
    fn stage_level_growth_over(
        &self,
        staged: &mut StagedRows,
        grown: &[Level<S>],
        bumps: &BTreeMap<(usize, S), EntryCnt>,
        pending_slot_rows: EntryCnt,
    ) -> Option<Level<S>> {
        let n = self.levels.len() + grown.len();
        let new_level_no = n as u8 + 1;

        let mut newtop = Level::new(new_level_no, &self.tier_capacity);
        if let Some(top) = grown.last().or_else(|| self.levels.last()) {
            let top_idx = n - 1;
            // Merged view of the top level: committed buckets overlaid
            // with this operation's pending updates.
            let mut view = top.buckets.clone();
            for ((i, floor), v) in bumps {
                if *i == top_idx {
                    view.insert(floor.clone(), *v);
                }
            }
            if view.len() as i128 <= self.tier_capacity.as_i128() {
                return None;
            }
            for (slot, cnt) in view {
                let floor = slot.floor_align(&newtop.floor_base);
                *newtop.buckets.entry(floor).or_insert(0) += cnt;
            }
        } else {
            // No tiers yet: level 0 acts as the current "top" tier — gate
            // on ITS row count exactly like the branch above gates on
            // `view.len()`. The count is O(1): the committed mirror
            // (`slot_rows`, exact while tier-less) plus the rows staged
            // earlier in this same operation. On promotion, build the new
            // level from the merged (committed ⊕ staged) level-0 stream,
            // so counts staged by a bulk load are folded in correctly.
            let rows = self.slot_rows + pending_slot_rows;
            if rows as i128 <= self.tier_capacity.as_i128() {
                return None;
            }
            for (rk, rv) in staged.scan_prefix(&self.store, &level_prefix(0)) {
                let (slot, cnt) = decode_level_row::<S>(&rk, &rv);
                let floor = slot.floor_align(&newtop.floor_base);
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

    /// Range over the per-slot count rows (level 0), bounds in slot space.
    fn level0_range(
        &self,
        lo: Bound<S>,
        hi: Bound<S>,
    ) -> impl DoubleEndedIterator<
        Item = (vsdb_core::common::RawKey, vsdb_core::common::RawValue),
    > + '_ {
        let lo = match &lo {
            Bound::Unbounded => Bound::Included(level_key(0, &S::MIN)),
            Bound::Included(s) => Bound::Included(level_key(0, s)),
            Bound::Excluded(s) => Bound::Excluded(level_key(0, s)),
        };
        let hi = match &hi {
            Bound::Unbounded => Bound::Excluded(level_prefix(1)),
            Bound::Included(s) => Bound::Included(level_key(0, s)),
            Bound::Excluded(s) => Bound::Excluded(level_key(0, s)),
        };
        self.store.range((bound_to_raw(lo), bound_to_raw(hi)))
    }

    /// Range over entry rows, bounds in slot space. `None` means the
    /// range is empty (an `Excluded(S::MAX)` lower bound).
    fn entry_range(
        &self,
        lo: Bound<S>,
        hi: Bound<S>,
    ) -> Option<
        impl DoubleEndedIterator<
            Item = (vsdb_core::common::RawKey, vsdb_core::common::RawValue),
        > + '_,
    > {
        let lo = entry_lower_bound(&lo)?;
        let hi = entry_upper_bound(&hi);
        Some(self.store.range((bound_to_raw(lo), bound_to_raw(hi))))
    }

    fn slot_entry_cnt(&self, slot: &S) -> EntryCnt {
        self.store
            .get(level_key(0, slot))
            .map(|v| decode_cnt(&v))
            .unwrap_or(0)
    }

    /// Descending walk over the per-slot count rows without any engine
    /// reverse iterator (mmdb backward resolution costs ~two orders of
    /// magnitude more per iterator than forward streaming).
    ///
    /// The level-1 bucket cache partitions slot space into chunks that
    /// each hold at most `tier_capacity` populated slots; the floors
    /// strictly above the window's lower bound serve as split points
    /// (descending, in memory). Each chunk is forward-scanned and
    /// reversed in memory, yielding the same stream as
    /// `level0_range(lo, hi).rev()`. Returns early when `visit` returns
    /// `false`.
    fn level0_walk_desc(
        &self,
        lo: Bound<S>,
        hi: Bound<S>,
        mut visit: impl FnMut(S, EntryCnt) -> bool,
    ) {
        // Split points: level-1 floors strictly above the lower bound,
        // within the upper bound, descending. For small tier capacities
        // the floors are thinned so each chunk spans ~32 slots — engine
        // iterator creation dominates tiny chunks, so fewer, larger
        // forward scans win even with the in-memory reversal.
        let splits: Vec<S> = match self.levels.first() {
            Some(l1) => {
                let above_lo = match &lo {
                    Bound::Included(s) | Bound::Excluded(s) => {
                        Bound::Excluded(s.clone())
                    }
                    Bound::Unbounded => Bound::Unbounded,
                };
                // `BTreeMap::range` panics on inverted windows (the engine
                // ranges below just yield nothing) — guard explicitly.
                let nonempty = match (&above_lo, &hi) {
                    (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
                    (
                        Bound::Included(a) | Bound::Excluded(a),
                        Bound::Included(b) | Bound::Excluded(b),
                    ) => a < b,
                };
                if nonempty {
                    let stride =
                        (32 / self.tier_capacity.as_i128()).clamp(1, 32) as usize;
                    l1.buckets
                        .range((above_lo, hi.clone()))
                        .rev()
                        .map(|(f, _)| f.clone())
                        .skip(stride - 1)
                        .step_by(stride)
                        .collect()
                } else {
                    vec![]
                }
            }
            None => vec![],
        };

        let mut buf: Vec<(S, EntryCnt)> = vec![];
        let mut upper = hi;
        for split in splits {
            buf.clear();
            for (rk, rv) in self.level0_range(Bound::Included(split.clone()), upper) {
                buf.push(decode_level_row::<S>(&rk, &rv));
            }
            for (slot, cnt) in buf.drain(..).rev() {
                if !visit(slot, cnt) {
                    return;
                }
            }
            upper = Bound::Excluded(split);
        }
        // Bottom chunk: from the window's own lower bound.
        buf.clear();
        for (rk, rv) in self.level0_range(lo, upper) {
            buf.push(decode_level_row::<S>(&rk, &rv));
        }
        for (slot, cnt) in buf.drain(..).rev() {
            if !visit(slot, cnt) {
                return;
            }
        }
    }

    // =====================================================================
    // Queries
    // =====================================================================

    /// Retrieves entries by page, a common use case for web services.
    ///
    /// # Arguments
    ///
    /// * `page_size` - The number of entries per page.
    /// * `page_index` - The zero-based index of the page to retrieve.
    /// * `reverse_order` - If `true`, returns entries in reverse order.
    ///
    /// # Returns
    ///
    /// A `Vec<K>` containing the entries for the specified page.
    ///
    /// # Note
    ///
    /// This is **offset-based** pagination (`page_size` × `page_index`),
    /// like SQL `LIMIT`/`OFFSET`: each call reflects the dataset as it is
    /// at that moment. If entries are inserted or removed between page
    /// requests, later pages may skip or repeat entries. Take a snapshot
    /// (or avoid concurrent mutation) when a stable full scan is required.
    pub fn get_entries_by_page(
        &self,
        page_size: PageSize,
        page_index: PageIndex, // Start from 0
        reverse_order: bool,
    ) -> Vec<K> {
        self.get_entries_by_page_slot(None, None, page_size, page_index, reverse_order)
    }

    /// Retrieves entries by page within a specified slot range.
    ///
    /// # Arguments
    ///
    /// * `slot_left_bound` - The inclusive left bound of the slot range.
    /// * `slot_right_bound` - The inclusive right bound of the slot range.
    /// * `page_size` - The number of entries per page.
    /// * `page_index` - The zero-based index of the page to retrieve.
    /// * `reverse_order` - If `true`, returns entries in reverse order.
    ///
    /// # Returns
    ///
    /// A `Vec<K>` containing the entries for the specified page and slot range.
    ///
    /// # Note
    ///
    /// Pagination is **offset-based** (see [`get_entries_by_page`]): pages
    /// are not stable across concurrent inserts/removes between requests.
    pub fn get_entries_by_page_slot(
        &self,
        slot_left_bound: Option<S>,  // Included
        slot_right_bound: Option<S>, // Included
        page_size: PageSize,
        page_index: PageIndex, // start from 0
        reverse_order: bool,
    ) -> Vec<K> {
        let (slot_min, slot_max, storage_is_reversed) =
            self.transform_range(slot_left_bound, slot_right_bound);

        if slot_max < slot_min {
            return vec![];
        }

        if 0 == page_size || 0 == self.total() {
            return vec![];
        }

        self.get_entries(
            slot_min,
            slot_max,
            page_size,
            page_index,
            reverse_order ^ storage_is_reversed,
        )
    }

    // Number of entries stored in slots strictly greater than `slot`
    // (whether the slot itself exists or not).
    fn distance_to_the_rightmost_slot(&self, slot: &S) -> Distance {
        if *slot == S::MAX {
            return 0;
        }
        self.total() as Distance
            - self.distance_to_the_leftmost_slot(slot)
            - self.slot_entry_cnt(slot) as Distance
    }

    // Exclude the slot itself-owned entries (whether it exists or not)
    fn distance_to_the_leftmost_slot(&self, slot: &S) -> Distance {
        if *slot == S::MIN {
            return 0;
        }
        let mut left_bound = S::MIN;
        let mut ret = 0;
        for lv in self.levels.iter().rev() {
            let right_bound = slot.floor_align(&lv.floor_base);
            ret += lv
                .buckets
                .range(left_bound.clone()..right_bound.clone())
                .map(|(_, cnt)| *cnt as Distance)
                .sum::<Distance>();
            left_bound = right_bound;
        }
        ret += self
            .level0_range(Bound::Included(left_bound), Bound::Excluded(slot.clone()))
            .map(|(_, v)| decode_cnt(&v) as Distance)
            .sum::<Distance>();
        ret
    }

    fn offsets_from_the_leftmost_slot(
        &self,
        slot_start: &S, // Included
        page_size: PageSize,
        page_index: PageIndex,
    ) -> (SkipNum, TakeNum) {
        let skip_n = self.distance_to_the_leftmost_slot(slot_start)
            + (page_size as Distance) * (page_index as Distance);
        (skip_n as SkipNum, page_size as TakeNum)
    }

    /// Single-pass page location using the in-memory level caches plus a
    /// bounded walk over the per-slot count rows.
    fn locate_page_start(&self, global_skip_n: EntryCnt) -> (Bound<S>, SkipNum) {
        let mut slot_start = Bound::Included(S::MIN);
        let mut remaining: u64 = global_skip_n;

        for lv in self.levels.iter().rev() {
            let mut hdr = lv
                .buckets
                .range((slot_start.clone(), Bound::Unbounded))
                .peekable();
            while let Some(entry_cnt) = hdr.next().map(|(_, cnt)| *cnt) {
                if entry_cnt > remaining {
                    break;
                } else {
                    slot_start = hdr
                        .peek()
                        .map(|(s, _)| Bound::Included((*s).clone()))
                        .unwrap_or(Bound::Excluded(S::MAX));
                    remaining -= entry_cnt;
                }
            }
        }

        let mut hdr = self
            .level0_range(slot_start.clone(), Bound::Unbounded)
            .peekable();
        while let Some(entry_cnt) = hdr.next().map(|(_, v)| decode_cnt(&v)) {
            if entry_cnt > remaining {
                break;
            } else {
                slot_start = hdr
                    .peek()
                    .map(|(rk, _)| {
                        Bound::Included(
                            S::from_bytes(rk[2..].to_vec())
                                .expect("SlotDex: corrupt level-row floor bytes"),
                        )
                    })
                    .unwrap_or(Bound::Excluded(S::MAX));
                remaining -= entry_cnt;
            }
        }

        (slot_start, remaining)
    }

    /// Single-pass reverse page location.
    ///
    /// Mirror of [`locate_page_start`](Self::locate_page_start):
    /// `global_skip_n` counts entries to skip walking from the greatest
    /// storage slot downward. Returns the upper bound at which the reverse
    /// entry walk must resume, plus the number of entries still to skip
    /// inside that boundary region.
    ///
    /// Consumed units are cut off with `Bound::Excluded(floor)`: bucket
    /// floors are left-aligned, so excluding a consumed floor also drops
    /// every finer-grained bucket (and data slot) belonging to it.
    fn locate_page_rstart(&self, global_skip_n: EntryCnt) -> (Bound<S>, SkipNum) {
        let mut slot_end: Bound<S> = Bound::Unbounded;
        let mut remaining: u64 = global_skip_n;

        for lv in self.levels.iter().rev() {
            for (floor, entry_cnt) in
                lv.buckets.range((Bound::Unbounded, slot_end.clone())).rev()
            {
                if *entry_cnt > remaining {
                    break;
                }
                slot_end = Bound::Excluded(floor.clone());
                remaining -= *entry_cnt;
            }
        }

        let mut slot_end_cell = slot_end;
        let mut remaining_cell = remaining;
        self.level0_walk_desc(
            Bound::Unbounded,
            slot_end_cell.clone(),
            |slot, entry_cnt| {
                if entry_cnt > remaining_cell {
                    return false;
                }
                slot_end_cell = Bound::Excluded(slot);
                remaining_cell -= entry_cnt;
                true
            },
        );

        (slot_end_cell, remaining_cell)
    }

    fn get_entries(
        &self,
        slot_start: S, // Included
        slot_end: S,   // Included
        page_size: PageSize,
        page_index: PageIndex,
        reverse: bool,
    ) -> Vec<K> {
        if slot_end < slot_start {
            return vec![];
        }

        if reverse {
            return self
                .get_entries_reverse(slot_start, slot_end, page_size, page_index);
        }

        let (global_skip_n, take_n) =
            self.offsets_from_the_leftmost_slot(&slot_start, page_size, page_index);

        let (slot_start_actual, local_skip_n) = self.locate_page_start(global_skip_n);

        let iter = match self.entry_range(slot_start_actual, Bound::Included(slot_end)) {
            Some(iter) => iter,
            None => return vec![],
        };

        iter.skip(local_skip_n as usize)
            .take(take_n as usize)
            .map(|(rk, _)| decode_entry_row::<S, K>(&rk).1)
            .collect()
    }

    /// Reverse-order paging: walk slots from `slot_end` down to `slot_start`
    /// in descending storage order while keeping each slot's entries in their
    /// natural ascending key order.
    ///
    /// Only the slot order is reversed, not the within-slot order: a slot is a
    /// set of keys, so its members stay ascending in every view. Reversing the
    /// whole result vector instead would corrupt within-slot order and shift
    /// page membership across slot boundaries when a slot holds >1 entry.
    ///
    /// The page start is located through
    /// [`locate_page_rstart`](Self::locate_page_rstart) — the
    /// tier-accelerated mirror of the forward path. The contributing slots
    /// are then planned from one reverse walk over the per-slot count rows
    /// (no entry data touched), and the entries are fetched with **one**
    /// forward scan over the contiguous slot interval of the page — engine
    /// iterators are expensive, so the per-slot range construction this
    /// replaces dominated the whole query.
    fn get_entries_reverse(
        &self,
        slot_start: S, // Included
        slot_end: S,   // Included
        page_size: PageSize,
        page_index: PageIndex,
    ) -> Vec<K> {
        // Skip counted from the greatest storage slot downward; entries in
        // slots above `slot_end` are prepended to the skip so the locate
        // walk can start from the global right end and consume whole tier
        // buckets without range-boundary bookkeeping.
        let global_skip_n = self.distance_to_the_rightmost_slot(&slot_end)
            + (page_size as Distance) * (page_index as Distance);
        let global_skip_n = u64::try_from(global_skip_n).unwrap_or(u64::MAX);

        let (slot_end_actual, local_skip_n) = self.locate_page_rstart(global_skip_n);

        let mut to_skip = local_skip_n as usize;
        let mut remaining = page_size as usize;

        // Plan the contributing slots (descending) from the count rows:
        // (slot, skip inside the slot, take from the slot).
        let mut plan: Vec<(S, usize, usize)> = vec![];
        self.level0_walk_desc(
            Bound::Included(slot_start.clone()),
            slot_end_actual,
            |slot, n| {
                let n = n as usize;
                if to_skip >= n {
                    to_skip -= n;
                    return true;
                }
                let take = (n - to_skip).min(remaining);
                plan.push((slot, to_skip, take));
                remaining -= take;
                to_skip = 0;
                remaining > 0
            },
        );
        let Some((last, _, _)) = plan.last() else {
            return vec![];
        };

        // One forward entry scan over the page's contiguous slot interval,
        // split into per-slot segments.
        let lo = last.clone();
        let hi = plan[0].0.clone();
        let iter = match self.entry_range(Bound::Included(lo), Bound::Included(hi)) {
            Some(iter) => iter,
            None => return vec![],
        };
        let mut segments: BTreeMap<S, Vec<K>> =
            plan.iter().map(|(s, ..)| (s.clone(), vec![])).collect();
        let quota: BTreeMap<S, (usize, usize)> = plan
            .iter()
            .map(|(s, skip, take)| (s.clone(), (*skip, *take)))
            .collect();
        let mut cur: Option<(S, usize, usize, usize)> = None; // slot, skip, take, seen
        for (rk, _) in iter {
            let (slot, k) = decode_entry_row::<S, K>(&rk);
            match &mut cur {
                Some((s, skip, take, seen)) if *s == slot => {
                    *seen += 1;
                    if *seen > *skip && segments[s].len() < *take {
                        segments.get_mut(s).expect("planned").push(k);
                    }
                }
                _ => {
                    let Some(&(skip, take)) = quota.get(&slot) else {
                        // A slot inside the interval that contributes
                        // nothing (fully consumed by the skip).
                        cur = Some((slot, usize::MAX, 0, 0));
                        continue;
                    };
                    if skip == 0 && take > 0 {
                        segments.get_mut(&slot).expect("planned").push(k);
                    }
                    cur = Some((slot, skip, take, 1));
                }
            }
        }

        // Emit slot groups in descending slot order, ascending within.
        let mut ret = Vec::with_capacity(page_size as usize);
        for (slot, ..) in &plan {
            ret.extend(segments.remove(slot).expect("planned"));
        }
        ret
    }

    /// Calculates the number of entries within a given slot range.
    ///
    /// This method can be used for data statistics and is called by `total_by_slot`.
    ///
    /// # Arguments
    ///
    /// * `slot_start` - The starting slot of the range.
    /// * `slot_end` - The ending slot of the range.
    ///
    /// # Returns
    ///
    /// The total number of entries (`EntryCnt`) within the specified range.
    pub fn entry_cnt_within_two_slots(&self, slot_start: S, slot_end: S) -> EntryCnt {
        let (slot_min, slot_max, _) =
            self.transform_range(Some(slot_start), Some(slot_end));

        if slot_min > slot_max {
            0
        } else {
            let cnt = self.distance_to_the_leftmost_slot(&slot_max)
                - self.distance_to_the_leftmost_slot(&slot_min)
                + self.slot_entry_cnt(&slot_max) as Distance;
            cnt as EntryCnt
        }
    }

    /// Returns the total number of entries within a specified slot range.
    ///
    /// # Arguments
    ///
    /// * `slot_start` - An `Option<S>` for the starting slot. If `None`, `S::MIN` is used.
    /// * `slot_end` - An `Option<S>` for the ending slot. If `None`, `S::MAX` is used.
    ///
    /// # Returns
    ///
    /// The total number of entries (`EntryCnt`) in the given range.
    pub fn total_by_slot(&self, slot_start: Option<S>, slot_end: Option<S>) -> EntryCnt {
        let slot_start = slot_start.unwrap_or(S::MIN);
        let slot_end = slot_end.unwrap_or(S::MAX);

        if S::MIN == slot_start && S::MAX == slot_end {
            self.total_cache
        } else {
            self.entry_cnt_within_two_slots(slot_start, slot_end)
        }
    }

    /// Returns the total number of entries in the `SlotDex`.
    pub fn total(&self) -> EntryCnt {
        self.total_by_slot(None, None)
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
    fn transform_range(
        &self,
        logical_min: Option<S>,
        logical_max: Option<S>,
    ) -> (S, S, bool) {
        let min = logical_min.unwrap_or(S::MIN);
        let max = logical_max.unwrap_or(S::MAX);

        if self.swap_order {
            // If storage is reversed:
            // logical [10, 20] -> storage [!20, !10]
            // And the storage order is reversed relative to logical order.
            (self.to_storage_slot(max), self.to_storage_slot(min), true)
        } else {
            (min, max, false)
        }
    }
}

/// Convenience alias for `SlotDex<u32, K>`.
pub type SlotDex32<K> = SlotDex<u32, K>;
/// Convenience alias for `SlotDex<u64, K>`.
pub type SlotDex64<K> = SlotDex<u64, K>;
/// Convenience alias for `SlotDex<u128, K>`.
pub type SlotDex128<K> = SlotDex<u128, K>;

// Compile-time proof that SlotDex is Send + Sync.
fn _assert_send_sync() {
    fn require<T: Send + Sync>() {}
    require::<SlotDex<u64, u64>>();
}

#[cfg(test)]
mod test;
