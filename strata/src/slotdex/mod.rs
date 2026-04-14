//! Slot-based index for efficient, timestamp-based paged queries.
//!
//! [`SlotDex`] is a skip-list-like data structure ideal for indexing and
//! querying large datasets where entries are associated with a slot
//! (e.g., a timestamp or sequence number).

use crate::{
    KeyEnDeOrdered, MapxOrd, ValueEnDe,
    basic::{mapx_ord::MapxOrdIter as LargeIter, orphan::Orphan},
    common::error::Result,
};
use parking_lot::Mutex;
use ruc::eg;
use serde::{Deserialize, Serialize, de};
use std::{
    collections::{BTreeMap, BTreeSet, btree_set::Iter as SmallIter},
    fmt,
    ops::{Bound, Not},
};

/// Trait for types usable as a slot key in [`SlotDex`].
///
/// Implemented for the native unsigned integers `u32`, `u64`, and `u128`.
pub trait SlotType:
    Clone
    + Ord
    + fmt::Debug
    + Not<Output = Self>
    + KeyEnDeOrdered
    + Serialize
    + de::DeserializeOwned
    + 'static
{
    /// The minimum value of this type.
    const MIN: Self;
    /// The maximum value of this type.
    const MAX: Self;

    /// Floor-align `self` to a multiple of `base`: `self / base * base`.
    fn floor_align(&self, base: &Self) -> Self;

    /// `self.checked_pow(exp)`, returning `None` on overflow.
    fn checked_pow(&self, exp: u32) -> Option<Self>;

    /// Returns the larger of `self` and `other`.
    fn max_val(self, other: Self) -> Self;

    /// Saturating addition.
    fn saturating_add(&self, rhs: &Self) -> Self;

    /// Widen to `i128` for distance arithmetic.
    fn as_i128(&self) -> i128;

    /// Widen to `u64` for entry-count arithmetic.
    fn as_u64(&self) -> u64;
}

macro_rules! impl_slot_type {
    ($($t:ty),+) => { $(
        impl SlotType for $t {
            const MIN: Self = <$t>::MIN;
            const MAX: Self = <$t>::MAX;
            #[inline]
            fn floor_align(&self, base: &Self) -> Self { self / base * base }
            #[inline]
            fn checked_pow(&self, exp: u32) -> Option<Self> { <$t>::checked_pow(*self, exp) }
            #[inline]
            fn max_val(self, other: Self) -> Self { Ord::max(self, other) }
            #[inline]
            fn saturating_add(&self, rhs: &Self) -> Self { <$t>::saturating_add(*self, *rhs) }
            #[inline]
            fn as_i128(&self) -> i128 { *self as i128 }
            #[inline]
            fn as_u64(&self) -> u64 { *self as u64 }
        }
    )+ };
}

impl_slot_type!(u32, u64, u128);

type EntryCnt = u64;
type SkipNum = EntryCnt;
type TakeNum = EntryCnt;

// Declare as a signed `int`!
type Distance = i128;

type PageSize = u16;
type PageIndex = u32;

const INLINE_CAPACITY_THRESHOLD: usize = 8;

/// A skip-list-like data structure for fast, timestamp-based paged queries.
///
/// `SlotDex` organizes data into "slots" (e.g., timestamps or sequence numbers),
/// which are then grouped into tiers. This hierarchical structure allows for
/// rapid seeking and counting, making it highly efficient for pagination and
/// range queries over large datasets.
///
/// The slot type `S` must implement [`SlotType`]; built-in support covers
/// `u32`, `u64`, and `u128`.
#[derive(Debug, Deserialize, Serialize)]
#[serde(bound = "S: SlotType + Serialize + de::DeserializeOwned")]
pub struct SlotDex<S, K>
where
    S: SlotType,
    K: Clone + Ord + KeyEnDeOrdered,
{
    data: MapxOrd<S, DataCtner<K>>,

    // How many entries are in this DB
    total: Orphan<EntryCnt>,

    tiers: Vec<Tier<S>>,

    tier_capacity: S,

    // Switch the inner implementation of the slot direction:
    // - positive => reverse
    // - reverse => positive
    //
    // Positive queries usually get better performance. If most use cases
    // are in reverse mode, swapping the low-level logic can improve performance.
    swap_order: bool,
}

impl<S, K> SlotDex<S, K>
where
    S: SlotType,
    K: Clone + Ord + KeyEnDeOrdered,
{
    /// Creates a new `SlotDex`.
    ///
    /// # Arguments
    ///
    /// * `tier_capacity` - The capacity of each tier, controlling the granularity of the index.
    /// * `swap_order` - If `true`, reverses the internal slot order. This can improve
    ///   performance for applications that primarily query in reverse chronological order.
    pub fn new(tier_capacity: S, swap_order: bool) -> Self {
        assert!(
            tier_capacity.as_u64() > 0,
            "SlotDex: tier_capacity must be > 0"
        );

        Self {
            data: MapxOrd::new(),
            total: Orphan::new(0),
            tiers: vec![],
            tier_capacity,
            swap_order,
        }
    }

    /// Returns the unique instance ID of this `SlotDex`.
    #[inline(always)]
    pub fn instance_id(&self) -> u64 {
        self.data.instance_id()
    }

    /// Persists this instance's metadata to disk so that it can be
    /// recovered later via [`from_meta`](Self::from_meta).
    ///
    /// Returns the `instance_id` that should be passed to `from_meta`.
    pub fn save_meta(&self) -> Result<u64> {
        let id = self.instance_id();
        crate::common::save_instance_meta(id, self)?;
        Ok(id)
    }

    /// Recovers a `SlotDex` instance from previously saved metadata.
    ///
    /// The caller must ensure that the underlying VSDB database still
    /// contains the data referenced by this instance ID.
    pub fn from_meta(instance_id: u64) -> Result<Self> {
        crate::common::load_instance_meta(instance_id)
    }

    /// Inserts a key into a specified slot.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot to insert the key into (e.g., a timestamp).
    /// * `k` - The key to insert.
    pub fn insert(&mut self, slot: S, k: K) -> Result<()> {
        let slot = self.to_storage_slot(slot);

        self.ensure_tier_capacity();

        let mut ctner = self.data.get(&slot).unwrap_or_default();
        if ctner.insert(k) {
            self.data.insert(&slot, &ctner);
            self.tiers.iter_mut().for_each(|t| {
                t.ensure_cache();
                let slot_floor = slot.floor_align(&t.floor_base);
                let c = t.cache.get_mut();
                let mut v = c.get(&slot_floor).copied().unwrap_or(0);
                if 0 == v {
                    *t.entry_count.get_mut() += 1;
                    if let Some(l) = t.len_cache.as_mut() {
                        *l += 1;
                    }
                }
                v += 1;
                c.insert(slot_floor.clone(), v);
                t.store.insert(&slot_floor, &v);
            });
            *self.total.get_mut() += 1;
        }

        Ok(())
    }

    /// Removes a key from a specified slot.
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot to remove the key from.
    /// * `k` - The key to remove.
    pub fn remove(&mut self, slot: S, k: &K) {
        let slot = self.to_storage_slot(slot);

        let (exist, empty, d) = match self.data.get(&slot) {
            Some(mut d) => {
                let existed = d.remove(k);
                (existed, d.is_empty(), d)
            }
            _ => {
                return;
            }
        };

        if empty {
            self.data.remove(&slot);
        } else if exist {
            self.data.insert(&slot, &d);
        }

        if exist {
            // Shrink degenerate top tiers (structural maintenance).
            loop {
                let dominated = self.tiers.last_mut().is_some_and(|top| {
                    if top.len() < 2 {
                        top.store.clear();
                        *top.entry_count.get_mut() = 0;
                        top.cache.get_mut().clear();
                        true
                    } else {
                        false
                    }
                });
                if dominated {
                    self.tiers.pop();
                } else {
                    break;
                }
            }

            self.tiers.iter_mut().for_each(|t| {
                t.ensure_cache();
                let slot_floor = slot.floor_align(&t.floor_base);
                let c = t.cache.get_mut();
                let cnt = match c.get(&slot_floor).copied() {
                    Some(n) => n,
                    None => return,
                };
                if 1 == cnt {
                    c.remove(&slot_floor);
                    t.store.remove(&slot_floor);
                    t.dec_len();
                } else {
                    let new_cnt = cnt - 1;
                    c.insert(slot_floor.clone(), new_cnt);
                    t.store.insert(&slot_floor, &new_cnt);
                }
            });
            *self.total.get_mut() -= 1;
        }
    }

    /// Clears the `SlotDex`, removing all entries and tiers.
    pub fn clear(&mut self) {
        *self.total.get_mut() = 0;
        self.data.clear();

        self.tiers.iter_mut().for_each(|t| {
            t.store.clear();
            *t.entry_count.get_mut() = 0;
            t.cache.get_mut().clear();
        });

        self.tiers.clear();
    }

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

    fn slot_entry_cnt(&self, slot: &S) -> EntryCnt {
        self.data
            .get(slot)
            .map(|d| d.len() as EntryCnt)
            .unwrap_or(0)
    }

    // Exclude the slot itself-owned entries (whether it exists or not)
    fn distance_to_the_leftmost_slot(&self, slot: &S) -> Distance {
        if *slot == S::MIN {
            return 0;
        }
        let mut left_bound = S::MIN;
        let mut ret = 0;
        for t in self.tiers.iter().rev() {
            t.ensure_cache();
            let right_bound = slot.floor_align(&t.floor_base);
            ret += t
                .cache
                .lock()
                .range(left_bound.clone()..right_bound.clone())
                .map(|(_, cnt)| *cnt as Distance)
                .sum::<Distance>();
            left_bound = right_bound
        }
        ret += self
            .data
            .range(left_bound..slot.clone())
            .map(|(_, d)| d.len() as Distance)
            .sum::<Distance>();
        ret
    }

    fn offsets_from_the_leftmost_slot(
        &self,
        slot_start: &S, // Included
        slot_end: &S,   // Included
        page_size: PageSize,
        page_index: PageIndex,
        reverse: bool,
    ) -> (SkipNum, TakeNum) {
        if slot_start > slot_end {
            return (0, 0);
        }

        if reverse {
            let mut skip_n = self.distance_to_the_leftmost_slot(slot_end)
                + self.slot_entry_cnt(slot_end) as Distance
                - (page_size as Distance) * (1 + page_index as Distance);

            let distance_of_slot_start = self.distance_to_the_leftmost_slot(slot_start);

            let take_n = if distance_of_slot_start <= skip_n {
                page_size
            } else {
                let back_shift = (distance_of_slot_start.saturating_sub(skip_n))
                    .min(PageSize::MAX as Distance);

                skip_n = distance_of_slot_start;

                page_size.saturating_sub(back_shift as PageSize)
            };

            (skip_n as SkipNum, take_n as TakeNum)
        } else {
            let skip_n = self.distance_to_the_leftmost_slot(slot_start)
                + (page_size as Distance) * (page_index as Distance);
            (skip_n as SkipNum, page_size as TakeNum)
        }
    }

    /// Single-pass page location using in-memory tier caches.
    fn locate_page_start(&self, global_skip_n: EntryCnt) -> (Bound<S>, SkipNum) {
        let mut slot_start = Bound::Included(S::MIN);
        let mut remaining: u64 = global_skip_n;

        for t in self.tiers.iter().rev() {
            t.ensure_cache();
            let c = t.cache.lock();
            let mut hdr = c.range((slot_start.clone(), Bound::Unbounded)).peekable();
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
            .data
            .range((slot_start.clone(), Bound::Unbounded))
            .peekable();
        while let Some(entry_cnt) = hdr.next().map(|(_, entries)| entries.len() as u64) {
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

        (slot_start, remaining)
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

        let (global_skip_n, take_n) = self.offsets_from_the_leftmost_slot(
            &slot_start,
            &slot_end,
            page_size,
            page_index,
            reverse,
        );

        let mut ret = Vec::with_capacity(take_n as usize);

        let (slot_start_actual, local_skip_n) = self.locate_page_start(global_skip_n);

        let mut skip_n = local_skip_n as usize;
        let take_n = take_n as usize;

        for (_, entries) in self
            .data
            .range((slot_start_actual, Bound::Included(slot_end)))
        {
            entries
                .iter()
                .skip(skip_n)
                .take(take_n - ret.len())
                .for_each(|entry| ret.push(entry));
            skip_n = 0;
            if ret.len() >= take_n {
                assert_eq!(ret.len(), take_n);
                break;
            }
        }

        if reverse {
            ret.reverse();
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
            self.total.get_value()
        } else {
            self.entry_cnt_within_two_slots(slot_start, slot_end)
        }
    }

    /// Returns the total number of entries in the `SlotDex`.
    pub fn total(&self) -> EntryCnt {
        self.total_by_slot(None, None)
    }

    // --- Private Helper Methods ---

    // Ensure there is enough tier capacity to cover the new slot.
    fn ensure_tier_capacity(&mut self) {
        let tiers_len = self.tiers.len();
        if let Some(top) = self.tiers.last_mut() {
            if (top.len() as u64) <= self.tier_capacity.as_u64() {
                return;
            }
            top.ensure_cache();
            let entries: Vec<(S, EntryCnt)> = top
                .cache
                .get_mut()
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect();
            let mut newtop = Tier::new(tiers_len as u32, &self.tier_capacity);
            for (slot, cnt) in entries {
                let slot_floor = slot.floor_align(&newtop.floor_base);
                let c = newtop.cache.get_mut();
                let v = c.get(&slot_floor).copied().unwrap_or(0);
                if 0 == v {
                    *newtop.entry_count.get_mut() += 1;
                    if let Some(l) = newtop.len_cache.as_mut() {
                        *l += 1;
                    }
                }
                let new_v = v + cnt;
                c.insert(slot_floor.clone(), new_v);
                newtop.store.insert(&slot_floor, &new_v);
            }
            self.tiers.push(newtop);
        } else {
            let mut newtop = Tier::new(tiers_len as u32, &self.tier_capacity);
            for (slot, entries) in self.data.iter() {
                let slot_floor = slot.floor_align(&newtop.floor_base);
                let c = newtop.cache.get_mut();
                let v = c.get(&slot_floor).copied().unwrap_or(0);
                if 0 == v {
                    *newtop.entry_count.get_mut() += 1;
                    if let Some(l) = newtop.len_cache.as_mut() {
                        *l += 1;
                    }
                }
                let new_v = v + entries.len() as EntryCnt;
                c.insert(slot_floor.clone(), new_v);
                newtop.store.insert(&slot_floor, &new_v);
            }
            self.tiers.push(newtop);
        }
    }

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

enum DataCtner<K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    Small(BTreeSet<K>),
    Large { map: MapxOrd<K, ()>, len: usize },
}

impl<K> fmt::Debug for DataCtner<K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Small(s) => f.debug_tuple("Small").field(&s.len()).finish(),
            Self::Large { len, .. } => {
                f.debug_struct("Large").field("len", len).finish()
            }
        }
    }
}

// Tag bytes for binary encoding
const TAG_SMALL: u8 = 0;
const TAG_LARGE: u8 = 1;

impl<K> ValueEnDe for DataCtner<K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    fn try_encode(&self) -> ruc::Result<Vec<u8>> {
        match self {
            Self::Small(set) => {
                let mut buf = vec![TAG_SMALL];
                let count = set.len() as u32;
                buf.extend_from_slice(&count.to_le_bytes());
                for k in set {
                    let kb = k.to_bytes();
                    buf.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&kb);
                }
                Ok(buf)
            }
            Self::Large { map, len } => {
                // Large variant: only persist the MapxOrd handle + len;
                // the actual entries live on disk inside MapxOrd already.
                let mut buf = vec![TAG_LARGE];
                let handle_bytes = map.encode();
                buf.extend_from_slice(&(handle_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&handle_bytes);
                buf.extend_from_slice(&(*len as u64).to_le_bytes());
                Ok(buf)
            }
        }
    }

    fn decode(bytes: &[u8]) -> ruc::Result<Self> {
        if bytes.is_empty() {
            return Err(eg!("empty DataCtner bytes"));
        }
        match bytes[0] {
            TAG_SMALL => {
                let mut off = 1;
                if bytes.len() < off + 4 {
                    return Err(eg!("truncated count"));
                }
                let count =
                    u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                let mut set = BTreeSet::new();
                for _ in 0..count {
                    if bytes.len() < off + 4 {
                        return Err(eg!("truncated key len"));
                    }
                    let klen =
                        u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap())
                            as usize;
                    off += 4;
                    if bytes.len() < off + klen {
                        return Err(eg!("truncated key data"));
                    }
                    let k =
                        K::from_slice(&bytes[off..off + klen]).map_err(|e| eg!(e))?;
                    off += klen;
                    set.insert(k);
                }
                Ok(Self::Small(set))
            }
            TAG_LARGE => {
                let mut off = 1;
                if bytes.len() < off + 4 {
                    return Err(eg!("truncated handle len"));
                }
                let hlen =
                    u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                if bytes.len() < off + hlen {
                    return Err(eg!("truncated handle data"));
                }
                let map =
                    MapxOrd::decode(&bytes[off..off + hlen]).map_err(|e| eg!(e))?;
                off += hlen;
                if bytes.len() < off + 8 {
                    return Err(eg!("truncated len"));
                }
                let len =
                    u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap()) as usize;
                Ok(Self::Large { map, len })
            }
            _ => Err(eg!("unknown DataCtner tag")),
        }
    }
}

impl<K> DataCtner<K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    fn new() -> Self {
        Self::Small(BTreeSet::new())
    }

    fn len(&self) -> usize {
        match self {
            Self::Small(i) => i.len(),
            Self::Large { len, .. } => *len,
        }
    }

    fn is_empty(&self) -> bool {
        0 == self.len()
    }

    fn try_upgrade(&mut self) {
        let inner_set = match self {
            Self::Small(set) if set.len() >= INLINE_CAPACITY_THRESHOLD => set,
            _ => return,
        };

        let set_len = inner_set.len();
        let new_map = inner_set.iter().fold(MapxOrd::new(), |mut acc, k| {
            acc.insert(k, &());
            acc
        });

        *self = Self::Large {
            map: new_map,
            len: set_len,
        };
    }

    fn insert(&mut self, k: K) -> bool {
        match self {
            Self::Small(set) => {
                // Only upgrade if we're about to exceed the inline threshold with a new key.
                if set.len() >= INLINE_CAPACITY_THRESHOLD && !set.contains(&k) {
                    // upgrade in-place (reuse existing helper)
                    self.try_upgrade();
                    // self is now Large, fall through by re-calling insert on the new state
                    return self.insert(k);
                }
                set.insert(k)
            }
            Self::Large { map, len } => {
                let existed = map.get(&k).is_some();
                map.insert(&k, &());
                if !existed {
                    *len += 1;
                }
                !existed
            }
        }
    }

    fn remove(&mut self, target: &K) -> bool {
        match self {
            Self::Small(i) => i.remove(target),
            Self::Large { map, len } => {
                let existed = map.get(target).is_some();
                if existed {
                    map.remove(target);
                    *len -= 1;
                }
                existed
            }
        }
    }

    fn iter(&self) -> DataCtnerIter<'_, K> {
        match self {
            Self::Small(i) => DataCtnerIter::Small(i.iter()),
            Self::Large { map, .. } => DataCtnerIter::Large(Box::new(map.iter())),
        }
    }
}

impl<K> Default for DataCtner<K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    fn default() -> Self {
        Self::new()
    }
}

enum DataCtnerIter<'a, K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    Small(SmallIter<'a, K>),
    Large(Box<LargeIter<'a, K, ()>>),
}

impl<K> Iterator for DataCtnerIter<'_, K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    type Item = K;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(i) => i.next().cloned(),
            Self::Large(i) => i.next().map(|j| j.0),
        }
    }
}

impl<K> DoubleEndedIterator for DataCtnerIter<'_, K>
where
    K: Clone + Ord + KeyEnDeOrdered,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Small(i) => i.next_back().cloned(),
            Self::Large(i) => i.next_back().map(|j| j.0),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(bound = "S: SlotType + Serialize + de::DeserializeOwned")]
struct Tier<S: SlotType> {
    floor_base: S,
    store: MapxOrd<S, EntryCnt>,
    #[serde(skip)]
    cache: Mutex<BTreeMap<S, EntryCnt>>,
    entry_count: Orphan<usize>,
    #[serde(skip)]
    len_cache: Option<usize>,
}

impl<S: SlotType> Tier<S> {
    fn new(tier_idx: u32, tier_capacity: &S) -> Self {
        let pow = 1 + tier_idx;
        Self {
            floor_base: tier_capacity
                .checked_pow(pow)
                .filter(|v| *v != S::MIN)
                .unwrap_or(S::MAX),
            store: MapxOrd::new(),
            cache: Mutex::new(BTreeMap::new()),
            entry_count: Orphan::new(0),
            len_cache: Some(0),
        }
    }

    /// Ensure cache is populated. Called lazily on first access after
    /// deserialization (cache is #[serde(skip)] so starts empty).
    /// Safe to call on &self thanks to Mutex interior mutability.
    fn ensure_cache(&self) {
        let mut c = self.cache.lock();
        if c.is_empty() && self.entry_count.get_value() > 0 {
            for (k, v) in self.store.iter() {
                c.insert(k, v);
            }
        }
    }

    #[inline(always)]
    fn len(&mut self) -> usize {
        if let Some(l) = self.len_cache {
            l
        } else {
            let l = self.entry_count.get_value();
            self.len_cache = Some(l);
            l
        }
    }

    fn dec_len(&mut self) {
        *self.entry_count.get_mut() -= 1;
        if let Some(l) = self.len_cache.as_mut() {
            *l -= 1;
        }
    }
}

// Compile-time proof that SlotDex is Send + Sync.
fn _assert_send_sync() {
    fn require<T: Send + Sync>() {}
    require::<SlotDex<u64, u64>>();
}

#[cfg(test)]
mod test;
