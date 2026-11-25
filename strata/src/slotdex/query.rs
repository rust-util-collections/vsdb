//! Snapshot-bound tier descent and bounded reverse slot traversal.

use super::*;
use vsdb_core::basic::mapx_raw::MapxRawIter;

const CHUNK_ROWS: usize = 256;
const CHUNK_BYTES: usize = 16 * 1024;

pub(super) fn level0_range<S: SlotType>(
    view: &MapxRawReadView<'_>,
    lo: S,
    hi: S,
) -> MapxRawIter<'static> {
    view.range_detached((
        Bound::Included(level_key(0, &lo)),
        Bound::Included(level_key(0, &hi)),
    ))
}

fn entries<S: SlotType>(
    view: &MapxRawReadView<'_>,
    lo: S,
    hi: S,
) -> MapxRawIter<'static> {
    let mut lower = vec![TAG_ENTRY];
    lower.extend_from_slice(lo.to_bytes().as_ref());
    let mut upper = vec![TAG_ENTRY];
    upper.extend_from_slice(hi.to_bytes().as_ref());
    view.range_detached((
        Bound::Included(lower),
        Bound::Excluded(prefix_successor(&upper).expect("entry tag below 0xff")),
    ))
}

pub(super) struct Query<'a, S: SlotType> {
    pub view: MapxRawReadView<'a>,
    pub total: u64,
    pub levels: Cow<'a, [Level<S>]>,
    pub capacity: u32,
}

impl<S: SlotType> Query<'_, S> {
    // Number of entries between a common bucket floor and an endpoint.
    fn before(&self, slot: S, mut floor: S, levels: &[Level<S>]) -> u64 {
        let mut n = 0;
        for level in levels.iter().rev() {
            let next = aligned(slot, level.floor_base);
            n += level
                .buckets
                .range(floor..next)
                .map(|(_, n)| n)
                .sum::<u64>();
            floor = next;
        }
        if let Some(hi) = slot.checked_pred().filter(|s| *s >= floor) {
            n += level0_range(&self.view, floor, hi)
                .map(|(_, v)| decode_cnt(&v))
                .sum::<u64>();
        }
        n
    }

    pub fn count(&self, lo: S, hi: S) -> u64 {
        if lo == S::MIN && hi == S::MAX {
            return self.total;
        }
        if lo == hi {
            return slot_entry_cnt(hi, |k| self.view.get(k));
        }
        // Once the two endpoints separate, all complete buckets between
        // their floors contribute directly. Descend only into the two
        // boundary buckets. A tier-less range reads just its own L0 rows.
        let boundary = self.levels.iter().enumerate().rev().find_map(|(i, level)| {
            let left = aligned(lo, level.floor_base);
            let right = aligned(hi, level.floor_base);
            (left != right).then_some((i, left, right))
        });
        let Some((i, left, right)) = boundary else {
            return level0_range(&self.view, lo, hi)
                .map(|(_, v)| decode_cnt(&v))
                .sum();
        };
        let whole: u64 = self.levels[i]
            .buckets
            .range(left..right)
            .map(|(_, n)| n)
            .sum();
        let finer = &self.levels[..i];
        whole - self.before(lo, left, finer)
            + self.before(hi, right, finer)
            + slot_entry_cnt(hi, |k| self.view.get(k))
    }

    // Seek to a global ascending entry rank. Return the slot, offset and
    // population so descending slot order can keep keys ascending within it.
    fn locate(&self, mut rank: u64) -> Option<(S, u64, u64)> {
        if rank >= self.total {
            return None;
        }
        let mut floor = S::MIN;
        for level in self.levels.iter().rev() {
            for (&bucket, &count) in level.buckets.range(floor..) {
                if rank < count {
                    floor = bucket;
                    break;
                }
                rank -= count;
            }
        }
        for (key, value) in level0_range(&self.view, floor, S::MAX) {
            let (slot, count) = decode_level_row(&key, &value);
            if rank < count {
                return Some((slot, rank, count));
            }
            rank -= count;
        }
        None
    }

    fn page_bounds(
        &self,
        lo: S,
        hi: S,
        reverse: bool,
        skip: u64,
    ) -> Option<(S, S, u64)> {
        if skip == 0 {
            return Some((lo, hi, 0));
        }
        let rank = if reverse {
            let past = if hi == S::MAX {
                self.total
            } else {
                self.before(hi, S::MIN, &self.levels)
                    + slot_entry_cnt(hi, |k| self.view.get(k))
            };
            past.checked_sub(skip + 1)?
        } else {
            self.before(lo, S::MIN, &self.levels).checked_add(skip)?
        };
        let (slot, offset, count) = self.locate(rank)?;
        if slot < lo || slot > hi {
            return None;
        }
        Some(if reverse {
            (lo, slot, count - offset - 1)
        } else {
            (slot, hi, offset)
        })
    }

    // Find the highest populated group of keys sharing all but their last
    // coordinate byte. Probe only forward: each probe halves the remaining
    // possibilities for one prefix byte. A returned key is an actual L0
    // floor; synthetic midpoint bytes need not decode as user-defined slots.
    fn highest_group(&self, lo: S, hi: S) -> Option<S> {
        let mut low = level_key(0, &lo);
        let mut high = level_key(0, &hi);
        let first = |lo: &[u8], hi: &[u8]| {
            self.view
                .range((Bound::Included(lo), Bound::Included(hi)))
                .next()
                .map(|(k, _)| k)
        };
        low = first(&low, &high)?;
        let prefix_len = low.len().saturating_sub(1);
        while let Some(pos) = (2..prefix_len).find(|&i| low[i] != high[i]) {
            let mid = low[pos] + (high[pos] - low[pos]).div_ceil(2);
            let mut pivot = low.clone();
            pivot[pos] = mid;
            pivot[pos + 1..].fill(0);
            if let Some(found) = first(&pivot, &high) {
                low = found;
            } else {
                high[pos] = mid - 1;
                high[pos + 1..].fill(0xff);
            }
        }
        Some(decode_slot(&low[2..]))
    }

    // Pick adjacent populated L1 buckets lazily. A tier-less index and giant
    // fan-outs can span more than CHUNK_ROWS actual slots. In those cases
    // find the last occupied 256-coordinate span, then scan it forward.
    fn read_slots(&self, lo: S, hi: S, slots: &mut Vec<(S, u64)>) -> Option<S> {
        let lower = if self.total <= CHUNK_ROWS as u64 {
            lo
        } else if self.capacity < CHUNK_ROWS as u32 {
            self.levels
                .first()
                .and_then(|level| {
                    let mut start = None;
                    let mut entries = 0u64;
                    // Sparse slots can span many bucket floors. Use their
                    // actual populations to avoid a seek per sparse floor.
                    for (&floor, &count) in level.buckets.range(lo..=hi).rev() {
                        #[cfg(test)]
                        test::REVERSE_SPLIT_FLOORS.with(|n| n.set(n.get() + 1));
                        if start.is_some() && count > 64 - entries {
                            break;
                        }
                        start = Some(floor);
                        if count >= 64 - entries {
                            break;
                        }
                        entries += count;
                    }
                    start
                })
                .unwrap_or(lo)
        } else {
            let grid = S::tier_stride(CHUNK_ROWS as u32, 1);
            let mut lower = aligned(hi, grid).max(lo);
            if level0_range(&self.view, lower, hi).next().is_none() {
                lower = self.highest_group(lo, hi)?;
            }
            lower
        };
        slots.extend(
            level0_range(&self.view, lower, hi).map(|(k, v)| decode_level_row(&k, &v)),
        );
        if lower == lo {
            None
        } else {
            lower.checked_pred()
        }
    }
}

/// A snapshot-consistent slot traversal. Slot groups are emitted in requested
/// order and each group's keys are decoded lazily, in ascending key order.
pub struct SlotDexIter<'a, S: SlotType, K> {
    query: Query<'a, S>,
    lo: S,
    // Upper bound of the next unvisited descending count chunk.
    hi: Option<S>,
    slots: Vec<(S, u64)>,
    stream: Option<MapxRawIter<'static>>,
    buffered: Vec<Vec<u8>>,
    direct_slots: usize,
    skip: u64,
    key: PhantomData<K>,
}

impl<'a, S: SlotType, K> SlotDexIter<'a, S, K> {
    pub(super) fn new(
        query: Query<'a, S>,
        bounds: Option<(S, S)>,
        reverse: bool,
        skip: u64,
    ) -> Self {
        let bounds =
            bounds.and_then(|(lo, hi)| query.page_bounds(lo, hi, reverse, skip));
        let mut stream = None;
        let (lo, hi, skip) = bounds.map_or((S::MIN, None, 0), |(lo, hi, skip)| {
            if reverse {
                (lo, Some(hi), skip)
            } else {
                stream = Some(entries(&query.view, lo, hi));
                (lo, None, skip)
            }
        });
        Self {
            query,
            lo,
            hi,
            slots: Vec::new(),
            stream,
            buffered: Vec::new(),
            direct_slots: 0,
            skip,
            key: PhantomData,
        }
    }

    fn fill(&mut self) -> Option<()> {
        while self.slots.is_empty() {
            self.hi = self.query.read_slots(self.lo, self.hi?, &mut self.slots);
        }
        let &(hi, high_count) = self.slots.last()?;
        let mut first = self.slots.len();
        let mut n = 0;
        // Keep a partially skipped starting slot streaming even if small.
        if self.skip == 0 && high_count < 32 && self.direct_slots == 0 {
            for (_, count) in self.slots.iter().rev() {
                if *count > CHUNK_ROWS as u64 - n {
                    break;
                }
                n += count;
                first -= 1;
            }
        }
        if first + 1 < self.slots.len() {
            let lo = self.slots[first].0;
            let mut bytes = 0;
            for (key, _) in entries(&self.query.view, lo, hi) {
                if key.len() > CHUNK_BYTES - bytes {
                    // Key sizes aren't stored in the counts. On discovering
                    // an oversized span, discard the trial and stream these
                    // slots individually without repeatedly probing the span.
                    self.buffered.clear();
                    self.direct_slots = self.slots.len() - first;
                    break;
                }
                bytes += key.len();
                self.buffered.push(key);
            }
            if !self.buffered.is_empty() {
                // Pop in descending slot order, ascending within each group.
                let mut start = 0;
                for &(_, count) in &self.slots[first..] {
                    let end = start + count as usize;
                    self.buffered[start..end].reverse();
                    start = end;
                }
                self.slots.truncate(first);
                return Some(());
            }
        }
        self.direct_slots = self.direct_slots.saturating_sub(1);
        self.slots.pop();
        self.stream = Some(entries(&self.query.view, hi, hi));
        Some(())
    }
}

impl<S: SlotType, K: KeyEnDeOrdered> Iterator for SlotDexIter<'_, S, K> {
    type Item = K;

    fn next(&mut self) -> Option<K> {
        loop {
            let key = if let Some(key) = self.buffered.pop() {
                key
            } else if let Some(stream) = self.stream.as_mut() {
                let Some((key, _)) = stream.next() else {
                    self.stream = None;
                    continue;
                };
                if self.skip != 0 {
                    self.skip -= 1;
                    continue;
                }
                key
            } else {
                self.fill()?;
                continue;
            };
            return Some(
                K::from_slice(&key[1 + S::Bytes::LEN..])
                    .expect("SlotDex: corrupt entry key"),
            );
        }
    }
}
