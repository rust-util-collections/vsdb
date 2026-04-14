//! Bit 63 dirty-flag helpers for crash-safe counters.
//!
//! Application-layer structures (VecDex, SlotDex) maintain a u64
//! counter alongside their data.  Bit 63 of that u64 is reserved as
//! a dirty flag: set when the process starts operating, cleared on
//! clean shutdown via `save_meta()`.  If set on the next recovery,
//! the counter is rebuilt from live data.

/// The dirty-flag bit (bit 63 of a u64 counter).
const DIRTY_BIT: u64 = 1 << 63;

/// Extract the pure count (bits 0..62).
#[inline(always)]
pub fn count(raw: u64) -> u64 {
    raw & !DIRTY_BIT
}

/// Returns `true` if the dirty flag is set.
#[inline(always)]
pub fn is_dirty(raw: u64) -> bool {
    raw & DIRTY_BIT != 0
}

/// Set the dirty flag, preserving the count.
#[inline(always)]
pub fn set_dirty(raw: u64) -> u64 {
    raw | DIRTY_BIT
}

/// Clear the dirty flag, preserving the count.
#[inline(always)]
pub fn clear_dirty(raw: u64) -> u64 {
    raw & !DIRTY_BIT
}

/// Increment the count by 1, preserving the dirty flag.
#[inline(always)]
pub fn inc(raw: u64) -> u64 {
    (count(raw) + 1) | (raw & DIRTY_BIT)
}

/// Decrement the count by 1 (saturating), preserving the dirty flag.
#[inline(always)]
pub fn dec(raw: u64) -> u64 {
    count(raw).saturating_sub(1) | (raw & DIRTY_BIT)
}

/// Reset count to zero, preserving the dirty flag.
#[inline(always)]
pub fn zero(raw: u64) -> u64 {
    raw & DIRTY_BIT
}

/// Overwrite the count with `new_count`, preserving the dirty flag.
#[inline(always)]
pub fn set_count(raw: u64, new_count: u64) -> u64 {
    new_count | (raw & DIRTY_BIT)
}
