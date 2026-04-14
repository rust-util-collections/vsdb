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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_value() {
        assert_eq!(count(42), 42);
        assert!(!is_dirty(42));
    }

    #[test]
    fn dirty_value() {
        let raw = set_dirty(42);
        assert!(is_dirty(raw));
        assert_eq!(count(raw), 42);
    }

    #[test]
    fn clear_dirty_preserves_count() {
        let raw = set_dirty(99);
        let cleaned = clear_dirty(raw);
        assert_eq!(cleaned, 99);
        assert!(!is_dirty(cleaned));
    }

    #[test]
    fn inc_preserves_dirty() {
        let clean = 5u64;
        assert_eq!(inc(clean), 6);
        assert!(!is_dirty(inc(clean)));

        let dirty = set_dirty(5);
        assert_eq!(count(inc(dirty)), 6);
        assert!(is_dirty(inc(dirty)));
    }

    #[test]
    fn dec_preserves_dirty() {
        let dirty = set_dirty(3);
        assert_eq!(count(dec(dirty)), 2);
        assert!(is_dirty(dec(dirty)));
    }

    #[test]
    fn dec_saturates_at_zero() {
        let dirty = set_dirty(0);
        assert_eq!(count(dec(dirty)), 0);
        assert!(is_dirty(dec(dirty)));

        assert_eq!(count(dec(0)), 0);
    }

    #[test]
    fn zero_preserves_dirty() {
        let dirty = set_dirty(100);
        assert_eq!(count(zero(dirty)), 0);
        assert!(is_dirty(zero(dirty)));

        assert_eq!(zero(100), 0);
        assert!(!is_dirty(zero(100)));
    }

    #[test]
    fn set_count_preserves_dirty() {
        let dirty = set_dirty(10);
        let updated = set_count(dirty, 99);
        assert_eq!(count(updated), 99);
        assert!(is_dirty(updated));

        let clean = 10u64;
        let updated = set_count(clean, 99);
        assert_eq!(updated, 99);
        assert!(!is_dirty(updated));
    }

    #[test]
    fn roundtrip_through_all_ops() {
        let mut v = 0u64;
        v = set_dirty(v);
        for _ in 0..100 {
            v = inc(v);
        }
        assert_eq!(count(v), 100);
        assert!(is_dirty(v));

        for _ in 0..30 {
            v = dec(v);
        }
        assert_eq!(count(v), 70);
        assert!(is_dirty(v));

        v = clear_dirty(v);
        assert_eq!(count(v), 70);
        assert!(!is_dirty(v));
    }
}
