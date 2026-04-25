# Audit Findings

> Auto-managed by /x-review and /x-fix.

## Open

(none)

## Won't Fix

### [LOW] typed-collections: inconsistent decode error handling (unwrap vs pnk!)
- **Where**: mapx_ord_rawkey/mod.rs, mapx_ord/mod.rs, mapx/mod.rs (various iterator methods)
- **What**: Value-decode calls use bare `.unwrap()` in some iterator methods and `pnk!()` in others
- **Reason**: Cosmetic inconsistency only; both panic on corrupt data with identical outcome. Changing would touch ~15 sites across 3 files with no correctness benefit. Not worth the churn risk.

### [LOW] common/macros: entry_or_insert_via_mock! and cow_bytes_bounds! are unnecessarily #[macro_export]
- **Where**: strata/src/common/macros.rs:151-184
- **What**: Helper macros are #[macro_export] making them public, but they are internal implementation details
- **Reason**: Rust's #[macro_export] cannot be scoped to pub(crate). Removing export would break cross-module usage within the crate. Renaming to `__` prefix is a semver break for any external user who discovered them.

### [LOW] error: VsdbError::Trie variant overlap with Other
- **Where**: strata/src/common/error.rs:50-54
- **What**: Merge failures route through VsdbError::Other rather than a dedicated MergeError variant
- **Reason**: No correctness impact; callers cannot distinguish merge errors from other errors without string parsing, but no caller currently needs to. Adding a variant is a future enhancement.

### [LOW] typed-collections: values_mut bypasses MapxOrdRawKey encapsulation
- **Where**: strata/src/basic/mapx/mod.rs:145, mapx_ord/mod.rs:157,174,199
- **What**: Mutable iterator paths reach through self.inner.inner (MapxRaw) directly
- **Reason**: Correct behavior; structural asymmetry with immutable path is a maintenance hazard but not a bug. Restructuring would require new iterator types.

### [LOW] vecdex: filtered search early termination can be overly aggressive
- **Where**: strata/src/vecdex/hnsw.rs:143-149
- **What**: Early-termination based on filtered result count can miss graph paths to better matching nodes with highly selective filters (< 2.5% match rate)
- **Reason**: Documented design tradeoff. Mitigated by 4x ef inflation in search_internal(). Code comments and search_ef_with_filter parameter explicitly address this. Acceptable as-is.
