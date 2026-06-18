# Audit Findings

> Auto-managed by /x-review and /x-fix.

## Open

(none)

---

## Won't Fix

### [LOW] common: instance-meta persistence is non-atomic (truncate-in-place write)
- **Where**: strata/src/common/mod.rs:24-32 (save_instance_meta)
- **What**: `fs::write` truncates then writes; a crash mid-write leaves a truncated meta file.
- **Reason**: Pre-existing codebase-wide convention (core/src/basic/mapx_raw/mod.rs save_meta uses the same `fs::write`); cold explicit-save path; the count is independently recoverable via the dirty-flag mechanism. Fixing one site without the matching core change is inconsistent and the churn/risk is disproportionate to a cold-path durability nicety.

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
