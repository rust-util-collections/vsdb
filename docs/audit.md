# Audit Findings

> Auto-managed by /x-review and /x-fix.
> Last sweep: 2026-07-03 — the entire Won't Fix backlog was re-evaluated under
> the project's alpha-stage policy (breaking changes allowed, fundamental fixes
> encouraged, correctness first). Eight formerly-deferred entries were fixed
> (SMT domain separation, atomic instance-meta writes, BitPath re-masking,
> bulk_load minimum occupancy, vecdex compact pre-validation, allocator-const
> rename, trie codec dedup, MPT proof coverage); one was promoted to Open
> (dagmap prune crash-atomicity) and subsequently fixed the same day via the
> merge→flush→re-parent→flush→clear phase redesign; the remainder stay
> deferred with reasoning re-verified below. Historical "fixed in ..." logs
> live in git history and CHANGELOG.md, not in this file.
>
> **Won't Fix ≠ permanent.** Every entry under `## Won't Fix` must be
> re-evaluated against the current codebase on each audit. Surrounding code
> changes, new callers, or API evolution may make a previously-disproportionate
> fix straightforward — or make the finding obsolete. Never silently carry
> forward a Won't Fix entry without fresh assessment.

## Open

*(none)*

---

## Won't Fix

### [HIGH] slotdex: offset-based pagination is not stable across inter-page mutation
- **Where**: strata/src/slotdex/mod.rs (get_entries_by_page / get_entries_by_page_slot)
- **What**: Pages are computed from `(page_size, page_index)` offsets, so an insert/remove between page requests can make a later page skip or repeat entries.
- **Reason**: This is the documented, intentional semantics of an offset-based pagination API (identical to SQL `LIMIT`/`OFFSET`). For a static dataset the result is exact. Satisfying stable cursor pagination would require a different, cursor-based API — a feature addition, not a bug fix. Re-verified 2026-07-03.

### [MEDIUM] common: dirty_count saturates instead of erroring on count overflow
- **Where**: strata/src/common/dirty_count.rs (inc)
- **What**: `inc()` clamps the live-entry count at `COUNT_MASK` (2^63-1) rather than signalling overflow.
- **Reason**: Bit 63 is the dirty flag; saturating below it is the intended, unit-tested behavior. The clamp is only reachable at 2^63 live entries — physically impossible (would require exabytes of storage). Changing well-tested behavior for an unreachable condition adds risk with no practical benefit. Re-verified 2026-07-03 (correctness-first policy cuts both ways: no change without a reachable failure).

### [MEDIUM] engine: failed WriteBatch commit is not retryable
- **Where**: core/src/common/engine/mmdb.rs (MmdbBatch::commit), core/src/common/engine/mod.rs (BatchTrait)
- **What**: `commit` moves the buffered `WriteBatch` into the engine's `write` call; on error the buffered operations are consumed, so a retry on the same batch object commits nothing (and reports success).
- **Reason**: mmdb's `write(batch)` takes the batch by value, so the operations cannot be restored without cloning every batch on the warm path. Elsewhere in the engine, mmdb write failures are treated as fatal (`.expect`) — the fail-stop policy is coherent for an embedded engine. The public typed wrappers already consume the batch (`commit(self)`), making retry impossible there; the remaining `MapxRaw::batch_entry` trait object documents the non-retryable contract on both `BatchTrait::commit` and `batch_entry`. Re-verified 2026-07-03.

### [LOW] encoding: blanket KeyEnDe cannot type-level-reject non-deterministic key types
- **Where**: strata/src/common/ende.rs (blanket KeyEn/KeyEnDe impls)
- **What**: The ergonomic blanket impl makes every `Serialize` type a key, including `HashMap`/`HashSet`/floats whose encoding is non-deterministic or non-canonical.
- **Reason**: Enforcing this at the type level (sealed/marker trait) would make every user key type require a marker — a ergonomics regression that conflicts with the project's hard usability constraint (blanket-impl convenience is the point). The documentation warns about these types. Re-verified 2026-07-03: still Won't Fix even under alpha breaking-change policy, because the cost is permanent API friction, not a one-time migration.

### [LOW] error: VsdbError lacks a dedicated variant for merge/rollback validation errors
- **Where**: core/src/common/error.rs (VsdbError); strata/src/versioned/map.rs (rollback_to / merge validation)
- **What**: Branch/commit *validation* failures (rollback target not an ancestor, self-merge, merging an empty source) route through `VsdbError::Other`.
- **Reason**: These are argument-validation errors, not data-path failures — the original "merge failures" premise does not hold (the merge algorithm itself is infallible). No caller matches on them; the enum is `#[non_exhaustive]`, so a variant can be added compatibly the moment one does. Re-verified 2026-07-03 with the premise corrected.

### [LOW] vecdex: compact() clear/re-insert is not crash-atomic
- **Where**: strata/src/vecdex/mod.rs (compact)
- **What**: A process crash (kill -9) between `clear()` and the completion of re-insertion loses the not-yet-reinserted vectors. (The *error-path* variant of this finding — insert failing mid-way — was fixed 2026-07-03 by pre-validating dimensions before the irreversible clear.)
- **Reason**: compact() is a cold, explicit maintenance API; crash-mid-compact leaves a structurally valid (dirty-flagged, recoverable) index containing the already-reinserted subset. The dagmap prune fix (merge→flush→re-parent→flush→clear) is not transplantable: it exploits DAG overlay-read transparency, which a flat HNSW graph lacks — crash-atomic compact needs a genuine COW rebuild into fresh prefixes plus an atomic meta flip, a disproportionate mechanism for a cold path. Re-verified 2026-07-03.
