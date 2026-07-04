# Audit Findings

> Auto-managed by /x-review and /x-fix.
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

### [MEDIUM] vecdex: `compact()` is not atomic across a hard process crash, losing not-yet-reinserted vectors
- **Where**: `strata/src/vecdex/mod.rs` (`compact`)
- **What**: `compact()` collects and dimension-validates all pairs before calling `clear()` (closing the `Result::Err` partial-state path), but `clear()` is irreversible and the re-insertion loop that follows spans many individual mutations. A `kill -9` between `clear()` and loop completion permanently loses every not-yet-reinserted pair. Metadata stays internally consistent (`recover_after_crash` correctly reconciles a valid-but-incomplete graph), so this is data loss, not corruption.
- **Reason**: A true fix requires rebuilding into a fresh set of prefixes and atomically flipping a pointer once the new graph is durably complete (mirroring `DagMapRaw::prune`'s COW re-parenting design). This is not viable as a contained patch: `VecDex` has no indirection layer between its 5 field collections' prefixes and any external reference to them (`save_meta`/`from_meta`, or the value bytes when `VecDex` is nested inside another collection) — those references are captured at a single point in time. Rebuilding into fresh prefixes and swapping `self`'s fields would silently desync any *earlier* `save_meta`/parent-collection snapshot from the live handle, since only a `ValueMut`-mediated write-back (not a bare field swap) propagates a changed prefix set back to such a reference. That failure mode (silent staleness in the common, non-crash case) is worse than the current rare-crash-window data loss it would be trading away. `compact()` is also explicitly documented as a cold, explicit maintenance API, not a hot/warm path. Revisit if `VecDex` ever gains a version-indirection layer for other reasons.
