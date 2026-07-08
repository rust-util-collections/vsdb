# Versioning Subsystem Review Patterns

## Files
- `strata/src/versioned/mod.rs` — VerMap core, BranchState, Commit types
- `strata/src/versioned/map.rs` — VerMap<K,V> implementation (core insert/commit/merge/rollback/gc logic)
- `strata/src/versioned/diff.rs` — incremental diff computation
- `strata/src/versioned/merge.rs` — three-way merge algorithm
- `strata/src/versioned/handle.rs` — `Branch`/`BranchMut` ergonomic branch handles (`BranchMut` must expose every `Branch` read method plus write ops — see its doc comment)
- `strata/src/versioned/read.rs` — read-path query operations (get/contains_key/iter/range)
- `strata/src/versioned/repair.rs` — ref-count recovery / rebuild after a dirty-flag-marked crash

## Architecture
- Git-model: branches point to commits, commits form a DAG
- Commits are immutable once created
- Branches are mutable pointers (move forward on commit, backward on rollback)
- Three-way merge: find common ancestor, diff both branches, source-wins on conflict
- Reference counting for garbage collection
- Dirty flag for crash recovery of GC operations

## Critical Invariants

### INV-V1: Ref-Count Balance
For every commit: `ref_count == number_of_branches_pointing_to_it + number_of_child_commits_referencing_it_as_parent`.
**Check**: Every branch create/delete and commit/merge operation must adjust ref-counts correctly. Verify both increment and decrement paths.

### INV-V2: DAG Acyclicity
The commit DAG must be a directed acyclic graph. A commit's parents must have been created before it.
**Check**: Verify merge creates a commit with two existing parents. Verify no operation can create a cycle.

### INV-V3: Source-Wins Merge Policy
When merging source into target, for keys modified on both branches since their common ancestor, the source value wins.
**Check**: Verify merge.rs conflict resolution logic. Verify "modified on both" detection uses the common ancestor as base, not the branch tips.

### INV-V4: Rollback Preserves Other Branches
Rolling back branch B to an older commit must not affect any other branch's data or ref-counts (except for the commits between old and new position that may lose one reference).
**Check**: Verify rollback only decrements ref-counts for commits that lose THIS branch's reference. Do not decrement if other branches still reference them.

### INV-V5: Dirty Flag Lifecycle
`dirty = true` BEFORE starting a destructive operation (GC, merge). `dirty = false` AFTER completion. On recovery, if dirty is true, repair is needed.
**Check**: Verify dirty flag is set atomically before the first mutation and cleared after the last. Verify crash between set and clear triggers recovery.

### INV-V6: Commit Immutability
Once a commit is created, its data (snapshot pointer, parent list) must never change.
**Check**: Verify no code path modifies a commit struct after creation.

## Common Bug Patterns

### Ref-Count Leak (technical-patterns.md 2.1)
Branch is deleted but its commit's ref-count is not decremented.
**Trigger**: Create branch B2 from main → delete B2 → commit's ref-count still shows 2.
**Impact**: Commit never GC'd, disk grows forever.

### Merge Loses Data (technical-patterns.md 2.2)
Key modified on source branch but not on target. Merge should keep source value but keeps base value instead.
**Trigger**: Common ancestor has key=v0. Source modifies to v1. Target doesn't touch it. After merge, key=v0 instead of v1.

### GC Deletes Live Commit (technical-patterns.md 2.1)
Ref-count reaches 0 while another branch still references the commit indirectly (through a child commit's parent link).
**Trigger**: Complex merge/rollback sequence leaves a commit reachable only through parent links, not branch pointers.

### Rollback/Merge Guard Asymmetry
`rollback_to` and `merge` both mutate `dirty_root`/branch state and must reject the operation when the affected branch has uncommitted changes — silently discarding them is data loss. A guard added for only ONE code path of a multi-branch function (e.g. only the `target == head` arm of `rollback_to`, leaving the `target == ancestor` arm unguarded) is easy to miss in review since each arm looks locally correct.
**Trigger**: Insert uncommitted changes → roll back to a commit that is a strict ancestor of head (not head itself) → the uncommitted edit is silently overwritten and `Ok(())` is returned instead of `VsdbError::UncommittedChanges`.

## Review Checklist
- [ ] Ref-count incremented on: branch create, merge (new commit references parents)
- [ ] Ref-count decremented on: branch delete, rollback (skipped commits), GC cascade
- [ ] Merge uses common ancestor for diff base, not branch tips
- [ ] Source-wins policy applied correctly for conflicts
- [ ] Rollback only affects the rolled-back branch's ref-count contributions
- [ ] Rollback rejects uncommitted changes on EVERY code path (target == head AND target == a strict ancestor), not just one arm — mirror `merge()`'s unconditional guard
- [ ] Dirty flag set before GC/merge, cleared after
- [ ] Commit data is immutable after creation
- [ ] No-op merge handled (source == target, or no diffs)
- [ ] Fast-forward merge handled (source is ancestor of target)
- [ ] `BranchMut` (handle.rs) exposes every read method `Branch` has, per its own doc comment ("provides all `Branch` read methods plus write operations") — check the two impl blocks stay in sync when either gains a new read method
