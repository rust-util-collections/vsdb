# Versioning Review Patterns

**Files:** `versioned/{mod,map,diff,merge,handle,read,repair}.rs`.

**Arch:** Git-model branches→commits DAG; commits immutable; source-wins 3-way
merge; ref-counts + dirty flag for cascade crash recover. `BranchMut` must mirror
all `Branch` reads + writes.

## Invariants

**V1 Ref balance** — `ref = branch_ptrs + child_commits_naming_as_parent`.
**V2 Acyclic** — parents earlier existing commits.
**V3 Source-wins** — both modified since **common ancestor** → source value.
**V4 Rollback** — only this branch’s ref contributions; other branches untouched.
**V5 Dirty** — durably true before non-idempotent ref cascade (commit/merge/branch±/rollback), durably false after all participating shards are fenced; recovery recounts only after validating every reachable commit. `gc()` idempotent — no new flag.
**V6 Commit immutable** after create.
**V7 Durable references** — fence nodes before publishing roots; fence commits before publishing HEAD; fence branch/commit removal before retiring roots. Persist allocator advancement before returning an ID and main-branch changes before deleting the old main. Program order across shard WALs alone is insufficient.

## Bugs

**Ref leak** — delete branch without dec.
**Merge drop** — keyed on source only keeps base.
**Live GC** — zero refs while still parent-linked.
**Rollback/merge guard asymmetry** — uncommitted changes must reject on **all** arms (`target==head` and strict-ancestor), like merge.

## Checklist

- [ ] Inc on branch create / new commit parents
- [ ] Dec on branch delete / rollback skips / GC cascade
- [ ] Merge diffs from common ancestor
- [ ] Source-wins on conflicts
- [ ] Rollback multi-path uncommitted guard
- [ ] Dirty brackets cascades; gc stays idempotent
- [ ] Incomplete reachable history fails before destructive repair, even when clean
- [ ] Publication/reclamation fences include construction, deep clone and restore
- [ ] No post-create commit mutate
- [ ] No-op and fast-forward merges
- [ ] BranchMut ≈ Branch reads kept in sync
