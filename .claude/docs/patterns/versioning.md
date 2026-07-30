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
**V5 Dirty** — true before non-idempotent ref cascade (commit/merge/branch±/rollback), false after; recovery recounts. `gc()` idempotent — no new flag.
**V6 Commit immutable** after create.

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
- [ ] No post-create commit mutate
- [ ] No-op and fast-forward merges
- [ ] BranchMut ≈ Branch reads kept in sync
