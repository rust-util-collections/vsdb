# Versioning Review Patterns

**Files:** `strata/src/versioned/{mod,map,diff,merge,handle,read,repair,test}.rs`.

**Arch:** Git-model branches→commits DAG; commits immutable; source-wins 3-way
merge; ref-counts + dirty flag for cascade crash recover. Reads go through a
`Snapshot` (captured root): `at(commit)` / `snapshot(branch)`; branch-id reads delegate to it.

## Invariants

**V1 Ref balance** — `ref = branch_ptrs + child_commits_naming_as_parent`.
**V2 Acyclic** — parents earlier existing commits.
**V3 Source-wins** — both modified since **every merge base** (all LCAs) → source value; bases disagree on a key → source.
**V4 Rollback** — only this branch’s ref contributions; other branches untouched. Inc target **before** dec old HEAD.
**V5 Dirty** — durably true before non-idempotent ref cascade (commit/merge/branch±/rollback), durably false after all participating shards are fenced; recovery recounts only after validating every reachable commit. `gc()` idempotent; its recount (dirty **or** any ref 0) brackets itself with the flag.
**V6 Commit immutable** after create — id/root/parents/timestamp; only `ref_count` is rewritten.
**V7 Durable references** — order nodes before publishing roots; commits before publishing HEAD; branch/commit rewrite or removal before retiring old roots; allocator advancement before returning an ID; main-branch changes before deleting the old main. Co-located maps (one shard, one WAL): program order provides this — `fence()` is a no-op, history ops `settle()` with one sync, and released nodes are registered only after a sync (deferred queue). Per-shard (legacy) maps: program order across shard WALs is insufficient — every `fence()` syncs.

## Bugs

**Ref leak** — delete branch without dec. Check zero **after** dec.
**Merge drop** — keyed on source only keeps base; conflict resolved to base/target instead of source (`merge.rs`).
**Live GC** — zero refs while still parent-linked.
**Rollback/merge guard asymmetry** — uncommitted changes must reject on **all** arms (`target==head` and strict-ancestor), like merge.

## Checklist

- [ ] Inc: branch create, rollback/FF target, merge source parent; `commit()` old HEAD is net 0 by design
- [ ] Dec: branch delete, rollback old HEAD; zero cascades to parents; gc recounts, never decs
- [ ] Merge diffs from every merge base
- [ ] Source-wins on conflicts
- [ ] Rollback multi-path uncommitted guard
- [ ] Dirty brackets cascades; gc stays idempotent
- [ ] Incomplete reachable history fails before destructive repair, even when clean
- [ ] Publication/reclamation fences include construction, deep clone and restore
- [ ] New components co-located with the node pool; no lazy-delete before a sync on co-located maps
- [ ] No post-create commit mutate except `ref_count`
- [ ] Merge shapes: same head → no-op; FF only into empty target; target-ancestor → 2-parent commit
