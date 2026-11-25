# Versioning Review Patterns

**Files:** `strata/src/versioned/{mod,store,map,diff,read,repair,test}.rs`; node-level
diff / three-way merge / staging live in `persistent_btree/{diff,merge,persist}.rs` (`patterns/btree.md`).

**Arch:** `#![forbid(unsafe_code)]`; Git-model branches→commits DAG; commits immutable; source-wins 3-way
merge replays sorted source deltas onto target via range-grouped `apply_leaf_run`. Single `MapxRaw` prefix (`0x00` `Header`, `0x01` `HEAD`, `0x02` `NAME`, `0x03` `BY_NAME`, `0x04` `COMMIT`, `0x05` `RETIRED`, `0x06` `STAGE`, `0x07` `REFS`, `0x10` pages, `0x12` blobs). Owned `'static` snapshots pin immutable roots via `TreeRuntime`.

## Invariants

**V1 Ref balance** — `ref = branch_ptrs + child_commits_naming_as_parent`.
**V2 Acyclic** — parents earlier existing commits.
**V3 Source-wins** — both modified since **every merge base** (all LCAs) → source value; bases disagree on a key → source.
**V4 Rollback** — only this branch’s ref contributions; other branches untouched. Inc target **before** dec old HEAD.
**V5 Publication & staging** — history changes and `bulk_load` commit metadata, page writes, and `[0x05]` root retirement in one `commit_sync()` batch; working edits (`insert`, `remove`, `write_batch`, `discard`) commit one unsynced batch. `> 16 MiB` `bulk_load`/`merge` chunks anchor unpublished roots at `[0x06]` `STAGE`; if a build errors or unwinds after `flush_stage`, cleanup must remove/defer those `[0x06]` anchors and private chunks so they do not leak or poison `[0x07]` `REFS` checkpoints (`has_anchors`).
**V6 Commit immutable** after create — id/root/parents/timestamp; only `ref_count` is rewritten.
**V7 Recovery** — validate branch heads, commit ancestry, reachable root headers and the highest object ID in each class before reclamation. Only trust a checksummed checkpoint bound to the matching map and generation. Deep verification also checks child pages and values.

**V8 Handle** — current-format single-prefix metadata preserves instance identity. Offline conversion, including v17 wire structures, lives under `migration/`.

**V9 Snapshot lifetime** — snapshot/iterator leases retain tree roots across alias
mutation, rollback, branch deletion and recount. Snapshots and leases own their handles; derived iterators retain the lease after
the view drops. Pins are runtime-only, do not retain commit records, and are excluded
from deep copies. Reclamation after final drop uses the pool-shared deferred queue.

## Bugs

**Ref leak** — delete branch without dec. Check zero **after** dec.
**Merge drop** — keyed on source only keeps base; conflict resolved to base/target instead of source (`persistent_btree/merge.rs`).
**Result ownership** — `tree.merge` returns an **unowned** fresh root (or an existing one); the caller acquires it for every reference (commit root + dirty root) **before**
publishing either row, so a reader lease cannot drop an otherwise unowned result.
**Live GC** — zero refs while still parent-linked.
**Rollback/merge guard asymmetry** — uncommitted changes must reject on **all** arms (`target==head` and strict-ancestor), like merge.

## Checklist

- [ ] Inc: branch create, rollback/FF target, merge source parent; `commit()` old HEAD is net 0 by design
- [ ] Dec: branch delete, rollback old HEAD; zero cascades to parents; gc recounts, never decs
- [ ] Merge diffs from every merge base
- [ ] Source-wins on conflicts
- [ ] Rollback multi-path uncommitted guard
- [ ] Atomic publication and idempotent GC
- [ ] Incomplete reachable history fails before destructive repair, even when clean
- [ ] Publication/reclamation fences include construction, deep clone and restore
- [ ] Publication and retired-root markers share the node prefix
- [ ] No post-create commit mutate except `ref_count`
- [ ] Typed frame and standalone migration fixture preserve identity (V8)
- [ ] Errors typed (`NotAncestor` / `SelfMerge` / `NoCommits` / `CommitNotFound`); `gc()` returns them, never panics on a damaged graph
- [ ] Merge shapes: same head → no-op; FF only into empty target; target-ancestor → 2-parent commit
- [ ] Snapshot and iterator roots survive alias writes/GC/restore; final drop releases once
- [ ] Recount preserves all live pins; clone owns only its copied persistent graph
