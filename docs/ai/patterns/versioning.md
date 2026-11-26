# Versioning Review Patterns

[Version store](../../../strata/src/versioned) · [B+ tree](btree.md)

**Arch:** `#![forbid(unsafe_code)]`; Git-model branches→commits DAG; commits immutable; source-wins 3-way
merge replays sorted source deltas onto target via range-grouped `apply_leaf_run`. Single `MapxRaw` prefix (`0x00` `Header`, `0x01` `HEAD`, `0x02` `NAME`, `0x03` `BY_NAME`, `0x04` `COMMIT`, `0x05` `RETIRED`, `0x06` `STAGE`, `0x07` `REFS`, `0x10` pages, `0x12` blobs). Owned `'static` snapshots pin immutable roots via `TreeRuntime`.

## Invariants

**V1 Ref balance** — `ref = branch_ptrs + child_commits_naming_as_parent`.

**V2 Acyclic** — parents earlier existing commits.

**V3 Source-wins** — both modified since **every merge base** (all LCAs) → source value; bases disagree on a key → source.

**V4 Rollback** — only this branch’s ref contributions; other branches untouched. Inc target **before** dec old HEAD.

**V5 Publication & staging** — history changes and `bulk_load` publish metadata and remaining pages with `commit_sync()`. Shared runtimes record `[0x05]` retirements in that publication; a changed working root (`insert`, `remove`, `write_batch`, `edit_working`, `discard`) is published with an unsynced batch. An unchanged root can skip publication. Large `bulk_load` and `merge` builds flush pending data at 16 MiB and anchor unpublished roots at `[0x06]` `STAGE`; if a build errors or unwinds after `flush_stage`, cleanup must remove/defer those `[0x06]` anchors and private chunks so they do not leak or poison `[0x07]` `REFS` checkpoints (`has_anchors`).

**V6 Commit immutable** after create — id/root/parents/timestamp; only `ref_count` is rewritten.

**V7 Recovery** — validate branch heads, commit ancestry, reachable root headers and the highest object ID in each class before reclamation. Only trust a checksummed checkpoint bound to the matching map and generation. Deep verification also walks reachable blobs and checks child heights, separators and occupancy.

**V8 Handle** — current-format single-prefix metadata preserves instance identity.

**V9 Snapshot lifetime** — snapshot/iterator leases retain tree roots across alias
mutation, rollback, branch deletion and recount. Snapshots and leases own their handles; derived iterators retain the lease after
the view drops. Pins are runtime-only, do not retain commit records, and are excluded
from deep copies. Reclamation after final drop uses the pool-shared deferred queue.

**V10 Cached branch heads** — `RefState.heads` is shared by aliases. Publication updates
the cache after the storage batch succeeds. A failed publication or recovery clears
cached heads. Branch point reads resolve and pin the working root under the ref
lock, then traverse with a scoped pin. Branch creation shares both the source
HEAD and its working root.

**V11 Dependent working edits** — `edit_working` reads the latest overlay
value or the captured working root. Replacing a staged value releases its
previous charge. A budget violation returns `InvalidConfig`. A changed
root detected after the callback returns `WorkingRootChanged`. Keep the
existing writer-exclusion contract across aliases. An error returned by
the callback or an unwind publishes none of its overlay. A read-only
callback leaves the generation unchanged and submits no write or fence.
Drop the private lease before publishing a changed root. Check that a real
external snapshot can still read the previous values after publication.
[Working-edit tests](../../../strata/src/versioned/test.rs) · [Leased page checks](btree.md) · [Write-submission measurements](../../evaluations/v18-publication/README.md)

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
- [ ] Restoring a typed frame preserves instance identity (V8)
- [ ] Merge, rollback and historical lookups return the documented branch or commit errors. Writable `gc()` propagates verification errors from the reachable graph.
- [ ] Merge shapes: same head → no-op; FF only into empty target; target-ancestor → 2-parent commit
- [ ] Snapshot and iterator roots survive alias writes/GC/restore; final drop releases once
- [ ] Recount preserves all live pins; clone owns only its copied persistent graph
