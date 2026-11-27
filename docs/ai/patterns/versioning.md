# Versioning Review Patterns

[VerMap storage layout](../../../strata/docs/versioned.md#storage-layout) · [Version store](../../../strata/src/versioned) · [B+ tree](btree.md)

## Invariants

**V1 Commit references** — Count branches pointing to the commit and child
commits naming it as a parent. A new commit moves its branch reference off
the previous head and adds a parent reference to that head. Branch deletion
and rollback release the old head. Cascade to parents when the count reaches
zero after decrement. Compare counts with a full recount.

**V2 Acyclic** — parents earlier existing commits.

**V3 Merge bases** — Apply the union of source changes relative to all lowest
common ancestors. A source value or deletion that differs from **any** base
wins for that key. If it matches every base, keep the target state. Equal
heads return the existing commit. Only a target without a head adopts the
source head directly. Other successful merges create a two-parent commit.
[Merge algorithm](../../../strata/docs/versioned.md#three-way-merge-algorithm)

**V4 Rollback** — Require a clean branch even when the requested target is
its current head. Acquire the target before releasing the old head. Preserve
every other branch's references.

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

## Publication and repair checks

- A merge can reuse a root or return a newly allocated, unowned root. Trace
  the acquisitions for both the commit and working-root references through
  publication, including when a reader drops its lease.
- Check durability and reclamation during construction, deep clone and
  restore. Retired markers and the roots they retire use the same prefix.
- Exercise a missing ancestor on an otherwise clean branch. Merge, rollback
  and historical lookups must surface the read or commit error. Writable
  verification must fail before sweeping an incomplete reachable graph.
  Repeating a successful collection must preserve the same live graph.
