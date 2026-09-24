# VSDB False Positive Guide

Match before reporting. Suppress, or say why the rule does not apply.

## FP-1: Safe Rust ownership

**Skip:** UAF/double-free/dangling in safe Rust (no `unsafe`/raw ptr).
**Keep:** Logical UAF (e.g. NodeId after GC).

## FP-2: Alias exclusion is caller’s

`shadow()` and collections document exclusion per key/op. Disjoint-key map writes OK;
same-key or structural multi-key ops need documented broader serialize.
**Keep:** SAFETY contradicted by call site, or overlapping same-key/structural writers.

## FP-3: Prefix isolation

Structures use unique u64 prefixes. Cross-structure interference needs a concrete
prefix collision (`technical-patterns.md` 1.1) — not assumed.

## FP-4: unwrap/expect on proven state

**Skip** unless a production path can fail it. Check same-scope population,
caller guards, tests (panics OK).

## FP-5: Clippy already enforces

CI is deny-warnings. Skip lints; focus on semantics.

## FP-6: Advice without downside

No pure “consider”. Need wrong result, crash, or leak scenario.

## FP-7: Test code / parallel model

Tests may unwrap and use isolated scratch paths. Parallel: do not assume exclusive
global allocator/registry/base-dir. Configure the base dir once (`vsdb_configure` is one-shot);
race-tolerant global assertions. **Keep:** incorrect or parallel-unsafe tests.

## FP-8: Documented unsafe

Read `// SAFETY:`. **Keep:** broken prereqs, invalidated assumptions, missing/vague comment.

## FP-9: Perf off hot path

**Hot:** get/contains, insert/remove/batch, iter/range next, B+ lookup, HNSW search,
SlotDex page, trie hash. **Warm:** commit, merge loop, split/merge, HNSW insert,
prove/verify, trie batch update. **Cold:** branch create/delete, rollback, discard,
GC, init. Perf only on hot/warm with evidence (benches under `*/benches/`).

## FP-10: COW alloc by design

New node on mutation is required. Remove’s underflow path re-allocates a leaf it
just copied (borrow/merge) and discards the first copy; merge rebuilds via
`bulk_load` without sharing — both by design. **Keep:** in-place mutate (no new
NodeId), or a superseded intra-op node that is never discarded.

## FP-11: Ref-count wrong granularity

Trace full create→use→destroy incl. error/rollback before claiming leak/premature free.

## FP-12: Merkle root change expected

Root must change with data. **Keep:** change with no data change; no change when data changed; collision of distinct data.

## FP-13: Disposition without re-check

Won't Fix/Rejected not permanent. Re-check when code/callers/assumptions/subsystem
in scope; full audit → all. Carry-forward without check → LOW process. No freshness dates.

## FP-14: Fresh batch per bulk chunk

`clone_in` / chunked bulk use a new batch per bounded chunk by design (memory + chunk atomicity).
**Keep:** unbounded chunks, leak across commits, bad cleanup/retry, broken whole-op atomicity promise.

## FP-15: Close holds REGISTRY_LOCK through teardown

`OPEN_NAMESPACES` released before slow teardown; `REGISTRY_LOCK` intentionally blocks
same-id reopen while old engine still owns files. **Keep:** real deadlock cycle, table lock spanning teardown, or replacement that loses exclusion with smaller CS.

## FP-16: Legitimate no-ops

`Ok(())` / `None` / empty often correct. Placeholder only if contract requires work and body is stub.

## FP-17: Design ID is not a finding

D-\* labels need trigger + outcome. Prefer subsystem guides when they already catalog the bug.
