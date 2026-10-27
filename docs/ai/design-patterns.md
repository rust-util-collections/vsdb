# VSDB Design Anti-Patterns

Design lens for reviews. Language/protocol bugs: `technical-patterns.md`,
`patterns/*`. Report only with a concrete failure; apply `false-positive-guide.md`
first. Public/on-disk: also `compatibility-policy.md`.

| Prefix | Question |
|--------|----------|
| D-LOCK | Convoy, deadlock, or needless serialization? |
| D-RES | Acquire released on every path under load/error? |
| D-BOUND | Work, fan-out, memory, or retries uncapped? |
| D-STATE | Concurrent/partial exec → illegal or split state? |
| D-FAIL | Error/crash outcome safe, observable, unambiguous? |
| D-API | Public/export/on-disk change break callers or old data? |

## D-LOCK

- **1** Global exclusive lock where finer locking/atomics/channels suffice.
- **2** Lock across full engine flush, join, or heavy COW rebuild.
- **3** Nested locks without documented order (registry vs table vs allocator).
- **4** Check-then-act open/create (namespace, prefix) without continuous guard.

Skip documented SWMR single-writer paths unless multi-writer convoy is shown.
`REGISTRY_LOCK` spanning teardown is intentional (FP-15) unless a deadlock cycle is shown.

## D-RES

- **1** Unbounded memory (pending buffers, tier cache, HNSW lists) without lifecycle cap.
- **2** Engine handle / path leak after close failure.
- **3** Background threads not reclaimed on namespace close.
- **4** Temp files / half-created roots not cleaned or rejected.
- **5** Work continues after ownership drop when protocol requires stop.

## D-BOUND

- **1** Unbounded batch/pending (bulk_load, clone_in, staged slabs) without chunking.
- **2** Unbounded fan-out (HNSW degree, DagMap children) without prune/cap.
- **3** Retry/reopen loops without budget that amplify load.
- **4** Hot-path full-structure materialization when streaming/bounded chunks suffice.
- **5** Expensive work before cheap reject (dim mismatch, bad meta, wrong ns).

## D-STATE

- **1** Multi-step durable transition not atomic (VerMap dirty cascade; staged
  batch; prune phasing; namespace open vs marker).
- **2** Dual write without single owner (parent slot vs registry; key↔node maps).
- **3** Illegal DAG/ref-count/COW transition (cycle, premature free, in-place mutate).
- **4** TOCTOU outside serializing lock (namespace open, prefix alloc).
- **5** Ordering/uniqueness from wall clock only under concurrency.

## D-FAIL

- **1** Durability side effect dropped after log/catch while reporting success.
- **2** Partial success as full success (multi-shard / multi-chunk ops).
- **3** Silent degrade (stale trie cache; wrong metric wire tag).
- **4** Distinct failures collapsed (retry vs fatal unclear).
- **5** Crash mid-protocol → unrecoverable or lying on-disk state.

## D-API

- **1** Public export/behavior change without docs/tests/CHANGELOG.
- **2** Defaults, error kinds, or collection semantics change without call-site evidence.
- **3** Internal detail leaked into public surface without need.
- **4** On-disk/tag/meta change without compatibility-policy (major + migration).

## Apply when

- Multi-subsystem or design-shaped diff: walk relevant families once.
- Full audit: after subsystem depth, on engine/ns lifecycle, versioning, COW,
  staged mutators, tries proofs, HNSW, public API.
- Small shape-preserving edit: skip empty families.

Severity: `review-core.md`. IDs label evidence; they are not findings alone.
