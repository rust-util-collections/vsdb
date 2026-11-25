# Independent VSDB v18 and MMDB v5 integration reviews

The four tools inspected independent, read-only snapshots of VSDB and the
pinned MMDB checkout. Each checkpoint used an identical English prompt and
the locally configured default model for Grok Build, agy, Claude Code and
OpenCode. The compressed JSON files preserve the exact checked revisions,
prompt, prompt digest and terminal assessments. Paths inside the original
reports refer to the reviewer's isolated checkout. The maintainers of this
change checked the findings against source, public operations, fixtures and
measurements. [Execution plan and acceptance](../../v18-mmdb-v5-execution.md)

## Plan

[All four plan reviews](plan.json.gz) cover the initial plan at `85c32a5`.
The plan was committed before the implementation. Its clarified acceptance
criteria cover the following decisions.

| Review topic | Verification or correction |
|---|---|
| A v5-only migration dependency would break v17 lookup and the v18-on-v4 upgrade route. | The runtime selects v5. The offline crate keeps the exact registry v4.3.15 reader and stages a complete generation before logical conversion. An earlier v4-backed converter is documented for a logical-first route. Both majors resolve in a clean workspace. [Upgrade procedure](../../../../migration/README.md) |
| Physical export must preserve identity and distinguish a successful rename from a durable acknowledgement. | The staging tree contains the universe, registered engines, allocator floor, application attachments and all bound external placements. A sync failure after rename leaves the selector with the application. An occupied destination is preserved. [Staging and interruption fixtures](../../../../migration/src/universe/staging.rs) |
| Busy must be observable on actual nonblocking operations; a consumed request needs a documented retry. | Two fallible batch entry points map immutable and L0 pressure to Busy and preserve the underlying error. Rebuild the logical batch to retry. Empty ordinary/sync batches allocate no sequence; `sync_wal` is the explicit fence. [Admission fixtures](../../../../core/src/common/engine/mmdb/v5_tests.rs) |
| A byte estimate or a threshold for one memory component cannot bound total RSS. | Compare capacities, observed charges, peak RSS and outstanding storage work. Count two stores in each pool once and report caller-held backing separately. Actual write-buffer overshoot and independent namespace budgets are part of this interpretation. [Sizing policy](../../shared-mem-pool.md) |
| Distinguish one working root from an engine submission and check VecDex locality at the complete-query level. | The publication trials count real issuing-thread engine requests separately from branch generations. The VecDex trial uses the same graph, ordered query trace, callbacks and visit cap, qualifies storage traffic per request, and compares bounded chunks with a checked scalar baseline. [Publication](../../../evaluations/v18-publication/README.md) · [VecDex](../../../evaluations/v18-vecdex-v5/README.md) |

Read an assessment directly from this directory:

```sh
python3 -c 'import gzip,json; print(json.load(gzip.open("plan.json.gz","rt"))["reports"]["grok"])'
```

## Engine adapter

At `b5f3153`, the new core's native write/read paths compiled and passed
their focused tests. The concurrently developed migration port subsequently
completed the workspace dependency transition in `6d594d5`.
[All four adapter reports](adapter.json.gz) · [v4-capable migration crate](../../../../migration/Cargo.toml)

| Observation | Verified disposition |
|---|---|
| Writable opening an early valid shard could recover it before rejecting a late incompatible shard; a read-only open adopts persisted geometry. | The adapter now inspects all existing v5 shard headers and CRCs before mutable recovery or initialization backfill. Ready and pending roots, wrong levels, wrong prefixes and untouched earlier files have dedicated tests. [Geometry preflight](../../../../core/src/common/engine/mmdb/geometry.rs) · [Reopen tests](../../../../core/tests/v5_adapter_test.rs) |
| A late scan failure during fallible cloning could unwind and skip cleanup. | The clone uses checked lending steps, propagates the original storage error and cleans already-written chunks. The regression creates a source corruption after 4,096 destination rows have been written. [Clone tests](../../../../core/src/common/engine/clone_tests.rs) |
| Ordinary live-map existence still copied the value. Facade error and telemetry types were missing. | The public live and captured views now use native existence checks. The top-level VSDB crate re-exports the documented types. Separate large-value measurements check allocations on each public route. [Adapter measurements](../../../evaluations/v18-adapter/README.md) |
| A fence after an asynchronous write must cover rotation; both transient pressure paths must remain retryable. | A child writes across rotation, fences, and exits without closing. Reopen verifies the sealed and residual rows. A real three-file L0 and a parked immutable flush each return typed Busy without changes to WAL, sequence or rows; reconstructing the batch succeeds once after pressure clears. [Native pressure tests](../../../../core/src/common/engine/mmdb/v5_tests.rs) |
| Scope of first-use allocation, grouped backing retention, named budget floors and root leases. | A raw map already allocates its prefix on first use, including a read; an untouched WAL fence has no assigned shard. Group size bounds slots; distinct block or arena backing can remain live for the visit. Default and named engine budgets retain their documented distinct input normalization. The working-session lease is released before root publication. [API guide](../../../../core/docs/api.md) · [Working-edit tests](../../../../strata/src/versioned/test.rs) |

## Collection operations

The [four collection reviews](collections.json.gz) examined the concrete
DagMap, VerMap and VecDex candidates at `0ecb57d`. Succession from a
single-root writer through publication was checked independently of later
candidate latency measurements.

- The edit callback retained a temporary root lease through publication.
  The regression observed a retired-root marker with a sole writer, then
  zero after the private lease was dropped. External snapshots retain their
  previous values. Changing the root through an alias during a callback
  now returns `WorkingRootChanged`; the byte budget has a distinct error.
  The edit API preserves the existing exclusion of conflicting writers.
  [Working-root tests](../../../../strata/src/versioned/test.rs)
- The three DagMap records now commit in one batch. A child-process fixture
  checks the parent's previous snapshot, its advanced edge ID, the child's
  ownership and a subsequent registration after fence/exit/reopen. Aliases
  of the same raw map use documented `here` then `there` order. Cross-shard
  and cross-namespace pairs fail before any row is written.
  [DAG test](../../../../strata/src/dagmap/raw/test.rs) · [Raw tests](../../../../core/tests/colocated.rs)
- A VecDex traversal could unwind on malformed key, state or adjacency
  data, silently drop a hit missing its reverse key, or fetch another
  adjacency after reaching the visit cap. Real fixtures verify typed
  failures, cap boundaries after a neighbor or component restart, and
  seed deduplication. Slot visits preserve filter order on success; an
  error can terminate a query after some callbacks. The ordinary query
  uses checked scalar pins. The grouped experiments report peak backing
  capacity and end-to-end query p99. [Traversal fixtures](../../../../strata/src/vecdex/test.rs) · [Published query controls](../../../evaluations/v18-vecdex-v5/README.md)
- The ordinary live-map existence check was routed through the v5 native
  probe, as was the captured-view check. [Allocation evidence](../../../evaluations/v18-adapter/README.md)

## Offline generation conversion

[All four migration reports](migration.json.gz) cover `a04e8e0`. The
protected input can span volumes. A successful operation publishes one
self-contained generation with absolute external placements bound to its
canonical path. The saved evidence includes these checks:

| Finding or hypothesis | Result |
|---|---|
| A mount alias or a recursive copy could put temporary output inside the source. A changed external alias could separate the checked and exported images. | Inventory stores physical directory identities, rejects overlaps and repeated directories before creating staging, and copies a finite set of files. Export and allocator validation read the same private image. Regular-file checks precede opening data or lock files. A local Linux mount-alias check rejected an aliased descendant. [Staging implementation and tests](../../../../migration/src/universe/staging.rs) |
| Protected backups may contain read-only locks. A different service account may lack access to a published owner-private generation. | Only private working lock files are recreated. A backup with files mode 0444 and directories 0555 staged successfully, and its new shard accepted a sync write. The operator guide explains preparing access for a different service account before the application selects the generation. [Upgrade guide](../../../../migration/README.md) |
| A lexical path comparison can fail through a symlinked destination parent, including an OS temporary directory. | Registry bindings and the CLI use the canonical path. The regression first failed its lexical comparison, then passed when the writable adapter reopened the same namespace through both aliases. [Generation fixture](../../../../migration/tests/staged_generation.rs) |
| Export used general MMDB defaults and the initial fixture lacked an untouched SST row. | Live opens and export share the same persistent format options, including the 16 KiB block target and bottom-level Zstd. A compressible export exercised that policy. A distinct fixture requires an SST-only row, a residual WAL replacement, an empty value and a deletion through both writable opens. [Integration tests](../../../../migration/tests/staged_generation.rs) |
| A relocated namespace could leave an unreferenced populated derived tree; a missing initializer could be misclassified. | The converter rejects ambiguous populated residue while accepting an empty placeholder. A header with neither an initializer nor a ready marker reports corruption. Truncating the second readable block demonstrates that the v5 scan preserves typed I/O, while v4's iterator only supplies its original text. [Frozen logical and physical upgrade tests](../../../../migration/tests/) |

The original files survive interruption, a racing publisher and a late
verification error. A successful exclusive rename followed by a failed
parent directory sync can leave a visible directory. Retry into another
absent path with the previous generation still selected. [Publication protocol](../../../../migration/README.md)
