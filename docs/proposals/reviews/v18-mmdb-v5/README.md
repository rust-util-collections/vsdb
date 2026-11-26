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

## Performance measurement tools

The four reviewers inspected the benchmark drivers and interpretation at
`cb6ca9d`. Later observations and corrections have their own source and
binary identifiers. [Four assessments and the English brief](measurements.json.gz)

| Finding | Checked outcome |
|---|---|
| A missed offered load or a burst of batch completions can make a candidate appear eligible. | The analyzer reports descriptive values for every process and withholds a decision gain when any matched block fails the schedule. It reports scalar and batch rates and the capacity to complete the prescribed 1:2 demand mix. Standalone v4/v4 controls assess v4 repeatability. Synthetic tests cover insufficient foreground completions, a favorable p99 with excessive backlog and a mismatched reference executable. [Adapter evidence tests](../../../evaluations/v18-adapter/test_evidence.py) |
| The publication tail includes dispatch delay, and one-shard DagMap sensitivity depends on the A/A denominator. | Arrival-to-ack was the predeclared foreground outcome. In-call service p99 on the same placement pairs gave 22.24 versus 14.51 µs, with a 32.25% paired gain and 99.31% A/A variation. For DagMap the one-shard 31.38% result clears a 25.39% directed band but falls within a 34.02% symmetric band. The default 16-shard gain of 31.05% clears 7.41% under either convention. A separate 1,000-empty-session diagnostic measured zero submissions, including fences. [Publication measurements](../../../evaluations/v18-publication/README.md) |
| RSS can include preload and caller allocations; block capacity retained by a query describes a different resource. | The shard budget applies to the observed process high-water mark. Endpoint gauges separately report current memtables, pins and both stores in the shared pool. One-hot and all-active runs have the same caller budget; the 16 and 6 MiB settings deliberately compare different per-shard buffering. VecDex measures per-process RSS separately from distinct simultaneously retained SST capacity, validates comparable arms, and requires qualifying per-request storage reads for the device regime. [Memory protocol](../../../evaluations/v18-adapter/README.md) · [VecDex resource checks](../../../evaluations/v18-vecdex-v5/summarize.py) |
| VecDex width eight and the tree prototypes needed independent controls. | The longer VecDex trial directly paired the shipped scalar path with width eight and calibrated shipped/shipped variation. The 9.15% paired gain fell below the 10% target and the 19.29% A/A spread. The first tree comparison reported its inherited Btrfs compression and used one v4 reference per seed. The later borrowed-leaf comparison includes a separate v4/v4 pair and shares the same filesystem settings within every pair. Neither trial attributes its latency to Btrfs. The two tree candidates have independent v5 controls, raw traces and binary identities. The scan-header and point-projection results support retaining the current tree. [VecDex trials](../../../evaluations/v18-vecdex-v5/README.md) · [Tree observations](../../../evaluations/v18-tree-v5/README.md) |


The adapter's original 17-writer workload measures the hot and all-active
write schedules. It neither populates a working set through application
reads nor establishes a general read/write RSS bound. Flushes and
compactions can themselves charge the pool. Later measurements under
ordinary cache reads and sustained writes are a separate stress test.
The original workload does not measure all cache stores at capacity or
the higher-level per-tree caches.
The whole-process ceiling includes preload as specified. [Adapter workloads and result coverage](../../../evaluations/v18-adapter/README.md)

The allocation packet's two probe hashes match the live allocation entries
in the build manifest. The v5 `contains_key` measurements used `d9f00f4`
and the memory timing binary used `b5f3153`. The reproduction instructions
build the later v5 allocation executable separately. These revisions represent
different measured source paths. [Binary and driver fingerprints](../../../evaluations/v18-adapter/builds.json)

## First integrated release review

The four reviewers inspected VSDB `7aafa3d` against the v4-backed
`6ddaed9` baseline and MMDB `255015a`. The source findings below were
checked after the individual checkpoint reviews. [Complete prompts and
terminal reports](release.json.gz)

| Finding | Verification and resolution |
|---|---|
| An existing allocator ceiling could equal a visible map prefix. | A real external v4 store reproduced successful staging with that stale ceiling. `18ba9a1` checks physical row owners during the verified export and checks the filenames of saved instances, including empty maps. Established and partially initialized namespaces now fail before publication, preserving the source. A complete foreign-handle frame in opaque values or an attachment leaves the local ceiling unchanged. [Cutover fixtures](../../../../migration/tests/staged_generation.rs) |
| Inserting into a VecDex graph with malformed adjacency could panic. | `bdde0a6` propagates the storage or decoding error through staged traversal, insertion and linked updates. The focused test inserts a second node after damaging a persisted adjacency row and requires `Corrupt` before publishing graph rows. [VecDex regression tests](../../../../strata/src/vecdex/test.rs) |
| A working edit could panic while reading a referenced tree page. | `470b462` provides checked lookup on the captured root. Absent user keys remain ordinary misses. Referenced missing pages and invalid reachable objects fail the callback. The reopen fixture confirms the error preserves the working root and generation and abandons the earlier staged edit. [Working edit tests](../../../../strata/src/versioned/test.rs) |
| A closing writer could release its lock after the migration's filename inventory. | `d544071` acquires the existing universe and shard locks, checks the registry used to discover them and then inventories files. With a v4 writer holding the lock, the old test hook reached inventory. The corrected stage refuses the active writer and a retry includes both the last acknowledged row and an application sidecar. [Closing-writer test](../../../../migration/src/universe/staging.rs) |
| The runtime classified missing required lifecycle markers as ordinary I/O, and DagMap namespace precedence differed by build profile. | `bcffffa` reports a required absent or malformed lifecycle record as `Corrupt` with its path. Probes cover default and named engines in writable and read-only opens. `32ae751` makes child constructors consistently inherit the parent namespace across raw, raw-key and typed maps. [Engine marker tests](../../../../core/tests/universe_header.rs) · [DagMap namespace tests](../../../../strata/tests/dagmap_namespace_test.rs) |

The fixture at `9733c19` confirms the released v17 rule when both
allocator records exist. The larger of the file and default-shard legacy
values supplies the exclusive floor. The physical exporter preserves all
visible rows, including a reserved one-byte legacy row. A v18 source with
an existing allocator file instead checks ownership against that file.
[Versioned allocator fixture](../../../../migration/tests/staged_generation.rs)

The benchmark revisions cited by the reviewers (`b5f3153`, `d9f00f4`,
`0481462`, `504630f`, `1300ad2` and `6854273`) are ancestors of the
reviewed integration. The change log and execution record now identify
the stock 16 MiB VecDex and tree comparisons separately from earlier
experimental builds. The integrated reviewers inspected sources. Native
gate results are recorded on the tested revision, with ARM macOS reserved
for the maintainer. [Implementation and verification record](../../v18-mmdb-v5-execution.md)

## Working-read page validation

[Four independent confirmations](leaf-confirmation.json.gz) cover the
final leaf-class correction at `2ae6149`. Shared validation derives the
page class from its actual slots. The fallible leased lookup checks that
class against the persisted ID, returns `Corrupt` on invalid references,
and distinguishes missing objects from absent keys. The tests changed
a real leaf to reference a valid older blob under an inline-only ID and
also corrupted a slot offset while leaving the header valid. Returning
either error from a callback leaves its prior staged edits unpublished.
The four reviewers found the correction satisfactory.
[Regression tests](../../../../strata/src/versioned/test.rs) · [Checked-path measurements](../../../evaluations/v18-publication/results/verified-leaf/summary.csv)

## Integrated correction verification

The [four correction reports](release-confirmation.json.gz) inspected
`246de8f` against `7aafa3d`. The following dispositions were verified
against the applicable format versions and ordinary public operations.

- A v17 writer could retain both a file ceiling and a higher, exclusive
  allocator value in default shard zero. Historical v17 used the maximum
  of the two. A frozen fixture accepts file `C`, live owner `C` and legacy
  floor `C+1`, then allocates `C+1`. For v18 the authoritative file cannot
  be contradicted by live map prefixes or saved IDs, even with a higher
  leftover legacy key. Opaque user values, including syntactically valid
  foreign handles, are preserved. Source bytes survive both outcomes.
  [Tests for both formats](../../../../migration/tests/staged_generation.rs)
- A working-edit read now distinguishes an absent key from an invalid
  class, out-of-range offset, missing object or zero-slot page reached
  from a committed root. An empty tree uses the distinguished empty root.
  Testing a deliberately malformed reachable page leaves the previous
  working head and generation intact. The 1,000 empty callbacks made zero
  native write or fence submissions; each callback still read a key.
  [Working-edit fixtures](../../../../strata/src/versioned/test.rs) · [Submission counters](../../../evaluations/v18-publication/README.md)
- Framing alone did not identify a valid-length adjacency containing an
  impossible future node ID. Tests reproduced a published bad mutation.
  At `1b51384`, persisted rows touched by a mutation use the committed
  allocation floor at chunk entry; staged rows use the current floor.
  A reference to an issued, subsequently removed node stays valid. The
  staged chunk returns `Corrupt` and leaves the full row stream unchanged
  on malformed data. [Mutation regressions](../../../../strata/src/vecdex/test.rs)
- The direct final stock upgrade packet was still being measured when
  reviewers inspected this checkpoint. The complete 80-process packet
  now provides independent v4/v4 and v4/v5 pairs, checked binary hashes
  and per-class rates in both placements. RSS and load-qualification
  outcomes are reported separately. [Selected upgrade results](../../../evaluations/v18-adapter/README.md)

## Corrected cutover and lifecycle operations

The [four final cutover assessments](cutover-confirmation.json.gz)
independently checked the production changes from `2ae6149` through
`a331ecc`. All confirmed the documented behavior. The source inventory
is now retained under all available source locks, after checking the
registry again. The closing-writer regression verifies the final storage
row and application attachment are copied together on retry. Only a
missing required lifecycle record maps to corruption; ordinary I/O
failures retain their original source. DagMap children inherit their
parent's placement in both build profiles. Referenced zero-slot pages
in a committed working root return a typed error without changing
the handling of a valid empty tree or temporary removal-builder pages.
[Lifecycle fixtures](../../../../core/tests/universe_header.rs) · [Namespace fixture](../../../../strata/tests/dagmap_namespace_test.rs)
