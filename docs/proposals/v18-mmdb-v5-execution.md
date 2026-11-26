# VSDB v18 on MMDB v5 — implementation and acceptance

The v18 collection and universe formats already exist at VSDB `6ddaed9`.
This plan integrates them with MMDB `255015a`, starting from a v4.3.15
runtime. It preserves the application-visible handle identity and collection
formats. Engine comparisons below apply to the indicated storage workload.
[Joint design](https://github.com/rust-util-collections/mmdb/blob/255015ad0f488f09c7985736a903d3171876bcaf/docs/proposals/mmdb-v5-vsdb-v18-improvements.md) · [v18 format inventory](v18-roadmap.md)

## Implementation record

The runtime selects MMDB v5 at `255015a`; its offline reader alone uses
registry MMDB 4.3.15. The normal Cargo dependency tree and fresh-clone
workspace builds verify that split. `stage_universe` stages the universe
and all registered placements for both supported v4 source versions.
Physical v18 fixture `40a0a8c` was produced by the real v4-backed runtime:
scanning the new universe verifies an embedded map, a historical commit,
a Merkle proof against the original root, a DAG edge, SlotDex order and
VecDex result distances. The separate WAL fixture forces an unclosed writer.
[Operator procedure](../../migration/README.md) · [Physical collection fixture](../../migration/tests/collections_generation.rs)

The tests exercise native fences across rotation, both retryable admission
paths and a malformed write, as well as checked scans, decoder errors and
a late clone failure after already-committed destination chunks. The
adapter checks the selected v5 geometry for every anchored shard before
recovering any writable shard. [Adapter contracts](../../core/tests/v5_adapter_test.rs) · [Admission and fence proofs](../../core/src/common/engine/mmdb/v5_tests.rs)

On the default 16 shards, DagMap child creation dropped from two engine
submissions to one and improved full-operation p99 by 31.05% against the
earlier native v5 path and 33.38% against v4-backed v18. Four dependent
VerMap updates became one submission. After adding checked working-root
reads, the final five-pair repeat showed p99 improving by 57.31%. For
operations whose final values are already known, `write_batch` remains the
direct path. These are independent operations on generated data, with
matched durability and separate ordinary versus diagnostic builds.
[Publication runs](../evaluations/v18-publication/README.md) · [Independent review record](reviews/v18-mmdb-v5/README.md)

The initial 6 MiB write-buffer trial failed the hot-shard throughput
floor against 16 MiB on the real adapter (−27.60% across matched pairs).
The selected per-shard formula is `clamp(B / 8 / S, 64 KiB, 512 MiB)`,
with four immutable slots. At the 2 GiB/16-shard fixture the larger
buffer met the measured 1 GiB screen in both placements, including the
separate all-active read/write fixture with a populated shared cache.
Record remaining L0/immutable work alongside the measured throughput.
[Real adapter evidence](../evaluations/v18-adapter/README.md) · [Units and application sizing](shared-mem-pool.md)

For warm B+ tree lookup the ordinary upgraded path was slower than the
v4-backed control. The tested header-reuse scan improved p99 by 9.40%;
the separate borrowed-leaf prototype cut requested allocation from about
1.7 KiB to under 80 bytes per lookup but improved hit/miss p99 by only
3.37%/4.36%. Both missed their preregistered latency criteria. Keep the
current tree implementation. The final stock v4/v5 comparison at `6854273`
confirmed 12.03% higher hit p99 and 11.83% higher miss p99. Scan throughput
was 16.82% lower; the 32.83% p99 cost stayed within the scan's 41.18% A/A
variation. [Final stock trial](../evaluations/v18-tree-v5/stock/README.md) · [Tree and upgrade measurements](../evaluations/v18-tree-v5/README.md) · [Leaf projection trial](../evaluations/v18-tree-v5/projection/README.md)

On the measured 8,192-node warm HNSW graph, v4-backed v18 had median
run p99 of 184.57 µs and the checked scalar v5 implementation with the
selected buffer profile at `6854273` had 253.38 µs, a **37.48% paired
regression** (v4/v4 maximum 22.19%). Each of five
pairs returned identical ordered results. Width 32 acquired fewer blocks
but missed the complete-query gate. Width eight's larger independent
repeat gained 9.15%, below the 10% target and its 19.29% control spread.
The scoring-cache candidate also missed. The ordinary search decodes one
pinned vector at a time and propagates storage and corrupt-row errors.
These upgrade costs and the publication gains describe different operations.
[VecDex results and matched controls](../evaluations/v18-vecdex-v5/README.md)

## Delivery sequence

| Unit | Change | Observable outcome and acceptance |
|---|---|---|
| 1. Engine adapter | Select the verified v5 git revision and a compatible version in the workspace. Keep a separate exact v4 reader for offline conversion. Use native WAL durability and fences, existence probes, prefixed writes and DB-bound snapshots. Expose retryable admission and typed storage failures. | Each nonempty ordinary or sync batch adds one logical WAL record. Empty ordinary and sync batches make no WAL change. An empty fence changes neither sequence nor visible rows. A successful fence covers preceding writes on the same shard, including rotations. Namespace fences retain their existing scoped and read-only behavior. Rejected no-slowdown writes preserve rows and remain distinguishable from malformed requests. |
| 2. Reads and resources | Decode against a borrowed pin where the value is consumed immediately. Preserve owned reads, detached scans, prefix bounds, typed iterator termination, and public panic/fallible contracts. Expose pool-wide capacities/charges once and member-local pins and memtables by shard. Size with an immutable limit of four and explicit nominal buffer and pool sizing. | Compare empty/hit/miss/deleted results and scan bounds at identical snapshots. Hold an iterator through writes, flush and view drop. Under a 2 GiB/16-shard example, evaluate a 30 MiB nominal per-shard buffer allowance (4 immutables × 6 MiB plus active) and 256 MiB per evictable pool store. Account for real RSS, per-tree caches and end-of-run L0 debt separately. Check small budgets, other shard counts and reopen. |
| 3. Offline cutover | Provide an executable or library path to a complete new generation from v18/v4 or v17/v4. Inventory the default and named namespaces, remap absolute placements and protect source/backup paths. Stream visible v4 key/value rows into fresh v5 shards with seven levels and eight-byte prefixes. Retain allocator floor, registry lifecycle, handles and application sidecars. | Compare row counts and a digest of ordered, length-framed keys and values on both sides; check final iterator status and writable v5 reopen with adapter geometry. Exercise real v4 SSTs, residual WAL, deletions, external namespaces, frozen v17 handles and typed readers. Missing established shards, mixed input, corrupt records, overlapping placements or an occupied destination fail before exposing a new generation. Incomplete staging cannot become active. Verify restart, protected source and fresh prefix allocation. |
| 4. Atomic DagMap child creation | Use one checked, shard-local, two-prefix batch for the child's metadata and the parent's edge plus allocator metadata. Preserve the existing crash-safe prefix allocator and prune recovery. | Snapshot and reopen see the old graph or a complete child with the next edge ID advanced. Reject different namespace or shard before append. Prove one batch per child versus the previous two and compare child throughput/latency. Unused reserved prefixes can remain unused. |
| 5. Dependent VerMap edits | Offer a bounded working-edit session on one branch and its captured working root under the existing writer-exclusion contract. Reads see earlier edits. On success reuse the existing single working-root publication. The history commit retains its own durability. | Test overwrites, delete/reinsert, an empty edit, user error, size limit, abort and reopen with old snapshots held. Compare repeated dependent edits to individual operations and compare precomputed edits with existing write_batch. Count publications and measure batch latency. |
| 6. VecDex and tree reads | Compare snapshot-bound scalar pinned decoding with bounded groups of vectors from one HNSW neighbor expansion. Replay distances, filter callbacks and ties in traversal order. Check a retained tree leaf header as a separate candidate while preserving mutable root refresh and iterator leases. | The two paths return identical IDs/distances and callback order on the same graph and queries, including visibility under mutation. Exercise clustered and scattered candidates, partial groups, caps and missing rows. Count probes/copies and measure end-to-end query p99, allocations or retained bytes. Keep scalar behavior where benefit fails the gate. |
| 7. Handoff | Update runtime, migration and collection guides, v18 roadmap, project guidance and affected skill checklists; preserve historical measurements with their original revision. Archive independent reviews and dispositions alongside measurements. | An ordinary clone resolves the dependency. The release notes explain fresh creation, all supported upgrade routes and rollback before new writes. The x86_64 gate and performance receipts identify the tested code. Native ARM macOS verification remains a maintainer handoff. |

## Offline generation and ownership

For v17, keep the v4-capable logical converter available at the frozen
VSDB revision. Preserve both the logical conversion and physical conversion
as independently testable operations. Document their order for each entry
point, including application-owned embedded handles, edge remapping and
saved tree roots. A v18/v4 source requires only physical conversion.
A fresh v18/v5 universe opens directly. An existing v18/v5 universe reopens
normally. Reject ambiguous or mixed shards. Preserve an uncreated or pending
engine's lifecycle so the runtime can complete its normal initialization.
Do not initialize missing shards in an established namespace.

Require a quiesced source and an absent destination. Validate external path
mappings before opening a converter; each staged path must be disjoint from
the source and other placements. Select a complete generation through one
application configuration switch, including external mounts. Before the
first new write the operator can return to the original generation and its
matching binary. After writes begin, rollback needs reconciliation. Cover
source/destination aliasing, an interruption between shards, an interrupted
or competing publication and failure while verifying the last shard. A
source snapshot of filenames and contents is the oracle for preservation.
[Existing logical fixtures and operator responsibilities](../../migration/README.md)

## Clarifications from the independent plan reviews

The two opt-in raw commit operations consume the batch. A caller that
receives `Busy` reconstructs its logical edits for a later attempt. A failed
I/O acknowledgement requires outcome reconciliation. Count storage requests
and working-root publications separately, including the empty case.
Keep the latest read view's lifetime identical when comparing query
implementations. Record the maximum pin backing, process RSS and selected
cache regime separately. The sizing example covers one engine in a
16-shard process. A legal record may exceed a memtable threshold.

The offline CLI publishes all placements beneath a single new generation.
The application selector switches only after the command has acknowledged
durability. A parent-sync error after exclusive rename can leave an entire
visible generation. Keep the original selector and retry into a fresh
absent path without replacing the visible generation.

## Measurement and promotion

Use local x86_64 Linux, an isolated filesystem directory, pinned CPU sets,
fresh processes, an identical generated dataset/query trace and the same
durability, cache budget and load within each pair. Record compiler, source
revisions, storage medium, geometry, sample counts and raw per-run results.
Record both within-pair changes and control-to-control variation. Use five
interleaved A/A and five A/B pairs for a latency promotion decision, at
least 10,000 completed queries per process for reporting p99. Shorter runs
can show operation counts or throughput. State any noise or fixture limits.
Use the existing v4 adapter as a separate upgrade reference on identical
logical workloads; use the v5 scalar or batch path as each candidate's control.

For candidate query optimizations seek at least 10% lower end-to-end p99,
above the matched control spread in at least four of five pairs, with at
most 5% median singleton or ordinary-path regression. For DagMap and VerMap,
also check the deterministic benefit in successful engine submissions per
logical operation. For the memory handoff, measure one hot and 16 active
shards under the 1 GiB engine envelope in the 2 GiB process scenario.
Compare RSS, admitted throughput (floor 95% of reference), p99 (ceiling
105%), and work remaining at the end. Isolate a foreground map write during
a large same-shard VerMap operation and again with separate shards.
Distinguish API service time from scheduled-arrival latency. Warm engine
cache, OS-cache-only and observed storage reads are different regimes.
Describe disk-read claims using measured filesystem/device counters.
[Measured MMDB controls](https://github.com/rust-util-collections/mmdb/blob/255015ad0f488f09c7985736a903d3171876bcaf/docs/benchmarks/engine-profile/results.md)

The per-shard prefix reservation redesign needs evidence that allocating
discarded prefixes is a limiting cost. Expose the existing reservation
invariants to tests before considering that change. Preserve existing
physical value placement and graph layout; generation-based rebuild and
physical checkpoints remain later design decisions in the joint roadmap.

## Validation and independent review

Run scoped Cargo tests for each behavior, with isolated `VSDB_BASE_DIR`.
Exercise subprocess exits for durability and independent reopen, real v4
stores for migration, and fallible callers for backend errors. Run the
seven commands in the [workspace gate](../ai/commit-protocol.md#final-workspace-gate)
on local x86_64, including debug/release test suites and doctests. Record
the actual target and any release prerequisite. The maintainer can run the
same commands natively on ARM macOS, including directory publication,
filesystem locks, read-only reopen and migration. Skip local ARM cross-builds.

Use Grok Build, agy, Claude Code and OpenCode in independent read-only
copies with English prompts and their configured default models. Review
the plan, the storage/upgrade and collection milestones, and the integrated
final code. Reproduce or check each material finding against source or tests
and record the disposition. Publish MMDB v5 to the registry before
publishing VSDB v18 crates; use its exact reachable Git revision during
joint development. Version 18 remains unreleased through this work.

## Native ARM macOS handoff

Run this from the VSDB checkout on the target Mac. Use local writable
storage supporting advisory locks, directory sync and exclusive rename.
The host gate here uses Linux/x86_64. The following run produces the
separate macOS/AArch64 correctness evidence.

```sh
uname -sm
rustc +stable -Vv
scratch=$(mktemp -d "${TMPDIR:-/tmp}/vsdb-v18.XXXXXX")
export VSDB_BASE_DIR="$scratch/db"
mkdir -p "$VSDB_BASE_DIR"
cargo +stable fmt --all -- --check
cargo +stable clippy --workspace
cargo +stable check --workspace --tests
cargo +stable check --workspace --benches
cargo +stable test --workspace --tests
cargo +stable test --workspace --release --tests
cargo +stable test --workspace --doc
```

The migration integration tests must publish a fresh generation exclusively,
preserve both an SST-only value and a residual WAL value, and open the
external namespace writable. A competing destination must survive a retry.
The adapter tests exercise read-only lock behavior, a native fence across
WAL rotation and an error in a later shard before any earlier shard is
recovered. The grouped-read device evidence in the Linux benchmarks uses
Linux process I/O counters. These native correctness tests work independently
of those counters. [Staged generation](../../migration/tests/staged_generation.rs) · [Adapter](../../core/tests/v5_adapter_test.rs)
