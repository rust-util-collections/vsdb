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
