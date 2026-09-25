# Synchronous batches and ordinary collection read views

Status: candidate design assessment, not an implementation decision.
Date: 2026-09-25 UTC.
Baseline: `8ad80b1ca2649307ef36044630343660bb193147` (VSDB 17.0.7).

## Purpose and design boundary

Expose two engine capabilities in the ordinary collection layer: atomically
publish a batch after its WAL synchronization, and read several keys/ranges
from one captured state. Both are useful to embedded applications beyond the
consumer that motivated this assessment.

VSDB remains persistent std-like collections over MMDB, with explicit
versioning and specialized structures in higher layers. It does not become
a service transaction framework. Application commands, retry decisions,
admission ledgers, JSON schemas and recovery coordination remain application
responsibilities.

The recommended additions preserve:

- Prefix isolation, current shard routing, anonymous namespace placement,
  collection identities and all existing disk/handle formats.
- Existing write defaults and the distinction between atomic visibility and
  power-loss durability. Ordinary inserts do not start synchronizing per call.
- `&mut` mutation, the documented alias discipline, and deep-copy `Clone` on
  collection handles.
- `Option` and ordinary iterator items for reads; fatal engine faults may
  panic. No mandatory parallel family of `try_*` collection methods.
- The current `Arc<NsInner>` ownership model and refusal to close namespaces
  with live owning handles. No force-close or per-read closed-state machinery.
- Independent higher-level versioning, index caches and trie state.

No structural change is necessary for the initial additions. If implementation
requires changing one of these foundations, stop and obtain the owner's
confirmation for a concrete revised design before making that change.

## Consumer requirements must be justified first

The owner subsequently questioned whether ACC's current storage guarantees
are themselves overdesigned. Preserving every current implementation detail
is not the objective, and this proposal must not turn those details into VSDB
requirements. The additions below are technically compatible candidates;
neither is automatically required merely because an ACC migration would use it.

The consumer review identifies these distinctions:

- Durable acknowledgement and atomic receipt/quota/index updates protect
  actual accepted work. Removing them can lose acknowledged data or charge
  quota inconsistently; this is more than a presentation preference.
- ACC's three independent engines create cross-WAL dependency edges. A result
  persisted in one lane must not depend on intake that can still disappear in
  another. Synchronizing before visibility is one solution. The documented
  benchmarks do not include a controlled one-engine versus three-engine
  comparison, so they do not establish that this topology is necessary.
- One engine/WAL could simplify internal dependency ordering, but does not
  automatically order outbound effects in other services. Outbox eligibility
  and delivery still need an appropriate durable boundary.
- A coherent view is valuable for multi-key authorization, index joins and
  state transitions. Requiring an entire display page to share one instant
  may be stronger than the business requires. Consolidating a logical state
  into one stored record can sometimes remove the multi-read requirement.
- Rebuildable indexes, scoring projections and transient progress need not
  automatically receive the same persistence schedule as accepted records.
  Their recovery and published-response contracts must be checked before
  changing durability. An index included in an already-required primary batch
  does not add a separate fsync merely by existing.
- The existing Proof storage facade is small (about 240 production lines),
  with one DB, one serialized writer and native snapshots. Replacing a small
  engine primitive with locks, revision retries or repair logic could increase
  complexity. The important review target is data layout and lifecycle, not
  the number of assertions or uses of the word "guarantee".

Prioritize a consumer model comparison before implementing these APIs on its
behalf: current three-lane ACC versus a single-engine candidate, and current
split state versus cohesive records. Measure equivalent durability and real
read/write workloads; do not compare different failure guarantees as if they
were a pure performance optimization. Merging stores is a structural proposal
requiring the owner's confirmation before implementation.

The low-cost `commit_sync` addition can still have independent library value.
Borrowed read views likewise fit the engine, but should be justified by actual
multi-query callers rather than demanded as a universal service architecture.
No implementation is authorized by this assessment itself.

## 1. Opt-in synchronous batch commit

### Public contract

Add `commit_sync(self) -> Result<()>` alongside existing `commit()` on
`MapxRawBatch`, `MapxOrdRawKeyBatch`, `MapxBatch` and `MapxOrdBatch`.
Both consume the batch; dropping an uncommitted batch remains a discard.
`batch_wiped()` automatically gains the same choice through its batch type.

On a successful nonempty synchronous commit, the WAL is synchronized before
the batch becomes visible through the engine's committed sequence. Visibility
is atomic across that collection's changed keys. There is no new cross-map,
cross-shard or cross-namespace transaction guarantee, even for colocated maps.

An empty batch stays a no-op after the existing capability checks. It does
not fence earlier writes; callers use `sync_wal` / `try_sync_wal` for that.
An ordinary asynchronous commit remains asynchronous. A reader may observe
such an ordinary commit before its later fence completes.

Keep `Result` here because the existing batch API already returns it, including
expected size/capability rejection. Accepting panic for fatal collection reads
does not require changing established fallible administrative or batch APIs.

Clarify failure semantics: rejection before submission is different from an
I/O failure after a WAL attempt. Do not promise that an error means the batch
can never appear after recovery, and do not retry it automatically. Preserve
diagnostics and the engine's fault handling.

### Smallest implementation

`MmdbBatch` already holds one MMDB `WriteBatch` and its owning engine/prefix.
Share its commit implementation internally and select `WriteOptions.sync`.
Do not expose MMDB's entire option structure, `disable_wal`, an engine handle,
or a generic transaction trait to users for this purpose.

The typed wrappers only forward the new commit choice after their existing
encoding. No new record representation, writer lock, scheduler or storage
buffer is needed. The current engine group-commit behavior remains available.

Use the existing synchronous write path, not `commit(); sync_wal()`: the latter
has a visible interval before the fence. Do not use `flush()`, which forces
memtable work. A single batch commit cannot promise zero unrelated background
compaction or zero engine contention; it merely avoids an unconditional flush.

### Scope exclusions

Do not automatically switch `StagedRows`, SlotDex, VecDex, DAG operations or
VerMap internals to synchronous commits. Those structures have their own
atomicity, cache and publication contracts. Existing VerMap WAL boundaries
remain intact. A later use case must justify each such change independently.

## 2. Borrowed read views for ordinary maps

### Public contract

Add a named read-only view through `map.read_view()`. The initial view supports
`get`, `contains_key`, `iter` and the corresponding map's existing range
semantics. It captures one committed state when created; subsequent queries
through that view use that state. It neither stages writes nor includes an
uncommitted batch. Missing keys remain `None`; fatal storage faults may panic.

Use map-specific names such as `MapxRawReadView<'a>` and typed equivalents.
Avoid reusing the existing top-level `Snapshot` name: that type describes
VerMap's versioned tree state. Proposed names are illustrative, not shipped
API declarations.

One view covers one map. Two views created separately need not represent the
same time, including within one namespace or shard. Neither transaction
isolation for writes nor global snapshots follow from this addition.
Use the original map's encoding and ordering rules; an unordered typed map
must not acquire a misleading new ordered-key contract.

### Ownership and implementation

The internal view can hold a copied collection prefix, a borrowed shard DB,
and MMDB's borrowed RAII snapshot. The borrow is rooted in the existing
collection handle and its namespace. This requires no self-referential object,
lifetime extension, leaked engine, new `unsafe`, or change from owned shard DBs
to `Arc<DB>`.

Keep the MMDB snapshot guard alive for the whole view lifetime. Saving only
its sequence and dropping the guard is incorrect: later compaction can retire
the required historical versions. Build every point read and range from that
guard's read options. Do not call the existing engine `range()` helper blindly,
because it captures a new snapshot internally.

Factor the internal range construction narrowly so current iterators and read
views share prefix bounds, inclusive/exclusive handling, source pruning and
`CheckedBidiIter` error checks. Preserve the existing iterator capture path.
Do not replace current range scans with eager whole-map copies.

Initially, iterators obtained from the view borrow it. This keeps the guard
alive with a simple lifetime contract. The existing `range_detached` behavior
is unaffected; a detached multi-query view is a separate capability.

Capturing a view may contend briefly with engine work. Holding it retains old
versions and can delay compaction reclamation. Document that cost and release
views promptly; do not introduce automatic expiry, reader quotas or a global
read/write lock as part of the library addition.

### Mutable access while a view exists

A view borrowed from `map` prevents borrowing that same Rust handle mutably.
This is expected Rust behavior, not a reason to weaken lifetimes. Existing
documented writer/reader aliases can operate concurrently, with the current
single-writer discipline.

If eliminating application-side `unsafe shadow()` is a demonstrated requirement,
the smallest follow-up is a distinct read-only handle:

```text
reader = map.reader()          // captures prefix and owns a Namespace clone
view = reader.read_view()      // borrows the reader and its engine
map.batch().commit_sync()      // original writable handle remains independent
view.get(key)                 // still reads the captured state
```

This handle would expose read capability only; it must never convert back to a
mutable map or inherit mutation through `Deref`. It would share identity, not
deep-copy records, and its name/documentation must make that distinction clear.
A namespace clone naturally prevents premature close. Typed handles would
carry their existing codec identity without changing serialized map handles.

This is a separable ergonomic addition, not a prerequisite for borrowed views.
Do not add a second full hierarchy of mutable handles or expand every
specialized structure's API merely to support it. Its minimum useful public
surface can be just view creation, rather than duplicating every map method.

## 3. Error behavior and diagnostics

Retain the accepted distinction:

| Situation | Treatment |
|---|---|
| Absent key / exhausted iterator | `None` |
| Expected configuration, capability or input rejection in an existing fallible API | Existing `VsdbError` result |
| Unrecoverable engine read fault / invalid persisted typed invariant | Panic with diagnostic context |
| Application rejects a business command | Application error; drop its uncommitted batch |

Audit and document panic sites rather than converting all methods to `Result`.
The present engine `expect` calls do not themselves prove that every lower-level
error is intrinsically unrecoverable. Classify the actual reachable cases and
avoid classifying ordinary user rejection as a fatal storage fault. Do not
change established failure behavior incidentally during these additions.

Diagnostics should retain the operation and underlying cause, without dumping
stored values. Existing `expect` messages and iterator errors already carry
causes; add missing context only where evidence shows it is needed.
Rust unwinding is not equivalent to returning `Err`, and `panic=abort` cannot
be intercepted. VSDB should describe those mechanics without imposing a
service restart supervisor or global invalidation policy on every caller.

## 4. Changes that require a separate decision

The owner explicitly requires confirmation before significant structural
changes. None below is part of the recommended initial implementation:

| Candidate | Why it is a different design decision |
|---|---|
| Cross-map/shard/namespace transactions or serializable write transactions | Introduces coordination and new atomicity/locking guarantees |
| Global reader/writer locking | Changes existing concurrency and read latency |
| Default fsync for inserts or existing commits | Changes latency and durability defaults for every consumer |
| New key prefixes, routing, shard defaults, handle tags or disk format | Affects persisted compatibility and migration |
| Replacing collection storage with VerMap or adding automatic commit history | Changes representation, write amplification and retention semantics |
| Replacing namespace-owned engines, force-close, or global poison flags | Changes ownership, teardown and failure scope |
| Owned detached multi-query snapshots requiring engine ownership changes | MMDB currently exposes `Snapshot<'a>` borrowing a DB; ownership must be designed explicitly |
| Broad conversion to fallible reads or removal of existing fallible APIs | Changes VSDB's API/error philosophy and compatibility |
| Promoting application command overlays, retries or restore orchestration into VSDB | Makes the library responsible for consumer-specific semantics |

In particular, do not use `transmute` or lifetime-erased raw pointers to make a
borrowed MMDB snapshot look owned. A genuine owned snapshot may warrant an
MMDB API extension, but it is unnecessary for the initial borrowed design.

## 5. Validation and delivery sequence

1. Implement synchronous commits through core and the three ordinary typed
   batch wrappers, with matching API documentation. Test ordinary and wiped
   batches, dropped and empty batches, read-only rejection and unchanged
   asynchronous defaults. Verify sync-before-publication with deterministic
   fsync pause/failure tests at the engine boundary; an after-commit fence
   must fail that test. A process kill after acknowledgement also checks
   recovery, but cannot substitute for power-loss or ordering evidence.
2. Implement borrowed ordinary-map views. Test repeated point/range reads
   across overwrite, delete, clear, sync, flush and compaction; prefix isolation;
   typed encoding parity; forward/reverse bounds; and independent new views
   seeing later state. Include lifetime/compile-fail examples showing that
   views cannot outlive their owner or release a namespace underneath them.
3. Add read-only handles only if the API exercise establishes their value.
   Prove concurrent reads/writes without new aliasing `unsafe`, immutable-only
   access, and namespace close refusal while a reader exists.
4. Keep error and lifecycle documentation aligned. Test the required failure
   cases without adding a general fault-injection framework or broad public
   engine abstraction solely for tests. If deterministic engine hooks require
   an MMDB change, propose that narrow testability change separately.
5. Run normal public-API workspace gates and consumer probes before publishing.
   No format bump is expected. Choose the release version through the existing
   compatibility/release policy after implementation, not in this assessment.

Implementation units should remain independently reviewable. No consumer
migration, admission policy, deployment or claim of improved throughput is
included here.

## Evidence and current limitations

The source review covered `core/src/common/engine/{mod,mmdb}.rs`, raw and typed
batch wrappers, `strata/src/common/staged.rs`, namespace close ownership, and
the existing iterator snapshot regression. The local workspace lock resolves
MMDB 4.3.1; the earlier consumer assessment used its pinned 4.3.0. Both engine
sources were inspected for synchronous publication and borrowed snapshot
semantics. This does not imply equivalence of all other engine behavior.

In both versions, synchronous write groups sync the WAL before memtable batch
publication, and `Snapshot` retains a DB borrow plus a registered sequence.
Those existing capabilities support the proposed local additions. The actual
new public interfaces, compile-fail cases and fault-injection coverage are not
implemented or validated by this document.

Related contracts: [compatibility policy](../../.claude/docs/compatibility-policy.md),
[namespace close](ns-close.md), [core API](../../core/docs/api.md),
[versioned storage](../../strata/docs/versioned.md).
