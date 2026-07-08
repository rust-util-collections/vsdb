# Changelog

All notable changes to this project will be documented in this file.

## [v16.3.2]

Post-v16.3.1 review fixes (the reviewed commit was never published):
the new SlotDex tier-capacity gate is now O(1) and bulk-load-aware,
plus style/doc alignment. mmdb upgraded to v4.2. No on-disk format
changes.

### Changed

- **mmdb upgraded to v4.2.**

### Fixed

- **`SlotDex` bulk loads grow tiers mid-batch**: v16.3.1's "no tiers yet"
  capacity gate counted only *committed* level-0 rows, which do not advance
  inside a single `insert_batch` — one bulk load of N ≫ `tier_capacity`
  distinct slots into a fresh index built **zero** tier levels, silently
  degrading every subsequent paged/count query to the O(N) level-0 walk
  (persisting across reopen, never healing on a read-mostly index). The gate
  now also counts rows staged earlier in the same batch and, on promotion,
  builds the new level from the merged committed ⊕ staged level-0 stream —
  restoring exact serial-`insert` cadence, as the batch-equivalence test now
  verifies with a single whole-workload batch.
- **`SlotDex` growth gate is O(1)**: the same v16.3.1 gate re-scanned (and
  materialized) every committed level-0 row on each insert while the index
  was tier-less. The gate now reads `slot_rows`, an in-memory mirror of the
  committed level-0 row count, maintained on each mutation's 0↔1 slot-row
  transition and re-derived from committed rows whenever the index (re)enters
  the tier-less state — at open (`hydrate`) and after `remove`'s tier
  truncation, both provably bounded scans.

### Style

- Hoisted repeated inline paths to imports (`Commit` in
  `versioned/handle.rs`, `VSDB` in `engine/mmdb.rs`); added the missing
  `// SAFETY:` comment on `DagMapRaw::shadow()`'s inner unsafe block;
  updated `dagmap.md` INV-DG6 to the namespace-scoped flush barriers.

## [v16.3.1]

Full-codebase audit: 8 confirmed correctness/crash-safety findings fixed
across engine, versioning, dagmap, vecdex, slotdex, and typed collections,
plus style/doc cleanup. No on-disk format changes.

### Fixed

- **Prefix allocator no longer leaks reservations**: `reserve_recovered_prefix`
  now tracks each thread's *live* issuance cursor (not just its batch's static
  bounds), so recovering an already-issued prefix within the same still-open
  allocation window is correctly recognized as never-recurring instead of
  being reserved forever — fixes an unbounded `RECOVERED_PREFIXES` growth and
  a permanent `alloc_prefix()` slow-path regression triggered by ordinary
  save/reload round-trips.
- **`rollback_to` rejects uncommitted changes to a strict ancestor**: the
  uncommitted-changes guard previously only fired when rolling back to the
  current HEAD; rolling back to an older commit silently discarded
  uncommitted work. Now guarded unconditionally, matching `merge()`.
- **`VecDex::remove` no longer deflates `max_layer`**: entry-point
  re-election (which still prefers linked candidates) is now decoupled from
  `max_layer` assignment, which always reflects the true global maximum
  layer among live nodes — previously a low-layer linked node could win
  re-election and its own layer would overwrite `max_layer`, making real
  upper-layer subgraphs silently unreachable.
- **`DagMap::prune` flush barriers are namespace-scoped**: replaced the
  global `vsdb_flush()` (which flushes every open namespace in the process)
  with `self.namespace().flush()` — a transient flush failure in an
  unrelated namespace could previously panic a `prune()` on a healthy one.
- **`DagMapRaw::destroy`/`prune_children` clear before unregistering**:
  reordered so a node (and its owned descendants) is fully cleared before
  its entry is removed from the parent's registry — the previous ordering
  could leak a subtree's storage permanently if a crash landed between the
  two steps.
- **`SlotDex` no longer promotes a premature tier on the first insert**: the
  "no tiers yet" growth path now gates on `tier_capacity` exactly like the
  "tiers already exist" path, instead of promoting unconditionally.
- **`Mapx`/`MapxOrd`/`MapxOrdRawKey` `PartialEq` compares decoded values**:
  replaced the derived, byte-level comparison (which disagreed with
  `f64`'s own equality for `-0.0`/NaN) with hand-written impls that decode
  values before comparing, matching `Orphan`'s existing behavior.
- **`BranchMut` exposes `head_commit`/`log`/`diff_uncommitted`**: these were
  missing despite the type's own doc comment promising "all `Branch` read
  methods plus write operations."

## [v16.3.0]

Per-engine block-cache pool + cache telemetry (shared-mem-pool RFC
tier (i), steps 0 + 4; mechanism shipped in mmdb v4.1.0).

### Added

- **Per-engine block-cache pool**: every engine (default namespace and
  every non-default namespace alike) now shares ONE `BlockCachePool`
  across its shards instead of statically splitting the cache slice
  per shard. Since routing is `prefix % shards`, a collection lives
  entirely inside one shard — under the old split a single hot map
  could only ever use `1/shards` of the engine's cache. The pool's
  capacity is exactly the sum of the former per-shard capacities, so
  engine memory totals are unchanged; this is a pure reallocation.
  Measured (Q1 gate benchmark, 64 MB budget, SST-backed random reads):
  skewed load (one hot map) **−72% (1 thread) / −81% (8 threads)**
  read latency; uniform all-shard load: parity within noise (after
  the pool's LRU store was 64-way segmented in mmdb — see mmdb
  v4.1.0). No isolation trade: the shards of one engine are one
  tenant (same dataset, same budget).
- **Per-shard property passthrough** (`Namespace::shard_properties`):
  one engine-property reading per shard in shard order (mmdb
  `DB::get_property` names) — the observability tier the RFC's
  trigger conditions require. Per-shard cache hit/miss counters stay
  per-shard under the pool (counted at each shard's read site);
  `"block-cache-usage"` reports the engine-wide pool total plus the
  shard's own pins.
- **`cache_pool` criterion bench** (`core/benches/cache_pool.rs`): the
  Q1 gate — skew and uniform read scenarios at 1/8 threads with a
  documented A/B protocol against the private-split baseline.

## [v16.2.1]

Hardening follow-ups from the post-v16.2.0 review — no on-disk format
changes (`VecDexDyn`'s new manual serde impls are byte-identical to
the derived encoding they replace).

### Fixed

- **`clone_in` error path reclaims its partial target**: a failed
  chunk commit now triggers a best-effort wipe — one O(1) range
  tombstone, committed before the error propagates — so a failed or
  retried clone no longer accumulates unreferenced garbage under
  never-returned prefixes. Only if the wipe itself also fails does
  the old residue behavior remain (documented).
- **`VecDexDyn` metric discriminant frozen**: manual
  `Serialize`/`Deserialize` impls over explicit, append-only wire
  tags replace the derived impls, decoupling the persisted metric tag
  from enum source order — reordering or inserting variants can never
  re-interpret existing metas, and an unknown tag is refused outright.
  Byte-identical to v16.2.0 metas; pinned by a wire-stability test.
- **`VecDexDyn::keys`/`iter` no longer box**: both return
  `impl Iterator` via an internal enum wrapper — zero heap
  allocation, matching `VecDex`'s iterator signatures (the cost-model
  doc now notes iterators dispatch per item).

### Internal

- `vsdb_ns_close` and `Namespace::close` now share one close-protocol
  implementation (`ns_close_impl`) — behavior and error messages
  unchanged, future close hardening lands in exactly one place.
- New tests: `VecDexDyn` wire-tag stability + unknown-tag rejection,
  and `f64` end-to-end dynamic-dispatch coverage (search, meta
  round-trip, scalar-width cross-rejection).

## [v16.2.0]

Three small backlog items shipped — all purely additive (no breaking
changes, no hot-path modifications).

### Added

- **Cross-namespace copy helpers** (`clone_in`, namespaces RFC P3):
  `MapxRaw`, `Mapx`, `MapxOrd`, `MapxOrdRawKey`, and `Orphan` gain
  `clone_in(&ns) -> Result<Self>` — the cross-namespace form of
  `Clone`, mirroring `new` vs `new_in`. The copy runs in bounded
  chunks (never buffering the whole map in memory); on error the
  partially-written target is abandoned as unreferenced, invisible
  garbage. `Clone` for the engine map now delegates to the same code
  path pinned to the source's namespace — behavior unchanged
  (identical chunking and panic message).
- **Consuming `Namespace::close(self)`** (ns-close RFC §9 deferred
  nicety): the handle-consuming form of `vsdb_ns_close`. The consumed
  handle is accounted for — no separate `drop(ns)` needed first.
  Refusal (other live handles, or the default namespace) returns
  `Err((Some(handle), e))` with the handle intact for continued use;
  a teardown error after the point of no return yields
  `Err((None, e))` (the namespace is no longer open, matching
  `vsdb_ns_close` error semantics).
- **Runtime distance-metric selection for VecDex** (`VecDexDyn`,
  vecdex backlog P3): `VecDexDyn<K, S>` selects the metric at
  construction via the new `distance::MetricKind` enum
  (`L2`/`Cosine`/`InnerProduct`) and mirrors the full `VecDex` API
  one-to-one. Enum dispatch happens once per public operation; the
  distance loops stay statically monomorphized, so per-query
  performance matches the equivalent `VecDex`. The metric is
  persisted in the instance meta and restored by `from_meta` without
  being re-stated; `VecDex` and `VecDexDyn` meta formats are
  deliberately distinct and refuse to cross-load.

## [v16.1.1]

Post-release audit fixes (registry: `docs/audit.md`).

### Fixed

- **`vsdb_ns_relocate` dataset check hardened**: the target must now
  hold mmdb's `CURRENT` manifest anchor in every expected shard dir,
  not merely the shard dirs themselves. `CURRENT` is exactly mmdb's
  recover-vs-create test at open (absent ⇒ a shard is silently
  recreated fresh), so the guard now asks the semantically right
  question — "would every shard take the *recover* path?" — refusing
  bare skeletons (a "prepared" volume, or a copy interrupted before
  any shard content landed) in addition to empty dirs. Which *dataset*
  lives there still cannot be verified (roots carry no namespace id);
  moving the right data remains the operator's documented contract.
- **Relocate validates the registered shard count**: the `1..=64`
  registry-damage guard (previously only in the open path, now shared
  via `validated_shards`) runs before the dataset probe — a corrupt
  `shards == 0` fails loudly instead of vacuously passing the
  per-shard checks and degrading the guard to marker-only.

## [v16.1.0]

In-process namespace `close()` — the ownership-inverted engine
lifecycle (RFC: `docs/proposals/ns-close.md`).

### Added

- **`vsdb_ns_close(id)`**: closes an open namespace, releasing **all**
  of its resources — engine memory, compaction threads, fds, and mmdb
  `LOCK` files — without a process restart. Active memtables are
  flushed and WALs synced first (errors surface, unlike a plain drop).
  Refuse-don't-poison: it succeeds only when every handle (collections,
  iterators, `Namespace` clones) is gone, otherwise it returns an error
  naming the live-handle count; a live handle is never invalidated.
  The registry entry survives: re-open via `Namespace::open`
  (restart-equivalent recovery) or reclaim via `vsdb_ns_destroy` —
  `create → fill → close → destroy` is the in-process epoch-rotation
  loop.

### Changed

- **Engines are owned, not leaked**: both `Box::leak` sites are gone.
  `NsInner` owns its `Engine`, `MmDB` owns its shard `DB`s
  (`Box<[DB]>`), and every engine reference is a plain borrow bounded
  by a live handle — the soundness invariant moved from review
  discipline into the type system. The default engine is owned by the
  default `Namespace` (a static, so it still lives for the whole
  process); the public `VSDB` singleton delegates to it.

## [v16.0.2]

### Changed

- **The test suite runs in parallel** — `--test-threads=1` is gone
  (RFC P2). Isolation comes from globally-unique prefixes (every
  collection instance owns a disjoint key range, in any namespace), so
  data-level cross-test interference is impossible by construction.
  Enabling changes:
  - lib tests no longer call `vsdb_set_base_dir` per test (racing
    `env::set_var` is unsound); they use the default base, wiped by
    `make test` between profiles.
  - multi-test integration binaries serialize their base-dir pick
    behind a `Once`.
  - global-allocator assertions are race-tolerant: they bound the
    test's OWN issued prefixes against monotone global state instead
    of demanding exact equality between racy snapshots.

## [v16.0.1]

Post-release audit fixes (all findings from the v16.0.0 deep review;
registry: `docs/audit.md`).

### Fixed

- **Namespace lifecycle TOCTOU**: the not-open check of
  `vsdb_ns_destroy`/`vsdb_ns_relocate` now runs under `REGISTRY_LOCK`
  (the same lock `open` holds while caching a live engine), so a racing
  open can no longer have its root deleted or repointed underneath it.
- **Base-dir freeze gap**: `Namespace::open` (and the admin fns) freeze
  the base dir before reading the registry — a later
  `vsdb_set_base_dir` fails loudly instead of moving the global
  allocator's backing store to another universe under live namespaces.
- **Explicit-root validation hardened**: `.`/`..` components rejected;
  overlap checks run on physically normalized paths (symlinked
  spellings of the base or another root are caught); adopting an
  existing non-empty dir is refused (foreign prefix provenance;
  importing/attaching stays an explicit non-goal — RFC §9). Empty dirs
  (fresh mount points) remain accepted.
- **Crash-resumable namespace creation**: shard-layout validation is
  completion-aware (keyed on the format marker, which is written only
  after every shard exists): a create that crashed mid-way is completed
  on the next open instead of bricking the root; genuine mismatches
  stay rejected.
- **DagMap cross-namespace split**: parented construction always
  inherits the parent's namespace (ambient scope overridden;
  mismatched explicit `new_in` is debug-asserted) — one DAG never
  spans namespaces.
- **DagMap destroy ownership order** (INV-DG5): the ownership check
  runs before duplicate suppression, so a stale foreign registry entry
  can no longer make `destroy` skip a genuinely owned child.
- **`InstanceId` canonical form**: `"42@0"` and wire-level
  `Some(DEFAULT_NS_ID)` fold to `ns: None` at parse/deserialize time —
  `Eq`/`Hash` are reliable for API-obtained tokens; new canonical
  constructor `InstanceId::new(map_id, ns)`.
- **Shard-layout lifecycle hardening** (follow-up review of the initial
  completion-aware rule): brand-new roots raise a durable
  `__SYSTEM__/__initializing__` sentinel before the first shard dir and
  retire it after the format marker — resumability now requires proof.
  A marker-present root missing shard dirs (even all of them) is
  damage and refused, never silently reinitialized; a partial set with
  neither sentinel nor marker (e.g. a corrupted legacy base) is
  refused instead of "resumed" into silent data loss; the scan checks
  the exact expected shard set (misnamed/non-dir `shard_*` entries are
  rejected, not counted).
- **Create rollback also clears the root**: a failed `create` leaves an
  explicit path immediately retryable (pre-existing empty dirs are
  emptied, never deleted) and no unregistered residue under derived
  roots.
- **Registry robustness**: an out-of-range shard count in a (damaged)
  registry entry is refused cleanly instead of reaching `prefix % 0`;
  `vsdb_ns_list` freezes the base dir like every other registry reader.
- **`flush_all_open` no longer holds the namespace table lock across
  engine flushes**; `Namespace::meta_path` is public and is the single
  source of truth for instance-meta naming (strata reuses it).

### Documentation

- RFC `save_as_meta` → the implemented `save_meta`; namespace subsystem
  added to `CLAUDE.md`, review-core mapping, and the engine pattern
  guide (per-namespace engines, global allocator, marker semantics,
  REGISTRY_LOCK rule).

## [v16.0.0]

Design: `docs/proposals/namespaces.md` (rev 10).

### Added (P1 — namespaces)

- **Namespaces: anonymous placement groups.** A `Namespace` is an
  independently-rooted engine instance (own dir tree — placeable on its
  own volume, own shards/WALs/compactions/memtable budget). Users never
  name one, never pass a path on the normal tier; the everyday primitive
  is co-location: `existing.namespace()` + `new_in`/`ns.scope(..)`.
  - `Namespace::{create, create_with, open, default_ns, current, scope,
    id, path, flush, system_dir, meta_dir}`;
    `NamespaceOpts { path, shards, mem_budget_mb }` (everything
    defaulted; explicit roots validated against nesting).
  - Admin tier: `vsdb_ns_list / vsdb_ns_destroy / vsdb_ns_relocate`
    (not-open-only; destroy = registry removal + `rm -rf` — O(1) bulk
    reclaim; relocate updates the registry pointer only).
  - Registry: `{base}/__SYSTEM__/__namespaces__` (postcard, durable
    atomic writes); derived roots under
    `{base}/__NAMESPACES__/{ns_id:016x}` recorded base-relative so the
    whole universe stays movable as one tree. Ids are never reused;
    `DEFAULT_NS_ID = 0` is a fixed constant — never registered, never
    looked up.
  - Every collection (`MapxRaw` → typed wrappers → `Orphan`,
    `PersistentBTree`, `VerMap`, `SlotDex`, `VecDex`,
    `DagMapRaw`/`DagMapRawKey`) gains `new_in(&ns, ..)` + `namespace()`;
    plain `new()` places into `Namespace::current()` (ambient scope,
    creation-time only — never routing). A composite and all its
    internal maps live in exactly one namespace. The one exception is
    `VerMapWithProof`, which is placed via
    `from_map(VerMap::new_in(&ns))` and exposes its namespace via
    `.map().namespace()`.
  - **One global prefix allocator** serves all namespaces: prefixes (=
    `map_id`s) stay unique across the whole registry by construction.
  - Per-namespace `__SYSTEM__` tree: instance metas and MPT/SMT cache
    files live beside their data (`destroy` reclaims them together);
    the `TrieCalc` trait now takes the cache dir explicitly.
- **`InstanceId { map_id, ns: Option<NsId> }`** — the complete public
  identity, mirroring the persisted meta bytes (`ns: None` ⇔ default
  namespace ⇔ the 16-byte pre-v16 meta form). `Display`/`FromStr` as
  `"42"` / `"42@7"`. `instance_id()`/`save_meta()` now return it;
  `from_meta(impl Into<InstanceId>)` still accepts bare `u64` ids
  (⇒ default namespace) — stored pre-v16 tokens keep working, and
  resolution is deterministic (never a search).
- **Meta wire format: optional `ns_id` suffix.** Default-namespace
  handles serialize byte-identically to v15 (`"VSMAPX01" ‖ prefix`,
  16 B); non-default handles append `ns_id_le` (24 B). One magic, no
  format versions; v15 data decodes as `None` verbatim.

### Changed (P0 — allocator persistence relocation)

- **The prefix-allocator ceiling now lives outside the shard DBs**, in
  `{base_dir}/__SYSTEM__/__prefix_ceiling__` (8-byte LE `u64`, written
  durably: tmp + fsync + rename + parent-dir fsync). This decouples
  prefix allocation from the default engine — the prerequisite for
  namespaces sharing one global allocator.
  - **Upgrade is automatic and idempotent**: at every open the ceiling
    is take-max-folded from the file, the legacy shard-0 key, and the
    allocation floor, then persisted; the legacy key is never written
    again. A pre-tripwire v15 binary that advanced the legacy key after
    migration is re-absorbed by the max-fold instead of causing prefix
    reuse.
  - The dataset is marked `__SYSTEM__/format_version = 16` durably at
    open, *before* the file-based allocator can issue anything, so
    v15.0.2+ binaries refuse the dataset cleanly (downgrade is
    unsupported by policy; see v15.0.2 notes).
  - `SUPPORTED_FORMAT_VERSION = 16`; datasets marked newer are refused.

## [v15.0.2]

### Added

- **On-disk format-version tripwire.** Opening a dataset now checks
  `{base_dir}/__SYSTEM__/format_version` (ASCII decimal, written by
  v16+; v15 itself writes nothing — absence is the v15 signature) and
  refuses to open anything newer than format 15 with a descriptive
  error. Rationale: a future layout (v16 relocates the prefix-allocator
  ceiling out of shard 0) would leave the legacy shard-0 ceiling stale;
  a v15 binary reading it would re-issue already-used prefixes —
  silent data corruption. Downgrade stays unsupported by policy; this
  makes the violation fail loudly instead of silently, and makes
  v15.0.2+ the safe landing point for out-of-contract rollbacks.
  (Design: `docs/proposals/namespaces.md` §7.)

### Changed

- Doc terminology: per-prefix key ranges are now called "prefix
  ranges" (previously "prefix namespaces"), freeing the term
  *namespace* for the upcoming v16 feature.

## [v15.0.1]

### Fixed

- **`clear()` is now crash-atomic everywhere — the v15 single-batch
  contract holds for every mutation, wipes included.** The engine-level
  `clear()` (all collection types) previously deleted rows in chunked
  batches; a hard crash mid-clear could leave a partially-cleared
  namespace that hydration silently trusted (stale SlotDex totals/tiers,
  stale VecDex graph state over missing rows). It is now **one write
  batch containing a single range tombstone** covering the whole prefix:
  all-or-nothing (even across a crash) and O(1) instead of O(n).
  - New primitive: `MapxRaw::batch_entry_wiped()` — a batch pre-staged
    with the whole-range wipe; operations added afterwards apply on top
    of it and the whole set commits atomically.
  - `StagedRows::wipe()` — a wiped overlay reads the committed store as
    empty and commits the tombstone plus all staged rows in one batch.
- **`VecDex::compact()` is atomic.** The rebuild is staged through one
  wiped transaction and commits in a single engine write batch: a crash
  (or error) leaves either the old graph or the new one, never anything
  in between. The former crash window (wipe applied, re-inserts pending)
  is gone.
- **`VecDex::clear()` persists the preserved `ef_search`.** The reset
  graph state row (carrying the live `ef_search`) commits in the same
  atomic batch as the wipe, so a post-clear restore no longer silently
  reverts to the creation-time value.

### Changed

- `VecDex` `Txn` decoded-vector cache and search-path cache now use an
  imported `RefCell`; staged/vecdex import groups tidied.
- Docs: slotdex pattern guide file list and stale multi-handle bug
  pattern refreshed; vecdex pattern guide compact section matches the
  atomic rebuild; `prune_and_detach` doc describes its actual return
  value.

## [v15.0.0]

### Changed (BREAKING)

- **SlotDex and VecDex are single-handle, crash-atomic structures.**
  All persistent state of each instance now lives in ONE `MapxRaw`
  handle, namespaced by a leading tag byte, and every mutation stages
  its rows through a read-your-writes overlay (`common/staged.rs`) and
  commits them in ONE atomic engine write batch. A crash can no longer
  leave either structure internally inconsistent, at any point.
  - SlotDex: entry rows (`0x00|slot|key`), level count rows
    (`0x01|level|floor`, level 0 = per-slot counts, levels >= 1 = the
    tier stack), grand total (`0x02`). The per-slot `DataCtner`
    (Small/Large) container model and the per-tier nested handles are
    gone; tier growth writes ordinary data rows.
  - VecDex: vectors (`0x00`), adjacency (`0x01`), key mappings
    (`0x02`/`0x03`), node info (`0x04`), graph state (`0x05`). The six
    separate handles are gone; the crash reconcile/relink pass
    (`recover_after_crash`) is gone because torn states are no longer
    representable.
- **The bit-63 dirty-count protocol is removed** (`common/dirty_count`
  module deleted). Counts are stored as plain values; crash consistency
  comes from mutation atomicity, not from a dirty flag + rebuild.
- **`save_meta` is uniform pure persistence again.** SlotDex and VecDex
  `save_meta` now take `&self`, have no side effects, and their
  serialized handle metadata is **create-time constant** (single prefix
  plus creation-time config): saving once at creation is sufficient for
  the lifetime of the instance, matching every other handle type. A
  top-level application struct that embeds these structures no longer
  needs periodic metadata re-saves or shutdown-time save cascades.
- **Persisted-layout compatibility:** SlotDex/VecDex metadata written by
  v14 and earlier is rejected on restore (explicit layout-version check;
  the old multi-handle payload cannot be decoded positionally). There is
  no in-place upgrade path — rebuild the index from source data.
- `hnsw::search_layer`/`get_neighbors` now read adjacency through the
  `AdjRead` abstraction (raw store on search paths, staged transaction
  on mutation paths); the adjacency compound key gained the namespace
  tag byte (10 bytes total).

### Performance (measured, 100k-entry SlotDex / 10k x dim-128 VecDex)

- SlotDex `insert` −53%, `remove` −64% (one write batch replaces
  multiple engine puts).
- SlotDex reverse paging −16..−65% for `tier_capacity >= 8` (engine
  reverse iterators are avoided entirely: descending walks now chunk
  forward scans off the in-memory level-1 anchors and reverse them in
  memory); `tier_capacity = 4` is neutral. Forward paging: large pages
  −2..−11%, small pages up to +18% at some capacities (fixed per-query
  iterator-creation cost over fewer, cheaper rows; low single-digit
  microseconds absolute).
- VecDex `insert` −33% wall-clock on a clean engine (the per-transaction
  decoded-vector cache removes redundant postcard decodes; one batch
  replaces dozens of puts). `insert_batch` now stages chunks of 64
  inserts per atomic batch.
- VecDex `search` on a flushed engine: unchanged (within noise). While
  a large unflushed write backlog sits in the single shard, search
  latency is ~2-3x the old multi-handle layout (transient read
  amplification; cleared by `vsdb_flush()` / memtable rotation) — flush
  after bulk loads, as write-cycle-flushing applications already do.

## [v14.0.14]

### Changed

- **B+ tree node writes are batched per operation.** `PersistentBTree`
  now stages the COW node group of each `insert`/`remove`/`bulk_load`
  in a write buffer and lands it through ONE engine write batch at the
  end of the operation, instead of one engine put per node — per-node
  shard-lock/WAL overhead is paid once per operation, and the node
  group becomes all-or-nothing (a torn, partially-written path-copy
  can no longer appear on disk; previously it was benign but had to be
  swept by recovery). Intra-operation churn (split/borrow/merge
  intermediates) is discarded from the buffer and never reaches the
  engine at all. `bulk_load` flushes in bounded chunks, so its buffer
  cannot grow with the dataset. No API or crash-ordering change:
  the buffer is always drained before a root escapes to the caller,
  so branch state still lands strictly after the nodes it references.
  Versioned write benches improve ~10-20% (insert −20%, merge −12%);
  read paths are untouched apart from one branch on an empty map.

## [v14.0.13]

### Added

- **`SlotDex::insert_batch`** — bulk insertion that is observationally
  identical to per-pair `insert` but amortizes engine writes: keys are
  grouped per slot, each touched container is loaded/persisted once,
  and container records plus per-tier counters are flushed through one
  write batch per collection. Intended for imports and index rebuilds.
- Missing invariant tests from the full audit: prefix-allocator
  uniqueness/monotonicity/ceiling-persistence (INV-E1), `MapxRaw`
  prefix isolation (INV-E3), VerMap rollback isolation (INV-V4), and a
  ground-truth ref-count recount (INV-V1).

### Fixed

- **MPT cache loader rejects mixed `Cached`/`InMemory` trees.** A
  checksum-valid but crafted cache file could place an unhashed
  (`InMemory`) child under a `Cached` parent; `commit_rec` skips
  `Cached` subtrees, so the child was never re-hashed and `prove()`
  panicked on its missing hash. Rejected at the load trust boundary,
  like the other cache-shape validations.
- `rollback_to` targeting the current head with a clean working state
  is now an early-return no-op instead of a full rewrite cycle that
  set `gc_dirty` and re-wrote identical state.
- `merge_empty_source_fails` test used a branch ID from the wrong
  `VerMap` instance (worked only by coincidence of ID layouts).

### Changed

- `Orphan` arithmetic/bit/negation operator impls no longer require
  `Ord + Eq`, so `f64` (and other `PartialOrd`-only types) can use
  `+`, `-`, `*`, `/`, unary `-`, etc.
- Serde deserialization of collection handles is now documented as an
  aliasing operation (same SWMR obligations as `shadow()`); the
  fast-forward precondition of `merge` and the net-zero ref-count
  convention of `commit()` are documented at the code site.
- `rand` moved from `vsdb_core`'s `[dependencies]` to
  `[dev-dependencies]` (only tests/benches use it); unused `hex`
  dev-dependency removed workspace-wide.

## [v14.0.12]

### Fixed

- **`VSDB_MEM_BUDGET_MB` is now truly authoritative (fixes the
  "verbatim" claim of v14.0.11).** The explicit override still
  participated in a min-fold against the already-derated detected
  limits, so an operator budget ABOVE the derated cgroup number was
  silently discarded (asking for 1700 MB inside a 2 GiB cgroup
  yielded 1536 MB). When set and non-zero it now replaces all
  detection -- host reading included: the operator asked for that
  exact number. Zero or unrepresentably-large values are ignored.
- **A cgroup file reporting `0` can no longer zero the engine
  budget.** The limit parser accepted a literal `0`, which the
  hierarchical min-fold then propagated into a zero budget and a
  zero-capacity block cache (mmdb treats capacity 0 as "caching
  disabled entirely"); `0` is now treated as undetectable, like
  `max`.
- **The block cache is floored at 4 MiB per shard**, mirroring the
  existing 4 MiB write-buffer floor, so degenerate budgets degrade
  to a small-but-functional cache instead of a disabled one.

### Changed

- Memory detection (host reading, cgroup walk, env override) runs
  once per process instead of once per shard: all 16 shards now size
  off the same numbers (the host reading is a moment-in-time value
  that could drift between shard opens).
- The budget computation is a pure function (`effective_mem_budget`)
  with unit tests pinning the semantics -- including the deliberate
  min-fold of derated detected limits: a cgroup line within 4/3 of
  the host reading must still cap the budget AND flip conservative
  write-buffer scaling, otherwise worst-case memtables (budget/4 per
  shard x5) can overshoot the line wholesale (host 24G under a 30G
  `memory.high` would size 30G of worst-case memtables), re-opening
  the v14.0.10 incident class.

## [v14.0.11]

### Fixed

- **Detected cgroup limits are derated to 3/4 before sizing engine
  memory.** `memory.high` is a throttle line, not a quota: an engine
  budgeted exactly to it reaches steady state pinned AT the line, where
  every allocation pays reclaim-stall latency. Observed in production:
  a follower under a 9626M `MemoryHigh` peaked at exactly 9.6G, its
  ingest pipeline stalled for most of an hour, and a SIGTERM drain
  could not complete inside the unit's stop timeout (SIGKILL -> dirty
  store -> minutes-long derived-state rebuild on next boot). The
  explicit `VSDB_MEM_BUDGET_MB` override is still applied verbatim --
  the operator asked for that exact number.

## [v14.0.10]

### Fixed

- **Write buffers now scale with a detected memory limit (follow-up to
  v14.0.9).** v14.0.9 clamped the sizing INPUT to the cgroup/env budget,
  but the write-buffer branch for budgets <= 16 GB is a fixed
  `1 GB / NUM_SHARDS` floor, and each shard holds one active memtable
  plus up to `max_immutable_memtables` (4) frozen ones -- a worst-case
  memtable footprint of ~5 GB regardless of a 2-3 GB ceiling. An ingest
  burst under such a limit pinned anonymous memory at the throttle line
  (`memory.high`), and the resulting reclaim pressure slowed the very
  flush threads that are the only way out: the process wedged at the
  limit with tens of thousands of `memory.events: high` entries
  (reproduced empirically; the v14.0.9 clamp alone shrank the block
  cache but not this). When (and only when) a limit tightened the
  budget, the per-shard write buffer is now additionally capped at
  `budget / 8 / NUM_SHARDS` (floor 4 MB), bounding the worst-case
  memtable footprint at ~5/8 of budget alongside the block cache's 1/8.
  Unconstrained hosts keep the legacy sizing byte-for-byte.

## [v14.0.9]

### Fixed

- **Engine cache sizing now respects the process's cgroup memory limit.**
  `mmdb_open` sized write buffers and the block cache from host-wide
  `MemAvailable` alone, so a process running under a systemd
  `MemoryHigh`/`MemoryMax` drop-in or a container memory limit computed
  budgets from memory it is not allowed to use -- on a 32 GB host with a
  12.8 GB cgroup ceiling, engine caches alone (~7.5 GB) grew the process
  to the OOM-kill line during bulk ingest (observed in production as
  unbounded-looking anonymous-memory growth of a service holding a
  1.2 GB store). The budget is now
  `min(host MemAvailable, cgroup limit, VSDB_MEM_BUDGET_MB)`: the cgroup
  walk covers v2 (`memory.max` + `memory.high`, unified hierarchy) and
  v1 (`memory.limit_in_bytes`), takes the tightest ancestor limit
  (limits are hierarchical), and treats `max`/PAGE_COUNTER_MAX-style
  sentinels as unlimited; the new `VSDB_MEM_BUDGET_MB` env var is an
  explicit highest-precedence bound for operators who want engine
  memory below any detected limit.

## [v14.0.8]

### Fixed

- **Safety comments relaxed to per-key granularity.** The `shadow()` SAFETY
  comments and docs incorrectly claimed a global single-writer constraint
  (SWMR / "all shadows must be dropped before the next write"). The actual
  contract is per-key: concurrent writers on disjoint keys are safe — the
  engine provides snapshot isolation and per-key shard routing.
- **`from_bytes()` no longer requires "same code version".** The doc comments
  incorrectly required the same code version for deserialization.
  `from_prefix_slice` performs no memory-unsafe operation itself; the real
  requirement is unique ownership of the prefix bytes.

## [v14.0.7]

Fixes for the two findings the v14.0.6 post-release review surfaced (both
residuals of the bug class that release addressed).

### Fixed

- **`VerMapWithProof` no longer serves a silently wrong Merkle root after a failed sync.** `batch_update` is documented non-atomic, so a diff whose op is rejected partway (concretely: a committed or uncommitted key over `MAX_MPT_KEY_LEN` with `T = MptCalc` — the `VerMap` layer imposes no key-length limit) left the trie holding a partially applied diff while the sync bookkeeping still claimed the previously synced commit. A later `merkle_root_at_commit(C1)` (or `merkle_root` after rolling the branch back to C1) short-circuited on that stale claim and returned a root over C1-plus-partial-diff data — no error, wrong root (empirically confirmed via both the committed-diff and dirty-overlay paths; v14.0.6 had fixed only the emptied-trie variant of this desync). `sync_to_commit` now poisons the sync state (default trie, no synced commit) on a failed incremental application, forcing a full rebuild on the next sync; `sync_to_branch` restores the clean HEAD snapshot taken just before the dirty overlay, so the trie keeps matching `sync_commit` exactly. Regression tests cover both paths, including re-syncing successfully after the failure.
- **Trie cache deserializers now validate whole-tree structure, closing the crafted-cache gap in v14.0.6's root-preservation guarantee.** Per-node checks (path ≤ 256 bits, file checksum) could not see cross-node violations, so a checksum-valid but malformed cache file could load trees the walkers can't handle: an SMT whose cumulative descent depth exceeds 256 bits made a later `insert` fail *after* consuming the working tree — silently emptying the whole tree one layer below v14.0.6's `SmtCalc`-level restore (its "a rejected insert never loses tree data" guarantee) — and mispositioned leaves could drive path arithmetic out of range (a release-mode panic); an MPT cache bypassed the insertion-time `MAX_MPT_KEY_LEN` stack-depth cap entirely, and out-of-range nibble values (> `0x0F`) panicked on branch-child indexing. The SMT deserializer now threads the routing prefix down the tree and rejects any leaf whose position+path doesn't reconstruct its own key hash exactly, any internal node pushing cumulative depth past 256 bits, and any cached hash that isn't 32 bytes; the MPT deserializer enforces the cumulative nibble budget (`2 * MAX_MPT_KEY_LEN`), rejects empty extension paths (organic tries never produce them; they were the only zero-progress construct, so the nibble budget is now a real recursion bound), out-of-range nibble values, and non-32-byte cached hashes. Accepted trees are exactly the canonically-positioned ones organic mutation builds; valid caches round-trip unchanged (no format/version change). The `commit()` doc comments on both tries were also corrected — they claimed the root is "restored" on a `commit_rec` failure, but that error arm drops the consumed working tree; it is now genuinely unreachable (MPT `commit_rec` is total; the SMT's only failure input — a bad cached hash length — is rejected at load).

## [v14.0.6]

Full-codebase audit sweep (9 parallel subsystem reviews) with every finding
fixed except one documented, deliberate exception.

### Breaking

- **`DagMapRaw`/`DagMapRawKey<V>` no longer implement `Default`.** The derived impl silently performed real disk I/O (an eager write through `Orphan::new()`'s parent slot) on every call, so generic code (`mem::take`, `Option::unwrap_or_default()`, `HashMap::entry().or_default()`) could create orphaned, unreclaimable on-disk state without any visible indication. Use `DagMapRaw::new(None)` / `DagMapRawKey::new(None)` explicitly. **Migration**: replace any `Default::default()`/`mem::take`/`.or_default()` usage on these types with an explicit `new(None)` call.
- **`TrieCalc`/`MptCalc`/`SmtCalc` now return `vsdb::Result<T>` (`VsdbError`), not the internal `TrieError`.** This closes a gap in the "single error type" invariant (`vsdb_core::common::error::VsdbError` is documented as the only error type across both crates' public APIs). `TrieError` is still exported (`vsdb::trie::TrieError`) for downstream matching via `VsdbError::Trie { detail }`, but is no longer the error type of the trie trait/struct methods themselves. **Migration**: replace `Result<T, vsdb::trie::TrieError>` bounds/matches with `vsdb::Result<T>` / `VsdbError::Trie`.
- **`vsdb_core::common::atomic_write_file` (and its `vsdb::common` re-export) now returns `Result<()>` (`VsdbError`) instead of `std::io::Result<()>`.** The only other raw-error-type leak found in either crate's public surface. **Migration**: handle `VsdbError::Io` instead of `std::io::Error`.

### Fixed

- **`MptCalc`/`SmtCalc` (`insert`/`remove`/`root_hash`/`batch_update`) no longer silently empty the trie/tree on a rejected mutation.** All four methods `mem::take` the root into a local working value before the fallible operation; previously, on `Err` (concretely reachable via `MptCalc::insert`/`batch_update` when a key exceeds `MAX_MPT_KEY_LEN`, a public 1024-byte constant), the function returned early without restoring `self.root`, permanently replacing the entire trie with an empty one. All eight methods now unconditionally restore `self.root`/`self.trie` from the working value before propagating any error — a rejected `batch_update` still applies operations before the failing one (documented as non-atomic) but never discards unrelated prior state. This also fixes `VerMapWithProof::merkle_root`, which could otherwise silently return the empty-trie root hash for legitimate, unmodified committed data after such a rejection desynced its incremental-sync bookkeeping from the trie's actual content.
- **`MmDB::new()` no longer leaks already-opened shard handles (and their background compaction threads) if a later shard or the meta-init step fails to open.** Shards are now opened into an owned, non-`'static` `Vec<DB>` first — so a mid-loop failure drops (and cleanly closes) every already-opened shard via normal `Drop` glue — and only `Box::leak`'d after every fallible step has succeeded.
- **SlotDex crash recovery now eagerly rebuilds the tier-acceleration stack** instead of leaving it empty until the next `insert()`. Previously, `ensure_count()` correctly discarded the (potentially skewed) tier stack on unclean-shutdown detection but deferred rebuilding it, silently degrading every pagination query to an O(N) raw scan (measured ~950–2600× slower at 200k entries) for as long as the process stayed idle or read-only after the crash.
- **`SmtMut::remove` no longer discards cached ancestor hashes on a no-op removal** (removing a key that shares a path prefix with real data but isn't actually present). Mirrors MPT's existing `rewrap`-based no-change path: `remove_rec` now threads a `changed` flag and restores the prior `Cached` hash when neither child subtree actually changed, instead of unconditionally reconstructing (and later re-hashing) the node via `compact`.
- **`DagMapRaw::is_dead()` now recognizes tombstoned entries.** `remove()` writes an empty-value tombstone rather than deleting outright (existing, documented convention also used by `get`/`get_mut`); `is_dead()` previously checked only for a literally-empty backing store, so a node whose sole key was removed incorrectly reported `is_dead() == false`.
- **`Mapx::keys()`/`MapxOrd::keys()` no longer decode values.** Both previously routed through `iter().map(|(k, _)| k)`, which unconditionally decoded `V` per entry (an `engine::Mapx::deserialize` call, including lock acquisition, for nested-VSDB-collection value types) before discarding it. A new `MapxOrdRawKey::keys()` decodes only the raw key bytes; `Mapx`/`MapxOrd::keys()` now build on it, decoding only `K`.
- **`core/src/common/engine/mmdb.rs`'s `PENDING_WINDOWS` registry no longer grows unboundedly for the life of the process.** A thread-per-task workload (e.g. a thread-per-request server) previously accumulated one entry per historical thread (since `ThreadId`s are never reused and no cleanup path existed). A `thread_local!` guard now removes a thread's entry via `Drop` when that thread exits — always safe, since a dead thread's un-issued batch tail can never be issued by any other thread.

### Added

- `from_prefix_slice` (core engine, `unsafe fn`) now has a `# Safety` doc comment at its definition, matching every other `unsafe fn` in the crate.
- `Orphan`/`Mapx`/`MapxOrd`/`MapxOrdRawKey`'s `from_meta()` now documents the aliasing hazard it shares with `shadow()` (restoring while the original handle is still live creates a second handle to the same storage) — previously this was undocumented on the one restore path that isn't `unsafe`.
- Typed collections' `batch_entry()` doc comments now state the raw layer's existing "failed commit is not retryable" caveat.
- Regression tests: rejected-mutation trie root preservation (MPT insert/batch_update), `VsdbError`-typed trie errors, SMT no-op-remove cache preservation, SlotDex crash-recovery tier rebuild (both the dirty-flag and invalid-empty-tier detection paths), DagMap tombstone-aware `is_dead()`, `keys()` never decoding values (typed collections), and a `PENDING_WINDOWS` thread-exit cleanup test.

### Won't Fix

- **`VecDex::compact()` remains non-atomic across a hard crash** (documented in `docs/audit.md`): a true fix requires a prefix-swap/version-indirection redesign, and a naive version would silently desync any earlier `save_meta`/parent-collection reference to the index — a worse failure mode (silent staleness) than the current rare-crash-window data loss it would trade away.

## [v14.0.5]

### Breaking

- **SMT hash domain switched to the Diem/JMT leaf-shortcut construction.** A subtree holding exactly one leaf now commits to `Keccak256(0x01 || key_hash || value)` directly — independent of depth — instead of folding the leaf hash through its ~246 residual path levels; internal nodes are unchanged (`Keccak256(0x00 || left || right)`, compressed internal prefixes still wrap through empty siblings). All SMT root hashes change. The SMT disk-cache format is now v3; v2 caches are rejected cleanly and the trie rebuilds from authoritative data. MPT is unaffected. This removes the dominant O(N × 256) hashing term: whole-tree hashing is now O(N) hash operations.
- **`SmtProof` is now compact (variable-length).** `siblings` holds hashes only from the root down to the terminal lone-leaf/empty subtree on the key's path (O(log N) entries instead of a fixed 256), and the `value: Option<Vec<u8>>` field is replaced by `leaf: Option<([u8; 32], Vec<u8>)>` — the lone leaf occupying the terminal subtree (`leaf.0 == key_hash` ⇒ membership; a different `leaf.0` ⇒ conflicting-leaf non-membership, checked for path-prefix consistency during verification; `None` ⇒ empty-slot non-membership). Use the new `SmtProof::value()` accessor for the proven value. Proof size drops from a fixed 8 KiB to typically well under 1 KiB, and verification folds O(log N) hashes instead of 256.

### Fixed


- **SlotDex reverse paging restored to tier-accelerated complexity** (commit `9758b70`, folded into this release): the v13.4.7 correctness fix had degraded `get_entries_by_page(.., reverse=true)` to a linear reverse scan (~17 ms vs ~10 µs forward at 100k entries). Reverse paging now mirrors the forward path via `locate_page_rstart` — a rightmost-distance offset plus a descending tier-cache locate — returning reverse pages to the 10–35 µs range while preserving the corrected slot-descending / within-slot-ascending semantics.
- **SMT tree walks no longer materialize a path slice per level.** `insert`/`remove`/`get`/`prove` compared the remaining key path by allocating `full_path.slice(depth, 256)` (a bit-by-bit copy) at every internal node; they now use allocation-free offset-based comparison (`BitPath::common_prefix_from` / `starts_with_from`, byte-wise with unaligned assembly). `BitPath` itself is now a zero-allocation inline `[u8; 32]` (paths never exceed 256 bits — a type invariant), with `slice`/`concat` rewritten from per-bit loops to byte-wise shifts, and the cache deserializer now rejects bit lengths over 256 instead of allocating attacker-controlled buffers. Combined with the hash-domain change: 1000-key insert 4.6 ms → 0.77 ms, remove 9.0 ms → 1.5 ms, get 3.9 ms → 0.41 ms, cold root hash 76.6 ms → 0.93 ms, verify 79 µs → 3.2 µs per proof (reference box).
- **`mapx / sequential / iter (5k entries)` bench measured an unbounded dataset.** The iterated map had accumulated entries from all preceding timed write benches (hundreds of thousands and growing), so the reported number was meaningless and unreproducible; the bench now iterates a dedicated 5000-entry map.

### Added

- SMT adversarial proof tests: sibling-list depth extension, sibling truncation, conflicting-leaf prefix substitution, proof compactness, plus the existing tamper/wrong-root suite adapted to the new format.
- Bench symmetry: `smt_batch_update_{100,1000}` and `mpt_prove_100` / `mpt_verify_100` cases in `trie_bench`, with `black_box` hygiene on discarded results.

## [v14.0.4]

Consolidates the unpublished v14.0.2/v14.0.3 work (v14.0.1 is the last published release) plus a full sweep of the deferred audit backlog.

### Breaking

- **Typed-handle instance metadata is envelope-tagged** (on-disk format `VSTYPE02`). Safe restore paths (`serde` / `from_meta`) of `Mapx`, `MapxOrd`, `MapxOrdRawKey`, `Orphan`, `VerMap`, `SlotDex`, `VecDex`, and `DagMapRawKey` now embed and validate an 8-byte hash of the concrete wrapper type (including **all** generic parameters — notably `VerMap<K, V>`'s key/value types, `VecDex`'s distance metric, and `DagMapRawKey<V>`'s value type, none of which occur in any field type), so loading persisted metadata under a different type fails loudly instead of silently misreading data. The tag derives from `std::any::type_name`, so persisted metas are additionally tied to the writing build's type paths/compiler rendering — a false rejection is always safer than type confusion. **Migration**: none; re-create metas with `save_meta` (older typed metas are rejected by the magic check).
- **Safe prefix restore is validated against the allocator.** `MapxRaw::from_meta` / serde deserialization reject prefixes outside the allocator-issued range and reserve still-pending prefixes so future allocations skip them; `unsafe from_bytes` remains the trusted escape hatch. The fast path is lock-free: allocator state is mirrored in process atomics, previous-run prefixes (the common restore case) are accepted without reservation, and the reservation set stays bounded (pending-window registry).
- **SMT internal-node hashing gained a `0x00` domain byte** (leaf/internal domain separation — standard second-preimage hardening). All SMT root hashes and proofs change; the SMT disk-cache format is now v2 and v1 caches are rejected cleanly (the trie rebuilds from authoritative data). MPT is unaffected. Measured cost: ~2% on SMT insert/get/remove (one extra byte per internal-node Keccak input); all other benchmark suites show no change beyond the noise floor.

### Fixed

- **`DagMapRaw::prune` is now crash-safe** (previously documented as not crash-atomic, directing callers to snapshot externally). The prune is re-phased as **destroy side branches → merge → flush → re-parent → flush → clear**: the genesis is enriched *in place* (keeping its instance ID, so pre-prune genesis metas keep resolving) while overlay top-down reads make the enrichment invisible through the head, and nothing is cleared before the merged genesis is durable and every surviving child has been re-pointed at it (the two `vsdb_flush` barriers pin the ordering across the engine's independently-recovered shards). A crash — `kill -9` or power loss — at any point leaves the canonical access paths (genesis, returned head, surviving children) observing either the complete pre-prune or the complete post-prune state, never a torn mix; interrupted-prune leftovers are plain storage leaks that the next prune reclaims, and re-running an interrupted prune either converges or is structurally refused (head clear order: parent → children → data). Destruction walks (`destroy`, `prune_children`, prune's side-branch sweep) now treat the children registry as an index only and verify each child's own parent slot before destroying, so stale double-registrations left by an interrupted prune can never kill a surviving node. Covered by `prune_crash_*` phase-boundary tests asserting value-exact views.
- **Accumulated on-disk data no longer degrades small writes into a permanent multi-ms stall.** Root cause was in the mmdb engine (fixed in **mmdb 4.0.4**, now the minimum dependency): a DB opened with a pre-existing L0 backlog — exactly what accumulates across short-lived processes, since collection handles never delete data on drop — never scheduled compaction (the only routine signal was post-flush), so every write slept in the L0-slowdown band against a stale cached file count, and small workloads never flushed to break the loop (measured ~5000 µs/put steady-state vs ~2 µs/put healthy). mmdb now kicks compaction at `DB::open` and from the slowdown path. On the vsdb side, `l0_compaction_trigger` returned to 4 (8 coincided exactly with mmdb's write-slowdown trigger, leaving background compaction no buffer zone to work in).
- **Merge-base search returns all lowest common ancestors with fork-region locality.** Criss-cross histories with multiple merge bases previously could violate the source-wins policy (one base chosen); the interim multi-base fix walked the full ancestry of both commits per merge/fork-point query. The final implementation is a git-style "paint down to common" walk (max-heap + STALE propagation): all merge bases, cost bounded by the fork region.
- **`PersistentBTree::bulk_load` now meets minimum occupancy (INV-BT3).** The trailing leaf chunk / internal group is rebalanced with its left sibling, so no non-root node is built below `MIN_KEYS` keys (`MIN_KEYS + 1` children).
- **Instance-meta writes are atomic** (`save_meta` / `save_instance_meta` in both crates): tmp file + fsync + rename replaces truncate-in-place `fs::write`, so a crash mid-save can no longer leave a truncated meta file.
- **`BitPath::from_packed` normalizes trailing bits** beyond `bit_len` to zero instead of relying on a caller contract.
- **`VecDex::compact()` pre-validates all vector dimensions** before the irreversible `clear()`, closing the (previously unreachable) mid-rebuild error path.
- The three-way merge decision matrix is now a single shared function for the single-base and multi-base paths (previously duplicated).

### Changed

- SMT point lookups compare the 32-byte key hash directly at leaves (provably equivalent to the former 256-bit path comparison) instead of materializing a `BitPath` per visit.
- The MPT and SMT disk caches share one codec-primitive module (`trie/codec_util.rs`) instead of duplicating varint/bytes/checksum helpers; encodings are byte-for-byte unchanged.
- MPT proof tests now cover divergence inside an Extension node and Extension→Branch→Extension proof chains.
- Internal rename: `RESERVED_ID_CNT` → `PREFIX_ALLOC_START` (private allocator constant; `BIGGEST_RESERVED_ID` public value unchanged).

## [v14.0.0]

### Breaking

- **Unified error type across the whole ecosystem.** `VsdbError` / `Result` now live in `vsdb_core::common::error` (re-exported as `vsdb::common::error`, `vsdb::VsdbError`, `vsdb::Result`). Every public API of **both** crates returns this type — including the `KeyEnDe` / `ValueEnDe` / `KeyEnDeOrdered` encoding traits, collection batch `commit()`, `save_meta` / `from_meta`, and `vsdb_set_base_dir` — all of which previously returned `ruc::Result` (`Box<dyn RucError>`). Implementing the encoding traits for custom types no longer requires a third-party error dependency. `ruc` remains internal-only; boundary conversions preserve the **complete** chain (every frame, with file/line context) via `stringify_chain`, and the new type additionally offers matchable variants (including new `Decode` and `BaseDirFrozen`), `std::error::Error` interop, and `Send + Sync`. The root alias `vsdb::VsdbResult` was renamed to `vsdb::Result`.
- **Legacy (pre-magic) instance-meta decoding removed.** The `with_legacy_mapx_meta_decode` escape hatch and the length-only prefix decode path are gone; deserialization now unconditionally requires the magic-tagged meta format introduced in v13.4. **Migration**: load and re-save (`save_meta`) any instance metas written by pre-v13.4 releases using a v13 build first.
- **Deprecated `MapxRaw::from_prefix_slice` / `MapxRaw::as_prefix_slice` removed** (deprecated since 13.0.0). Use `from_bytes` / `as_bytes`.
- **`DagMapRaw::new` / `DagMapRawKey::new` redesigned**: the signature is now `new(parent: Option<&mut DagMapRaw>) -> Self` (previously `new(&mut Orphan<Option<DagMapRaw>>) -> Result<Self>`). Each node now **owns** its parent slot instead of aliasing a caller-managed `Orphan` shared by all siblings. Consequences: `destroy()` persistently unlinks the node from its parent chain, so stale clones, shadows, and `from_meta`-restored handles can no longer resolve inherited reads through a destroyed node (previously a documented per-handle-tombstone limitation); constructing a node can no longer fail. The on-disk serde format (3-tuple) is unchanged; DAGs whose siblings were created from one shared `Orphan` slot under v13 keep that sharing until the affected nodes are recreated.
- **`vsdb::SlotDex` now names the generic struct** `slotdex::SlotDex<S, K>` instead of silently aliasing `SlotDex64<K>` (the same name previously referred to different types depending on the import path). **Migration**: replace `vsdb::SlotDex<K>` with `vsdb::SlotDex64<K>`.
- **Internal macros un-exported**: `define_map_wrapper!`, `entry_or_insert_via_mock!`, `cow_bytes_bounds!` (vsdb) and `parse_int!` / `parse_prefix!` (vsdb_core) were implementation details accidentally exported via `#[macro_export]`; they are now crate-private.
- **`NULL` constant removed** from both crates' root re-exports (it was an empty byte slice with no in-tree users).
- **mmdb engine updated to 4.0.0** and **`ruc` updated to 11.0.0.** The mmdb 4.0 `DbOptions` dropped the `max_write_buffer_number`, `memtable_prefix_bloom_ratio`, `level_compaction_dynamic_level_bytes`, and `allow_concurrent_memtable_write` tuning knobs (now internal); VSDB no longer sets them and relies on the engine defaults. No on-disk format or public API change.
- **`SlotDex::new` now asserts `tier_capacity >= 2`** (previously `> 0`). A capacity of 1 could never terminate tier growth, causing unbounded disk/memory usage.
- **MPT keys are now bounded** — `MptCalc` insert paths reject keys longer than `MAX_MPT_KEY_LEN` (1024 bytes) to prevent stack-overflow crashes from adversarially deep tries. SMT is unaffected (depth hard-capped at 256 bits).
- **`DagMapRaw` mutable-value tombstone guard** — writing an empty value back through `get_mut()`'s `ValueMut` now panics, matching the existing `insert()` guard (the empty byte string is the internal deletion tombstone).
- **`vsdb_set_base_dir` env contract documented** — the function must be called before spawning threads (it performs `env::set_var`). Internal database initialization no longer mutates the process environment, and the `VSDB_CUSTOM_DIR` environment variable is no longer set.

### Fixed

- **`PersistentBTree` crash recovery could reuse NodeIds** — `rebuild_ref_counts` now advances the node allocator past the maximum stored NodeId, preventing post-recovery allocations from overwriting live nodes (cross-snapshot corruption).
- **`VerMap` working-state crash window** — `insert`/`remove`/`discard` released the old dirty root before persisting the branch pointer; a crash in that window (with compaction triggered by the release) could leave the durable branch state pointing at physically deleted B+ tree nodes. The branch pointer is now persisted first.
- **`VecDex` crash recovery hardening** — dirty recovery now reconciles all per-node rows (dropping torn insert/remove leftovers), prefers entry-point candidates that still have base-layer edges (an edge-less node can no longer hide the whole graph), and relinks surviving nodes whose edge writes were lost.
- **`VecDex`/`SlotDex` crash-recovery completeness** — mutations after `save_meta()` re-set the persisted dirty bit, and recovery now rebuilds all derived metadata (VecDex: `next_node_id`, `entry_point`, `max_layer`; SlotDex: per-tier floor counts and `Large`-container lengths), not just the entry count.
- **`VerMap::fork_point` / `commit_distance` validated inputs** — two identical nonexistent commit IDs were previously reported as their own fork point / distance 0.
- **Base-directory freeze covers derived directories** — reading `vsdb_get_custom_dir` / `vsdb_get_system_dir` / `vsdb_get_meta_dir` before `vsdb_set_base_dir` now freezes the base dir, so a later `vsdb_set_base_dir` fails loudly (`VsdbError::BaseDirFrozen`) instead of silently splitting the directory tree across two bases.
- **Cosine distance small-norm misclassification** — the divide-by-zero guard used machine epsilon as an absolute threshold, classifying all small-magnitude f32 vector pairs (norms ≲ 3.5e-4) as maximally dissimilar. The guard now triggers only on a true zero denominator.
- **MPT read-path subtree cloning** — `get()`, `prove()`, and extension compaction no longer deep-clone entire subtrees per descent step (O(N) → O(depth) per lookup).
- **`DagMapRaw::new` storage leak** — one orphaned engine slot was leaked per node creation; prune also no longer accumulates deletion tombstones in the genesis node.
- **`VerMap::rollback_to` validation gap** — a branch with no commits could be rolled back to any commit in the DAG; it now fails with the documented "not an ancestor" error.
- **`MapxRaw` clone memory usage** — cloning now commits in bounded chunks instead of buffering the entire map in one in-memory write batch.

## [v12.0.0]

### Breaking

- **Replaced CBOR codec with postcard** — `serde_cbor_2` has been removed and replaced with `postcard` as the sole serialization codec. Existing data serialized with CBOR is incompatible; a migration step is required.

## [v11.0.0]

### Breaking

- **Removed RocksDB backend** — MMDB is now the sole storage engine. The `backend_rocksdb` and `backend_mmdb` feature flags have been removed. No C/C++ toolchain required.
- **Commit reference counting** — `Commit` gains a `ref_count: u32` field. `delete_branch` and `rollback_to` immediately hard-delete orphaned commits via cascading ref-count decrement. No manual `gc()` call needed for commit cleanup.
- **`VerMapWithProof`: automatic cache lifecycle** — `save_cache()` and `load_cache_and_sync()` have been removed from the public API. The trie cache is now eagerly saved after each `sync_to_commit` and auto-loaded on construction. No manual calls required.

### Added

- **Commit ref counting** — each commit tracks the number of branch HEADs and child parent-links pointing to it. `commit()`, `create_branch()`, `delete_branch()`, `merge()`, and `rollback_to()` all maintain ref counts automatically.
- **B+ tree in-memory node ref counting** — `PersistentBTree` maintains a `HashMap<NodeId, NodeRef>` for zero-overhead lifecycle tracking. Dead nodes are cascade-released in memory; disk reclamation happens on `gc()` / startup.
- **`VerMapWithProof` auto-cache** — auto-load in `new()`/`from_map()`, eager save after each `sync_to_commit`. A `cache_dirty` flag avoids redundant serialization in read-only scenarios.

### Removed

- `backend_rocksdb` feature flag and all RocksDB-related code, Makefile targets, and documentation.
- `strata/docs/engine-comparison.md` (no longer applicable).
- `pending_gc`, `next_gc_seq`, `process_pending_gc()`, `recover_pending_gc()` — replaced by commit ref counting.

## [v10.0.0] - 2026-03-19

### Breaking

- **Removed msgpack codec** — CBOR (`serde_cbor_2`) is now the only serde encoding. Existing data serialized with msgpack is incompatible; a migration step is required.
- **Default backend for `vsdb` crate** — `backend_mmdb` is now enabled by default so that `vsdb = "10.0.0"` works out of the box without a C/C++ toolchain (previously required explicit feature selection).

### Added

- **MMDB backend** (`backend_mmdb`) — a pure-Rust LSM-Tree alternative to RocksDB. No C/C++ dependency; suitable for cross-compilation and WASM targets.
- **Engine comparison guide** — `strata/docs/engine-comparison.md` with detailed benchmarks of MMDB vs RocksDB.
- **`make all-rocksdb`** target and RocksDB-specific lint/test/bench targets in Makefile (default targets use MMDB).

### Changed

- **SlotDex performance** — tier data backed by an in-memory `BTreeMap` cache (auto-hydrated via `RefCell`), reducing page query latency from ~1 ms to ~8 us.
- Aligned MMDB DB options with RocksDB configuration for consistent behavior.
- Fixed mmdb 2.2 API: replaced removed `prefix_iterator` with `iter_with_prefix`.
- Replaced `unwrap`/`panic` with `c(d!())` error chains; hardened decode bounds.
- Expanded benchmark coverage and fixed methodological issues.
- Bumped dependencies.

### Removed

- Removed `lint-codecs` CI target (no longer needed with single codec).
- Removed RocksDB pre-built static lib cache from Makefile.

## [v9.1.0] - 2026-03-09

### Changed

- **Merged `vsdb_trie_db` and `vsdb_slot_db` into `vsdb`** — they are now modules (`trie` and `slotdex`) instead of separate crates. The workspace is reduced to two crates: `vsdb_core` and `vsdb`.
- **Renamed `trie_db` -> `trie`**, inner `trie/trie` -> `trie/mpt`, `slot_db` -> `slotdex`.
- **Moved `VerMapWithProof`** from `versioned::proof` to `trie::proof`, alongside `MptCalc` and `SmtCalc`.
- **Removed `merkle` feature gate** — the `trie` module (including `sha3` and `thiserror`) is always compiled.
- All public types re-exported from crate root: `MptCalc`, `SmtCalc`, `SmtProof`, `VerMapWithProof`, `SlotDex`.

### Added

- **`SmtCalc`** — Sparse Merkle Tree with 256-level proofs (`prove` / `verify_proof`).
- **SMT cache** — `save_cache` / `load_cache` for `SmtCalc` (disposable on-disk persistence).
- Comprehensive SMT test suite (24 tests) and benchmarks.
- Architecture diagram in `trie` module docs.

## [v9.0.0] - 2026-03-07

### Added

- **`VerMap` convenience APIs**: `branch_id`, `branch_name`, `has_uncommitted`, `range`, `iter_at_commit`, `get_commit` — small, high-value methods for common caller patterns.
- **Versioned benchmarks**: Criterion benchmarks covering single-branch CRUD, commit/rollback, branching, iteration/range, historical reads, three-way merge, and GC.
- **Comprehensive test coverage**: 65 new tests for the new APIs; 136 total versioned tests.

### Fixed

- Fixed all Clippy warnings for Rust 1.93+ (collapsible `if`-let chains, `ptr_arg`, `needless_borrows_for_generic_args`, `type_complexity`).

## [v8.3.0] - 2024-07-27

### Changed

- **License changed from GPL-3.0 to MIT.** The entire project is now licensed under the MIT license, allowing for more permissive use and integration.
