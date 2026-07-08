# Audit Findings

> Auto-managed by /x-review and /x-fix.
>
> **Won't Fix ≠ permanent.** Every entry under `## Won't Fix` must be
> re-evaluated against the current codebase on each audit. Surrounding code
> changes, new callers, or API evolution may make a previously-disproportionate
> fix straightforward — or make the finding obsolete. Never silently carry
> forward a Won't Fix entry without fresh assessment.

## Open

*(empty)*

---

## Resolved

### [LOW] docs: CLAUDE.md not updated for `VecDexDyn` and `clone_in`
- **Where**: `CLAUDE.md` (Architecture table "Vector Index" row; top feature-bullet list)
- **What**: This diff adds a new public type `VecDexDyn` (+ `MetricKind`, `strata/src/vecdex/dynamic.rs`) and a new `clone_in(&Namespace)` method spanning `MapxRaw`/`Mapx`/`MapxOrd`/`MapxOrdRawKey`/`Orphan`. `README.md:104` and `strata/README.md:22` were updated in the same diff to mention `VecDexDyn`, but `CLAUDE.md`'s Vector Index table row and Namespaces bullet were not touched (confirmed: `CLAUDE.md` has zero diff in this commit range, and contains no occurrence of `VecDexDyn`, `MetricKind`, or `clone_in`).
- **Why**: Violates this repo's own "Doc-code alignment" convention (public API changes must update corresponding docs, `CLAUDE.md` included) — the same convention already enforced by the "namespace architecture is missing from review/project docs" resolved entry below.
- **Suggested fix**: Add `VecDexDyn`/`MetricKind` to the Vector Index row, and mention `clone_in` in the Namespaces bullet (or a new row), mirroring the wording already added to `README.md`/`strata/README.md`.
- **Resolution**: Fixed — CLAUDE.md Vector-index bullet and Architecture-table row now name `VecDexDyn`/`MetricKind`; the Namespaces bullet now names `clone_in(&ns)` and the consuming `Namespace::close(self)`.

### [LOW] vecdex: pattern guide's Critical Invariants/Bug Patterns not updated for `VecDexDyn`'s frozen-wire-tag invariant
- **Where**: `.claude/docs/patterns/vecdex.md` (Critical Invariants / Common Bug Patterns / Review Checklist sections)
- **What**: The Files list was updated to mention `dynamic.rs`/`VecDexDyn`'s frozen wire tags and `DynIter`, but no corresponding invariant, Common Bug Pattern entry, or Review Checklist line was added for "wire tags must stay frozen/append-only, never re-derived from enum/derive variant order" — the exact bug class just fixed in this diff (see the "persisted discriminant followed enum source order" entry below).
- **Why**: This pattern guide is the designated authoritative source for catching regressions of this exact class in future reviews; the Files list alone doesn't carry the checkable invariant forward.
- **Suggested fix**: Add "INV-VD6: Frozen Wire Tags" to Critical Invariants (wire tags are explicit append-only constants, never derived variant/enum order), plus a matching Common Bug Pattern and Review Checklist line.
- **Resolution**: Fixed — added INV-VD6 (Frozen Wire Tags, aligned with the actual `WIRE_TAG_*` manual-serde implementation and `MetricKind`'s derive status), a "Wire-Tag Drift" Common Bug Pattern, and a matching Review Checklist line.

---

### [MEDIUM] engine: `clone_in` error path abandoned committed chunks as unreclaimable garbage
- **Where**: `core/src/common/engine/mod.rs` (`Mapx::clone_in`)
- **Resolution** *(post-v16.2.0 review)*: a failed chunk commit now
  triggers a best-effort wipe of the partial target — one O(1) range
  tombstone (`batch_begin_wiped`, the same primitive `clear()` uses)
  committed before the error propagates — so a failed or *retried*
  clone (each retry allocates a fresh prefix) no longer accumulates
  invisible garbage under never-returned prefixes.  The tombstone is
  tiny enough to stand a chance exactly where a 4096-pair data batch
  just failed (e.g. disk-full); if the wipe itself also fails, the
  residue matches the old documented contract.  Since `Clone` panics
  *after* `clone_in` returns `Err`, the panic path is cleaned up too.
  Doc comments updated at the engine, `MapxRaw`, and typed-wrapper
  levels.

### [MEDIUM] vecdex: `VecDexDyn`'s persisted discriminant followed enum source order
- **Where**: `strata/src/vecdex/dynamic.rs`
- **Resolution** *(post-v16.2.0 review)*: the derived
  `Serialize`/`Deserialize` impls (postcard = variant-*index* tagged,
  so inserting or reordering variants would silently re-map existing
  metas) were replaced with manual impls over explicit **frozen wire
  tags** (`WIRE_TAG_L2 = 0`, `WIRE_TAG_COSINE = 1`,
  `WIRE_TAG_INNER_PRODUCT = 2`), documented append-only.  The encoding
  is byte-identical to what the derived impls wrote in v16.2.0 (a
  `u8` 0/1/2 is the same single byte as postcard's varint variant
  index), so existing metas load unchanged — verified by
  `dyn_wire_tags_are_frozen`, which pins each metric's first meta byte
  and rejects an out-of-range tag outright instead of mis-decoding it
  as some existing variant's payload.

### [LOW] namespace: `Namespace::close(self)` duplicated the whole close protocol of `vsdb_ns_close`
- **Where**: `core/src/common/namespace.rs`
- **Resolution** *(post-v16.2.0 review)*: a single
  `ns_close_impl(id, caller_handle: Option<Namespace>)` now owns the
  protocol (default-ns guard → `REGISTRY_LOCK` → table lock →
  ref-accounting with `accounted = 1 + consumed-handle` → entry
  removal → out-of-lock engine teardown); both public entry points are
  one-line wrappers and both refusal messages are preserved verbatim
  (the consuming form's "other" qualifier included).  Future hardening
  of the close protocol now has exactly one place to land.

### [LOW] vecdex: `VecDexDyn::keys`/`iter` boxed their iterators
- **Where**: `strata/src/vecdex/dynamic.rs`
- **Resolution** *(post-v16.2.0 review)*: `Box<dyn Iterator>` (a heap
  allocation per call, and a signature mismatch against the "mirrors
  `VecDex` one-to-one" contract) replaced by a private three-variant
  enum iterator (`DynIter`) returned as `impl Iterator` — zero
  allocation, `size_hint` forwarded.  The cost-model doc now states
  that iterators dispatch per *item* through the same single `match`.

### [LOW] vecdex: no dynamic-dispatch coverage for the non-default scalar (`f64`)
- **Where**: `strata/src/vecdex/test.rs`
- **Resolution** *(post-v16.2.0 review)*: added `dyn_f64_end_to_end` —
  Cosine/`f64` insert + search through the dispatch layer,
  `save_meta`/`from_meta` round-trip, and scalar-width
  cross-rejection (an `f64` dyn meta refuses to load as the `f32`
  default or as any static handle).

### [MEDIUM] namespace: `root_holds_dataset` validates structure, not content — a fabricated or partially-destroyed skeleton still bypasses `vsdb_ns_relocate`'s new check
- **Where**: `core/src/common/engine/mmdb.rs` (`root_holds_dataset`), `core/src/common/namespace.rs` (`vsdb_ns_relocate`)
- **Resolution**: The check now requires mmdb's `CURRENT` manifest anchor
  inside every expected shard dir, not merely the dirs' existence.
  `CURRENT` is exactly mmdb's own recover-vs-create test at open (absent ⇒
  the shard is silently recreated fresh — the precise loss shape being
  refused), so the guard now asks the semantically right question: "would
  every shard take the *recover* path?". Zero false positives: the format
  marker is written only after every shard opened, and each open creates
  `CURRENT` — verified against mmdb source (`version_set.rs::open_with_cache`)
  and empirically on a fresh zero-data namespace. Bare skeletons (marker +
  empty shard dirs — a "prepared" volume, or a copy interrupted before any
  shard content landed) are refused; pinned by a new fabricated-skeleton
  arm in `namespace_lifecycle`. Doc comment, error message, and
  `vsdb_ns_relocate`'s public doc were reworded to state exactly what is
  (and is not) verified: which *dataset* lives there cannot be checked —
  roots carry no namespace id — so moving the right data remains the
  operator's documented contract, same trust boundary as `destroy`.

### [LOW] namespace: `vsdb_ns_relocate` doesn't bounds-check the registered shard count before calling `root_holds_dataset`
- **Where**: `core/src/common/namespace.rs` (`validated_shards`)
- **Resolution**: The `1..=64` registry-damage guard was extracted from
  `open_record_locked` into `validated_shards(rec)` (identical semantics at
  both sites, unlike the deliberately-divergent DEFAULT_NS_ID guards) and
  relocate now validates through it before probing the target — a corrupt
  `shards == 0` fails loudly as registry damage instead of vacuously
  passing `(0..0).all(..)` and degrading the dataset check to
  marker-only.

### [HIGH] namespace: `vsdb_ns_relocate` accepts an unpopulated target, silently orphaning data
- **Where**: `core/src/common/namespace.rs` (`vsdb_ns_relocate`), `core/src/common/engine/mmdb.rs` (`root_holds_dataset`)
- **Resolution** *(post-v16.1.0 review)*: relocate now refuses a target that
  does not already hold this namespace's dataset — `root_holds_dataset`
  requires the format marker plus exactly the record's shard dirs before
  `save_registry` runs, so "relocate before moving the data" fails loudly
  instead of durably repointing the registry at an empty dir (whose next
  `open` would silently initialize a fresh root, orphaning the real data).
  Regression test covers both arms in-process (thanks to `vsdb_ns_close`):
  write → close → relocate-to-empty is refused; `fs::rename` the tree →
  relocate succeeds → the persisted `InstanceId` resolves and the data is
  visible at the new root.

### [MEDIUM] strata: `from_bytes` on typed wrappers doesn't document its new ambient-namespace dependency
- **Where**: `strata/src/common/macros.rs` (`define_map_wrapper!`), `strata/src/basic/orphan/mod.rs`
- **Resolution** *(post-v16.1.0 review)*: the wrapper-level safety docs now
  state the ambient-namespace binding (mirroring `MapxRaw::from_bytes`'s
  wording: a raw prefix carries no namespace information of its own), and
  `from_bytes_in(ns, s)` was added to `define_map_wrapper!` (covering
  `Mapx`/`MapxOrd`/`MapxOrdRawKey`) and `Orphan` for parity with `MapxRaw` —
  the whole delegation chain terminates at `MapxRaw::from_bytes_in`.

### [MEDIUM] docs: CHANGELOG.md claims `VerMapWithProof` gains `new_in`/`namespace()`, but it has neither
- **Where**: `CHANGELOG.md` (`[v16.0.0]` collection list)
- **Resolution** *(post-v16.1.0 review)*: bullet corrected — the collection
  list drops "tries-with-proof" and names the exception explicitly:
  `VerMapWithProof` is placed via `from_map(VerMap::new_in(&ns))` and
  exposes its namespace via `.map().namespace()`.

### [LOW] docs: `strata/docs/vecdex.md` API table stale after the `InstanceId` migration
- **Where**: `strata/docs/vecdex.md` (API Reference table)
- **Resolution** *(post-v16.1.0 review)*: `instance_id`/`save_meta` now
  documented as returning `InstanceId`, `from_meta` as taking
  `impl Into<InstanceId>` (bare `u64` still works for default-namespace
  instances); `new_in` and `namespace` rows added.

### [LOW] docs: CHANGELOG.md refers to a nonexistent `TrieCache` trait
- **Where**: `CHANGELOG.md`
- **Resolution** *(post-v16.1.0 review)*: s/`TrieCache`/`TrieCalc`/ — the
  actual trait whose `save_cache`/`load_cache` take the cache dir.

### [LOW] style: ungrouped `use self::mmdb::…` imports
- **Where**: `core/src/common/engine/mod.rs`
- **Resolution** *(post-v16.1.0 review)*: merged into one grouped
  `pub(crate) use self::mmdb::{…}` (which also exports the new
  `root_holds_dataset`).

### [LOW] style: ungrouped `std::` imports in trie/mod.rs
- **Where**: `strata/src/trie/mod.rs`
- **Resolution** *(post-v16.1.0 review)*: merged into `use std::{mem, path::Path};`.

### [LOW] engine: `migrate_ceiling` doc describes the wrong call order relative to `open_at`
- **Where**: `core/src/common/engine/mmdb.rs` (`migrate_ceiling` doc comment)
- **Resolution** *(post-v16.1.0 review)*: doc rewritten to describe the
  actual mark-then-fold order (`open_at` writes the marker before the fold)
  and why it is safe: every allocation path funnels through
  `ensure_alloc_init`, which refuses to issue prefixes until a fold has
  produced the ceiling file; the legacy key is preserved forever and the
  fold is idempotent take-max, so no crash point between marker and fold
  can lead to prefix reuse.

### [HIGH] namespace: unvalidated registry shard count can panic on `prefix % 0`
- **Where**: `core/src/common/namespace.rs` (`open_record_locked`)
- **Resolution** *(third-party review)*: registry entries are written
  pre-clamped, so an out-of-range `shards` means file corruption or
  hand-editing; `open_record_locked` now refuses `0` / `>64` with a clean
  "registry damaged" error instead of letting `shards == 0` reach
  `prefix % 0` (a release-mode panic) on the first routed operation.

### [MEDIUM] namespace: `vsdb_ns_list` missing the base-dir freeze
- **Where**: `core/src/common/namespace.rs` (`vsdb_ns_list`)
- **Resolution** *(third-party review)*: `list` reads the registry and
  materializes base-derived root paths — it now freezes the base dir first,
  the same contract as open/destroy/relocate, so returned roots cannot be
  split from the universe by a later `vsdb_set_base_dir`.

### [LOW] identity: `InstanceId` construction duplicated at call sites
- **Where**: `core/src/common/namespace.rs` (`InstanceId::new`),
  `core/src/basic/mapx_raw/mod.rs`, `strata/src/common/macros.rs`
- **Resolution** *(third-party review; reported as "fields silently
  dropped", which is inaccurate — struct literals are exhaustive, so a new
  field is a compile error — but the duplication was real)*: added the
  canonical constructor `InstanceId::new(map_id, ns)` (folds
  `DEFAULT_NS_ID` to `None`) and routed both handle-side construction
  sites through it.

### [LOW] strata: instance-meta path building duplicated across the crate boundary
- **Where**: `strata/src/common/mod.rs`, `core/src/common/namespace.rs`
- **Resolution** *(third-party review)*: `Namespace::meta_path` is now
  `pub` (the single source of truth for meta naming) and strata's
  `save_instance_meta`/`load_instance_meta` call it instead of re-joining
  `meta_dir()` + a hand-formatted name.

### [LOW] engine: `write_file_durable` lock discipline was implicit
- **Where**: `core/src/common/engine/mmdb.rs` (`write_file_durable`)
- **Resolution** *(third-party review)*: the caller-must-hold-the-class-lock
  contract (SYS_META_LOCK for allocator/marker/sentinel files,
  REGISTRY_LOCK for the registry) is now documented on the function.
  Internalizing the lock is not possible: several callers already hold it
  and parking_lot mutexes are non-reentrant.

### [LOW] namespace: `flush_all_open` held the table lock across engine flushes
- **Where**: `core/src/common/namespace.rs` (`flush_all_open`)
- **Resolution** *(third-party review)*: handles are cloned out first;
  flushes (which can take seconds) no longer block concurrent
  `Namespace::open`/meta restores on `OPEN_NAMESPACES`.

### [LOW] tests: `namespace_lifecycle` leaked sibling scratch dirs
- **Where**: `core/tests/namespace_test.rs`
- **Resolution** *(third-party review)*: `{dir}_mnt` and `{dir}_ro_parent`
  are removed in the teardown alongside the main dir.

### [HIGH] engine: marker-present roots with missing shard dirs can be silently reinitialized
- **Where**: `core/src/common/engine/mmdb.rs` (`validate_shard_layout`)
- **Resolution**: Marker-present validation is now strict equality on the
  exact expected shard set — including ZERO shard dirs (e.g. a manually
  deleted `mmdb/`), which previously matched the "fresh root" arm and was
  silently reinitialized over. Damage is refused loudly. Pinned by
  `shard_layout_lifecycle_states`.

### [HIGH] engine: corrupt marker-absent legacy roots with missing shards were treated as resumable
- **Where**: `core/src/common/engine/mmdb.rs` (`INIT_SENTINEL_REL_PATH`, `open_at`, `validate_shard_layout`)
- **Resolution**: "Resumable" now requires proof, not inference: brand-new
  roots raise a durable `__SYSTEM__/__initializing__` sentinel BEFORE the
  first shard dir exists (retired after the format marker lands). Partial
  shard sets are resumable only under the sentinel; a partial set with
  neither sentinel nor marker (e.g. a legacy 16-shard base missing dirs) is
  damage — "resuming" it would present silent data loss as success — and is
  refused with destroy-and-recreate guidance. Pinned by
  `shard_layout_lifecycle_states`.

### [MEDIUM] engine: shard validation counted prefixes instead of the exact shard set
- **Where**: `core/src/common/engine/mmdb.rs` (`scan_shard_layout`)
- **Resolution**: The scan now checks the exact expected names
  (`shard_00..shard_{N-1}`, each a directory) and rejects any unexpected
  `shard_*` entry (wrong index, misnamed, or non-directory) — `shard_backup`
  + `shard_00` can no longer masquerade as a complete 2-shard set.

### [MEDIUM] namespace: failed explicit create could leave an unretryable root
- **Where**: `core/src/common/namespace.rs` (`cleanup_failed_root`)
- **Resolution**: The create rollback now also clears the root the failed
  open (partially) filled. Safe by construction: the adoptable check proved
  the root was absent or empty beforehand — everything inside is ours. A
  pre-existing empty dir (mount point) is emptied but kept; a dir we created
  is removed. The same path is immediately retryable. Covered by the
  read-only-parent retry sub-scenario in `namespace_lifecycle`.

### [MEDIUM] namespace: derived-root create failure could leave unregistered VSDB-owned residue
- **Where**: `core/src/common/namespace.rs` (`cleanup_failed_root`)
- **Resolution**: Same cleanup applies to derived roots
  (`__NAMESPACES__/{id}` is always VSDB-created): after rollback nothing
  unreachable survives under an id that will never be issued again.

### [LOW] docs: RFC still called destroy-crash orphan roots "re-attachable"
- **Where**: `docs/proposals/namespaces.md` §4.7
- **Resolution**: Reworded — orphaned dirs are manually removable;
  re-attachment is excluded along with all foreign-root adoption (§9).

### [LOW] tests: rollback test missed the post-registry-save rollback path
- **Where**: `core/tests/namespace_test.rs`
- **Resolution**: The FILE-blocker case now documents that it exercises the
  pre-registry refusal; a new unix sub-scenario (read-only parent) forces the
  failure AFTER the registry save, asserting rollback, a clean root, and a
  successful retry of the same path once writable.

### [LOW] tests: repeated inline `std::fs` paths
- **Where**: `core/tests/namespace_test.rs`
- **Resolution**: `use std::{fs, path::PathBuf};` — inline paths replaced.

### [LOW] namespace: repeated inline `ErrorKind::NotFound` path
- **Where**: `core/src/common/namespace.rs`
- **Resolution**: `io` added to the std import group; all three call sites
  use `io::ErrorKind::NotFound`.

### [CRITICAL] namespace/engine: namespace open can run before the default allocator base is frozen
- **Where**: `core/src/common/namespace.rs` (`open`, `vsdb_ns_destroy`, `vsdb_ns_relocate`)
- **Resolution**: `Namespace::open` (and the admin fns) now call
  `vsdb_freeze_base_dir()` before reading the registry — the same rule every
  other base-derived path already followed (`create_with` had it; `open` was
  the gap). A later `vsdb_set_base_dir` fails loudly with `BaseDirFrozen`
  instead of moving the allocator's backing store under live namespaces.

### [CRITICAL] namespace lifecycle: destroy and relocate can race a concurrent open
- **Where**: `core/src/common/namespace.rs` (`vsdb_ns_destroy`, `vsdb_ns_relocate`)
- **Resolution**: The not-open check now runs UNDER `REGISTRY_LOCK` (open
  inserts into `OPEN_NAMESPACES` while holding the same lock), closing the
  TOCTOU window where a racing open could cache a live engine whose root was
  about to be deleted/repointed.

### [CRITICAL] namespace paths: raw lexical overlap checks are bypassable by path aliases
- **Where**: `core/src/common/namespace.rs` (`validate_explicit_root`, `normalize_physical`)
- **Resolution**: `.`/`..` components are rejected outright; overlap
  comparisons run on physically normalized paths (canonicalize the deepest
  existing ancestor + lexical tail), so symlinked spellings of the base or of
  another root are caught. Symlinks created after registration are documented
  out of scope (filesystem administration, not addressing). Covered by the
  `..`-rejection sub-scenario in `namespace_lifecycle`.

### [CRITICAL] namespace allocator: explicit existing roots are adopted without prefix-ceiling reconciliation
- **Where**: `core/src/common/namespace.rs` (`ensure_root_adoptable`)
- **Resolution**: `create_with` now refuses an explicit root that exists and
  is non-empty — foreign datasets have unknown prefix provenance, and
  importing/attaching foreign roots is an explicit non-goal (RFC §9; a future
  attach must first raise the local ceiling). Empty dirs (fresh mount points)
  remain accepted. Covered by occupied-root and empty-mount sub-scenarios in
  `namespace_lifecycle`.

### [HIGH] namespace init: a crash during shard creation makes the namespace unreopenable
- **Where**: `core/src/common/engine/mmdb.rs` (`validate_shard_layout`)
- **Resolution**: Shard-layout validation is now completion-aware, keyed on
  the root's format marker (written only after every shard exists): marker
  absent + `existing < shards` = resumable half-created root (reopen
  completes initialization — safe, no allocation ever targeted the root);
  marker present + any mismatch, or `existing > shards` in any state, stays
  rejected. The pre-v16 default base (marker-less, exactly 16/16) takes the
  equal path. A follow-up review found remaining marker-present,
  corrupt-legacy, and exact-shard-set edge cases; those are resolved by the
  initialization-sentinel redesign (see the two HIGH entries and the MEDIUM
  exact-set entry above) — resumability is now gated on explicit proof, and
  the original completion-aware test was superseded by
  `shard_layout_lifecycle_states`.

### [HIGH] dagmap: parented construction can split one DAG across namespaces
- **Where**: `strata/src/dagmap/raw/mod.rs`, `strata/src/dagmap/rawkey/mod.rs`
- **Resolution**: Parented construction now ALWAYS inherits the parent's
  namespace (`new` wraps the build in `parent.namespace().scope(..)`),
  overriding any ambient scope; `new_in` with a mismatched explicit namespace
  is a caller bug (`debug_assert`ed, inherited in release). One DAG never
  spans namespaces.

### [MEDIUM] dagmap: stale registry entries can make destroy skip an owned child
- **Where**: `strata/src/dagmap/raw/mod.rs` (`destroy`)
- **Resolution**: Ownership check now runs BEFORE duplicate suppression —
  foreign entries no longer poison `seen`, so the true owned entry for the
  same node is never skipped (INV-DG5). Cycle safety is preserved: foreign
  entries never extend the stack, and owned nodes still enter `seen` before
  their children are cleared.

### [MEDIUM] namespace metadata: `InstanceId` accepts a non-canonical default namespace form
- **Where**: `core/src/common/namespace.rs` (`InstanceId`)
- **Resolution**: `Some(DEFAULT_NS_ID)` now folds to `None` at every
  constructor under the type's control: `FromStr` (`"42@0"` → canonical) and
  `Deserialize` (via a `#[serde(from)]` wire mirror); `From<u64>` and
  `instance_id()` were already canonical. Canonical-form contract documented
  on the type. Covered by non-canonical parse/deserialize assertions in
  `namespace_lifecycle`.

### [LOW] docs: namespace proposal references a non-existent `save_as_meta` API
- **Where**: `docs/proposals/namespaces.md`
- **Resolution**: All `save_as_meta` references renamed to the implemented
  `save_meta`.

### [LOW] docs: namespace architecture is missing from review/project docs
- **Where**: `CLAUDE.md`, `.claude/docs/review-core.md`, `.claude/docs/patterns/engine.md`
- **Resolution**: Namespace subsystem added to the CLAUDE.md feature list and
  architecture table, to review-core.md's subsystem→guide mapping
  (`namespace.rs` → `engine.md`), and engine.md's file list + architecture
  section rewritten for per-namespace engines, the global allocator, the
  format marker, and the REGISTRY_LOCK TOCTOU rule.

### [LOW] tests: repeated inline `PathBuf::from` violates import convention
- **Where**: `core/tests/namespace_test.rs`
- **Resolution**: `use std::path::PathBuf;` added; inline paths replaced.

### [LOW] namespace: create_with consumes id on engine-open failure with no automatic rollback
- **Where**: `core/src/common/namespace.rs` (`create_with`)
- **Resolution**: Non-crash engine-open failures now roll the just-persisted
  registry entry back inline (under the already-held `REGISTRY_LOCK`) before
  propagating the error — a failed `create` leaves no registry residue.
  `next_id` deliberately stays advanced (ids are never reused; a burnt id is
  free). The two narrower follow-up regressions (rollback leaving
  newly-created root contents behind; the test not exercising the post-save
  rollback arm) are both closed: `cleanup_failed_root` now clears whatever
  the failed open left under the root, and `namespace_test.rs`'s read-only-parent
  sub-scenario exercises the post-save rollback path.

### [LOW] engine: missing `// SAFETY:` comment on inner unsafe block in `from_prefix_slice`
- **Where**: `core/src/common/engine/mod.rs` (`from_prefix_slice`)
- **Resolution**: Added the per-block `// SAFETY:` comment (forwards the fn's
  contract verbatim), matching the repo convention that every `unsafe {}`
  block carries its own justification.

---

## Won't Fix

### [REJECTED] vecdex: "renaming a `VecDexDyn` variant breaks persisted metas"
- **Where**: `strata/src/vecdex/dynamic.rs`
- **What**: Third-party review claimed a variant rename (e.g. `L2` →
  `Euclidean`) invalidates saved metas because "postcard's
  externally-tagged enum encoding expects the key 'L2'".
- **Reason**: False premise — postcard is a non-self-describing,
  index-tagged format: `serialize_newtype_variant` writes only the
  numeric variant index; variant *names* never reach the wire
  (externally-tagged string keys are serde_json behavior). Renames
  were format-compatible before and remain so; with the frozen wire
  tags shipped for the reorder finding, the on-disk mapping is now
  pinned by explicit constants regardless of any source refactor.

### [REJECTED] namespace: "`close(self)` drops the handle inside the table-lock scope unnecessarily"
- **Where**: `core/src/common/namespace.rs` (`ns_close_impl`)
- **What**: Proposed deferring the consumed handle's drop past the
  `OPEN_NAMESPACES` lock release, like the engine teardown.
- **Reason**: The drop *is* the exclusivity-accounting step: releasing
  the consumed handle's ref under the table lock (an O(1) atomic
  decrement, count 2→1) is what entitles the removed entry to
  `Arc::try_unwrap` as the sole strong ref. The slow part — engine
  teardown — already runs outside the table lock. Deferring the
  decrement would buy nanoseconds of lock-hold time in exchange for a
  second post-lock unwrap dance. Documented at the shared impl.

### [REJECTED] vecdex: "`dispatch!` pattern bindings can shadow same-named caller variables"
- **Where**: `strata/src/vecdex/dynamic.rs` (`dispatch!`)
- **What**: Hypothesized `let idx = precompute();
  dispatch!(self, idx => idx.search(idx, k))` silently passing the
  inner `VecDex` as the query.
- **Reason**: FP-6 — the finding's own scenario does not compile (the
  shadowing binding is a `&VecDex`, not a `&[S]`; every such misuse is
  a type error, and the inner handle type coincides with no query/key
  parameter type in the API). The caller writes the binding identifier
  explicitly at the call site (`idx =>`) — the exact semantics of a
  closure parameter `|idx| ...`, where shadowing is equally visible
  and equally the caller's own choice. `macro_rules!` hygiene is not
  involved for identifiers the caller passes in.

### [REJECTED] engine: "`OnceLock::get_or_init` can run `alloc_prefix` twice under concurrent reads"
- **Where**: `core/src/common/engine/mod.rs` (`Mapx::prefix_bytes`)
- **What**: Third-party review claimed concurrent shared-handle reads could
  both enter the `OnceLock` initializer and leak a prefix id.
- **Reason**: False — `std::sync::OnceLock::get_or_init` documents that when
  many threads call it concurrently, **exactly one** initializing closure
  runs (competing callers block until it completes). No double allocation is
  possible; a leaked id would in any case be waste, not corruption.

### [REJECTED] namespace: "`DEFAULT_NS_ID` guard copy-pasted in admin functions"
- **Where**: `core/src/common/namespace.rs` (`open`, `vsdb_ns_destroy`,
  `vsdb_ns_relocate`, `ns_close_impl`)
- **What**: Third-party review proposed extracting the `id ==
  DEFAULT_NS_ID` guards into a shared helper. Originally raised against
  3 sites; `ns_close_impl` (v16.2.1, shared by `vsdb_ns_close` and
  `Namespace::close`) is a 4th guard of the same shape, re-checked here.
- **Reason**: The guards have deliberately different semantics — `open`
  short-circuits to `default_ns()` (success), destroy/relocate/close each
  return distinct, context-specific errors with actionable guidance. A
  shared helper would need flags/closures to reproduce the divergence: DRY
  for DRY's sake, net readability loss for four 3-line guards.

### [REJECTED] dagmap: "public `Orphan::get_mut()` allows cycle creation"
- **Where**: `strata/src/dagmap/raw/mod.rs` (`parent` field), `strata/src/basic/orphan/mod.rs` (`get_mut`)
- **What**: Claimed that because `parent` is an `Orphan<Option<DagMapRaw>>` and `Orphan` has a public `get_mut()`, users can re-parent a node onto its own descendant in safe code.
- **Reason**: False premise — the `parent` field is private to the `dagmap::raw` module and no public API returns it (or any handle to it): `new/get/get_mut(key)/insert/remove/prune*/destroy` never expose the parent `Orphan`. `Orphan::get_mut` being public is irrelevant without access to the field. The only external route is deliberately reinterpreting a serialized payload as a different type, which is the generic serde-aliasing hazard already documented on the deserialization impls, not a dagmap-specific hole. Parent-chain cycle *tolerance* (lookups degrade to `None`, `prune` errors) remains covered by existing cycle guards and tests.

### [REJECTED] engine: "derated cgroup comparison undercuts host when cgroup is not binding"
- **Where**: `core/src/common/engine/mmdb.rs` (`effective_mem_budget`)
- **What**: External review of ca1335f proposed comparing the RAW cgroup limit against the host reading and derating only when the cgroup is the binding constraint.
- **Reason**: The proposed fix is unsafe: in that band it leaves `budget_limited` unset, and the unconstrained write-buffer path can overshoot the cgroup line wholesale. `budget_limited` is not merely "which number won"; it gates conservative budget-scaled memtable sizing. The min-fold is the documented, deliberate semantic and is strictly conservative. Now pinned by `effective_mem_budget_semantics` and the function's doc comment.

### [REJECTED] engine: "derating should not apply to memory.max, only memory.high"
- **Where**: `core/src/common/engine/mmdb.rs` (`cgroup_mem_limit_bytes`)
- **What**: External review claimed `memory.max` (and v1 `limit_in_bytes`) are "safe at 100%" because they have no reclaim-stall phase.
- **Reason**: Sizing engine caches to 100% of the OOM-kill line is worse than sitting at the throttle line: crossing it is SIGKILL → dirty store → derived-state rebuild. Under `memory.max` the kernel *does* reclaim/stall before killing once the cgroup is at its limit. Headroom below a hard kill line is more necessary, not less.

### [LOW] engine: 16 GiB write-buffer threshold is a sizing cliff
- **Where**: `core/src/common/engine/mmdb.rs` (`mmdb_open`, `legacy_wr`)
- **What**: `legacy_wr` switches from `budget/4/NUM_SHARDS` to a fixed `1G/NUM_SHARDS` floor at the 16 GiB budget boundary.
- **Reason**: Pre-existing tuning discontinuity, not a correctness issue. The low side of the cliff is the conservative side. Smoothing it changes sizing for every unconstrained host, which is a tuning campaign.

### [LOW] engine: budget decisions are not logged at startup
- **Where**: `core/src/common/engine/mmdb.rs` (`MEM_BUDGET`)
- **What**: Operators cannot see which constraint bound the budget or the resulting per-shard sizes.
- **Reason**: `vsdb_core` has no logging facade and is a library — unconditional `eprintln!` from a storage engine at first use is worse than silence. Revisit if a logging facade is ever adopted workspace-wide.
