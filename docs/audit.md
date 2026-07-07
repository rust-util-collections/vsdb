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
  free). Follow-up review found that the rollback can leave newly-created root
  contents behind and that the current test no longer exercises the post-save
  rollback arm; those narrower regressions are tracked under `## Open`.

### [LOW] engine: missing `// SAFETY:` comment on inner unsafe block in `from_prefix_slice`
- **Where**: `core/src/common/engine/mod.rs` (`from_prefix_slice`)
- **Resolution**: Added the per-block `// SAFETY:` comment (forwards the fn's
  contract verbatim), matching the repo convention that every `unsafe {}`
  block carries its own justification.

---

## Won't Fix

### [REJECTED] engine: "`OnceLock::get_or_init` can run `alloc_prefix` twice under concurrent reads"
- **Where**: `core/src/common/engine/mod.rs` (`Mapx::prefix_bytes`)
- **What**: Third-party review claimed concurrent shared-handle reads could
  both enter the `OnceLock` initializer and leak a prefix id.
- **Reason**: False — `std::sync::OnceLock::get_or_init` documents that when
  many threads call it concurrently, **exactly one** initializing closure
  runs (competing callers block until it completes). No double allocation is
  possible; a leaked id would in any case be waste, not corruption.

### [REJECTED] namespace: "`DEFAULT_NS_ID` guard copy-pasted in 3 admin functions"
- **Where**: `core/src/common/namespace.rs` (`open`, `vsdb_ns_destroy`, `vsdb_ns_relocate`)
- **What**: Third-party review proposed extracting the three `id ==
  DEFAULT_NS_ID` guards into a shared helper.
- **Reason**: The three guards have deliberately different semantics — `open`
  short-circuits to `default_ns()` (success), destroy/relocate return
  distinct, context-specific errors with actionable guidance. A shared
  helper would need flags/closures to reproduce the divergence: DRY for
  DRY's sake, net readability loss for three 3-line guards.

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
