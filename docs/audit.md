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
  equal path. Pinned by `shard_layout_completion_aware`. (Note: the original
  finding's "cannot be destroyed" clause was wrong — destroy never opens the
  engine — but the unreopenable-and-uncompletable half remains valid.)

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
  free). A failed rollback write can still leave a visible registry entry;
  the partial-shard crash/idempotence window is tracked separately under
  `## Open`. Covered by `namespace_lifecycle` (failed-create rollback
  sub-scenario).

### [LOW] engine: missing `// SAFETY:` comment on inner unsafe block in `from_prefix_slice`
- **Where**: `core/src/common/engine/mod.rs` (`from_prefix_slice`)
- **Resolution**: Added the per-block `// SAFETY:` comment (forwards the fn's
  contract verbatim), matching the repo convention that every `unsafe {}`
  block carries its own justification.

---

## Won't Fix

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
