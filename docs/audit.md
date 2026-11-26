# Audit Findings

> Auto-managed by $x-review, $x-fix, $x-commit, and $x-overhaul.
> Registry edits are not code writes.
>
> **Won't Fix is not permanent.** Re-evaluate an entry when a review touches
> its code, callers, assumptions, or subsystem; a full audit checks every entry.
>
> Disproven findings are removed rather than kept as backlog.
> Resolved history belongs in Git and CHANGELOG.

## Open

### [LOW] configuration: vsdb_configure cannot be retried after initialization fails
- **Where**: `core/src/common/mod.rs` (`vsdb_configure`)
- **What**: The selection is frozen and stored before `universe::initialize` runs. If initialization fails, for example with `Io(WouldBlock)` from another writer, a second `vsdb_configure` returns `BaseDirFrozen`. Other APIs meanwhile retry initialization with the frozen selection.
- **Why**: The `# Errors` section mentions only `BaseDirFrozen`. A retry loop around `vsdb_configure` therefore never succeeds, and nothing tells the caller that the selection remains in effect. Reproduced in the same fixture as the getter finding.
- **Suggested fix**: Document the initialization errors and that later API calls retry with the frozen selection. Alternatively, let an identical selection retry while no universe is open. Regression test: release the lock and retry.

---

### [LOW] engine: flush panics on shard errors without documenting it
- **Where**: `core/src/common/engine/mmdb.rs` (`MmDB::flush`), `core/src/common/namespace.rs` (`Namespace::flush`, `flush_all_open`), `core/src/common/mod.rs` (`vsdb_flush`)
- **What**: `MmDB::flush` calls `db.flush().expect(..)`. mmdb returns an error when a fail-stop error has latched, when a shard is closed, or when flush I/O fails.
- **Why**: After such an error, `Namespace::flush` and `vsdb_flush` panic. `flush_all_open` then skips the remaining namespaces, and a flush in a destructor during unwinding aborts the process. Neither public API documents a panic or offers a fallible form, while AGENTS.md requires infallible APIs to document their panics. Acknowledged writes remain WAL-durable.
- **Suggested fix**: Add `# Panics` sections, and optionally a `try_flush` that returns the first shard error. Regression test: flushing a closed shard surfaces the error.

---

### [LOW] VecDex tests: schema rejection guards have no effective test
- **Where**: `strata/src/vecdex/test.rs` (`dyn_wire_tags_are_frozen`, `invalid_configs_are_rejected`); the guards are in `strata/src/vecdex/mod.rs` (`validate_store`)
- **What**:
  - The final assertion of `dyn_wire_tags_are_frozen` decodes `[9]` as a handle. That fails as a truncated postcard byte string before any schema row is read.
  - `invalid_configs_are_rejected` fails on the schema binding before it reaches `config.validate()`.
  - No test writes a binding-valid schema with an unknown metric tag, trailing bytes or an invalid stored configuration.
- **Why**: A regression that decoded unknown metric tags, accepted trailing schema bytes or skipped stored-config validation would still pass the suite, although the VD6 compatibility rules rely on those rejections.
- **Suggested fix**: Rewrite the three schema cases to go through `validate_store` with a recomputed binding, and drop the `[9]` assertion. Test-only change.

---

### [LOW] dagmap tests: two tests do not exercise their stated cases
- **Where**: `strata/src/dagmap/raw/test.rs` (`prune_works_in_a_non_default_namespace`, `fallible_dagmap_operations_propagate_corrupt_rows_and_sst_errors`)
- **What**: The first test prunes `head`, which is a root, so `prune` only fences and every assertion would hold without any prune. The second injects malformed rows but no SST or storage error.
- **Why**: Prune publication in a non-default namespace and storage-error propagation lack the coverage the test names and comments claim.
- **Suggested fix**: In the first test, prune `child` and assert on the merged root and the consumed child. Add an SST-corruption case to the second test, or rename it. Test-only change.

---

### [LOW] versioning docs: Commit::parents misdescribes a branch's first commit
- **Where**: `strata/src/versioned/mod.rs` (`Commit::parents`)
- **What**: The rustdoc says the parent list is empty for a branch's first commit. `create_branch` copies the source head, though, so the first commit on a fork names that head as its parent. Only a commit on a branch with no head has no parents.
- **Why**: Callers that use `parents().is_empty()` to find where a branch starts get the wrong commit.
- **Suggested fix**: Describe an empty list as a commit created on a branch that has no head.

---

### [LOW] docs: live docs name the engine release as MMDB v5
- **Where**: `docs/storage.md`, `docs/v18.md`, `core/README.md`; labels in `AGENTS.md` and `docs/ai/review-core.md`
- **What**: These docs say the runtime uses MMDB v5, but `Cargo.toml` pins `mmdb = "6.0.1"` (MANIFEST format 5). `docs/v18.md` links to the manifest that contradicts it. mmdb now versions its release line separately from its on-disk format.
- **Why**: A reader following the v5 release line pins the wrong engine crate next to VSDB, while the re-exported memory-usage types and the boxed `mmdb::Error` source come from mmdb 6.
- **Suggested fix**: Write "MMDB 6 (format 5)" where the release is meant and "format 5" where the on-disk format is meant. Note the dependency change in the next release's CHANGELOG.

---

### [LOW] benches: two bench programs cannot run as documented
- **Where**: `core/benches/cache_pool.rs` (A/B protocol in the module doc), `strata/benches/thread_overhead_demo.rs`, `strata/benches/thread_spawn_test.rs`, `strata/Cargo.toml`
- **What**:
  - Step 1 of the cache-pool A/B protocol passes a value cast `as usize` to `with_block_cache_capacity`, which takes a `u64`. It also leaves the `pool` parameter of `mmdb_open` unused, which `deny(warnings)` rejects.
  - The two thread demos define `fn main()` but have no `[[bench]]` entry with `harness = false`, so Cargo runs them under libtest and their `main` never executes.
- **Why**: The documented private-split baseline doesn't build. `cargo bench --bench thread_overhead_demo` runs nothing, yet both targets still compile under `make lint`.
- **Suggested fix**: In the control-build instructions, drop the cast and keep `pool` referenced. Declare the demos with `harness = false`, move them to `examples/`, or delete them.

---

### [LOW] review guides: the SlotDex checklist names a removed helper
- **Where**: `docs/ai/patterns/slotdex.md` (checklist)
- **What**: The checklist asks for a pure `floor_base_of`, which cc2f41c removed. The current functions are `SlotType::tier_stride` and `floor_align`, applied through `aligned`.
- **Why**: review-core §4 requires the guides to stay aligned with the code.
- **Suggested fix**: Name `SlotType::tier_stride` and `floor_align` instead.

## Won't Fix

### [MEDIUM] dagmap: an unmarked fold left by earlier releases without children is undetectable
- **Where**: `strata/src/dagmap/raw/mod.rs` (`prune`, `inspect_below_head`)
- **What**: Releases before the single-batch prune publication could stop after folding a line into the root and before writing any clearing marker. If the head had no live children to reparent, nothing on disk records that fold. A later prune entering at an ancestor treats the line as unfolded and folds that ancestor's rows over the root, so keys the head had overridden revert to the ancestor's values.
- **Reason**: The state leaves no evidence to detect it, and new prunes publish their markers together with the fold, so they cannot create it. Retrying the original head, the documented recovery, folds the full line again. Detecting old cases would need a migration that compares every root with its lines, which is disproportionate for a crash window in earlier releases.

---

### [MEDIUM] platform support: memory-sizing constants overflow on 32-bit targets

- **Where**: `core/src/common/engine/mmdb.rs` (`effective_mem_budget_semantics`) and both `benches/units/legacy_budget.rs` helpers in `core` and `strata`.
- **What**: The budget test expects `64 * G` bytes in a `usize`. Both benchmark helpers use `1usize << 60` when interpreting a cgroup limit. These expressions overflow on 32-bit targets.
- **Reason**: Both expressions fit on the supported `x86_64` and `aarch64` targets. See the [platform policy](ai/compatibility-policy.md#supported-architectures).

---

### [LOW] engine: lazy-delete auto-sweep threshold stays disabled
- **Where**: `core/src/common/engine/mmdb.rs` (`mmdb_open`), mmdb `DbOptions::lazy_delete_compaction_threshold`
- **What**: The default threshold is zero. `Namespace::flush()` flushes memtables and can drain a full L0, but VSDB never requests a dead-key sweep. Registered keys are reclaimed during organic level compaction while the process remains open; a restart loses registrations for rows that compaction has yet to rewrite.
- **Reason**: Enabling automatic sweeps rewrites populated levels and consumes I/O even on a store with no organic compaction debt. MMDB releases the DB mutex while building SSTs. Unreachable B+ tree pages do not affect reads and are dropped as subsequent writes trigger normal compaction. [MMDB 6.0.1 sweep semantics](https://github.com/rust-util-collections/mmdb/blob/80b635a8926ec75fcfb1c9e734940688de30ad2a/src/db.rs#L2812-L2826)

---

### [LOW] engine: budget decisions are not logged at startup
- **Where**: `core/src/common/engine/mmdb.rs` (`MEM_BUDGET`)
- **What**: Startup does not report which configuration source selected the budget. Public callers can inspect the derived cache capacities and per-shard write-buffer sizes via `Namespace::memory_usage()`.
- **Reason**: `vsdb_core` has no logging facade and is a library; unconditional stderr output from a storage engine is worse than silence. Revisit if a workspace-wide logging facade is adopted.

---

### [LOW] benches: fixtures and persisted state are not isolated from measured work
- **Where**: `core/benches/units/batch_write.rs`, `strata/benches/versioned.rs`, `strata/benches/units/concurrent.rs`, `strata/benches/vecdex.rs`
- **What**: Several timed closures build fixtures (fresh maps, prepopulation, rollback/discard state) or reuse a map that grows across thread-count runs; `gc_bench` measures an already-cleaned graph after warmup. The VecDex insert benches time `clear()`, and `bench_insert` also times vector generation. Fresh collections are also never wiped, so storage grows during a run.
- **Reason**: These affect benchmark interpretation and disk usage, not library correctness or durability. Fixing them is a benchmark-tooling overhaul (Criterion `iter_batched` fixtures, per-sample state reset, and bench-owned directory cleanup) that should be validated separately from this correctness pass; the numbers remain comparable within a single run only with that caveat.
