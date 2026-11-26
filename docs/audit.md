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

### [CRITICAL] dagmap: an interrupted prune leaves folded mainline nodes live
- **Where**: `strata/src/dagmap/raw/mod.rs` (`prune`, `reparent`, `mark`, `collect_marked_below_head`)
- **What**: `prune` publishes, in separate batches, the fold, each survivor's root registration and owner update, and then one `Clearing` marker per mainline node. A crash or returned error after the fold but before the first marker leaves `mid` and `head` live. A later `mid.prune()` finds no marker and treats `mid` as an unfolded live head. It destroys the survivors already reparented onto the root as side branches, then re-folds `mid`'s stale rows over the root. If only the head marker was written, `mid` still accepts `insert` and `new(Some(&mut mid))`. The documented retry, `head.prune()`, then marks and wipes `mid`, discarding the acknowledged edit and orphaning the late child.
- **Why**: Persisted data is lost, and the merged root reaches a state no complete prune produces: shared keys revert to `mid`'s values. Reproduced with the existing fold, reparent and mark helpers:
  - no marker: the survivor's rows are destroyed and the root reads `k1 == v1`;
  - head marker only: the root's `late` key and the late child's inherited `k0` both read `None`.

  1c1f2e8 handles only the case where a marker already exists, and the consumed-node guard checks only each node's own state. `prune(self)` also drops the head on `Err`, so a caller that did not save it can only continue from an ancestor.
- **Suggested fix**:
  - Publish the fold rows, the survivors' root edges and owner records, and every mainline `Clearing` marker in one co-located shard batch, so a folded node is never live. This needs an N-map, put-and-delete form of `MapxRaw::insert_colocated`.
  - For states already on disk, detect a live line whose head children are already owned by the root, and resume that prune.
  - Document that a retry needs a saved or serialized head.

  Regression tests: both sequences above. No row or state format change.

---

### [MEDIUM] versioning: history and diff operations panic on unreadable tree pages
- **Where**: `strata/src/versioned/map.rs` (`has_uncommitted`, `rollback_to`, `merge`, `raw_diff_commits`, `raw_diff_uncommitted`), `strata/src/basic/persistent_btree/diff.rs` (`differs`, `Cursor`, `materialize_val`), `strata/src/basic/persistent_btree/mod.rs` (`object`, `height`), `strata/src/basic/persistent_btree/merge.rs` (`source_changes`)
- **What**: These `Result`-returning APIs compare trees through the infallible `BTreeDiffIter`, whose page reads `expect` success. A missing, malformed or unreadable page under either compared root causes a panic. `VerMapWithProof` streams the same iterator when it updates a root.
- **Why**: Callers get a panic instead of `VsdbError::Corrupt` or the storage error. AGENTS.md requires fallible storage APIs to use `VsdbError`, and the v18.0.3 notes say VerMap's fallible operations propagate engine read errors. Reproduced by deleting a commit root page: `merge`, `has_uncommitted`, `rollback_to` and `raw_diff_commits` each panic in `PersistentBTree::object`. `build_root` restores state before re-raising, so persisted data stays intact. The rustdoc for these methods documents decode panics only.
- **Suggested fix**:
  - Add a fallible diff traversal with checked page reads, and make `differs` return `Result<bool>`.
  - Propagate those errors from `source_changes` and the raw diffs.
  - Keep the panicking iterator for standalone callers.

  Regression test: after removing a commit root page, each API returns `Corrupt` and the branch is unchanged. No format change.

---

### [MEDIUM] dagmap: get reports unreadable node metadata as an absent key
- **Where**: `strata/src/dagmap/raw/mod.rs` (`get`, `child_id`)
- **What**: `get` maps any error from its own `meta()` or from a parent's `open_with_meta` to `None` with `.ok()?`. That includes `VsdbError::Storage` and `Io` from a failed metadata block read, while edge and data reads in the same loop panic. `child_id` does the same.
- **Why**: A storage failure on an ancestor's metadata row silently hides every inherited value. Other storage failures on this infallible read path panic instead. `try_owned` already distinguishes storage errors from authentication failures.
- **Suggested fix**: Panic on `Storage` and `Io`, or add a fallible `try_get`, and return `None` only for authentication failures. Regression test: a parent metadata block with SST corruption, built as in the SlotDex SST fixtures, must not read as `Ok(None)`. No format change.

---

### [MEDIUM] namespace lifecycle: destroy fails after deleting an adopted root's contents
- **Where**: `core/src/common/namespace.rs` (`Namespace::destroy`)
- **What**: `create_with` adopts an existing empty root, and mount points are a documented case. `destroy` removes the registry entry, then calls `remove_dir_all` on the root itself. When the root can't be removed (`EBUSY` on a mount point, `EACCES` under a non-writable parent), the namespace is already unregistered and empty, yet `destroy` returns the error.
- **Why**: Every destroy of such a namespace reports failure after it has completed, and a retry returns "not registered". Reproduced with a root under a 0o555 parent: `Err(PermissionDenied)`, with the entry gone and the root empty. Failed-create cleanup keeps pre-existing roots instead. An earlier review rejected rolling the registry back on this error; this fix needs no rollback.
- **Suggested fix**: Remove the root's children, then call `remove_dir`. For an emptied root, treat `PermissionDenied` or `ResourceBusy` from that final removal as success, sync the root, and document the behavior. Regression test: the 0o555-parent scenario returns `Ok` and leaves an empty root. No format change.

---

### [MEDIUM] configuration: a failed first initialization poisons the derived-directory getters
- **Where**: `core/src/common/mod.rs` (`VSDB_SYSTEM_DIR`, `VSDB_CUSTOM_DIR`, `vsdb_get_system_dir`, `vsdb_get_custom_dir`)
- **What**: Both statics are `LazyLock`s whose initializers `expect` a successful `universe::active()`. A transient initialization error panics inside the initializer and poisons it. One example is another writer holding `__SYSTEM__/LOCK`.
- **Why**: Every later call panics for the rest of the process, even after the universe opens through other APIs; `DEFAULT_NS` and the universe itself use retryable `OnceLock`s. Reproduced: after the competing lock is released, `Namespace::list()` succeeds while `vsdb_get_custom_dir()` still panics with a poisoned `LazyLock`. Neither getter documents a panic.
- **Suggested fix**: Use `OnceLock::get_or_init`, which leaves the cell uninitialized when the initializer panics, or initialize fallibly. Add `# Panics` docs. Regression test: in a separate binary, hold the lock, observe the failure, release the lock and expect the custom directory path. No format change.

---

### [LOW] configuration: vsdb_configure cannot be retried after initialization fails
- **Where**: `core/src/common/mod.rs` (`vsdb_configure`)
- **What**: The selection is frozen and stored before `universe::initialize` runs. If initialization fails, for example with `Io(WouldBlock)` from another writer, a second `vsdb_configure` returns `BaseDirFrozen`. Other APIs meanwhile retry initialization with the frozen selection.
- **Why**: The `# Errors` section mentions only `BaseDirFrozen`. A retry loop around `vsdb_configure` therefore never succeeds, and nothing tells the caller that the selection remains in effect. Reproduced in the same fixture as the getter finding.
- **Suggested fix**: Document the initialization errors and that later API calls retry with the frozen selection. Alternatively, let an identical selection retry while no universe is open. Regression test: release the lock and retry.

---

### [LOW] tests: the non-UTF-8 path test fails on macOS
- **Where**: `core/tests/configure_test.rs` (`non_utf8_environment_paths_select_requested_directories`)
- **What**: The test runs on every Unix target and creates directories with invalid UTF-8 names. APFS and HFS+ reject those names with `EILSEQ`, so on native aarch64 macOS the child process panics in `Namespace::default_ns()` with `Io(Os { code: 92 })`.
- **Why**: `cargo test --workspace --tests` (debug and release) and `make test` fail on a supported host architecture. Without `--no-fail-fast`, the later `vsdb_core` integration binaries and the remaining `make test` steps don't run. A failed run also leaves its `vsdb_non_utf8_*` scratch directory behind.
- **Suggested fix**: Probe whether the filesystem accepts a non-UTF-8 name and skip with a message on `EILSEQ`, or restrict the test to Linux. Remove the scratch directory on every path. Verify on macOS and Linux.

---

### [LOW] tests: a second namespace test breaks the binary's single-configuration rule
- **Where**: `core/tests/namespace_test.rs` (`failed_create_on_uncreatable_explicit_root_clears_pending_record`)
- **What**: The file documents a single `#[test]` because `namespace_lifecycle` must call `vsdb_configure` first. The added test calls `Namespace::create_with` without configuring, which freezes the base at `VSDB_BASE_DIR` or `$HOME/.vsdb`. It sorts first, so with `--test-threads=1` `namespace_lifecycle` fails with `BaseDirFrozen` (reproduced); parallel runs race. Its exact-bytes check of the shared pending-create file also races the other test's creations.
- **Why**: Failures depend on test order, and a run without `VSDB_BASE_DIR` can bootstrap or write to `$HOME/.vsdb`.
- **Suggested fix**: Move the scenario into `namespace_lifecycle`, or into its own binary with a scratch configuration. Check only that no record names the failed path.

---

### [LOW] collections: keys() copies every value
- **Where**: `strata/src/basic/mapx_ord_rawkey/mod.rs` (`keys`), `strata/src/basic/mapx/mod.rs` (`keys`), `strata/src/basic/mapx_ord/mod.rs` (`keys`), `core/src/common/engine/mmdb.rs` (`MmdbIter::next`)
- **What**: The rustdoc says value bytes are skipped entirely. In fact `keys()` maps the owned `(key, value)` iterator, and `MmdbIter::next` copies each lent value with `to_vec()` before dropping it.
- **Why**: Each value costs an allocation and a copy on an iteration hot path. Reproduced: listing eight keys whose values are 256 KiB each allocated about 2.1 MB. Results are unaffected.
- **Suggested fix**: Return a key-only adapter over `try_step_next` and `try_step_prev` that copies only keys and keeps the existing panic behavior, or correct the three rustdocs. Regression test: an allocation-counting test binary.

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
