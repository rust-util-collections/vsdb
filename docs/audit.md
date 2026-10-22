# Audit Findings

> Auto-managed by /x-review, /x-fix, /x-commit, and /x-overhaul.
> Registry edits are not code writes.
>
> **Won't Fix is not permanent.** Re-evaluate an entry when a review touches
> its code, callers, assumptions, or subsystem; a full audit checks every entry.
>
> Disproven findings are removed rather than kept as backlog.
> Resolved history belongs in Git and CHANGELOG.

## Open

### [MEDIUM] vecdex: non-entry top-layer removal triggers a full-graph rescan
- **Where**: `strata/src/vecdex/mod.rs` (`stage_remove`, `repair_entry_point`)
- **What**: `stage_remove` calls `repair_entry_point` whenever the removed node's layer equals `state.max_layer`, even when the actual entry point survives. Repair scans every `TAG_INFO` row and vector, making a top-layer delete O(N) (and every delete O(N) when `max_layer == 0`).
- **Why**: The condition tests the removed node's layer, not whether the entry point was lost or is unhealthy.
- **Suggested fix**: Repair only when the entry point is removed, missing, has a mismatched layer, or has no live base edge. Regression test with a test-only repair counter, plus entry-point-removal coverage.

### [MEDIUM] trie: dirty-state synchronization deep-clones the whole trie
- **Where**: `strata/src/trie/proof.rs` (`sync_to_branch`, `sync_to_commit`); deep clone in `strata/src/trie/node/mod.rs`
- **What**: Every dirty `merkle_root`/`prove_at` clones the full head trie to restore and re-snapshot the dirty overlay, even when no map write intervened. Repeated proofs on an unchanged dirty branch cost O(total state) in allocations and memory.
- **Why**: The dirty fast path only recognizes a clean HEAD; there is no applied-diff identity or reverse-apply.
- **Suggested fix**: Store the applied dirty diff and reverse it in O(diff); short-circuit when the branch dirty root/HEAD is unchanged; on reverse/apply failure poison the sync state and full-rebuild. Regression test with a clone-counting trie backend.

### [HIGH] build: `make test`/`make bench` recursively delete user VSDB data
- **Where**: `Makefile` (`test`, `bench` recipes)
- **What**: Every cargo invocation is preceded by `rm -rf ~/.vsdb /tmp/.vsdb /tmp/vsdb_testing $(VSDB_BASE_DIR)`. `$HOME/.vsdb` is the library default universe, and `VSDB_BASE_DIR` is a user-facing database path; the unquoted expansion can also split into unrelated arguments.
- **Why**: The recipes have no ownership check; CI and humans run them directly.
- **Suggested fix**: Use a per-invocation `mktemp -d` scratch `VSDB_BASE_DIR` with an EXIT trap; delete only that path. Do not delete `$HOME/.vsdb`, `/tmp/vsdb_testing`, or caller-provided paths. Update the workflow docs and add a dry-run regression check.

### [MEDIUM] tests: integration binaries without configuration write the user default universe
- **Where**: `core/tests/{iterator_snapshot_test,read_view_test,sync_batch_test}.rs`; `strata/tests/{proof_context_test,slotdex_iter_test,trie_cache_lifecycle_test,trie_reclaimed_commit_test}.rs`
- **What**: These binaries touch the default namespace or create path-derived namespaces without calling `vsdb_configure`, so bare `cargo test` inherits `$HOME/.vsdb` or `VSDB_BASE_DIR`, writes user data, and can contend on engine locks.
- **Why**: The per-binary one-shot configuration convention is not applied.
- **Suggested fix**: Add a `parking_lot::Once` setup in each affected binary using a unique temp base dir before any engine access; add a sentinel-directory regression check.

### [LOW] engine: abandoned-window recovered-prefix reservations are never evicted
- **Where**: `core/src/common/engine/mmdb.rs` (`PendingWindowGuard::drop`, `reserve_recovered_prefix`)
- **What**: A reservation for a prefix in a thread's still-open window survives that thread's exit. The prefix can never be generated again, so the entry and `RECOVERED_NONEMPTY` persist for the process lifetime, forcing every allocation through the locked path.
- **Why**: The guard removes only the window entry; it does not purge reservations in `[cursor, end)`.
- **Suggested fix**: When a window is removed, purge reservations in its un-issued range under the existing lock order and clear `RECOVERED_NONEMPTY` when empty. Regression test with a spawned thread, a reservation, and thread exit.

### [LOW] docs: release gate omits the doctest step used by the Makefile and CI
- **Where**: `.claude/docs/commit-protocol.md` (Final workspace gate); `Makefile` (`test`); `.github/workflows/rust.yml`
- **What**: The prescribed gate runs test and release-test targets but not `cargo test --workspace --doc`, so a doctest/compile-fail regression can reach CI despite a “full gate” release.
- **Why**: The gate list is not aligned with the Makefile/CI test recipe.
- **Suggested fix**: Add `cargo test --workspace --doc` to the final gate and keep the lists aligned.

## Won't Fix

### [MEDIUM] cached indexes: independently restored handles do not share runtime caches
- **Where**: `strata/src/slotdex/mod.rs`, `strata/src/vecdex/mod.rs`, `strata/src/vecdex/dynamic.rs`
- **What**: serde/from_meta restores the same backing store with separate cached totals, tiers or graph state. Alternating writes through those handles can overwrite counters or node IDs; independent readers can observe stale cached state after another handle mutates.
- **Reason**: sharing or invalidating all generic cached state requires a runtime ownership/cache redesign, or reloading index state on ordinary operations with material cost. Recovery is intended to replace the active handle. Public struct and recovery docs now require all access during mutation to use one live instance (shared behind a lock when needed); multiple independent immutable readers remain supported. This documents the current limitation rather than claiming alias coherence is fixed.

---

### [LOW] engine: lazy-delete auto-sweep threshold stays disabled
- **Where**: `core/src/common/engine/mmdb.rs` (`mmdb_open`), mmdb `DbOptions::lazy_delete_compaction_threshold`
- **What**: keys registered via `lazy_delete` are physically dropped only when organic compaction rewrites their level; registrations resting in cold levels can outlive the process (registrations are memory-only and documented best-effort).
- **Reason**: mmdb documents the auto-sweep as holding the DB mutex for each level's full rewrite, stalling readers and writers; administrative `compact()` additionally holds the write queue for its full call. Enabling it by default trades unbounded stalls for space reclaim. Correctness never depends on physical removal (dead B+ tree nodes are unreachable by id), and administrative `compact()` remains available.

---

### [MEDIUM] dagmap: serde decomposition can expose the private parent slot
- **Where**: `strata/src/dagmap/raw/mod.rs`, `strata/src/basic/orphan/mod.rs`
- **What**: callers can deserialize `DagMapRaw`'s public tuple representation into its public component types, retain an alias to the private parent `Orphan`, and later create a parent cycle through safe APIs. The same deliberate decomposition can also plant a foreign registry entry that `owned_or_residue`'s `None`-parent residue arm treats as reclaimable, so a later destroy/prune walk can wipe a live root (`parent == None`) — value loss under representation abuse, not hang/UB.
- **Reason**: stopping deliberate decomposition requires a handle format that old bytes cannot decode into the public component types. A tombstone that treats `None` as residue only when present would stop the planted-root wipe, but a crash on the current build can already have `parent == None` with data still intact and no tombstone; requiring the tombstone would skip that residue and, if the registry entry is then dropped, make it unreachable. That recovery regression is not an acceptable silent patch. Casual misuse still cannot hang or cause memory unsafety. Keep the debt until a DagMap format redesign.

---

### [LOW] engine: 16 GiB write-buffer threshold is a sizing cliff
- **Where**: `core/src/common/engine/mmdb.rs` (`mmdb_open`, `legacy_wr`)
- **What**: `legacy_wr` uses `1 GiB / shards` up to and including a 16 GiB budget, then switches to `budget / 4 / shards`; the later `budget / 8 / shards` clamp makes a default 16-shard active buffer jump from 64 MiB to approximately 128 MiB just above the boundary.
- **Reason**: This is a pre-existing tuning discontinuity, not a correctness issue; the low side is conservative. Smoothing it changes sizing for every unconstrained host and requires a dedicated tuning campaign.

---

### [LOW] engine: budget decisions are not logged at startup
- **Where**: `core/src/common/engine/mmdb.rs` (`MEM_BUDGET`)
- **What**: Operators cannot see whether the configured option, environment override or fixed default selected the budget, or the resulting per-shard sizes.
- **Reason**: `vsdb_core` has no logging facade and is a library; unconditional stderr output from a storage engine is worse than silence. Revisit if a workspace-wide logging facade is adopted.

---

### [LOW] trie: cache decoders materialize a crafted payload before structural validation
- **Where**: `strata/src/trie/cache.rs` (`deserialize_handle`), `strata/src/trie/smt/cache.rs` (`deserialize_node`)
- **What**: A checksum-valid but malformed cache file just under `MAX_CACHE_FILE_BYTES` expands into an in-memory node graph before canonicality checks run; a branch-heavy payload can allocate orders of magnitude more than its encoded size.
- **Reason**: Cache files are local, disposable artifacts behind a checksum and size cap; loading is not a remote input path, and a damaged cache safely falls back to rebuilding from the authoritative map. Adding a decoded-node/memory budget needs a new tuning constant and can reject legitimate large caches; track it with a future cache-format hardening change rather than a patch-release heuristic.

---

### [LOW] benches: fixtures and persisted state are not isolated from measured work
- **Where**: `core/benches/units/batch_write.rs`, `strata/benches/versioned.rs`, `strata/benches/units/concurrent.rs`
- **What**: Several timed closures build fixtures (fresh maps, prepopulation, rollback/discard state) or reuse a map that grows across thread-count runs; `gc_bench` measures an already-cleaned graph after warmup. Fresh collections are also never wiped, so storage grows during a run.
- **Reason**: These affect benchmark interpretation and disk usage, not library correctness or durability. Fixing them is a benchmark-tooling overhaul (Criterion `iter_batched` fixtures, per-sample state reset, and bench-owned directory cleanup) that should be validated separately from this correctness pass; the numbers remain comparable within a single run only with that caveat.
