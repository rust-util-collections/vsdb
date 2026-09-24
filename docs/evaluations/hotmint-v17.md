# Hotmint integration assessment of unpublished VSDB v17

Date: 2026-09-24. Scope: migrate the local Hotmint workspace, exercise real
storage and UTXO code, and fix demonstrated VSDB problems before v17 is
published. Production data was not used.

## Result

VSDB v17 fits Hotmint's block, consensus-state and evidence stores with a
small API migration. It also preserves Hotmint's UTXO commitments and proofs.
The exercise found two inherited VSDB problems and fixed them:

1. A cached Merkle root could be returned for a commit already reclaimed by
   rollback. Both MPT and SMT bypassed the store's missing-commit error.
2. Each committed-state root calculation rewrote the entire disposable trie
   cache. This hid full-state work inside an incremental operation and made
   root calculation dominate the tested single-spend block workload.

Neither issue is about downgrading a database or rolling back application
binaries. Both reproduce on v16.3.9 as well as the pre-fix v17 code.

The second fix deliberately changes cache policy in unpublished v17:
`VerMapWithProof::save_cache(commit)` is now explicit. Root computation and
handle destruction do not save caches. Existing cache files still load;
missing/stale caches rebuild or catch up from the authoritative versioned map.
No collection data-layout migration is required.

## Baselines and migration effort

- Hotmint: `132f7859ba7fc12e637e16dd5fb5c048790044fa`, VSDB `16.3.9`.
- First requested VSDB push: `6ae24dc` (`17.0.2`), including qualified typed
  handle identity (`a0da8bf`) and fallible read-only constructors (`162e464`).
- Hotmint-driven fixes: `41a71f6` (reclaimed commit validation) and `07c4cb8`
  (explicit cache checkpoints), included in `17.0.3`.
- Both Hotmint dependency graphs use MMDB `4.3.2`; the standalone VSDB
  workspace checks use its existing MMDB `4.3.1` lock resolution.
- Linux x86_64, 8 logical CPUs, about 7.7 GiB RAM; rustc `1.97.0`
  (`2d8144b78`), Cargo `1.97.0`. Build jobs: 2, incremental compilation off,
  dev debug info off. Artifacts and datasets use task-specific `/tmp` paths.

The migration changes four initialization calls, one SlotDex constructor,
one SlotDex removal and two SlotDex page calls, plus the workspace dependency
and associated documentation. Existing `BranchId` annotations continue to
work with the v17 newtype; no manual integer conversions are needed.
`Namespace::create()` needs no change. Block/state/evidence sidecar formats
and their 64-bit map IDs remain unchanged.

The fixed-size UTXO index constructor and its writable mutation use explicit
`expect` messages because the example's public methods remain infallible.
Dynamic configuration or read-only application flows should propagate VSDB's
`Result` instead. This does not require suppressing compiler warnings.

The Hotmint adaptation and three new integration test files are preserved in
[hotmint.patch](hotmint-v17/hotmint.patch). They are also present, uncommitted,
in the sibling Hotmint worktree. Only VSDB is committed and pushed by this task.

## Correctness and persistence evidence

- Real `VsdbBlockStore`, `PersistentConsensusState`, and
  `PersistentEvidenceStore`: seed 128 blocks, check blocks by hash/height,
  commit QCs, transaction locations, block results, consensus fields and
  evidence; append block 129, prune/add evidence, restart and verify again.
  Child processes explicitly flush and then exit without Rust destructors.
- Cross-version run: the v16 binary writes the actual old metadata; the v17
  binary reads, appends, exits and reopens it. A read-only v17 open preserves
  all 85 files' bytes, sizes and modification times.
- Versioned UTXO restart probe: 64 real Hotmint `TxOutput` values under
  `OutPoint` keys, written by v16 and reopened/updated by v17. Root hashes
  agree with an independent SMT rebuild, and all 65 queried inclusion or
  exclusion proofs verify before and after the spend.
- Real `UtxoState` model test: 128 genesis UTXOs, 64 spends across eight blocks,
  four address groups, seven-entry pages. Dirty and committed roots, balances,
  total supply, page contents and proofs agree with an independent in-memory
  model and fresh SMT construction.
- Existing Hotmint integration tests exercise Rust/Go ABCI, four-node P2P
  consensus, one silent validator, node failure and validator-set changes.

These are process-restart and logical-consistency tests, not an exhaustive
power-loss/fault-injection campaign. The UTXO example itself has no application
reopen/checkpoint API: the restart probe exercises its real key/value types
and VSDB versioned collections, not a nonexistent `UtxoState::open` method.

## Fix details

### Reclaimed historical roots

Reproduction: commit A, commit B, compute B's root, roll the branch back to A,
then request B's root again. `VerMap::at(B)` rejects the deleted commit, but
the pre-fix proof wrapper returned B's cached root successfully. The same
problem survives reopening from a valid cache file stamped with B's ID.

`sync_to_commit` now validates the commit before any cache shortcut. The
[regression](../../strata/tests/trie_reclaimed_commit_test.rs) covers both MPT
and SMT, including a surviving disk cache and recovery to the valid branch.
A cache is not a retained `Snapshot`; it must not revive deleted history.

### Full cache writes on root calculation

Before the change, `sync_to_commit` serialized and saved the full trie after
every synchronization and swallowed I/O errors; `Drop` could retry. This
coupled the latency of a root query to database size and filesystem behavior.

The wrapper now offers `save_cache(commit) -> Result<()>`. Applications choose
checkpoint frequency and handle errors. The call synchronizes proofs to the
selected committed state, excluding dirty changes; call `merkle_root(branch)`
again before proving that branch's working state. No map mutation or durability
fence was removed. Checkpoint serialization still costs full-state work.
Applications that skip checkpoints accept a larger restart rebuild cost.

[Lifecycle tests](../../strata/tests/trie_cache_lifecycle_test.rs) check that
root calculation and Drop leave an old checkpoint untouched, stale checkpoints
catch up, dirty data remains authoritative, and save failures are observable
without a hidden destructor retry. Read-only and namespace-replacement tests
cover the new explicit write boundary too.

## Performance measurements

The benchmark calls the real Hotmint `UtxoState`, not the repository's
`bench-utxo` program (which currently runs a NoopApplication cluster). Each
sample spends one existing output, inserts one replacement, commits, computes
a root and verifies a proof. Sizes are 128, 2,048 and 8,192 live UTXOs; each
fresh database supplies 20 measured blocks after initialization.

| Live UTXOs | v16 root (ms) | v17 before cache fix (ms) | Optimized v17 root (ms) | v16 block storage path (ms) | Optimized v17 block storage path (ms) |
|---:|---:|---:|---:|---:|---:|
| 128 | 0.377 | 0.294 | 0.045 | 10.496 | 1.311 |
| 2,048 | 4.380 | 3.001 | 0.073 | 14.748 | 1.544 |
| 8,192 | 18.379 | 14.161 | 0.088 | 28.923 | 1.513 |


For v16 and optimized v17, the values are the median of three per-run medians,
with 20 samples per run and alternating version order. These six runs used
fresh stores after this task's builds and cluster tests had finished. The
pre-fix v17 column is one initial 20-sample run per size, so its comparison is
indicative rather than a controlled multi-round benchmark.

At 8,192 UTXOs the root cost changes from about 14.16 ms before the cache fix
to 0.088 ms afterward. The entire tested storage path falls from 28.92 ms on
v16 to 1.51 ms on optimized v17; this broader difference also includes
pre-existing v17 mutation/commit improvements and cannot all be attributed
to the cache change. These are measurements on one host, not performance
promises for other datasets or hardware.

This is storage-path latency, not end-to-end chain TPS. It excludes transaction
signature validation, network traffic and consensus scheduling. Explicit cache
checkpoints still need a separate latency budget; saving on every block would
restore the full serialization cost.

## Validation

| Workspace | Debug tests | Release tests | Clippy, all targets, warnings denied |
|---|---:|---:|---|
| Hotmint v16 baseline | 201 passed | — | — |
| VSDB final behavior | 673 passed | 673 passed | Passed |
| Hotmint with final behavior | 204 passed | 204 passed | Passed |

The full gates ran on the final Rust code (`07c4cb8`) before the manifest-only
bump from 17.0.2 to 17.0.3. After that bump, both package versions and all five
Hotmint VSDB dependency paths were verified; the VSDB regressions, Hotmint
storage/UTXO suites and VSDB all-target Clippy were rerun successfully.

All completed checks pass. The five VSDB ignored tests are subprocess helpers
invoked by parent tests. Hotmint's one ignored test is the performance probe,
which was run explicitly. VSDB doctests also pass (17 executed; six examples
are marked ignored). Strict rustdoc and formatting checks pass.

Raw sample arrays, test summaries, command descriptions, log checksums and
cross-version outcomes are in [results.json](hotmint-v17/results.json).
Runtime logs remain in `/tmp/hotmint-vsdb17-eval` on the evaluation host.

## Reproducing the core checks

Apply `hotmint-v17/hotmint.patch` to the Hotmint baseline above, with VSDB as
its sibling directory. Set a fresh `VSDB_BASE_DIR` and a scratch
`CARGO_TARGET_DIR`; raise the file descriptor limit to 10240. Then:

```sh
cargo test --workspace --tests
cargo test --workspace --release --tests
cargo clippy --workspace --all-targets -- -D warnings
cargo test --release -p utxo-chain-example --test vsdb_workload \
  utxo_block_cost -- --ignored --nocapture
```

For a v16 workload comparison, copy only `vsdb_workload.rs` and
`vsdb_history.rs` into an untouched Hotmint baseline; both files compile
unchanged against v16.3.9. Pin both `vsdb` and `vsdb_core` to `16.3.9`,
and MMDB to `4.3.2`, for the dependency versions used in this comparison.
For the storage process probe, replace its options
initialization with `vsdb_set_base_dir(dir.join("db"))` and omit the v17-only
read-only phase. Select cross-version phases through `HOTMINT_VSDB_PHASE` and
`HOTMINT_VSDB_DIR`; the history probe also takes `VSDB_BASE_DIR=<dir>/db`.
Build each version separately and retain its test executable before rebuilding.

The new VSDB regressions run with:

```sh
cargo test -p vsdb --test trie_reclaimed_commit_test \
  --test trie_cache_lifecycle_test --test read_only_test
```

## Applicability limits

The tested persistence and proof APIs are suitable for Hotmint. The explicit
cache checkpoint is a better fit for block processing because it separates
state commitment from restart optimization without introducing background
threads or implicit throttling policy.

This exercise does not certify the UTXO example as a production chain. Besides
its missing application reopen path, it already discards several write/root
errors and computes address balance from only the first `u16::MAX` page.
Those are consumer-level limitations, not evidence of VSDB v17 corruption;
the measured data sizes are below that page limit. They should be addressed
when turning the example into a durable full application.
