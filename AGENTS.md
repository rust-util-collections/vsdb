# Working on VSDB

## Repository language

Write all human-readable text in this repository in English. This includes
documentation, source comments, rustdoc, examples, test and benchmark
descriptions, diagnostics, user-facing messages, and all agent and skill
instructions.

## Workspace layout

| Crate | Responsibility |
| --- | --- |
| [vsdb](strata/README.md) | Typed maps and values, persistent B+ trees, VerMap, Merkle tries, SlotDex, DagMap and VecDex |
| [vsdb_core](core/README.md) | MMDB 6 (format 5) integration, namespace memory, raw maps, prefix batches and co-located publication |

Start at the [documentation index](docs/README.md) for public guides, design
records and historical measurements. For a code change, the [subsystem
map](docs/ai/review-core.md#subsystem-map) links each part of the workspace to
its implementation and review guide. MMDB's bundled Zstd backend requires a C
toolchain.

## Supported architectures

The project guarantees correct behavior on 64-bit x86 (`x86_64`) and 64-bit
Arm (`aarch64`). Limit platform-specific review and validation requirements to
these architectures. Other targets carry no testing or correctness guarantee.
[Platform policy](docs/ai/compatibility-policy.md#supported-architectures)

## Build & Test

```bash
make all          # fmt + lint + test
make test         # cargo test --workspace (release + debug + doc tests, parallel)
make lint         # cargo clippy --workspace + check tests/benches
make bench        # criterion benches (core: basic + cache_pool; strata: basic, versioned, slotdex, slotdex_batch, dagmap, trie_bench, vecdex)
```

Tests run in parallel. Use globally unique prefixes for collection isolation and race-tolerant assertions about shared state. If tests configure a base directory, do so once per binary with `parking_lot::Once`.

`make test` / `make all` / `make bench` are manual/CI convenience targets. They
keep the database and temporary files in separate directories under one
per-invocation scratch root and remove only that root; they do not touch `$HOME/.vsdb` or caller-provided data. The agent
still uses bare Cargo unless you explicitly ask for a make target. For an
isolated bare-Cargo run, set
`VSDB_BASE_DIR="$(mktemp -d /tmp/vsdb-test.XXXXXX)"` in the parent shell when
launching Cargo; do not mutate environment variables inside parallel tests.

## Skills

Project skills live under `.agents/skills/<name>/SKILL.md` and are
explicitly invoked with `$x-review`, `$x-fix`, `$x-commit` or `$x-overhaul`.
Invocation arguments are plain prompt text; run the selected skill's preflight
commands explicitly. Each skill's `agents/openai.yaml` preserves explicit-only
selection with `allow_implicit_invocation: false`.

| Skill | Input | Result |
| --- | --- | --- |
| [`$x-review`](.agents/skills/x-review/SKILL.md) | Latest commit by default, or a count, hash, range, `staged`, `worktree` or `all`. Optional `--fix`. | Reviews regressions and reconciles the audit registry. Only `--fix` edits code. |
| [`$x-fix`](.agents/skills/x-fix/SKILL.md) | Open findings in the [audit registry](docs/audit.md). Takes no arguments. | Verifies and resolves entries, with one local commit per root cause. |
| [`$x-commit`](.agents/skills/x-commit/SKILL.md) | Intended worktree changes, optionally restricted by Git pathspecs. | Reviews, fixes, validates and commits the intended units. Unfixed defects go to the registry. |
| [`$x-overhaul`](.agents/skills/x-overhaul/SKILL.md) | Same review scopes as `$x-review`. Takes one scope at most. | Reviews the scope, fixes confirmed defects and creates local commits. The `staged` and `worktree` scopes also commit the selected work. |

### Shared skill guidance

- Start with the [workflow policy](docs/ai/workflow-policy.md) for preflight and worktree ownership and [maintenance scope](docs/ai/workflow-policy.md#maintenance-scope).
- Use the [subsystem map](docs/ai/review-core.md#subsystem-map) to trace a change
  through the MMDB adapter and its collection callers. The
  [commit protocol](docs/ai/commit-protocol.md#per-unit-validate-and-commit)
  names focused tests for these boundaries.
- Use the [compatibility policy](docs/ai/compatibility-policy.md) for toolchain, public API and wire-format decisions. The [commit protocol](docs/ai/commit-protocol.md) covers per-unit checks and the final gate, lockstep bump, CHANGELOG and local annotated tag for commit-producing skills.

## Conventions

- **Rust toolchains** — use the official latest stable Rust for edits,
  dependency updates, CI and releases. Update/select stable before validation.
  Define no compatible Rust version range or minimum compiler; do not add
  `rust-version`, numeric toolchain pins or older-compiler compatibility shims.
  [compatibility-policy.md](docs/ai/compatibility-policy.md) §Rust toolchain is the rule's source of truth.
- All clippy warnings are errors (`#![deny(warnings)]` in lib.rs)
- **No `#[allow(...)]`** — fix warnings at the source, never suppress them
- **Prefer imports over inline paths** — avoid `std::foo::Bar::new()` inline in function bodies when the same path appears 3+ times in a file; add `use std::foo;` at file top (or `use std::foo::Bar;`) instead. Function-body `use` statements (scoped imports) are fine and don't count as inline paths. 1-2 inline uses of common `std::` items are acceptable.
- **Grouped imports** — merge common prefixes: `use std::sync::{Arc, OnceLock};`
- **Doc-code alignment** — public API changes must update corresponding docs
- `parking_lot` for every `Mutex`, `RwLock`, and `Once` in library and test code (prefix allocator, `VSDB_BASE_DIR` global, one-time test setup); do not mix in `std::sync` lock primitives
- Fallible storage APIs use `VsdbError`. Engine errors retain their source and a stable classification. Preserve documented panic behavior on infallible convenience APIs.
- Use postcard for handle and application serialization.
- Serialize writes through aliases of one composite index. SlotDex detects stale writer generations. VecDex reads graph state per operation and uses snapshots for graph queries.
- Tests run in parallel; isolation comes from globally unique prefixes and per-invocation directories for file-level scratch; global-state assertions must be race-tolerant
- Derive the live unsafe inventory during review. A raw or typed map `shadow()` requires exclusion of concurrent writes to the same key. Structural multi-key edits require the collection's documented writer exclusion. Mutable collection `Clone` performs a deep copy.
- **No Co-Authored-By in commits** — never add `Co-Authored-By:` or similar trailers to commit messages; project commits are authored only by the human contributor
