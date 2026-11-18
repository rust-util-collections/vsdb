---
name: x-overhaul
description: Review VSDB changes or the full repository for collection correctness, storage durability, and compatibility regressions, fix confirmed in-scope defects, and create atomic local commits. Use only when the user explicitly invokes $x-overhaul; empty scope selects the latest commit and all selects the full repository.
---

# VSDB Review, Fix, and Commit

Resolve repository paths from the Git root; Markdown links are relative to
this file. Read invocation arguments from the user prompt.

Scoped review → fix confirmed in-scope defects → validate → atomic local commits.
Never automatically create a branch, push, open a PR, publish, deploy, or
rewrite history. No autonomous major. User-invoked only. New commits only.

Apply [pragmatic-engineering.md](../../../docs/ai/pragmatic-engineering.md) §Maintenance scope:
ground every finding and fix in a named VSDB collection, storage/recovery
protocol, concurrency/`unsafe` contract, or public/on-disk compatibility rule.

## Input

Accept at most one scope from the user prompt — same scope tokens and revision
verification rules as [x-review §Input](../x-review/SKILL.md#input), **without**
`--fix`:

| Input | Scope | Commit & fix boundary |
|-------|-------|-----------------------|
| *(empty)* | Latest commit (`HEAD`) | Fix still-present defects rooted in `HEAD` + in-scope `Open` |
| `N` / `<hash>` / `<a>..<b>` | Committed diff (endpoint diff, never merge-base; merge `<hash>` uses `<hash>^1 <hash>`) | Fix only defects still present at `HEAD` and rooted in that diff + in-scope `Open` |
| `staged` | Index (`git diff --cached`) | Review, fix in-unit, and commit staged units + resolve in-scope `Open`; unstaged/untracked stay baseline |
| `worktree` | Staged + unstaged + intended untracked | Review, fix in-unit, and commit intended units + resolve in-scope `Open` |
| `all` | Full tracked repository (`git ls-files`) | Full-repo audit + resolve all actionable `Open` and confirmed findings |

Verify every commit revision with `git rev-parse --verify --quiet '<rev>^{commit}'`.
An all-digit token is `N`; if it also resolves as a commit, ask. Reject `--fix`,
multiple scopes, unknown options, or unresolvable revisions; never guess.
Surrounding callers, tests, and guides provide evidence, not license for
unrelated fixes or baseline commits.

## Setup

Read [workflow-policy.md](../../../docs/ai/workflow-policy.md),
[commit-protocol.md](../../../docs/ai/commit-protocol.md),
[pragmatic-engineering.md](../../../docs/ai/pragmatic-engineering.md),
[review-core.md](../../../docs/ai/review-core.md),
[technical-patterns.md](../../../docs/ai/technical-patterns.md),
[false-positive-guide.md](../../../docs/ai/false-positive-guide.md), and
[compatibility-policy.md](../../../docs/ai/compatibility-policy.md); load
[design-patterns.md](../../../docs/ai/design-patterns.md) when multi-subsystem,
design-shaped, or `all`. Also read the composed protocols in
[x-review](../x-review/SKILL.md), [x-fix](../x-fix/SKILL.md), and (for
`staged`/`worktree`) [x-commit](../x-commit/SKILL.md).

One invocation owns one baseline, `starting_HEAD`, frozen scope, ownership
ledger ([commit-protocol.md](../../../docs/ai/commit-protocol.md) §Invocation ledger),
final gate, version bump, and release tag. Do not launch nested standalone skill
workflows or repeat their finalization; standalone read-only/no-commit rules,
`x-review` Phases 5–7, `x-commit` §4, and `x-fix` §4 are replaced by the
protocol below.

## Protocol

### Phase 1 — Scope and Ledger

1. Run preflight (`workflow-policy.md` §1): stop on merge/rebase/cherry-pick or
   detached `HEAD`. Separate staged, unstaged, and untracked baseline; freeze
   owned paths and record whether a patch bump or release tag is already owed.
2. Uncommitted-scope guards:
   - `staged`: if any staged path also has unstaged working-tree hunks, stop on
     baseline overlap (`workflow-policy.md` §2) so unstaged edits are never
     swept or tested as staged.
   - `worktree`: ask if an untracked file's intent is unclear.
   - Empty `staged`/`worktree`: if a release tag is owed, resume it in Phase 4.
     Otherwise, when a bump is owed, skip to Phase 4 if `docs/audit.md` `Open` is empty,
     or report the `Open` entries blocking the bump. Otherwise report nothing
     to overhaul.
3. Map scope via `review-core.md` Subsystem Map and load mapped
   `docs/ai/patterns/*.md` guides:
   - Diff scopes: full diff + changed functions, callers, failure/recovery
     paths, and tests.
   - `all`: build a complete tracked-file ledger (`git ls-files`) across both
     crates (`core` and `strata`: `src/`, `tests/`, `benches/`, crate `docs/`
     and `README.md`), build/CI/tools (`Cargo.toml`, `.cargo/config.toml`,
     `rustfmt.toml`, `Makefile`, `tools/fmt.sh`, `.github/`), live project docs
     (`README.md`, `CHANGELOG.md`, `docs/audit.md`, implemented contracts vs
     explicitly deferred candidates in `docs/proposals/`), historical baselines
     (`docs/evaluations/**` and historical `CHANGELOG.md` notes — classify as
     point-in-time records, not live drift), and agent governance (`AGENTS.md`,
     `.agents/skills/`, `docs/ai/`). Never silently drop tracked files.

### Phase 2 — Review

1. Apply `x-review` Phases 2–4 (`review-core.md` §2–4 + `false-positive-guide.md`):
   - Small single-subsystem scope → review directly.
   - Multi-subsystem or `all` → disjoint **read-only** subsystem batches (one
     primary owner per Rust file; cover cross-module seams such as
     `persistent_btree/{diff,merge}.rs` ↔ `versioned/` and `core`
     engine/namespace ↔ `strata` handles explicitly), then cross-subsystem and
     `design-patterns.md` checks only for gaps. Run `fmt`/`check`/`clippy` via
     Cargo tools, never review subagents.
2. Verify every candidate from full implementations, callers, guards, and
   existing tests; refute via the FP guide and merge findings sharing one root
   cause. Check public API and persisted compatibility (`VSTYPE03`/`VSTYPE02`,
   meta/key/wire tags, shard/namespace layout).
3. Reconcile confirmed findings with `docs/audit.md` **in the ledger**
   (`review-core.md` §5: intersecting `Open`/`Won't Fix` for narrow scopes; all
   entries for `all`). Do not write transient `Open` entries to disk for defects
   that Phase 3 fixes in this run.

### Phase 3 — Resolve and Commit

Edits and commits run sequentially on one tree. Register newly discovered fix
paths in the ledger before editing after checking them against baseline
(`workflow-policy.md` §1.5). If a needed fix overlaps unowned baseline and
cannot be separated safely, leave the baseline untouched, keep the finding
`Open` with the blocker recorded, and continue independent safe units.

1. **Owned uncommitted work (`staged` / `worktree` only)** — run
   `x-commit` §1–3 using the frozen outer scope instead of prompt pathspecs.
   Split coherent atomic units; fix defects inside owned units **in-unit** with
   regression tests before committing. Never commit a known-defective unit.
   Preserve staged/unstaged boundaries and unowned baseline (use a disposable
   worktree per `workflow-policy.md` §4 when dirty baseline could affect
   per-unit validation).
2. **Confirmed findings and still-applicable in-scope `Open` entries** — run
   `x-fix` §1–3 in severity order (`CRITICAL` → `LOW`), one root cause per
   commit: minimal fix + focused pre-fix-failing regression test (launch every
   `cargo test` with a parent-shell scratch `VSDB_BASE_DIR`, never `$HOME/.vsdb`)
   + public/migration docs + removal of any **pre-existing** `docs/audit.md`
   entry resolved by that commit. Do not accept a public API or persisted-format
   break on the user's behalf; keep unaccepted breaks, blocked fixes, and
   unverified candidates `Open` (never relabel them `Won't Fix` to clear the
   list).
3. **Registry-only dispositions** — write net remaining `docs/audit.md` updates
   (unfixed/blocked `Open` entries, justified `Won't Fix` entries, and
   pre-existing entries proven already fixed or disproven without a code edit),
   merging without reverting unowned edits in `docs/audit.md`. When owned
   registry-only changes remain, commit them in one separate `docs` commit after
   code fixes.

### Phase 4 — Final Gate, Version, and Tag

1. Re-review the aggregate `starting_HEAD..HEAD` diff for cross-commit
   interactions, missed callers/error paths, and doc/guide alignment.
2. Update/select official `stable` Rust (`compatibility-policy.md` §Rust
   toolchain) and run the applicable final gate from `commit-protocol.md`
   §Final workspace gate once on the stable code state (parent-shell scratch
   `VSDB_BASE_DIR` for test steps; if unowned dirty Rust/manifest baseline
   exists in the live tree, validate `HEAD` in a disposable worktree per
   `workflow-policy.md` §4):
   - Full 7-step Cargo workspace gate when `scope` is `all` (even with zero
     fixes), a version bump is owed, `starting_HEAD..HEAD` touches `.rs`,
     `Cargo.toml`, `.cargo/config.toml`, or `rustfmt.toml`, or the reviewed
     scope includes unreleased manifest/toolchain changes.
   - `git diff --check` plus link/anchor and Markdown/YAML checks when this
     invocation committed only documentation/registry/governance edits and none
     of the full-gate conditions above apply.
   - Reuse checks already passed on the exact unchanged code/manifest state.
3. If aggregate review or a gate check exposes a regression, land a **new**
   focused fix commit (never amend), re-run affected checks, and stop on a
   repeated failure with no progress or baseline overlap.
4. Apply `commit-protocol.md` §Lockstep version and tag once. Bump after the
   required checks pass with no in-scope `Open` findings or coverage gaps.
   Never choose a major bump autonomously. If blocked, retain validated local
   commits and report any owed bump or tag for the next run. Skip empty commits.

## Output

Report scope and `starting_HEAD`, coverage ledger and any gaps, finding
dispositions (fixed, pruned as fixed/disproven, `Won't Fix`, remaining `Open`
with blockers), validation checks run (toolchain + scratch `VSDB_BASE_DIR` /
worktree isolation), public/on-disk compatibility, local commit hashes and
subjects, version, release tag, and untouched baseline.
