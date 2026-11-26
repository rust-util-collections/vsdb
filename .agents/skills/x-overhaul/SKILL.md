---
name: x-overhaul
description: Review VSDB changes or the full repository for collection correctness, storage durability, and compatibility regressions, fix confirmed in-scope defects, and create atomic local commits. Use only when the user explicitly invokes $x-overhaul; empty scope selects the latest commit and all selects the full repository.
---

# VSDB Review, Fix, and Commit

Resolve repository paths from the Git root; Markdown links are relative to
this file. Read invocation arguments from the user prompt.

One invocation: scoped review → fix confirmed defects → validate → atomic local
commits → owed patch release. **This file is the complete protocol.** Do not
invoke or load `x-review`, `x-fix`, or `x-commit`; shared rules live in
`docs/ai/` and are linked where they apply.

**Hard rules** ([workflow-policy.md](../../../docs/ai/workflow-policy.md)):
user-invoked only · local commits and local annotated release tags only — never
create a branch, push, open a PR, publish, deploy, amend, or rewrite history ·
one root cause per commit · never touch unowned baseline · never accept a public
API or persisted-format break or choose a major bump on the user's behalf.
Ground every finding and fix in a named VSDB collection, storage/recovery
protocol, concurrency/`unsafe` contract, or public/on-disk compatibility rule
([pragmatic-engineering.md](../../../docs/ai/pragmatic-engineering.md) §Maintenance scope).

## Input

At most one scope token. Reject `--fix`, multiple scopes, unknown options, and
unresolvable revisions; never guess.

| Input | Review evidence | Fix & commit boundary |
|-------|-----------------|-----------------------|
| *(empty)* | `git show HEAD` (merge: `git diff HEAD^1 HEAD`) | defects rooted in `HEAD` and still present |
| `N` | `git log HEAD~N..HEAD` + `git diff HEAD~N HEAD` | defects rooted in that diff and still present at `HEAD` |
| `<hash>` | `git show <hash>` (merge: `git diff <hash>^1 <hash>`) | same |
| `<a>..<b>` | `git log <a>..<b>` + `git diff <a> <b>` (endpoints, never merge-base) | same |
| `staged` | `git diff --cached` | review, fix in-unit, and commit staged units; unstaged/untracked stay baseline |
| `worktree` | `git diff HEAD` + intended `git ls-files --others --exclude-standard` | review, fix in-unit, and commit intended units |
| `all` | tracked-file ledger from `git ls-files` | every confirmed finding |

Every scope also resolves its **in-scope `Open`**: `docs/audit.md` entries whose
**Where** paths or subsystem intersect the scope (`all`: every entry).

Verify each revision (including `HEAD~N`) with
`git rev-parse --verify --quiet '<rev>^{commit}'`. An all-digit token is `N`; if
it also resolves as a commit, ask. Callers, tests, and guides outside the scope
are evidence, not license for unrelated fixes or baseline commits.

## Reading

Use the [review core](../../../docs/ai/review-core.md#1-context) for shared checks, subsystem guides and compatibility. Run the [worktree preflight](../../../docs/ai/workflow-policy.md#1-preflight) and track owned paths, validation, versions and tags in the [invocation ledger](../../../docs/ai/commit-protocol.md#invocation-ledger).

## Phase 0 — Preflight and ledger

1. Record `git status --short`, branch, and `starting_HEAD`; separate staged,
   unstaged, and untracked baseline (`workflow-policy.md` §1).
2. Determine whether a patch bump or release tag is already owed
   (`commit-protocol.md` §Lockstep version and tag).
3. Early exits:

   | Situation | Action |
   |-----------|--------|
   | merge/rebase/cherry-pick in progress, or detached `HEAD` | stop |
   | `staged`: a staged path also has unstaged hunks | stop — baseline overlap; unstaged edits must never be swept or tested as staged |
   | `worktree`: intent of an untracked file unclear | ask |
   | empty `staged`/`worktree`, release tag owed | Phase 4 step 5 (tag only) |
   | empty `staged`/`worktree`, bump owed, `docs/audit.md` `Open` empty | Phase 4 |
   | empty `staged`/`worktree`, bump owed, `Open` non-empty | report the entries blocking the bump |
   | empty `staged`/`worktree`, nothing owed | report "nothing to overhaul" |

4. Freeze owned paths and start the ledger (`commit-protocol.md` §Invocation
   ledger) plus: scope, in-scope `Open` entries, and a work queue where each
   unit/finding has one status — `pending`, `committed <hash>`,
   `open: <blocker>`, `won't fix`, or `pruned`. For `all`, or any run likely to
   outlast the context window, keep the ledger in a scratch file outside the
   worktree (`mktemp /tmp/vsdb-overhaul.XXXXXX`) and update it after every
   commit. Never write it into the repository.

## Phase 1 — Map

Assign each in-scope source or test to the operation it supports in the
[subsystem map](../../../docs/ai/review-core.md#subsystem-map). Classify
build files, skills and evidence using the full-repository coverage rules.

- **Diff scopes:** full diff + whole changed functions, callers,
  failure/recovery paths, and tests.
- **`all`:** follow the [full-repository coverage](../../../docs/ai/review-core.md#full-repository-coverage) ledger, including historical measurements and the shared skill guidance.

## Phase 2 — Review

1. **Strategy**
   - One subsystem and a small diff: review directly.
   - For a larger scope, assign read-only batches for the affected rows of the
     subsystem map. Give each Rust file one owner. Trace each changed operation
     across the relevant boundary: a VerMap edit through its leased tree root
     and publication, a DagMap child through the checked two-prefix batch,
     a VecDex mutation through staged adjacency and chunk commit, or a
     generation cutover through the v4 source, v5 shard format and restored
     collection. Run independent batches in parallel when available. Use the
     cross-subsystem design pass for remaining gaps.
   - `cargo fmt`/`check`/`clippy` run as commands by you, never inside review
     batches.
2. **Batch brief** — fill in and send as-is:

   ```text
   Read-only VSDB review. Do not edit files, run cargo, or report style.
   Files: <list>. Evidence: <diff command | full files>. Seams: <list | none>.
   Read first: docs/ai/review-core.md §2-3, docs/ai/false-positive-guide.md,
   <mapped docs/ai/patterns/*.md>.
   Report each candidate as: path (fn) · invariant · realistic trigger ·
   wrong outcome · why existing guards fail · smallest failing test ·
   public/on-disk compatibility impact. Then list files read and files not read.
   ```

3. **Dimensions** (only those that apply): correctness/invariants; interrupted
   writes, concurrency, and `unsafe` contracts; design shape when
   locks/resources/bounds/install/failure/API are involved; compatibility
   ([runtime wire formats and historical decoders](../../../docs/ai/compatibility-policy.md#typed-handle-tags-and-wire-decoders));
   quantified hot-path cost; incomplete implementations (`review-core.md` §3).
4. **Verify** every candidate yourself from the full implementation, callers,
   guards, and existing tests; refute via the FP guide; merge findings that
   share a root cause. If it is still ambiguous, ask one independent reviewer.
   Agreement between reviewers is not evidence. Drop speculation and style.
5. **Completeness** — diff scopes: every changed file, public/persisted
   contract, failure path, and relevant test. `all`: compare the ledger with
   each batch's "files read", and review only the gaps. Record any remaining
   gap; it blocks the release.
6. **Reconcile** with `docs/audit.md` in the ledger, not on disk
   (`review-core.md` §5): queue still-present in-scope `Open`; mark entries
   proven fixed or disproven as `pruned`; re-check intersecting `Won't Fix`
   (`all`: every entry). Queue confirmed findings CRITICAL → LOW. Do not write
   `Open` entries for defects this run will fix.

## Phase 3 — Fix and commit

Work the queue sequentially on one tree; only investigation and validation may
run in parallel. Order: **A** owned units → **B** findings and in-scope `Open`
(CRITICAL → LOW) → **C** registry-only commit.

Before editing a path not yet owned, check its diff against the baseline and
register it in the ledger (`workflow-policy.md` §1.5). If a fix overlaps unowned
baseline (including the same hunk) and cannot be separated safely, leave the
baseline untouched, mark the item `open: baseline overlap`, and continue with
independent items.

Launch every `cargo test` with a scratch base created in the parent shell and
remove only that directory afterward (`commit-protocol.md` §Per-unit validate
and commit, step 3); never `$HOME/.vsdb`.

### A. Owned units (`staged` / `worktree` only)

1. Split the owned diff into coherent units: one issue/behavior each, with its
   tests, docs, migration, and audit edits. Keep pre-staged boundaries.
2. Review each unit as in Phase 2 and fix confirmed defects **inside** the unit
   with a regression test. Never commit a known-defective unit. A unit that
   contains an unaccepted public/on-disk break stays uncommitted and `Open`.
3. Validate and commit per `commit-protocol.md` §Per-unit validate and commit.
   When other units or dirty baseline could affect the result, validate in a
   disposable worktree (`workflow-policy.md` §4).

### B. Findings and in-scope `Open` — one root cause per commit

1. **Re-confirm at current `HEAD`**, since earlier commits may have changed the
   code:
   - disproven → `pruned`, with the refutation noted for the report;
   - real, but no safe fix is proportionate → `won't fix` + reason;
   - needs an unaccepted break, lacks evidence, or failed validation →
     `open: <blocker>`.

   Never relabel an `Open` entry `Won't Fix` just to clear the list.
2. **Test first:** write the smallest regression test and watch it fail on the
   unfixed code. For races or crash timing, use a storage-error test double or
   an isolated restart fixture (`review-core.md` §3). If it still cannot be
   reproduced deterministically, say "traced, not reproduced" in the commit
   message.
3. **Fix** the root cause minimally. Trace SWMR, COW/ref-count, crash recovery,
   cleanup, and compatibility. Watch the test pass.
4. **Same commit:** public/migration docs, plus removal of the pre-existing
   `docs/audit.md` entry this fix resolves. Leave `CHANGELOG.md` to the
   release commit.
5. Validate and commit per `commit-protocol.md` §Per-unit validate and commit,
   then update the ledger.

### C. Registry-only dispositions

Write the net `docs/audit.md` changes: unfixed/blocked `Open` entries, justified
`Won't Fix` entries, and entries pruned without a code edit. Shape and severity
follow `review-core.md` §5. If owned changes remain, commit them once as a
separate `docs(audit): …` commit after the code fixes. If `docs/audit.md` also
carries unowned baseline edits, stage only the owned hunks (build a patch and
use `git apply --cached`). If they cannot be separated, leave the update
uncommitted and report it. Never revert unowned edits.

## Phase 4 — Final gate and release

1. **Aggregate re-review** of `starting_HEAD..HEAD` (skip if nothing was
   committed): cross-commit interactions, missed callers/error paths, and
   alignment with docs and guides.
2. **Toolchain:** `rustup update stable` (`compatibility-policy.md` §Rust
   toolchain). If offline, use the installed stable toolchain and report
   `rustc -V`.
3. **Gate** — pick the first matching row:

   | Condition | Gate |
   |-----------|------|
   | scope `all`; or bump owed; or `starting_HEAD..HEAD` touches `.rs`, `Cargo.toml`, `.cargo/config.toml`, or `rustfmt.toml`; or the scope contains unreleased manifest/toolchain changes | full 7-step `commit-protocol.md` §Final workspace gate |
   | only docs/registry/governance committed | `git diff --check starting_HEAD HEAD`; every changed relative link and `#anchor` resolves; changed YAML parses |
   | nothing committed and nothing owed | none |

   Reuse checks that already passed on the identical code/manifest state. If
   unowned dirty Rust/manifest baseline exists, gate `HEAD` in a disposable
   worktree (`workflow-policy.md` §4).
4. **Failures:** a re-review finding or gate failure gets a new focused fix
   commit (never amend), then re-run the affected checks.
5. **Release** once, per `commit-protocol.md` §Lockstep version and tag: re-run
   the owed check, then bump the patch version, update the CHANGELOG, and
   create the annotated tag. Do this only when the full gate passed on the final
   code state and no in-scope `Open` entry or coverage gap remains. If blocked,
   keep the validated commits and report the owed bump/tag; the next run can
   detect it. Never make an empty commit.

## Stop conditions

- **Stop:** the same failure repeats with no progress; a release file
  (`core/Cargo.toml`, `strata/Cargo.toml`, `migration/Cargo.toml`, root `Cargo.toml`, `CHANGELOG.md`)
  carries unowned baseline edits; a conflicting release tag exists.
- **Continue:** a blocked item never stops independent items. Partial progress
  is a valid outcome — every commit is validated on its own.
- **Do not ask mid-run** about public/on-disk breaks or contract choices: leave
  them `Open` and list them under *Decisions needed*.

## Output

```text
Scope: <token> · starting_HEAD <hash> · toolchain <rustc -V>
Coverage: <subsystems / file categories reviewed> · gaps: <none | list>
Commits: <hash> <subject>   (one per line, in order)
Findings: fixed <n> · pruned <list> · won't fix <list> · open <list + blocker>
Decisions needed: <unaccepted breaks, contract choices | none>
Validation: <checks run> · isolation: <scratch VSDB_BASE_DIR / worktree>
Compatibility: <public / on-disk impact | none>
Release: <vX.Y.Z + tag | owed, blocked by … | not owed>
Baseline untouched: <paths | clean>
```
