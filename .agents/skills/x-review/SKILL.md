---
name: x-review
description: Review VSDB Rust changes or the full repository for collection correctness, storage durability, and compatibility regressions. Use only when the user explicitly invokes $x-review.
---

# VSDB Correctness and Regression Review

Resolve repository paths from the Git root; Markdown links are relative to
this file. Read invocation arguments from the user prompt.

Evidence-based review of the local Rust workspace and its documented contracts.
Apply [pragmatic-engineering.md](../../../docs/ai/pragmatic-engineering.md) §Maintenance scope.
Code read-only unless `--fix`. `docs/audit.md` is the registry
exception: updating it is not a code write. Merge it; do not revert unrelated edits.
Never commit or push. User-invoked only.

## Setup

Run the [worktree preflight](../../../docs/ai/workflow-policy.md#1-preflight). Follow the [review core](../../../docs/ai/review-core.md#1-context) for common checks and the guide for each affected subsystem.

## Input

Arguments from the user prompt — at most one scope + optional `--fix`:

| Input | Scope | Evidence |
|-------|-------|----------|
| *(empty)* | Latest commit | as `<hash>` = `HEAD` |
| `N` | Last N commits | `git log HEAD~N..HEAD` + `git diff HEAD~N HEAD` |
| `staged` | Index | `git diff --cached` |
| `worktree` | Staged + unstaged + untracked | `git diff HEAD` + `git ls-files --others --exclude-standard` |
| `all` | Full repo | tracked-file ledger (`git ls-files`) |
| `<hash>` | One commit | `git show <hash>`; merge → `git diff <hash>^1 <hash>` |
| `<a>..<b>` | Range | `git log <a>..<b>` + `git diff <a> <b>` |

Ranges use the two endpoints consistently; do not substitute a merge-base diff.
Resolve every rev (incl. `HEAD~N`) with `git rev-parse --verify --quiet '<rev>^{commit}'`.
An all-digit token is `N`; if it also resolves as a commit, ask. Reject anything
else; never guess. `--fix`: apply confirmed fixes after the report. Historical
scope: report only defects still present at HEAD.

## Protocol

### Phase 1 — Scope

1. Worktree baseline ([workflow-policy.md](../../../docs/ai/workflow-policy.md)).
2. Changed files + full diff + callers/tests. `worktree` includes untracked.
   For `all`, build the tracked-file ledger using [full-repository coverage](../../../docs/ai/review-core.md#full-repository-coverage).
3. Use the [subsystem map](../../../docs/ai/review-core.md#subsystem-map) to
   select the source and proof for the change. For example, a native shard
   fence needs the write/rotation/reopen path, while a generation export
   needs the v4 source, v5 destination and collection handles.
4. Mark generated/vendored/out-of-scope in the ledger — do not silent-drop.

### Phase 2 — Evidence

Small single-subsystem → review direct. Agents only if context split helps
(read-only; exact scope + guides + high-signal rule).

Non-trivial dimensions (minimum sufficient):

- correctness / invariants
- interrupted writes / concurrency / Rust `unsafe` contracts
- design shape if locks/resources/bounds/install/failure/API (`design-patterns.md`)
- API / compatibility / quantified perf / placeholders (`review-core.md`)

`all`: disjoint subsystem batches (each Rust file one owner); cross-subsystem +
design only for gaps. fmt/compile/clippy → tools, not agents.

Each candidate: location + invariant · realistic trigger · wrong outcome · why
guards fail · minimal fix + test · compatibility/migration if public/persisted.
Drop style, speculation, FP hits.

### Phase 3 — Verify

Re-read each candidate's implementation, callers, and existing tests. Check
whether normal guards or documented caller obligations already explain the
result. Use one independent reviewer only when evidence remains ambiguous.
Agreement is not evidence. Keep only code-demonstrable defects; merge findings
with the same root cause.

### Phase 4 — Completeness

Diff: every changed file, public/persisted contract, failure path, relevant test.
`all`: ledger vs depth results; critic only uncovered files/invariants. No rework.

### Phase 5 — Audit registry

Update `docs/audit.md` per `review-core.md` §5 (scope rules, shape, severity).

### Phase 6 — Report

Scope, coverage, findings (severity, loc, trigger, outcome, compatibility, fix).
Zero → say so + what was covered.

### Phase 7 — `--fix` only

Sequential fixes; preserve baseline; stop on inseparable ownership overlap. Regression test +
per-unit checks from [commit-protocol.md](../../../docs/ai/commit-protocol.md) steps 2–4 per fix (no
staging); re-review; update audit. No version/commit/push — user runs
`$x-commit` after inspect.
