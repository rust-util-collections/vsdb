---
name: x-overhaul
description: Audit VSDB (full repo or scoped range), resolve findings safely, and create atomic local commits. Use only when the user explicitly invokes /x-overhaul.
argument-hint: "[N | all | staged | worktree | <hash> | <hash1>..<hash2>]"
disable-model-invocation: true
---

# VSDB Audit-Fix-Commit Pipeline

Review scope → dispose every confirmed finding → fix actionable → local commits.
Unlike `/x-review`, always fixes and commits (no `--fix`). Never push.
User-invoked only. New commits only.

## Input

`$ARGUMENTS` — same scopes as `/x-review` without `--fix`:

| Input | Scope |
|-------|-------|
| *(empty)* or `all` | Full repo (default) |
| `N` / `staged` / `worktree` / hash / range | Diff-bound |

Non-full: fix only findings rooted in that diff. Post-fix re-review = files
this run changed.

## Setup

Preflight (`workflow-policy.md`); `pragmatic-engineering.md`; read `x-review` +
`x-fix` skills + `commit-protocol.md` + `compatibility-policy.md`; ledger before mutations.

## Phase 1 — Review

`/x-review <scope>` without `--fix`:

1. Coverage: full ledger (`all`) or diff+callers.
2. Agents with disjoint ownership when needed; `all` → each Rust file once in depth.
3. Cross-subsystem / design / completeness only for depth gaps.
4. Verify + dedupe (incl. public/on-disk compatibility).
5. Update `docs/audit.md` (`all` re-evals all sections; narrow scopes prune/merge
   in-scope without dropping unrelated Open unless proven fixed). No timestamps.
6. If registry changed → docs-only inventory commit before fixes (may list many findings).

## Phase 2 — Resolve

Full `/x-fix` on Phase-1 (and still-applicable Open) findings: severity order;
safe complete fix or Won't Fix/Rejected; one root cause per commit; mutations
sequential; re-review changed files only. Correctness > open-count cosmetics.
Out-of-scope Open untouched.

## Phase 3 — Gate, version, tag

Final gate; regressions in new commits. Rust changed → one lockstep bump
(patch or major+migration) + separate release commit + annotated tag.
Nothing changed → no empty commit/bump/tag.

## Output

Scope, coverage, dispositions, validations, compatibility/migration, hashes/subjects, version/tag, baseline left alone.
