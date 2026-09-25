---
name: x-overhaul
description: Audit a VSDB scope (same as /x-review; empty = latest commit), resolve findings safely, and create atomic local commits. Use only when the user explicitly invokes /x-overhaul.
argument-hint: "[N | all | staged | worktree | <hash> | <hash1>..<hash2>]"
disable-model-invocation: true
---

# VSDB Audit-Fix-Commit Pipeline

Review scope → dispose every confirmed finding → fix actionable → local commits.
Unlike `/x-review`, always fixes and commits (no `--fix`). Never push. No tag.
No autonomous major. User-invoked only. New commits only.

## Input

`$ARGUMENTS` — same scopes and rev resolution as `/x-review`, without `--fix`.
Reject `--fix` and any other unknown arg; never guess.

| Input | Scope |
|-------|-------|
| *(empty)* | Latest commit |
| `all` | Full repo |
| `N` / hash / range | Committed diff |
| `staged` / `worktree` | Uncommitted diff — owned, see below |

Non-full: fix only still-present findings rooted in that diff. Related callers,
tests, and contracts are evidence scope, not permission for unrelated fixes.

`staged` / `worktree`: the scoped changes are this invocation’s work to commit.
Apply `/x-commit` §1 (freeze, unit split, same-hunk overlap → stop); for `staged`,
a path that also has unstaged hunks is overlap → stop. Defects in them are fixed
inside their own units before commit, not inventoried; only findings left unfixed
reach the registry.

## Setup

Preflight (`.claude/docs/workflow-policy.md`); read
`.claude/docs/pragmatic-engineering.md`, `.claude/skills/x-review/SKILL.md` (and
its Setup docs), `.claude/skills/x-fix/SKILL.md`, `.claude/docs/commit-protocol.md`,
and `.claude/docs/compatibility-policy.md`; for `staged` / `worktree` also
`.claude/skills/x-commit/SKILL.md`. Ledger before mutations; note whether a patch
bump is owed.

This skill owns one invocation: one starting HEAD, scope, ownership ledger,
final gate, and version decision. Reuse the review and fix protocols below;
do not launch nested standalone workflows or repeat their finalization. The
composed skills’ read-only / no-commit rules, x-review Phases 6–7, and x-fix
step 4 do not apply — the phases here replace them.

## Phase 1 — Review

Apply the `x-review` evidence and registry protocol without code fixes:

1. Coverage: full ledger (`all`) or diff+callers.
2. Read-only agents with disjoint ownership when useful; otherwise review directly.
   `all` → each Rust file once in depth. Record reviewed paths/invariants and any
   coverage gaps; searches and passing tests alone do not establish depth.
3. Cross-subsystem / design / completeness only for depth gaps.
4. Verify + dedupe (incl. public/on-disk compatibility).
5. Update `docs/audit.md` per `.claude/docs/review-core.md` §5 (`staged` /
   `worktree`: skip findings inside the owned units — Phase 2 fixes them in-unit).
6. If registry changed → docs-only inventory commit before fixes (may list many findings).

## Phase 2 — Resolve

`staged` / `worktree`: first commit the owned units per `/x-commit` §2–3.
Then apply `x-fix` triage, atomic fix/commit, and self-review to Phase-1 findings and
still-applicable **in-scope** Open entries. Resolve in severity order; mutations
sequential. Re-review each fix plus affected callers, failure paths, tests, and
public/persisted contracts; use the invocation diff to catch interactions.

Won't Fix requires a real defect and a reason the safe fix is disproportionate;
disproven entries require refutation and are removed. Blocked or unverified
fixes remain Open with the blocker recorded. Continue independent safe units; report remaining Open and
coverage gaps honestly. Out-of-scope Open is not fixed or re-disposed.

## Phase 3 — Gate and version

Run the final gate from `.claude/docs/commit-protocol.md` once per stable code
state; an `all` audit runs it even when no Rust fix was needed. Reuse checks
already passed on the same state. Regressions → new atomic fixes + affected checks again.

Only after required validation passes and no in-scope Open or coverage gap
remains: the owed bump from `.claude/docs/commit-protocol.md`. No tag. No autonomous major.
Incomplete run → preserve validated local commits and report blockers; the owed
bump stays for the next run. Nothing owed and nothing changed → no empty commit.

## Output

Scope, coverage, dispositions, validations, compatibility/migration, hashes/subjects, version, baseline left alone.
