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

Non-full: fix only still-present findings rooted in that diff. Related callers,
tests, and contracts are evidence scope, not permission for unrelated fixes.

## Setup

Preflight (`../../docs/workflow-policy.md`); read
`../../docs/pragmatic-engineering.md`, `../x-review/SKILL.md`,
`../x-fix/SKILL.md`, `../../docs/commit-protocol.md`, and
`../../docs/compatibility-policy.md`; ledger before mutations.

This skill owns one invocation: one starting HEAD, scope, ownership ledger,
final gate, and release decision. Reuse the review and fix protocols below;
do not launch nested standalone workflows or repeat their finalization.
Pass `all` explicitly to the review protocol when input is empty.

## Phase 1 — Review

Apply the `x-review` evidence and registry protocol without code fixes:

1. Coverage: full ledger (`all`) or diff+callers.
2. Read-only agents with disjoint ownership when useful; otherwise review directly.
   `all` → each Rust file once in depth. Record reviewed paths/invariants and any
   coverage gaps; searches and passing tests alone do not establish depth.
3. Cross-subsystem / design / completeness only for depth gaps.
4. Verify + dedupe (incl. public/on-disk compatibility).
5. Update `docs/audit.md` (`all` re-evals all sections; narrow scopes prune/merge
   in-scope without dropping unrelated Open unless proven fixed). No timestamps.
6. If registry changed → docs-only inventory commit before fixes (may list many findings).

## Phase 2 — Resolve

Apply `x-fix` triage, atomic fix/commit, and self-review to Phase-1 findings and
still-applicable **in-scope** Open entries. Resolve in severity order; mutations
sequential. Re-review each fix plus affected callers, failure paths, tests, and
public/persisted contracts; use the invocation diff to catch interactions.

Won't Fix requires a real defect and a reason the safe fix is disproportionate;
Rejected requires refutation. Blocked or unverified fixes remain Open with the
blocker recorded. Continue independent safe units; report remaining Open and
coverage gaps honestly. Out-of-scope Open stays untouched.

## Phase 3 — Gate, version, tag

Run the final gate from `commit-protocol.md` once per stable code state; an
`all` audit runs it even when no Rust fix was needed. Reuse checks already passed
on the same state. Regressions → new atomic fixes + affected checks again.

Only after required validation passes and no in-scope Open or coverage gap
remains: Rust changed → one lockstep bump (patch or major+migration), separate
release commit, annotated tag. Incomplete run → preserve validated local commits
and report blockers; no release. Nothing changed → no empty commit/bump/tag.

## Output

Scope, coverage, dispositions, validations, compatibility/migration, hashes/subjects, version/tag, baseline left alone.
