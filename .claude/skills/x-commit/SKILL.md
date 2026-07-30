---
name: x-commit
description: Review, fix, validate, and commit VSDB worktree changes as atomic commits. Use only when the user explicitly invokes /x-commit.
disable-model-invocation: true
---

# Self-Reviewing Commit for VSDB

Review owned worktree changes → fix confirmed defects → validate → local commits.
Never push. User-invoked only. New commits only (no amend/rebase/force-push).

## Setup

Read `workflow-policy.md`, `commit-protocol.md`, `pragmatic-engineering.md`,
`review-core.md`, `technical-patterns.md`, `false-positive-guide.md`;
public/persisted → `compatibility-policy.md`; design-shaped → `design-patterns.md`.
Preflight + ledger (incl. **frozen paths**).

## Protocol

### 1. Scope

1. `git status --short`, full diffs, intended untracked (`git diff HEAD` misses untracked).
2. Nothing intended → “nothing to commit”, stop.
3. Freeze owned paths before edit; later stage freeze + this-invocation fix/format only.
4. Split coherent units: one issue/root cause/behavior each; tests/docs/migration/audit stay with unit;
   keep pre-staged boundaries unless user changes them.
5. Unrelated same-hunk overlap → stop (no stash/revert/absorb).
6. Multi-unit tree: combined validation ≠ per-unit proof — disposable worktree when isolation matters.

### 2. Review and fix (per unit)

Map guides → full functions/callers/errors/tests → COW/ref-count, DAG, trie proof,
prefix/shard, SWMR/unsafe, staged mutation, resources, quantified hot-path,
placeholders, design (if any) → classify public/on-disk compatibility
(`compatibility-policy.md`; never hide a break in a patch) → refute via FP guide
→ fix completely + regression → re-review until clean. No-progress → stop and report.

Investigate parallel OK; edit/commit sequential.

### 3. Validate and commit

`commit-protocol.md` per unit: format, lint, targeted tests (no `/home` cleanup),
exact stage, inspect cached diff, one new commit. Never amend.

### 4. Final gate, version, tag

After behavior commits: full workspace gate + single lockstep version-and-tag
policy (major + migration if break). Post-commit regression → new focused commit.

## Output

Files/subsystems, fixes, validations, compatibility, hashes/subjects, version/tag, untouched baseline.
