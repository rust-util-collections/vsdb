---
name: x-commit
description: Review, fix, validate, and commit VSDB worktree changes as atomic commits. Use only when the user explicitly invokes /x-commit.
disable-model-invocation: true
---

# Self-Reviewing Commit for VSDB

Review owned worktree changes → fix confirmed defects → validate → local commits.
Never push. No tag. No autonomous major. User-invoked only. New commits only
(no amend/rebase/force-push).

## Setup

Read `.claude/docs/workflow-policy.md`, `.claude/docs/commit-protocol.md`,
`.claude/docs/pragmatic-engineering.md`, `.claude/docs/review-core.md`,
`.claude/docs/technical-patterns.md`, `.claude/docs/false-positive-guide.md`;
public/persisted → `.claude/docs/compatibility-policy.md`; design-shaped →
`.claude/docs/design-patterns.md`. Preflight + ledger (incl. **frozen paths** and
whether a patch bump is owed).

## Protocol

### 1. Scope

1. `git status --short`, full diffs, intended untracked (`git diff HEAD` misses untracked).
2. Nothing intended and no owed bump → “nothing to commit”, stop. Owed bump only →
   skip to the final gate.
3. Freeze owned paths before edit; later stage freeze + this-invocation fix/format only.
4. Split coherent units: one issue/root cause/behavior each; tests/docs/migration/audit stay with unit;
   keep pre-staged boundaries unless user changes them.
5. Unrelated same-hunk overlap → stop (no stash/revert/absorb).
6. Multi-unit tree: combined validation ≠ per-unit proof — disposable worktree when isolation matters.

### 2. Review and fix (per unit)

Map guides → full functions/callers/errors/tests → COW/ref-count, DAG, trie proof,
prefix/shard, SWMR/unsafe, staged mutation, resources, quantified hot-path,
placeholders, design (if any) → classify public/on-disk compatibility
(`.claude/docs/compatibility-policy.md`; never hide a break in a patch; do not
ship a break the user has not accepted — leave it Open) → refute via FP guide
→ fix completely + regression → re-review until clean. Confirmed and unfixed →
Open in `docs/audit.md`. Real but unsafe to fix here → Won't Fix there. Do not
leave it only in the chat. No-progress → stop and report.

Investigate parallel OK; edit/commit sequential.

### 3. Validate and commit

`.claude/docs/commit-protocol.md` per unit: format, lint, targeted tests (no `/home` cleanup),
exact stage, inspect cached diff, one new commit. Never amend.

### 4. Final gate and version

`.claude/docs/commit-protocol.md`: full workspace gate, then the owed bump.
Post-commit regression → new focused commit, then the owed bump still applies.

## Output

Files/subsystems, fixes, validations, compatibility, hashes/subjects, version, untouched baseline.
