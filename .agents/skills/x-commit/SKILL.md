---
name: x-commit
description: Review VSDB worktree changes against Rust correctness and storage contracts, fix confirmed regressions, validate, and create atomic local commits. Use only when the user explicitly invokes $x-commit.
---

# Self-Reviewing Commit for VSDB

Resolve repository paths from the Git root; Markdown links are relative to
this file. Read invocation arguments from the user prompt.

Review owned worktree changes → fix confirmed defects → validate → local commits.
Never push. No tag. No autonomous major. User-invoked only. New commits only
(no amend/rebase/force-push).

Apply [pragmatic-engineering.md](../../../docs/ai/pragmatic-engineering.md) §Maintenance scope. The requested
work is the owned local diff plus the callers and tests needed to validate it.

## Input

Arguments from the user prompt — optional Git pathspecs. Given → only matching changes are
candidates; everything else is baseline. Empty → all intended changes. Flags →
reject; never guess. Pathspecs matching no change → nothing intended (§1.2).
Pathspecs bound review units, not the owed release (`commit-protocol.md`).

## Setup

Read [workflow-policy.md](../../../docs/ai/workflow-policy.md), [commit-protocol.md](../../../docs/ai/commit-protocol.md),
[pragmatic-engineering.md](../../../docs/ai/pragmatic-engineering.md), [review-core.md](../../../docs/ai/review-core.md),
[technical-patterns.md](../../../docs/ai/technical-patterns.md), [false-positive-guide.md](../../../docs/ai/false-positive-guide.md);
public/persisted → [compatibility-policy.md](../../../docs/ai/compatibility-policy.md); design-shaped →
[design-patterns.md](../../../docs/ai/design-patterns.md). Preflight + ledger (incl. **frozen paths** and
whether a patch bump is owed).

## Protocol

### 1. Scope

1. `git status --short`, full diffs, intended untracked (`git diff HEAD` misses
   untracked). Unclear whether an untracked file is intended → ask.
2. Nothing intended and no owed bump → “nothing to commit”, stop. Owed bump only →
   skip to the final gate.
3. Freeze owned paths before edit; later stage freeze + this-invocation fix/format only.
4. Split coherent units: one issue/root cause/behavior each; tests/docs/migration/audit
   stay with the unit; keep pre-staged boundaries unless the user changes them.
5. Unrelated same-hunk overlap → stop (no stash/revert/absorb).
6. Multi-unit tree: combined validation ≠ per-unit proof — disposable worktree when isolation matters.

### 2. Review and fix (per unit)

1. Map paths via the Subsystem Map; read full changed functions, callers, error
   paths, and tests.
2. Check the mapped invariants: COW/ref-count, DAG, trie proof, prefix/shard,
   SWMR/unsafe, staged mutation, resources, quantified hot-path cost,
   placeholders; design families when design-shaped.
3. Classify public/on-disk compatibility. Never hide a break in a patch; a break
   the user has not accepted stays uncommitted and Open.
4. Try to refute each candidate via the FP guide.
5. Fix confirmed defects completely + regression test; re-review until clean.
6. Real but safe fix disproportionate → Won't Fix + reason; confirmed but blocked
   or unfixed → Open (`docs/audit.md`, `review-core.md` §5). Never only in chat.
7. Repeat pass with no progress → stop and report.

Investigate parallel OK; edit/commit sequential.

### 3. Validate and commit

[commit-protocol.md](../../../docs/ai/commit-protocol.md) per unit: format, lint, targeted tests (no `/home` cleanup),
exact stage, inspect cached diff, one new commit. Never amend.

### 4. Final gate and version

[commit-protocol.md](../../../docs/ai/commit-protocol.md): full workspace gate, then the owed bump.
Post-commit regression → new focused commit, then the owed bump still applies.

## Output

Files/subsystems, fixes, validations, compatibility, hashes/subjects, version, untouched baseline.
