---
name: x-commit
description: Review VSDB worktree changes against Rust correctness and storage contracts, fix confirmed regressions, validate, and create atomic local commits. Use only when the user explicitly invokes $x-commit.
---

# Self-Reviewing Commit for VSDB

Resolve repository paths from the Git root; Markdown links are relative to
this file. Read invocation arguments from the user prompt.

Review owned worktree changes → fix confirmed defects → validate → local commits.
Never push. No autonomous major. User-invoked only. New commits only
(no amend/rebase/force-push).

Follow the [maintenance scope](../../../docs/ai/workflow-policy.md#maintenance-scope). The requested
work is the owned local diff plus the callers and tests needed to validate it.

## Input

Arguments from the user prompt — optional Git pathspecs. Given → only matching changes are
candidates; everything else is baseline. Empty → all intended changes. Flags →
reject; never guess. Pathspecs matching no change → nothing intended (§1.2).
Pathspecs bound review units, not the owed release (`commit-protocol.md`).

## Setup

Run the [worktree preflight](../../../docs/ai/workflow-policy.md#1-preflight) and freeze the intended paths in the [invocation ledger](../../../docs/ai/commit-protocol.md#invocation-ledger), including any owed bump or tag. Use the [review core](../../../docs/ai/review-core.md#1-context) to select the shared and subsystem-specific checks for each unit.

## Protocol

### 1. Scope

1. `git status --short`, full diffs, intended untracked (`git diff HEAD` misses
   untracked). Unclear whether an untracked file is intended → ask.
2. Nothing intended and no owed bump or tag → “nothing to commit”, stop.
   Owed bump or tag only → skip to §4.
3. Freeze owned paths before edit; later stage freeze + this-invocation fix/format only.
4. Split coherent units: one issue/root cause/behavior each; tests/docs/migration/audit
   stay with the unit; keep pre-staged boundaries unless the user changes them.
5. Unrelated same-hunk overlap → stop (no stash/revert/absorb).
6. Multi-unit tree: combined validation ≠ per-unit proof — disposable worktree when isolation matters.

### 2. Review and fix (per unit)

1. Map the unit to its [source, callers and proof](../../../docs/ai/review-core.md#subsystem-map).
   Read full changed functions and their failure paths.
2. Check the affected invariants. A v5 adapter change can affect both raw
   and typed callers. A change to shared shard format options can also affect
   v4-to-v5 export. Include the applicable tests, public guide and migration
   contract in the same unit.
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

### 4. Final gate, version, and tag

[commit-protocol.md](../../../docs/ai/commit-protocol.md) governs the full
workspace gate, owed bump, and local annotated release tag.
Post-commit regression → new focused commit, then the owed bump still applies.

## Output

Files/subsystems, fixes, validations, compatibility, hashes/subjects, version,
release tag, untouched baseline.
