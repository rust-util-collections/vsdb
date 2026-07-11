---
name: x-review
description: Deep regression review of VSDB changes or the full repository. Use only when the user explicitly invokes /x-review.
argument-hint: "[N | all | staged | worktree | <hash> | <hash1>..<hash2>] [--fix]"
disable-model-invocation: true
---

# Deep Regression Review for VSDB

Review VSDB changes with high-signal, evidence-based analysis. Source code stays
read-only unless the user supplied `--fix`; the normal workflow may update only
`docs/audit.md`. It never commits or pushes.

## Setup

1. Read `.claude/docs/workflow-policy.md`.
2. Read `.claude/docs/technical-patterns.md`.
3. Read `.claude/docs/review-core.md` and use its Subsystem Map as the canonical
   file-to-guide mapping.
4. Read `.claude/docs/false-positive-guide.md`.
5. Read `.claude/docs/compatibility-policy.md` when public API, serialized
   metadata, durable keys/values, namespace layout, or format markers may change.

## Input

Arguments: `$ARGUMENTS`

Accept exactly one optional scope plus an optional `--fix`:

| Input | Scope |
|-------|-------|
| *(empty)* | Latest commit |
| `N` | Last N commits; N must be a positive integer |
| `staged` | Staged changes (`git diff --cached`) |
| `worktree` | All staged, unstaged, and untracked worktree changes |
| `all` | Full repository audit |
| `<hash>` | One commit |
| `<hash1>..<hash2>` | Exact commit range |

Validate revisions with Git. Reject unknown, ambiguous, or extra arguments with
the usage string; never guess the intended range.

`--fix` applies confirmed fixes to the current worktree after reporting. For a
historical scope, first prove the candidate still exists at current `HEAD`;
never fix or register an already-resolved bug.

## Protocol

### Phase 1: Scope and coverage

1. Record the worktree baseline required by `workflow-policy.md`.
2. Read the complete diff plus surrounding implementations, callers, and tests.
   - `worktree` includes untracked paths from `git status --short`.
   - `all` uses a tracked-file ledger covering both crates' source, tests,
     benches/build scripts, manifests/CI, public docs, and `.claude/`.
3. Map every code file through the Subsystem Map and load all mapped guides.
4. Mark generated, vendored, ignored, or explicitly excluded files in the
   ledger instead of silently omitting them.

### Phase 2: Evidence collection

Review a small, single-subsystem diff directly. Use agents only when separate
context materially improves coverage:

- Agents are read-only and receive exact files, mapped guides, and the
  high-signal rule.
- For a non-trivial diff, use only needed dimensions: correctness/invariants;
  crash/concurrency/unsafe; API/compatibility/performance/error paths.
- For `all`, partition the depth pass into disjoint subsystem batches. Every
  tracked Rust source file has one owner. Add a later cross-subsystem pass only
  for interactions a file-local pass cannot establish.
- Compiler, formatter, and Clippy diagnostics belong to deterministic tools,
  not LLM review agents.

Every candidate finding includes:

1. exact location and invariant;
2. realistic trigger;
3. incorrect observable outcome;
4. existing guard/protocol checked and why it is insufficient;
5. minimal fix direction and regression test;
6. compatibility/migration impact when persisted or public behavior changes.

Discard preferences, unsupported speculation, and false-positive-guide matches.

### Phase 3: Critical verification

The orchestrator re-reads and actively tries to disprove every candidate. Use one
independent read-only verifier only when control flow or an invariant remains
genuinely ambiguous; correlated agent majority voting is not proof.

A finding survives only when its trigger and outcome follow from current code.
Deduplicate symptoms sharing one root cause.

### Phase 4: Completeness

For diff scopes, account for every changed file, public/persisted contract,
failure path, and relevant test. For `all`, reconcile the tracked-file ledger
and run a focused critic over uncovered files or invariants only.

### Phase 5: Audit registry

Update `docs/audit.md` from current-code evidence:

1. Remove fixed/obsolete `Open` entries; resolution history belongs in Git and
   CHANGELOG, not an ever-growing `Resolved` section.
2. Add confirmed actionable findings to `Open`, deduplicated and sorted
   CRITICAL → HIGH → MEDIUM → LOW.
3. Re-evaluate `Won't Fix` entries whose code, callers, assumptions, or
   subsystem intersect this review; `all` re-evaluates every entry.
4. Keep a real defect/debt under `Won't Fix` only with a concrete reason.
5. Record under `Rejected` only an existing or plausibly recurring claim with
   useful counter-evidence. Rejected is not a severity.
6. Never add dates, timestamps, or freshness markers.

```markdown
## Open

### [SEVERITY] subsystem: summary
- **Where**: file:line_range
- **What**: concrete defect
- **Why**: trigger, outcome, and violated invariant
- **Suggested fix**: minimal safe direction

## Won't Fix

### [SEVERITY] subsystem: summary
- **Where**: file:line_range
- **What**: concrete defect/debt
- **Reason**: why a safe fix is currently disproportionate

## Rejected

### subsystem: rejected claim
- **Where**: file:line_range
- **Claim**: allegation
- **Reason**: evidence showing why it is not a bug
```

### Phase 6: Report

Report scope, covered subsystems/invariants, and each confirmed finding's
severity, location, trigger, outcome, compatibility impact, and fix direction.
If none survive, state that plainly and summarize meaningful coverage.

### Phase 7: Fix (`--fix` only)

1. Apply fixes sequentially; never run mutating agents in parallel.
2. Preserve baseline changes and stop on unsafe overlap.
3. Add focused regression coverage and run the smallest safe validation.
4. Re-review the changed code and update `docs/audit.md`.
5. Do not bump versions, commit, amend, or push. The user may invoke
   `/x-commit` after inspecting the worktree.
