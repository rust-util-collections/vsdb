---
name: x-review
description: Deep regression review of VSDB changes or the full repository. Use only when the user explicitly invokes /x-review.
argument-hint: "[N | all | staged | worktree | <hash> | <hash1>..<hash2>] [--fix]"
disable-model-invocation: true
---

# Deep Regression Review for VSDB

High-signal review. Code read-only unless `--fix`; may update only
`docs/audit.md`. Never commit or push. User-invoked only.

## Setup

Read: `workflow-policy.md`, `pragmatic-engineering.md`, `technical-patterns.md`,
`review-core.md` (Subsystem Map), `false-positive-guide.md`. Public/persisted
change → `compatibility-policy.md`. Design-shaped / multi-subsystem →
`design-patterns.md`.

## Input

`$ARGUMENTS` — one optional scope + optional `--fix`:

| Input | Scope |
|-------|-------|
| *(empty)* | Latest commit |
| `N` | Last N commits (positive int) |
| `staged` | `git diff --cached` |
| `worktree` | Staged + unstaged + untracked |
| `all` | Full repo |
| `<hash>` | One commit |
| `<hash1>..<hash2>` | Range |

Validate revs with Git. Reject bad args; never guess. `--fix`: apply confirmed
fixes after report. Historical scope: only still-present HEAD defects.

## Protocol

### Phase 1 — Scope

1. Worktree baseline (`workflow-policy.md`).
2. Changed files + full diff + callers/tests. `worktree` includes untracked.
   `all` → ledger: both crates’ source/tests/benches, manifests/CI, public docs, `.claude/`.
3. Map via Subsystem Map; load guides.
4. Mark generated/vendored/out-of-scope in the ledger — do not silent-drop.

### Phase 2 — Evidence

Small single-subsystem → review direct. Agents only if context split helps
(read-only; exact scope + guides + high-signal rule).

Non-trivial dimensions (minimum sufficient):

- correctness / invariants
- crash / concurrency / unsafe
- design shape if locks/resources/bounds/install/failure/API (`design-patterns.md`)
- API / compatibility / quantified perf / placeholders (`review-core.md`)

`all`: disjoint subsystem batches (each Rust file one owner); cross-subsystem +
design only for gaps. fmt/compile/clippy → tools, not agents.

Each candidate: location + invariant · realistic trigger · wrong outcome · why
guards fail · minimal fix + test · compatibility/migration if public/persisted.
Drop style, speculation, FP hits.

### Phase 3 — Verify

Orchestrator re-reads and tries to **refute**. One independent verifier only if
still ambiguous. Voting ≠ proof. Keep only code-demonstrable items; merge same
root cause.

### Phase 4 — Completeness

Diff: every changed file, public/persisted contract, failure path, relevant test.
`all`: ledger vs depth results; critic only uncovered files/invariants. No rework.

### Phase 5 — Audit registry

Update `docs/audit.md` from current code:

1. Prune fixed/obsolete Open (history → Git/CHANGELOG, not Resolved section).
2. Add confirmed Open, dedupe, CRITICAL→LOW.
3. Re-check intersecting Won't Fix (`all` → all).
4. Disproportionate real → Won't Fix + Reason.
5. Material disproven → Rejected (no severity); drop routine noise.
6. No dates/freshness markers.

```markdown
## Open
### [SEVERITY] subsystem: summary
- **Where**: file:line_range
- **What**: defect
- **Why**: trigger, outcome, invariant
- **Suggested fix**: direction

## Won't Fix
### [SEVERITY] subsystem: summary
- **Where** / **What** / **Reason**

## Rejected
### subsystem: claim
- **Where** / **Claim** / **Reason**
```

### Phase 6 — Report

Scope, coverage, findings (severity, loc, trigger, outcome, compatibility, fix).
Zero → say so + what was covered.

### Phase 7 — `--fix` only

Sequential fixes; preserve baseline; stop on unsafe overlap. Regression tests +
smallest validate per fix; re-review; update audit. No version/commit/push —
user runs `/x-commit` after inspect.
