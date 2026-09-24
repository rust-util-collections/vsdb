---
name: x-review
description: Deep regression review of VSDB changes or the full repository. Use only when the user explicitly invokes /x-review.
argument-hint: "[N | all | staged | worktree | <hash> | <hash1>..<hash2>] [--fix]"
disable-model-invocation: true
---

# Deep Regression Review for VSDB

High-signal review. Code read-only unless `--fix`. `docs/audit.md` is the registry
exception: updating it is not a code write. Merge it; do not revert unrelated edits.
Never commit or push. User-invoked only.

## Setup

Read: `.claude/docs/workflow-policy.md`, `.claude/docs/pragmatic-engineering.md`,
`.claude/docs/technical-patterns.md`, `.claude/docs/review-core.md` (Subsystem Map),
`.claude/docs/false-positive-guide.md`. Public/persisted change →
`.claude/docs/compatibility-policy.md`. Design-shaped / multi-subsystem →
`.claude/docs/design-patterns.md`.

## Input

`$ARGUMENTS` — at most one scope + optional `--fix`:

| Input | Scope | Evidence |
|-------|-------|----------|
| *(empty)* | Latest commit | as `<hash>` = `HEAD` |
| `N` | Last N commits | `git log HEAD~N..HEAD` + `git diff HEAD~N HEAD` |
| `staged` | Index | `git diff --cached` |
| `worktree` | Staged + unstaged + untracked | `git diff HEAD` + `git ls-files --others --exclude-standard` |
| `all` | Full repo | tracked-file ledger (`git ls-files`) |
| `<hash>` | One commit | `git show <hash>`; merge → `git diff <hash>^1 <hash>` |
| `<a>..<b>` | Range | `git log <a>..<b>` + `git diff <a>...<b>` |

Resolve every rev (incl. `HEAD~N`) with `git rev-parse --verify --quiet '<rev>^{commit}'`.
An all-digit token is `N`; if it also resolves as a commit, ask. Reject anything
else; never guess. `--fix`: apply confirmed fixes after the report. Historical
scope: report only defects still present at HEAD.

## Protocol

### Phase 1 — Scope

1. Worktree baseline (`.claude/docs/workflow-policy.md`).
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

Update `docs/audit.md` per `review-core.md` §5 (scope rules, shape, severity).

### Phase 6 — Report

Scope, coverage, findings (severity, loc, trigger, outcome, compatibility, fix).
Zero → say so + what was covered.

### Phase 7 — `--fix` only

Sequential fixes; preserve baseline; stop on unsafe overlap. Regression test +
per-unit checks from `.claude/docs/commit-protocol.md` steps 2–4 per fix (no
staging); re-review; update audit. No version/commit/push — user runs
`/x-commit` after inspect.
