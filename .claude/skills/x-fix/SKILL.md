---
name: x-fix
description: Resolve the VSDB audit backlog sequentially, with one finding per validated local commit. Use only when the user explicitly invokes /x-fix.
disable-model-invocation: true
---

# Fix the VSDB Audit Backlog

Clear actionable `docs/audit.md` Open → self-review → local commits. Never push.
No tag. No autonomous major. User-invoked only. New commits only.

## Input

None. Non-empty `$ARGUMENTS` → reject; never guess a filter.

## Setup

Read `.claude/docs/workflow-policy.md`, `.claude/docs/commit-protocol.md`,
`.claude/docs/compatibility-policy.md`, `.claude/docs/pragmatic-engineering.md`,
`.claude/docs/review-core.md`, `.claude/docs/technical-patterns.md`,
`.claude/docs/false-positive-guide.md`. Preflight + ledger (freeze paths as work
proceeds; note whether a patch bump is owed). Empty Open and no owed bump →
“nothing to fix”. Empty Open but an owed bump → skip fixes; standalone continues
at step 4, and a composed run leaves the bump to the parent.

When composed by `x-overhaul`, inherit its scope, starting HEAD, and ledger;
run steps 1–3 only. The parent owns the final gate and version. Empty in-scope
Open skips fixes, not the parent's audit, validation, or owed bump.

## Protocol

### 1. Triage (CRITICAL → LOW)

Per entry before edit: code/callers/tests + guides (+ `.claude/docs/design-patterns.md`
if design); reproduce from current code; dedupe root causes; false → Rejected; real and
disproportionate to fix safely → Won't Fix + reason. A break the user has not
accepted → leave Open; do not ship it. Missing evidence, failed validation, or
ownership overlap is a blocker, not a disposition: leave Open.

### 2. One finding → one commit (blocking)

1. Root-cause fix + focused regression.
2. Trace SWMR, COW, crash, cleanup, compatibility.
3. Drop that Open entry (code + tests + migration docs + registry = unit).
4. Per-unit validation (`.claude/docs/commit-protocol.md`).
5. Stage freeze + fix paths; inspect cached; commit before next.

Registry-only disposition = one unit. Same root cause may batch symptoms. Mutating
agents never parallel (read/validate may).

### 3. Self-review

Review `starting_HEAD..HEAD` + this invocation’s uncommitted paths with
`review-core.md` §3 evidence and the FP guide; registry per §5.
New confirmed → Open → same one-finding loop. Stop on no-progress or baseline overlap.

### 4. Final gate and version

`.claude/docs/commit-protocol.md` once. Required checks pass and no in-scope Open
remains → the owed bump. No tag. No autonomous major.
Blocked → retain validated local commits and report remaining Open; the owed bump
stays for the next run.

## Output

Dispositions, fixes, Rejected/Won't Fix, validations, compatibility, hashes/subjects, version, baseline left alone.
