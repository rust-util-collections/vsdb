---
name: x-fix
description: Fix confirmed VSDB correctness and compatibility defects recorded in docs/audit.md, with one defect per validated local commit. Use only when the user explicitly invokes $x-fix.
---

# Fix Recorded VSDB Defects

Resolve repository paths from the Git root; Markdown links are relative to
this file. Read invocation arguments from the user prompt.

Clear actionable `docs/audit.md` Open → self-review → local commits. Never push.
No tag. No autonomous major. User-invoked only. New commits only.

Apply [pragmatic-engineering.md](../../../docs/ai/pragmatic-engineering.md) §Maintenance scope. Verify each
recorded defect against the current Rust implementation before changing code.

## Input

None. Non-empty argument text → reject; never guess a filter.

## Setup

Read [workflow-policy.md](../../../docs/ai/workflow-policy.md), [commit-protocol.md](../../../docs/ai/commit-protocol.md),
[compatibility-policy.md](../../../docs/ai/compatibility-policy.md), [pragmatic-engineering.md](../../../docs/ai/pragmatic-engineering.md),
[review-core.md](../../../docs/ai/review-core.md), [technical-patterns.md](../../../docs/ai/technical-patterns.md),
[false-positive-guide.md](../../../docs/ai/false-positive-guide.md). Preflight + ledger (freeze paths as work
proceeds; note whether a patch bump is owed). Empty Open and no owed bump →
“nothing to fix”. Empty Open but an owed bump → skip fixes; standalone continues
at step 4, and a composed run leaves the bump to the parent.

When composed by `x-overhaul`, inherit its scope, starting HEAD, and ledger;
run steps 1–3 only. The parent owns the final gate and version. Empty in-scope
Open skips fixes, not the parent's audit, validation, or owed bump.

## Protocol

### 1. Triage (CRITICAL → LOW)

Per entry before edit: code/callers/tests + guides (+ [design-patterns.md](../../../docs/ai/design-patterns.md)
if design); reproduce from current code; dedupe root causes; disproven → remove
the entry with refutation evidence; real and disproportionate to fix safely →
Won't Fix + reason. A break the user has not
accepted → leave Open; do not ship it. Missing evidence, failed validation, or
ownership overlap is a blocker, not a disposition: leave Open.

### 2. One finding → one commit (blocking)

1. Root-cause fix + focused regression.
2. Trace SWMR, COW, crash, cleanup, compatibility.
3. Drop that Open entry (code + tests + migration docs + registry = unit).
4. Per-unit validation ([commit-protocol.md](../../../docs/ai/commit-protocol.md)).
5. Stage freeze + fix paths; inspect cached; commit before next.

Registry-only disposition = one unit. Same root cause may batch symptoms. Mutating
agents never parallel (read/validate may).

### 3. Self-review

Review `starting_HEAD..HEAD` + this invocation’s uncommitted paths with
`review-core.md` §3 evidence and the FP guide; registry per §5.
New confirmed → Open → same one-finding loop. Stop on no-progress or baseline overlap.

### 4. Final gate and version

[commit-protocol.md](../../../docs/ai/commit-protocol.md) once. Required checks pass and no in-scope Open
remains → the owed bump. No tag. No autonomous major.
Blocked → retain validated local commits and report remaining Open; the owed bump
stays for the next run.

## Output

Dispositions, fixes, disproven entries removed / Won't Fix, validations, compatibility, hashes/subjects, version, baseline left alone.
