---
name: x-fix
description: Resolve the VSDB audit backlog sequentially, with one finding per validated local commit. Use only when the user explicitly invokes /x-fix.
disable-model-invocation: true
---

# Fix the VSDB Audit Backlog

Clear actionable `docs/audit.md` Open → self-review → local commits. Never push.
User-invoked only. New commits only.

## Setup

Read `workflow-policy.md`, `commit-protocol.md`, `compatibility-policy.md`,
`pragmatic-engineering.md`, `review-core.md`, `technical-patterns.md`,
`false-positive-guide.md`. Preflight + ledger (freeze paths as work proceeds).
Empty Open → “nothing to fix”.

## Protocol

### 1. Triage (CRITICAL → LOW)

Per entry before edit: code/callers/tests + guides (+ `design-patterns.md` if design);
reproduce from current code; dedupe root causes; false → Rejected; real but unsafe/disproportionate → Won't Fix + reason.

### 2. One finding → one commit (blocking)

1. Root-cause fix + focused regression.
2. Trace SWMR, COW, crash, cleanup, compatibility.
3. Drop that Open entry (code + tests + migration docs + registry = unit).
4. Per-unit validation (`commit-protocol.md`).
5. Stage freeze + fix paths; inspect cached; commit before next.

Registry-only disposition = one unit. Same root cause may batch symptoms. Mutating
agents never parallel (read/validate may).

### 3. Self-review

Review `starting_HEAD..HEAD` + remaining worktree via `/x-review` evidence rules.
New confirmed → Open → same one-finding loop. Stop on no-progress or baseline overlap.

### 4. Final gate, version, tag

Full gate once. Rust source changed → lockstep version-and-tag once for the invocation
(major + migration if break). Finish with no Open unless blocked (report blocker).
No Resolved section or dates in audit.

## Output

Dispositions, fixes, Rejected/Won't Fix, validations, compatibility, hashes/subjects, version/tag, baseline left alone.
