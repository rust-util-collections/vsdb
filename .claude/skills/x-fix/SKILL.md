---
name: x-fix
description: Resolve the VSDB audit backlog sequentially, with one finding per validated local commit. Use only when the user explicitly invokes /x-fix.
disable-model-invocation: true
---

# Fix the VSDB Audit Backlog

Resolve every actionable `docs/audit.md` entry, self-review the fixes, and
create local commits. Never push.

## Setup

1. Read `.claude/docs/workflow-policy.md`,
   `.claude/docs/commit-protocol.md`,
   `.claude/docs/compatibility-policy.md`,
   `.claude/docs/review-core.md`,
   `.claude/docs/technical-patterns.md`, and
   `.claude/docs/false-positive-guide.md`.
2. Run preflight and record the commit-protocol invocation ledger.
3. Read `docs/audit.md`. If `Open` is empty, report "nothing to fix" and stop.

## Protocol

### 1. Triage

Process CRITICAL → HIGH → MEDIUM → LOW. Before editing each finding:

1. Re-read cited code, callers, tests, and mapped guides.
2. Reproduce its trigger from current code.
3. Deduplicate entries sharing one root cause.
4. Move a disproven recurring claim to `Rejected` with evidence.
5. Move a real but currently disproportionate issue to `Won't Fix` with reason.

### 2. Fix one finding, then commit it

**One finding/root cause per commit is blocking.**

1. Implement the complete root-cause fix.
2. Add focused regression coverage.
3. Trace SWMR, COW, crash, cleanup, and compatibility effects.
4. Remove that `Open` entry; code, tests, migration docs, and registry update
   form one commit unit.
5. Run per-unit validation, stage exact paths/hunks, inspect the cached diff,
   and commit before starting the next finding.

A registry-only disposition for one finding is one unit. Mutating fix agents
never run in parallel.

### 3. Self-review

1. Review `starting_HEAD..HEAD` plus remaining worktree changes using
   `/x-review` evidence/verification rules.
2. Process each newly confirmed issue through the same one-finding loop.
3. Stop on unsafe baseline overlap or repeated no-progress validation failure;
   never stash, reset, or rewrite prior commits.

### 4. Final gate and version

Run the final workspace gate. If Rust source changed, apply the lockstep version
policy once for the invocation. A breaking change requires the major-version
and migration protocol.

`docs/audit.md` ends with no unresolved `Open` entry unless execution is blocked
and reported. Never add a `Resolved` history section or freshness markers.

## Output

Report initial dispositions, fixes, rejected/deferred entries, validations,
compatibility result, every commit hash/subject, version, and untouched baseline.
