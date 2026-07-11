---
name: x-overhaul
description: Audit the full VSDB repository, resolve every finding safely, and create atomic local commits. Use only when the user explicitly invokes /x-overhaul.
disable-model-invocation: true
---

# Full VSDB Audit-Fix-Commit Pipeline

Audit the complete repository, explicitly disposition every confirmed finding,
fix actionable findings, and create local commits. Never push.

## Setup

1. Read `.claude/docs/workflow-policy.md` and run preflight.
2. Read `.claude/skills/x-review/SKILL.md`,
   `.claude/skills/x-fix/SKILL.md`,
   `.claude/docs/commit-protocol.md`, and
   `.claude/docs/compatibility-policy.md`.
3. Record the commit-protocol invocation ledger before mutation.

## Phase 1: Full review

Follow `/x-review all` without `--fix`:

1. Build a tracked-file ledger for both crates' source/tests/benches/build
   scripts, manifests/CI, public docs, and `.claude/`.
2. Give read-only agents disjoint subsystem ownership; every Rust source file
   is accounted for exactly once in the depth pass.
3. Run focused cross-subsystem/completeness passes only for remaining gaps.
4. Verify and deduplicate candidates, including public/on-disk compatibility.
5. Fully re-evaluate all existing `Open`, `Won't Fix`, and `Rejected` entries.
6. If `docs/audit.md` changed, commit that review inventory as one
   documentation-only unit before fixes. It is a review snapshot, not a batched
   fix commit.

## Phase 2: Resolve findings

Follow `/x-fix`:

1. Process findings sequentially by severity.
2. Resolve each safely/completely, or record justified `Won't Fix`/`Rejected`.
3. Enforce one independent finding/root cause per validated commit.
4. Keep mutations sequential; parallelism is read-only investigation/validation.
5. Re-review changed files and process new findings through the same loop.

The goal is sound dispositions, not a cosmetic zero count at any cost.

## Phase 3: Final gate and version

Run the final workspace gate. Fix regressions in new focused commits. Apply one
lockstep version bump for the pipeline: patch for compatible changes, or a major
bump plus concrete migration documentation for an accepted break.

If nothing changed, create no empty commit or version bump.

## Output

Report coverage, dispositions, validations, compatibility/migration result,
every commit hash/subject, version, and untouched baseline.
