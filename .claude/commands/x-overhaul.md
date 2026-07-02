---
description: Full codebase overhaul — audit all source files, fix every finding, and commit
---

# Full Codebase Audit-Fix-Commit Pipeline

You are performing a full codebase audit: review ALL source files (not just uncommitted changes), fix every finding, and commit.

## Phase 1: Full Codebase Review

Execute `/x-review all` — the full audit protocol.

1. Read the Setup section of `.claude/commands/x-review.md` and load all required documentation.
2. Perform the **Full Audit Protocol** (the `all` mode section at the end of x-review.md):
   - Launch parallel agents per subsystem
   - Each agent reads all source files in its subsystem, loads the corresponding pattern guide
   - Perform deep analysis: invariants, boundary conditions, failure paths, concurrency, unsafe audit
3. Aggregate and deduplicate all findings.
4. Manage `docs/audit.md` — prune fixed entries, merge new findings sorted by severity.

## Phase 2: Fix

Execute the full `/x-fix` protocol.

1. Read `docs/audit.md`.
2. Fix every open finding — 100% resolution is the goal.
3. Move truly unfixable items to `## Won't Fix` with reasons.
4. Run `make fmt` and `make lint` to validate.
5. Update `docs/audit.md`.

If fixes introduced new issues, re-review the CHANGED files only (not the full codebase again) and fix any new findings. Iterate until `docs/audit.md` has zero open entries (or only Won't Fix).

## Phase 3: Commit

Execute Tasks 3–5 of `.claude/commands/x-commit.md` (Format & Lint → Bump Patch
Version → Commit). Key points:

1. `make fmt`, then `make lint` — must pass clean.
2. Bump patch version — mandatory if any `.rs` file changed (3-file update).
3. Draft a commit message covering the audit scope and fixes applied.
4. Stage specific files with `git add` (not `-A`), commit via HEREDOC —
   **no co-author line** — then `git status` to verify success.

## Output Format

```
## Full Audit Pipeline Summary

### Review (full codebase)
**Subsystems audited**: <list>
**Total findings**: N (X critical, Y high, Z medium, W low)

### Fix
**Fixed**: X | **Won't Fix**: Y | **Remaining**: 0

### Commit
**Commit**: <short hash> <subject line>
```
