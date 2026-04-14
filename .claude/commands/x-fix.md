# Fix All Audit Findings

You are resolving every open finding recorded in `.claude/audit.md`. The goal is 100% resolution — no partial fixes, no TODOs left behind.

## Setup

1. Read `.claude/audit.md` — this is your work list.
2. Read the `/x-review` skill file (`.claude/commands/x-review.md`) Setup section — load the same documentation it references (technical patterns, review methodology, false-positive guides, subsystem patterns).

## Execution Protocol

### Task 1: Triage

1. Read `.claude/audit.md`. If no `## Open` entries exist, report "nothing to fix" and stop.
2. Sort open findings by severity: CRITICAL → HIGH → MEDIUM → LOW.
3. For each finding, read the code at the reported location with full context (100+ lines).

### Task 2: Fix

For each open finding, in severity order:

1. **Understand** the root cause — read the code, trace call sites, understand the invariant being violated.
2. **Implement** a complete fix. The fix must:
   - Fully resolve the finding — not a band-aid, not a workaround
   - Not introduce new issues (check boundary conditions, error paths, concurrency)
   - Follow the project's existing code style and conventions
3. **Verify** the fix by reading the modified code and tracing its effects.
4. If the finding **cannot be fixed** (technical limitation, disproportionate risk, or would require architectural redesign), move it to `## Won't Fix` with a clear `**Reason**` explaining why.

### Task 3: Validate

1. After all fixes are applied, re-read every modified file to check for regressions.
2. Run `make fmt` to ensure formatting is consistent.
3. Run `make lint` (or the project's lint command) to catch any new warnings.

### Task 4: Update Audit Registry

1. Remove all fixed entries from `## Open`.
2. For entries moved to `## Won't Fix`, add the `**Reason**` field.
3. Write the updated `.claude/audit.md`.

## Output Format

```
## Fix Summary

**Open before**: N findings
**Fixed**: X
**Won't Fix**: Y (moved with reasons)
**Remaining**: Z

### Fixes Applied

- [SEVERITY] summary — file:line — what was done

### Won't Fix

- [SEVERITY] summary — reason
```
