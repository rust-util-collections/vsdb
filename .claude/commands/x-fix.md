# Fix Audit Backlog

You are resolving every open finding in `.claude/audit.md`, then self-reviewing and committing the result.

**How this differs from `/x-commit`:**
- `/x-commit` = "I've made changes — review them and commit." (starts from uncommitted diff)
- `/x-fix` = "Work through the audit backlog — fix, verify, commit." (starts from `.claude/audit.md`)

## Setup

1. **MANDATORY**: Read `.claude/docs/technical-patterns.md` — bug pattern reference.
2. Read `.claude/docs/review-core.md` — review methodology.
3. Read `.claude/docs/false-positive-guide.md` — consult before reporting any finding.
4. Read `.claude/audit.md` — this is your **primary work list**.

## Phase 1: Fix

### Task 1: Triage

1. Read `.claude/audit.md`. If no `## Open` entries exist, report "nothing to fix" and stop.
2. Sort open findings by severity: CRITICAL → HIGH → MEDIUM → LOW.
3. For each finding, read the code at the reported location with full context (100+ lines).
4. For each affected subsystem, read the corresponding pattern file from `.claude/docs/patterns/`.

### Task 2: Fix

For each open finding, in severity order:

1. **Understand** the root cause — read the code, trace call sites, understand the invariant being violated.
2. **Implement** a complete fix. The fix must:
   - Fully resolve the finding — not a band-aid, not a workaround
   - Not introduce new issues (check boundary conditions, error paths, concurrency)
   - Respect the SWMR contract, COW semantics, and crash safety invariants
   - Follow the project's existing code style and conventions
3. **Verify** the fix by reading the modified code and tracing its effects.
4. If the finding **cannot be fixed** (technical limitation, disproportionate risk, or would require architectural redesign), move it to `## Won't Fix` with a clear `**Reason**` explaining why.

### Task 3: Validate

1. After all fixes are applied, re-read every modified file to check for regressions.
2. Run `make fmt` to ensure formatting is consistent.
3. Run `make lint` to catch any new warnings.

### Task 4: Update Audit Registry

1. Remove all fixed entries from `## Open`.
2. For entries moved to `## Won't Fix`, add the `**Reason**` field.
3. Write the updated `.claude/audit.md`.

## Phase 2: Self-Review

1. Run `git diff HEAD` to see all changes from audit fixes.
2. If the diff is empty, report "nothing to commit" and stop.
3. Execute the `/x-review` Execution Protocol on the diff — including invariant checks, boundary conditions, concurrency (SWMR), and crash safety analysis.
4. Cross-reference every finding with `false-positive-guide.md`.
5. If the review produces **new findings**:
   - Fix them immediately.
   - Update `.claude/audit.md`.
   - Repeat until `## Open` has zero entries (or only Won't Fix).

## Phase 3: Commit

1. Run `make fmt`.
2. Run `git diff HEAD --stat` and `git log -5 --oneline` to understand scope and style.
3. Stage all changed files with `git add` (specific files, not `-A`).
4. Bump patch version if any `.rs` files changed (see `/x-commit` Task 4 for the 3-file version update).
5. Draft a commit message summarizing the audit fixes.
6. Commit using a HEREDOC — **do NOT include any co-author line**:

```
git commit -m "$(cat <<'EOF'
<commit message here>
EOF
)"
```

7. Run `git status` to verify success.

## Output Format

```
## Audit Fix Summary

**Open before**: N findings
**Fixed**: X
**Won't Fix**: Y (moved with reasons)

### Self-Review
**New findings**: N (all resolved)

### Commit
**Commit**: <short hash> <subject line>
```
