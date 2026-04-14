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
4. Manage `.claude/audit.md` — prune fixed entries, merge new findings sorted by severity.

## Phase 2: Fix

Execute the full `/x-fix` protocol.

1. Read `.claude/audit.md`.
2. Fix every open finding — 100% resolution is the goal.
3. Move truly unfixable items to `## Won't Fix` with reasons.
4. Run `make fmt` and `make lint` to validate.
5. Update `.claude/audit.md`.

If fixes introduced new issues, re-review the CHANGED files only (not the full codebase again) and fix any new findings. Iterate until `.claude/audit.md` has zero open entries (or only Won't Fix).

## Phase 3: Commit

Execute the full commit protocol from `/x-commit`:

1. Bump patch version (Task 4 of x-commit.md) — mandatory if any `.rs` file changed.
2. Run `make fmt`.
3. Run `git diff HEAD --stat` and `git log -5 --oneline` to understand scope and style.
4. Stage all changed files with `git add` (specific files, not `-A`).
5. Draft a commit message covering the audit scope and fixes applied.
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
## Full Audit Pipeline Summary

### Review (full codebase)
**Subsystems audited**: <list>
**Total findings**: N (X critical, Y high, Z medium, W low)

### Fix
**Fixed**: X | **Won't Fix**: Y | **Remaining**: 0

### Commit
**Commit**: <short hash> <subject line>
```
