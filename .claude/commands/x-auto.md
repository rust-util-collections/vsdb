# Automated Review-Fix-Commit Pipeline

Execute three phases in strict sequence: review, fix, commit. Each phase must complete before the next begins.

## Phase 1: Review

Execute the full `/x-review` protocol on all uncommitted changes (`git diff HEAD`).

1. Read the Setup section of `.claude/commands/x-review.md` and load all required documentation.
2. Perform the complete review (all tasks in the Execution Protocol).
3. Manage `.claude/audit.md` — prune fixed entries, merge new findings.

## Phase 2: Fix

Execute the full `/x-fix` protocol.

1. Read `.claude/audit.md`.
2. Fix every open finding — 100% resolution is the goal.
3. Move truly unfixable items to `## Won't Fix` with reasons.
4. Run `make fmt` and `make lint` to validate.
5. Update `.claude/audit.md`.

If fixes introduced changes that warrant re-review, repeat Phase 1 on the new diff and Phase 2 on any new findings. Iterate until `.claude/audit.md` has zero open entries (or only Won't Fix).

## Phase 3: Commit

1. Run `make fmt`.
2. Run `git diff HEAD --stat` and `git log -5 --oneline` to understand scope and style.
3. Stage all changed files with `git add` (specific files, not `-A`).
4. Draft a commit message covering both the original work and any review fixes.
5. Commit using a HEREDOC — **do NOT include any co-author line**:

```
git commit -m "$(cat <<'EOF'
<commit message here>
EOF
)"
```

6. Run `git status` to verify success.

## Output Format

```
## Auto Pipeline Summary

### Review
**Subsystems**: <list>
**New findings**: N

### Fix
**Fixed**: X | **Won't Fix**: Y | **Remaining**: 0

### Commit
**Commit**: <short hash> <subject line>
```
