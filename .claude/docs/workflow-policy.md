# Workflow Safety and Atomic Commit Policy

Canonical safety policy for `/x-review`, `/x-commit`, `/x-fix`, and
`/x-overhaul`.

## 1. Preflight

Before mutation or commit:

1. Record `git status --short`, branch, and `HEAD`.
2. Separate staged, unstaged, and untracked baseline changes.
3. Stop for an in-progress merge/rebase/cherry-pick or detached `HEAD` unless
   the user resolves it explicitly.
4. Define invocation-owned files/hunks. Existing unrelated work remains owned
   by its author.

A globally clean tree is optional; a clean ownership boundary is mandatory.

## 2. Preserve existing work

- Never use `git stash`, `git clean`, `git checkout --`, `git restore`, or
  destructive `git reset` to manufacture a clean tree.
- Never revert, overwrite, stage, or commit unrelated baseline changes.
- Stop when required work overlaps an existing change inseparably.
- Review agents are read-only. Parallelize only independent investigation or
  validation; edits and commits in one worktree are sequential.

## 3. Atomic commits

One independent issue, root cause, or behavior change gets one commit.

- Include required tests, public/migration docs, and audit update.
- Same-root symptoms may share a commit; unrelated cleanup may not.
- Stage exact paths/hunks, never `git add -A`; inspect `git diff --cached`.
- Create new commits only. Never amend, rebase, rewrite/reset history, filter
  history, or force-push.
- These workflows create local commits only and never push.

## 4. Safe validation

- Run the smallest relevant validation before each unit and the workspace gate
  once after the final behavior change.
- A dirty-tree validation covers everything present. When other units could
  affect the result, validate `HEAD` plus only the candidate unit in a
  disposable worktree/copy. Never use a stash; remove temporary worktrees.
- VSDB tests already isolate data with globally unique prefixes. Automated
  skills must not delete `$HOME/.vsdb` or shared `/tmp/vsdb_testing`; use direct
  Cargo test commands rather than cleanup-bearing `make test`.
- If Cargo discovers an unexpected test source, check `git ls-files` and
  `git status` before attributing its failure. Ignore—but never delete—another
  session's untracked/ignored scratch test.
- Fix unit-caused failures. Report proven pre-existing failures without
  claiming success.
- Stop a repeated no-progress failure loop and report the blocker.

## 5. Audit dispositions

- `Open`: confirmed actionable defect/debt.
- `Won't Fix`: confirmed defect/debt whose safe fix is disproportionate.
- `Rejected`: recurring/material claim disproven by current code; not severity.

Resolved history belongs in Git/CHANGELOG. Audit entries contain no dates or
freshness markers.
