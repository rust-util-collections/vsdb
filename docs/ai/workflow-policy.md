# Workflow Safety and Atomic Commit Policy

SSOT safety for `$x-review`, `$x-commit`, `$x-fix`, `$x-overhaul`. Skills must
not weaken it. See also `pragmatic-engineering.md`.

**Hard rules:** user-invoked only · local commits only (never push) · no history
rewrite · one independent issue per commit · no auto tag · no autonomous major.

## Skill discovery and invocation

[AGENTS.md](../../AGENTS.md) is the shared repository entrypoint. Skills live
under [`.agents/skills/`](../../.agents/skills/) and are explicitly invoked with
`$x-review`, `$x-fix`, `$x-commit` or `$x-overhaul`. Each skill's
`agents/openai.yaml` sets `allow_implicit_invocation: false`. Read arguments from
the user's invocation text and execute preflight commands explicitly; skill
files do not substitute arguments or execute embedded shell commands.
Ordinary tasks do not implicitly invoke a commit or release workflow. These
instructions describe workflow scope; they do not grant tool permissions or
replace the runtime's approval controls. An explicit user request to push is
separate authorization; none of these skills performs that step on its own.

## 1. Preflight

Before mutate/commit:

1. Record `git status --short`, branch, `HEAD`.
2. Separate staged / unstaged / untracked baseline.
3. Stop on merge/rebase/cherry-pick or detached HEAD unless the user resolves it.
4. Define this invocation’s owned files/hunks; baseline stays with its author.
5. **Commit workflows:** freeze owned paths (+ planned units) before review edits.
   Stage only freeze set + this invocation’s fix/format paths — never paths that
   appeared later from concurrent work.
   Register newly discovered fix paths before editing, after checking their
   current diffs against the baseline; a new path is not automatically owned.

Dirty tree OK; clear ownership required.

## 2. Preserve existing work

- No `stash` / `clean` / `checkout --` / `restore` / destructive `reset` to fake a clean tree.
- Never touch unrelated baseline (revert, overwrite, stage, commit).
- `docs/audit.md` is the registry exception: every skill may update it, including a
  read-only review. That is not a code write. Merge; do not revert unrelated edits there.
- If a needed fix overlaps baseline and cannot be separated safely → stop and report.
- Review agents read-only. Parallelism: investigation/validation only. Edits and
  commits on one tree: sequential.

## 3. Atomic commit units

One issue / root cause / behavior change → one commit.

- Bundle only its tests, public/migration docs, and audit update.
- Multiple symptoms only if same root cause.
- No drive-by cleanup, format churn, or refactors.
- Stage exact paths/hunks; no broad `git add -A`, `-u`, `.`, or `git commit -a`. Inspect `git diff --cached` before every commit.
- New commits only — no amend, rebase, history rewrite, or force-push. No remote
  push or automatic PR. Do not run destructive `make clean_all` as part of a skill.

## 4. Validation and failure

- Smallest relevant checks per unit; workspace gate once after last behavior change.
- Dirty-tree validation covers everything present. If other units can interfere,
  validate `HEAD` + only the candidate in a disposable worktree (no stash);
  remove it after:

  ```bash
  wt=$(mktemp -d /tmp/vsdb-wt.XXXXXX); git worktree add --detach "$wt" HEAD
  git diff HEAD -- <unit paths> | git -C "$wt" apply --index  # copy unit's untracked files too
  (cd "$wt" && CARGO_TARGET_DIR="$OLDPWD/target" cargo test -p <pkg> <filter>)
  git worktree remove --force "$wt"
  ```
- VSDB tests isolate via unique prefixes. The agent uses bare Cargo (`cargo test`,
  `cargo clippy`, `cargo check`, `cargo fmt`), not make targets, unless the user
  explicitly asks for one. `make test`, `make all`, and `make bench` use a
  per-invocation scratch `VSDB_BASE_DIR` and remove only that directory; they do
  not delete `$HOME/.vsdb`, `/tmp/vsdb_testing`, or caller-provided data.
- Storage-failure and restart regressions use focused test doubles or isolated
  child-process fixtures with invocation-owned temporary data. Describe the
  exact database operation and expected recovery result before adding a fixture.
- Unexpected test discovery: check `git ls-files` / `git status` before blame;
  never delete another session’s scratch test.
- Unit-caused failure → fix before commit. Pre-existing → report with evidence.
- Same failure repeats with no progress → stop and report.

## 5. Audit dispositions

| state | meaning |
|-------|---------|
| Open | confirmed, actionable |
| Won't Fix | real; safe fix currently disproportionate |

Disproven entries are removed, not retained in a Rejected section. Record a
useful refutation in the review output or commit history; routine noise needs
no registry entry.

Registry rules and entry shape: `review-core.md` §5.
