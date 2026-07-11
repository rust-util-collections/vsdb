---
name: x-commit
description: Review, fix, validate, and commit VSDB worktree changes as atomic commits. Use only when the user explicitly invokes /x-commit.
disable-model-invocation: true
---

# Self-Reviewing Commit for VSDB

Review all intended worktree changes, fix confirmed defects, validate them, and
create local commits. Never push.

## Setup

1. Read `.claude/docs/workflow-policy.md`.
2. Read `.claude/docs/commit-protocol.md`.
3. Read `.claude/docs/review-core.md`,
   `.claude/docs/technical-patterns.md`, and
   `.claude/docs/false-positive-guide.md`.
4. Read `.claude/docs/compatibility-policy.md` for public or persisted changes.
5. Run the workflow preflight and record the commit-protocol invocation ledger.

## Protocol

### 1. Establish scope

1. Read `git status --short`, staged and unstaged diffs, and every intended
   untracked file. `git diff HEAD` alone omits untracked files.
2. If no intended changes exist, report "nothing to commit" and stop.
3. Partition the worktree into commit units before editing:
   - one independent issue, root cause, or behavior change per unit;
   - required tests, docs, migration guidance, and audit update stay with it;
   - preserve pre-staged boundaries unless the user explicitly changes scope.
4. Stop if unrelated changes overlap inseparably; never stash, revert, or
   absorb them.
5. When multiple pre-existing units coexist, use the disposable-worktree
   procedure for isolated validation rather than claiming the combined tree
   proves each commit independently.

### 2. Review and fix

For each unit:

1. Map files through the Subsystem Map and load relevant guides.
2. Read complete functions, callers, error/crash paths, and tests.
3. Check COW/ref-count, DAG, trie proof, prefix/shard, SWMR/unsafe,
   staged-mutation, resource, and hot-path invariants as applicable.
4. Classify public/on-disk compatibility. A necessary break follows
   `compatibility-policy.md`; never hide it inside a patch release.
5. Refute candidates using the false-positive guide.
6. Fix retained defects completely and add focused regression coverage.
7. Re-review until clean; stop on repeated no-progress failure.

Read-only investigation may run in parallel. Edits and commits are sequential.

### 3. Validate and commit each unit

Apply `.claude/docs/commit-protocol.md`: run unit-appropriate deterministic
checks/tests, stage exact paths/hunks, inspect the cached diff, and create one
new commit. Never amend an earlier commit.

### 4. Final gate, version, and tag

After all behavior commits, run the final workspace gate and single lockstep
version-and-release-tag policy. Any regression found later is fixed in a new
focused commit.

## Output

Report reviewed files/subsystems, findings fixed, validations, compatibility
result, every commit hash/subject, version and release-tag result, and
untouched baseline work.
