---
name: x-overhaul
description: Review scoped changes in VSDB, fix confirmed in-scope defects, and create atomic local commits. Invoke explicitly as $x-overhaul; empty scope is the latest commit and all selects the full repository.
---

# Review, fix and commit VSDB

Read [the workflow policy](../../../docs/ai/workflow-policy.md) and
[the review guide](../../../docs/ai/review-core.md). Resolve repository paths from the Git
root; links above are relative to this file. Read invocation arguments from
ordinary user prompt text.

Accept one scope (`N`, `all`, `staged`, `worktree`, commit or `A..B`). Empty
means the latest commit; only `all` means the full repository. Reject `--fix`,
multiple scopes and unknown options. This explicit invocation authorizes
atomic local fix commits. `staged`/`worktree` also authorizes committing the
intended scoped changes after review.

1. Record one baseline, starting HEAD, frozen scope and ownership ledger for
   the whole invocation. Apply the shared scope-resolution rules. For `all`,
   inventory tracked files and record reviewed paths/invariants and coverage
   gaps across source/tests, build/CI, tools, docs and agent instructions.
2. Review with the repository's evidence standard and subsystem guides. Read
   full implementations, callers, guards and tests; refute each candidate.
   Historical scopes fix only still-present defects rooted in that diff;
   callers provide evidence, not unrelated fix scope. Reconcile confirmed
   findings with the audit without clearing unrelated entries.
3. For `staged`/`worktree`, review, fix and commit the owned changes under the
   [x-commit protocol](../x-commit/SKILL.md), using the frozen outer scope.
   Preserve staged/unstaged boundaries and unrelated baseline. Otherwise leave
   baseline changes uncommitted. Do not commit a known-defective unit.
4. Recheck and resolve new findings and still-applicable in-scope Open entries
   under the [x-fix protocol](../x-fix/SKILL.md). Work sequentially, one root
   cause per commit with its regression, docs and audit removal. Keep blocked
   or unverified findings Open; do not relabel them just to clear the list.
   Continue independent safe units. Registry-only dispositions form a separate
   documentation commit after fixes when needed.
5. Re-review the aggregate diff and interactions. Run the final applicable
   repository gates once on the stable code state; `all` runs the full audit
   gate even if no fix was needed. Reuse unchanged-state results. Apply the
   repository's documented version protocol once, only after required checks
   pass with no in-scope Open findings or coverage gaps. Do not accept a public
   API or persisted-format break or choose a major bump on the user's behalf.
6. Report scope, coverage/gaps, dispositions, validation, compatibility, local
   hashes/subjects, version outcome and preserved baseline. Keep validated
   commits if later work is blocked. No changes and no owed version step means
   no empty commit.

This composes review/fix/commit procedures under one invocation and ledger;
do not launch nested standalone workflows or repeat their finalization.
Standalone read-only/no-commit rules do not suppress this invocation's fixes
or commits. Language-specific validation and version rules stay in the linked
repository policy. Never automatically create a branch, tag, push, open a PR,
publish, deploy or rewrite history.
