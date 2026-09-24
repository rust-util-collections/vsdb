# Atomic Commit and Version Protocol

Validate → commit → version for `/x-commit`, `/x-fix`, `/x-overhaul`.
Use with `.claude/docs/workflow-policy.md` and `.claude/docs/compatibility-policy.md`.

## Invocation ledger

Before first edit, record:

- start `HEAD`, branch, and the three version locations at that `HEAD` and in the worktree;
- staged / unstaged / untracked baseline;
- **frozen owned paths** (sorted) and planned units;
- whether a patch bump is already owed (below);
- whether any unit is a public/on-disk break the user has not accepted.

Keep the ledger across commits. Stage only freeze set + this-invocation fix/format paths.

## Per-unit validate and commit

1. One issue/root cause/behavior change + its tests/docs/migration/audit only.
   Agent-written units leave `CHANGELOG.md` to the release commit; a user’s own
   CHANGELOG edit stays with the unit it documents.
2. Checks:
   - Docs/config only: `git diff --check` + structure sanity; skip Rust gates.
   - Rust: `cargo fmt --all -- --check`; if needed, format only owned files and
     inspect the diff. Do not use `make fmt`.
   - Rust: targeted package lint/check for the affected targets — no `#[allow(...)]`;
     the final gate runs the workspace Cargo lint below. Reuse results on unchanged code.
3. Smallest proving tests (no global cleanup):
   - core → `cargo test -p vsdb_core <filter>`;
   - strata → `cargo test -p vsdb <filter>`;
   - cross-crate / public / persisted → package suites or
     `cargo test --workspace --tests`.
4. On fail: fix if unit-caused; else report pre-existing with evidence.
5. Stage exact freeze + unit fix/format paths — never `git add -A`. Index already
   holds baseline → do not stage; `git commit --only -F - -- <unit paths>` commits
   exactly those paths and leaves the baseline staged.
6. `git diff --cached` (or, with `--only`, `git diff HEAD -- <unit paths>`) =
   exactly one unit, no baseline/post-freeze paths.
7. Match repo commit style; HEREDOC multi-line; no co-author/generated-by.
8. Verify commit; compare `git status --short` to baseline.

Never amend a prior commit for a later fix.

## Final workspace gate

After last behavior commit (once per stable code state):

1. `cargo fmt --all -- --check`
2. `cargo clippy --workspace`
3. `cargo check --workspace --tests`
4. `cargo check --workspace --benches`
5. `cargo test --workspace --tests`
6. `cargo test --workspace --release --tests`

Regression → new atomic commit, then re-run affected checks. Reuse completed
checks on the same code/manifest state; do not repeat a gate just because another
skill composed this protocol. Docs-only: skip Rust gates unless the caller is
performing a full audit or a bump is owed — a release always needs the full gate
passed on its code state. A required failing/blocked check prevents release.

## Lockstep version (no tag)

A patch bump is **owed** when tracked `.rs` changed after the latest commit that
changed `version = ` in `core/Cargo.toml`, including `.rs` this invocation will
commit. Docs-only does not owe a bump. Do not create a git tag. Never push.

```bash
base=$(git log -1 --format=%H -G'^version = ' -- core/Cargo.toml)
git diff --name-only "$base" HEAD -- '*.rs'   # non-empty → owed
```

Re-run after the last behavior commit; that result decides, not the ledger’s.

Bump only after the required gate passes and no in-scope Open or coverage gap
remains. If blocked, keep the validated commits and report; the owed bump stays
detectable. A later run with nothing else to commit still finishes it — do not
stop at “nothing to commit”.

Compatible only: current agreed `X.Y.Z` → `X.Y.(Z+1)`, once per owed release, from
the manifests now — not a stale start-HEAD. Already at that target → verify only.
The three versions must agree. Divergence, or a lower version → report; do not overwrite.

A public/on-disk break is not accepted by the agent. Do not ship that unit unless
the user explicitly accepted it in this conversation; otherwise leave it
uncommitted and Open. If they already accepted it, bump `(X+1).0.0` instead of
the patch, and put the migration in the behavior commits
(`.claude/docs/compatibility-policy.md`).

Release commit `chore: bump version to X.Y.Z` updates exactly `core/Cargo.toml`,
`strata/Cargo.toml`, the root `vsdb_core` dep, and `CHANGELOG.md` when
`## [vX.Y.Z]` is missing at HEAD (an owned uncommitted draft is completed, not
duplicated). Any of those files with unowned baseline edits → stop and report;
never sweep them. The section summarizes user-visible changes in
`git log "$base"..HEAD`, grouped under the `###` headings prior sections use
(Fixed / Changed / Added / Breaking …); do not invent entries or duplicate an
existing section. Then `cargo metadata --no-deps --format-version 1` must show
both package versions and the workspace dependency. No empty commit. Do not
force-add `Cargo.lock`.

## Final state

Report hashes/subjects, compatibility, version. No tag. Owned work committed;
unrelated baseline untouched.
