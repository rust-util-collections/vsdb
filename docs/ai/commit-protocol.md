# Atomic Commit and Version Protocol

Validate → commit → bump version → tag the release for `$x-commit`, `$x-fix`, `$x-overhaul`.
Use with `docs/ai/workflow-policy.md` and `docs/ai/compatibility-policy.md`.

## Invocation ledger

Before first edit, record:

- start `HEAD`, branch, and the four version locations (`core/Cargo.toml`, `strata/Cargo.toml`, `migration/Cargo.toml`, root `Cargo.toml` `vsdb_core` dep) at that `HEAD` and in the worktree;
- staged / unstaged / untracked baseline;
- **frozen owned paths** (sorted) and planned units;
- whether a patch bump or release tag is already owed (below);
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
3. Smallest proving tests (no global cleanup). Launch every `cargo test`
   with a scratch base in the parent shell, then remove only that directory.
   Library unit tests and doctests call `Mapx::new` / `MapxRaw::new` without
   `vsdb_configure`. With `HOME` set and `VSDB_BASE_DIR` unset they open
   `$HOME/.vsdb`, take its shard locks, and advance the real prefix allocator.
   Unique prefixes separate collections inside whichever root is already open;
   they do not choose that root.

   ```bash
   scratch=$(mktemp -d /tmp/vsdb-test.XXXXXX)
   VSDB_BASE_DIR="$scratch" cargo test -p vsdb_core <filter>
   rm -rf "$scratch"
   ```

   - core → `cargo test -p vsdb_core <filter>`;
   - strata → `cargo test -p vsdb <filter>`;
   - migration → `cargo test -p vsdb_migrate <filter>`;
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

Apply `compatibility-policy.md` §Rust toolchain first: run the workspace gate
on the official latest stable toolchain. No MSRV floor is declared or checked.

After last behavior commit (once per stable code state):

1. `cargo fmt --all -- --check`
2. `cargo clippy --workspace`
3. `cargo check --workspace --tests`
4. `cargo check --workspace --benches`
5. `VSDB_BASE_DIR="$scratch" cargo test --workspace --tests`
6. `VSDB_BASE_DIR="$scratch" cargo test --workspace --release --tests`
7. `VSDB_BASE_DIR="$scratch" cargo test --workspace --doc`

`scratch` is `mktemp -d /tmp/vsdb-test.XXXXXX`, created in the parent shell
before step 5 and removed after step 7. Steps 1–4 do not open a database.
One scratch directory covers steps 5–7; do not point them at `$HOME/.vsdb`.

Regression → new atomic commit, then re-run affected checks. Reuse completed
checks on the same code/manifest state; do not repeat a gate just because another
skill composed this protocol. Docs-only: skip Rust gates unless the caller is
performing a full audit or a bump is owed — a release always needs the full gate
passed on its code state. A required failing/blocked check prevents release.

## Lockstep version and tag

A patch bump is **owed** when tracked `.rs` changed after the latest commit that
changed `version = ` in `core/Cargo.toml`, including `.rs` this invocation will
commit. Docs-only does not owe a bump. Every version bump requires a local
annotated `vX.Y.Z` tag on its release commit.

```bash
base=$(git log -1 --format=%H -G'^version = ' -- core/Cargo.toml)
git diff --name-only "$base" HEAD -- '*.rs'   # non-empty → owed
```

Re-run after the last behavior commit; that result decides, not the ledger’s.

Bump only after the required gate passes and no in-scope Open or coverage gap
remains. If blocked, keep the validated commits and report; the owed bump stays
detectable. A later run with nothing else to commit still finishes it.

Compatible only: current agreed `X.Y.Z` → `X.Y.(Z+1)`, once per owed release, from
the current manifests. If the release commit already exists, verify it and
complete the required tag.
The four versions must agree (`migration/Cargo.toml`'s `vsdb` path dependency major version must also match). Divergence, or a lower version → report; do not overwrite.

A public/on-disk break is not accepted by the agent. Do not ship that unit unless
the user explicitly accepted it in this conversation; otherwise leave it
uncommitted and Open. If they already accepted it, bump `(X+1).0.0` instead of
the patch (including `migration/Cargo.toml`'s `vsdb` dependency), and put the migration in the behavior commits
(`docs/ai/compatibility-policy.md`).

Release commit `chore: bump version to X.Y.Z` updates exactly `core/Cargo.toml`,
`strata/Cargo.toml`, `migration/Cargo.toml`, the root `vsdb_core` dep, and `CHANGELOG.md` when
`## [vX.Y.Z]` is missing at HEAD (an owned uncommitted draft is completed, not
duplicated). Any of those files with unowned baseline edits → stop and report;
never sweep them. The section summarizes user-visible changes in
`git log "$base"..HEAD`, grouped under the `###` headings prior sections use
(Fixed / Changed / Added / Breaking …); do not invent entries or duplicate an
existing section. Then `cargo metadata --no-deps --format-version 1` must show
all three workspace package versions (`vsdb_core`, `vsdb`, `vsdb_migrate`) and matching path dependencies. No empty commit. Do not
force-add `Cargo.lock`.

Check the intended tag name before making the release commit. If the name is
taken, accept it only when it is an annotated tag on the intended release
commit. A conflicting tag blocks the release. Report it and preserve it.
Once the release commit is verified, create any missing tag with
`git tag -a "$tag" "$release_commit" -m "$tag"`. Confirm that
`git cat-file -t "refs/tags/$tag"` returns `tag` and
`git rev-parse "refs/tags/$tag^{commit}"` returns the release commit hash.
Leave tags local.

If a release commit made under this tagging rule is missing its tag, verify
that commit and resume tagging even when no Rust changes are pending.

## Final state

Report hashes/subjects, compatibility, version, and release tag. Owned work committed;
unrelated baseline untouched.
