# Atomic Commit and Version Protocol

Canonical validation/commit/version procedure for `/x-commit`, `/x-fix`, and
`/x-overhaul`. Apply with `workflow-policy.md` and `compatibility-policy.md`.

## Invocation ledger

Before editing, record:

- starting `HEAD`, branch, and the three version locations at `HEAD` and in the
  worktree;
- staged, unstaged, and untracked baseline paths;
- planned commit units;
- whether tracked Rust source changes;
- whether any unit is a public/on-disk compatibility break.

Keep this ledger across commits; later `git diff HEAD` cannot see earlier units.

## Per-unit validation and commit

For each independent unit:

1. Confirm one issue/root cause or behavior change plus only required tests,
   docs, migration guidance, and audit entry.
2. Deterministic checks:
   - Docs/config only: `git diff --check` plus affected path/structured-file
     validation; skip Rust lint/tests.
   - Rust source: `cargo fmt --all -- --check`, then `make lint`.
   - If formatting is needed, run `make fmt` only when resulting changes stay
     within invocation-owned files; inspect immediately.
3. Run the smallest proving tests without global cleanup:
   - core: targeted `cargo test -p vsdb_core <filter>`;
   - strata: targeted `cargo test -p vsdb <filter>`;
   - cross-crate/public/persisted behavior: relevant package suites or
     `cargo test --workspace --tests`.
4. Fix unit-caused failures; report proven pre-existing failures.
5. Stage exact paths/hunks, inspect `git diff --cached`, and verify one unit with
   no unrelated baseline work.
6. Match repository commit style and create a new commit. Use a HEREDOC for a
   multi-line message; omit co-author/generated-by trailers.
7. Verify the commit and compare `git status --short` with the baseline.

Never amend an earlier commit.

## Final workspace gate

After the final behavior change:

1. `cargo fmt --all -- --check`
2. `make lint`
3. `cargo test --workspace --tests`
4. `cargo test --workspace --release --tests`

Run once per unchanged final code state. Fix a regression in a new atomic commit
and repeat. Documentation-only workflows skip Rust validation.

## Lockstep version and release tag

If tracked Rust source changed, bump exactly once from the invocation-start
`HEAD` version:

- compatible change: `X.Y.Z` → `X.Y.(Z+1)`;
- accepted breaking change: `X.Y.Z` → `(X+1).0.0`, only after satisfying
  `compatibility-policy.md`.

Update exactly:

1. `core/Cargo.toml` package version;
2. `strata/Cargo.toml` package version;
3. root `Cargo.toml` workspace `vsdb_core` dependency version.

Both crates remain lockstep. If the intended target already exists in baseline,
verify it instead of incrementing again. Run
`cargo metadata --no-deps --format-version 1`, stage the three manifests
explicitly, inspect the cached diff, and commit release metadata separately.
Create an annotated git tag pointing at the release commit:
`git tag -a "vX.Y.Z" -m "vX.Y.Z"` (using the lockstep version). The tag must
use the `v` prefix.

`Cargo.lock` is intentionally ignored; do not force-add it. Skip the bump and
tag when no Rust source changed and never create an empty commit.

## Final state

Report all new commit hashes/subjects, compatibility result, version, and
release tag.
Invocation-owned work must be committed; unrelated baseline work stays intact.
