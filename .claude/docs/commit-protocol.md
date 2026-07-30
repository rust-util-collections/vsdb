# Atomic Commit and Version Protocol

Validate → commit → version for `/x-commit`, `/x-fix`, `/x-overhaul`.
Use with `workflow-policy.md` and `compatibility-policy.md`.

## Invocation ledger

Before first edit, record:

- start `HEAD`, branch, and the three version locations at that `HEAD` and in the worktree;
- staged / unstaged / untracked baseline;
- **frozen owned paths** (sorted) and planned units;
- whether tracked Rust source changes;
- whether any unit is a public/on-disk break.

Keep the ledger across commits. Stage only freeze set + this-invocation fix/format paths.

## Per-unit validate and commit

1. One issue/root cause/behavior change + its tests/docs/migration/audit only.
2. Checks:
   - Docs/config only: `git diff --check` + structure sanity; skip Rust gates.
   - Rust: `cargo fmt --all -- --check`; if needed, `make fmt` only on owned paths (inspect).
   - Rust: `make lint` — no `#[allow(...)]`.
3. Smallest proving tests (no global cleanup):
   - core → `cargo test -p vsdb_core <filter>`;
   - strata → `cargo test -p vsdb <filter>`;
   - cross-crate / public / persisted → package suites or
     `cargo test --workspace --tests`.
4. On fail: fix if unit-caused; else report pre-existing with evidence.
5. Stage exact freeze + unit fix/format paths — never `git add -A`.
6. `git diff --cached` = exactly one unit, no baseline/post-freeze paths.
7. Match repo commit style; HEREDOC multi-line; no co-author/generated-by.
8. Verify commit; compare `git status --short` to baseline.

Never amend a prior commit for a later fix.

## Final workspace gate

After last behavior commit (once per stable code state):

1. `cargo fmt --all -- --check`
2. `make lint`
3. `cargo test --workspace --tests`
4. `cargo test --workspace --release --tests`

Regression → new atomic commit, then re-run. Docs-only: skip Rust gates.

## Lockstep version and release tag

If any tracked `.rs` changed in this invocation, bump **once** from start-HEAD version:

- compatible → `X.Y.Z` → `X.Y.(Z+1)`;
- accepted break → `X.Y.Z` → `(X+1).0.0` only after `compatibility-policy.md`.

Update exactly: `core/Cargo.toml`, `strata/Cargo.toml`, root workspace
`vsdb_core` dep — lockstep. Baseline already at target → verify only.

Then: `cargo metadata --no-deps --format-version 1` → stage three manifests →
inspect → separate release commit → annotated tag `vX.Y.Z` on that commit.

Skip when no Rust source changed. No empty commits. Do not force-add `Cargo.lock`.

## Final state

Report hashes/subjects, compatibility result, version, tag. Owned work committed;
unrelated baseline untouched.
