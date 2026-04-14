# Self-Reviewing Commit for VSDB

You are performing a self-reviewing commit: review all uncommitted changes, fix every issue found, format, and commit.

## Setup

1. **MANDATORY**: Read `.claude/docs/technical-patterns.md` — bug pattern reference.
2. Read `.claude/docs/review-core.md` — review methodology.
3. Read `.claude/docs/false-positive-guide.md` — consult before reporting any finding.

## Execution Protocol

### Task 1: Deep Self-Review

1. Run `git diff HEAD` to collect all uncommitted changes.
2. If the diff is empty, report "nothing to commit" and stop.
3. Identify ALL affected subsystems by mapping changed files:
   - `core/src/common/engine/` → engine, shard routing
   - `core/src/basic/mapx_raw/` → raw KV layer
   - `strata/src/basic/mapx/`, `mapx_ord/`, `mapx_ord_rawkey/` → typed collections
   - `strata/src/basic/persistent_btree/` → B+ tree
   - `strata/src/versioned/` → versioning, commit DAG, merge
   - `strata/src/trie/` → Merkle tries
   - `strata/src/slotdex/` → slot indexing
   - `strata/src/dagmap/` → DAG collections
   - `strata/src/vecdex/` → vector index
   - `strata/src/common/ende.rs` → encoding
4. For EACH affected subsystem, read the corresponding pattern file from `.claude/docs/patterns/`.
5. Perform the full regression analysis from review-core.md:
   - **Classify** each change (COW, version DAG, unsafe, control flow, encoding, etc.)
   - **Invariant check** — verify all invariants from Phase 3.1 of review-core.md
   - **Boundary conditions** — check edge cases from Phase 3.2
   - **Failure paths** — analyze error handling per Phase 3.3
   - **Concurrency** — verify SWMR contract per Phase 3.4
6. Check cross-cutting concerns:
   - **Crash safety** — dirty flag, intermediate states
   - **Performance** — hot path overhead
   - **API compatibility** — observable behavior changes
7. Enforce code style rules:
   - No `#[allow(...)]` — fix warnings at the source
   - Prefer imports over inline paths (3+ uses)
   - Grouped imports with common prefixes
   - Doc-code alignment for public API changes
8. Audit any added/modified `unsafe` blocks.
9. Cross-reference every finding with `false-positive-guide.md` — only retain findings with **concrete evidence**.

### Task 2: Fix All Findings

For EVERY finding from Task 1 (CRITICAL, HIGH, MEDIUM, or LOW):

1. Fix the issue completely — no TODOs, no "fix later", no partial fixes.
2. After all fixes are applied, re-run `git diff HEAD` and repeat Task 1 analysis on the new diff.
3. If new findings emerge from the fixes, fix those too. Iterate until the review is clean.
4. Report the final list of fixes applied.

### Task 3: Format

1. Run `make fmt` to apply code formatting.

### Task 4: Bump Patch Version — MANDATORY

**You MUST complete every step below before proceeding to Task 5. Do NOT skip this task.**

1. Run `git diff HEAD --name-only` — if it lists any `.rs` file, a version bump is required. Skip this task ONLY if every changed file is a non-code file (`.md`, `.toml` version-only, etc.).
2. Read `core/Cargo.toml` line 3 to get the current `version = "X.Y.Z"`.
3. Compute `NEW = X.Y.(Z+1)` (e.g., `13.4.0` → `13.4.1`).
4. Update these three locations with the NEW version:
   - `core/Cargo.toml` — `version = "NEW"`
   - `strata/Cargo.toml` — `version = "NEW"`
   - `Cargo.toml` (workspace root) — `vsdb_core = { path = "core", version = "NEW", ... }`
5. **Verify**: grep all three files for the NEW version string — all three must match. If any mismatch, fix it before continuing.

### Task 5: Commit

1. Run `git diff HEAD --stat` and `git log -5 --oneline` to understand scope and commit style.
2. Draft a commit message:
   - Follow the repo's existing commit message style (type prefix: `fix:`, `feat:`, `style:`, `refactor:`, etc.)
   - Summarize the "why" not the "what" — keep it concise (1-2 sentences for the subject)
   - Add a body with key details if the change spans multiple subsystems
3. Stage the relevant files with `git add` (specific files, not `-A`).
4. Commit using a HEREDOC — **do NOT include any co-author line**:

```
git commit -m "$(cat <<'EOF'
<commit message here>
EOF
)"
```

5. Run `git status` to verify the commit succeeded.

## Output Format

```
## Self-Review Commit Summary

**Reviewed**: <number of files changed>
**Subsystems**: <list>
**Findings**: <N found, N fixed> (or "0 — clean")
**Commit**: <short hash> <subject line>
```
