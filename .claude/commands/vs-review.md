# Deep Regression Analysis for VSDB

You are performing a deep code review of changes to VSDB, a versioned key-value database built on mmdb.

## Setup

1. **MANDATORY**: Read `.claude/docs/technical-patterns.md` first — this is your bug pattern reference.
2. Read `.claude/docs/review-core.md` — this is your review methodology.
3. Read `.claude/docs/false-positive-guide.md` — consult before reporting any finding.

## Input

Arguments: `$ARGUMENTS`

Parse the arguments to determine review scope:

| Input | Scope | How |
|-------|-------|-----|
| *(empty)* | Latest commit | `git diff HEAD~1`, `git log -1` |
| `N` (integer) | Last N commits | `git diff HEAD~N`, `git log -N --oneline` |
| `all` | Full codebase audit | Read all source files by subsystem (see Full Audit Protocol below) |
| `<commit hash>` | Specific commit | `git diff <hash>~1 <hash>` |
| `<hash1>..<hash2>` | Commit range | `git diff <hash1> <hash2>` |

For diff-based reviews (everything except `all`), proceed to the Execution Protocol below.
For `all`, skip to the **Full Audit Protocol** section at the end of this document.

## Execution Protocol

### Task 1: Context & Classification

1. Read the full diff carefully
2. Identify ALL affected subsystems by mapping changed files:
   - `core/src/common/engine/` → engine, shard routing
   - `core/src/basic/mapx_raw/` → raw KV layer
   - `strata/src/basic/mapx/`, `mapx_ord/` → typed collections
   - `strata/src/basic/persistent_btree/` → B+ tree
   - `strata/src/versioned/` → versioning, commit DAG, merge
   - `strata/src/trie/` → Merkle tries
   - `strata/src/slotdex/` → slot indexing
   - `strata/src/dagmap/` → DAG collections
   - `strata/src/common/ende.rs` → encoding
3. For EACH affected subsystem, read the corresponding pattern file:
   - `.claude/docs/patterns/btree.md`
   - `.claude/docs/patterns/versioning.md`
   - `.claude/docs/patterns/trie.md`
   - `.claude/docs/patterns/slotdex.md`
   - `.claude/docs/patterns/dagmap.md`
   - `.claude/docs/patterns/engine.md`
4. Classify each change per the review-core methodology

### Task 2: Deep Regression Analysis

For each HIGH or CRITICAL classified change:

1. **Read the surrounding code** — at least 50 lines of context around each change
2. **Trace call sites** — use grep/LSP to find all callers of changed functions
3. **Check invariants** — verify each invariant from review-core.md Phase 3.1
4. **Boundary conditions** — check edge cases from review-core.md Phase 3.2
5. **Failure paths** — analyze error handling per review-core.md Phase 3.3
6. **Concurrency** — verify SWMR contract per review-core.md Phase 3.4

For each finding:
- Cross-reference with `technical-patterns.md` — which pattern does it match?
- Cross-reference with `false-positive-guide.md` — is this a known false positive?
- Only report if you have **concrete evidence**

### Task 3: Cross-Cutting Analysis

Check every change for:
1. **Crash safety** — what happens if `kill -9` hits at this exact line? Is dirty flag set?
2. **Performance** — does this add overhead to hot paths?
3. **API compatibility** — does this change observable behavior?

### Task 4: Code Style Enforcement

Check changed files against project style rules:

1. **No lint suppression** — `#[allow(...)]` is forbidden. All warnings must be fixed at the source.
2. **Prefer imports over inline paths** — Avoid inline `std::foo::Bar::new()` when the same path appears 3+ times in a file. Function-body `use` (scoped imports) are fine. 1-2 inline uses are acceptable.
3. **Import grouping** — Imports with common prefix must be merged: `use std::sync::{Arc, Mutex};`
4. **Doc-code alignment** — If the change modifies a public function signature, struct field, module structure, or adds/removes/renames a public type or module, verify docs still match. Specifically check:
   - `CLAUDE.md` architecture table (subsystem paths, type names, serialization crate)
   - `CLAUDE.md` conventions (unsafe count, dependency names)
   - `.claude/docs/review-core.md` subsystem path mappings (Phase 1)
   - `.claude/commands/vs-review.md` full-audit subsystem partitioning table
   - `.claude/docs/patterns/` guides — referenced file lists and invariants

### Task 5: Unsafe Code Audit

If ANY `unsafe` block is added or modified:
1. Verify SAFETY comment exists and is accurate
2. For `shadow()`: verify SWMR enforcement at call site
3. For `from_bytes()`: verify input comes from trusted source
4. For pointer casts: verify no aliasing violation
5. Verify no undefined behavior

## Output Format

Report findings as:

```
## Review Summary

**Commit**: <hash> <subject>
**Subsystems**: <list of affected subsystems>
**Risk Level**: CRITICAL / HIGH / MEDIUM / LOW

## Findings

### [SEVERITY] subsystem: one-line summary

**Where**: file:line_range
**What**: Description
**Why**: Invariant/pattern violated (cite technical-patterns.md)
**Fix**: Suggested fix or questions

---

(repeat for each finding)

## No Issues Found

(list areas checked where no issues were found, to demonstrate coverage)
```

If zero findings after full analysis, report:
```
## Review Summary
**Result**: LGTM — no regressions found
**Coverage**: <list of subsystems and invariants checked>
```

---

## Full Audit Protocol (for `all` mode)

When `$ARGUMENTS` is `all`, perform a full codebase audit.

### Strategy: Parallel Subsystem Audit

Launch **one Agent per subsystem** in parallel. Each agent receives:
1. The subsystem file list to read
2. The corresponding pattern file from `.claude/docs/patterns/`
3. `technical-patterns.md` and `false-positive-guide.md`
4. The code style rules from Task 4

### Subsystem Partitioning

| Subsystem | Files | Pattern Guide |
|-----------|-------|---------------|
| engine & raw KV | `core/src/common/engine/mmdb.rs`, `core/src/common/mod.rs`, `core/src/basic/mapx_raw/` | `engine.md` |
| typed collections | `strata/src/basic/mapx/`, `strata/src/basic/mapx_ord/`, `strata/src/basic/mapx_ord_rawkey/`, `strata/src/basic/orphan/` | `engine.md` |
| B+ tree | `strata/src/basic/persistent_btree/` | `btree.md` |
| versioning | `strata/src/versioned/` (mod.rs, map.rs, handle.rs, diff.rs, merge.rs) | `versioning.md` |
| Merkle tries | `strata/src/trie/` (mpt/, smt/, node/, cache.rs, proof.rs) | `trie.md` |
| slot index | `strata/src/slotdex/` | `slotdex.md` |
| DAG collections | `strata/src/dagmap/` | `dagmap.md` |
| encoding & common | `strata/src/common/ende.rs`, `strata/src/common/macros.rs`, `strata/src/lib.rs` | (cross-cutting) |

### Aggregation

After all agents complete:
1. Collect all findings
2. Deduplicate cross-subsystem findings
3. Sort by severity: CRITICAL → HIGH → MEDIUM → LOW
4. Output a unified audit report:

```
## Full Audit Report

**Scope**: All source files
**Subsystems Audited**: <list>
**Total Findings**: N (X critical, Y high, Z medium, W low)

## Findings

(sorted by severity, grouped by subsystem)

## Clean Areas

(subsystems with no findings — list what was checked)
```
