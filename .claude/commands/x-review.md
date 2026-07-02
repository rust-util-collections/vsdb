---
description: Deep regression review of VSDB changes (latest commit, N commits, hash, range, or full audit)
argument-hint: "[N | all | <hash> | <hash1>..<hash2>]"
---

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
2. Identify ALL affected subsystems using the **subsystem mapping table in
   `review-core.md` Phase 1** (single source of truth)
3. For EACH affected subsystem, read its pattern guide from `.claude/docs/patterns/`
   (the table lists which guide covers which subsystem — skip guides for unaffected subsystems)
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
   - `.claude/docs/review-core.md` Phase 1 subsystem mapping table (the single source of truth)
   - `.claude/docs/patterns/` guides — referenced file lists and invariants

### Task 5: Unsafe Code Audit

If ANY `unsafe` block is added or modified:
1. Verify SAFETY comment exists and is accurate
2. For `shadow()`: verify SWMR enforcement at call site
3. For `from_bytes()`: verify input comes from trusted source
4. For pointer casts: verify no aliasing violation
5. Verify no undefined behavior

### Task 6: Audit Registry (docs/audit.md)

After completing the analysis:

1. Read `docs/audit.md` from the project root (create if absent).
2. **Prune**: For each entry under `## Open`, verify against the current codebase. Remove entries that are 100% fixed.
3. **Merge**: Add new findings from this review under `## Open`, deduplicating against existing entries. Sort by severity (CRITICAL → HIGH → MEDIUM → LOW).
4. **Re-evaluate Won't Fix**: For each entry under `## Won't Fix`, re-read the
   code at the reported location and assess whether the reasoning still holds
   against the **current** codebase.  The "Won't Fix" label is a snapshot
   judgment made at a past point in time — surrounding code may have changed,
   new callers may have been added, or a previously-disproportionate fix may
   now be straightforward.  For each entry:
   - If the original reason still holds → leave it in place.
   - If the code has changed such that the finding is now fixable with
     reasonable effort → promote it to `## Open` with an updated assessment.
   - If the code has changed such that the finding is no longer applicable →
     remove it entirely.
   Never silently carry forward a Won't Fix entry without fresh evaluation.
5. Write the updated `docs/audit.md`.

The file format:

```markdown
# Audit Findings

> Auto-managed by /x-review and /x-fix.

## Open

### [SEVERITY] subsystem: one-line summary
- **Where**: file:line_range
- **What**: description
- **Why**: invariant/pattern violated
- **Suggested fix**: how to fix

---

## Won't Fix

### [SEVERITY] subsystem: one-line summary
- **Where**: file:line_range
- **What**: description
- **Reason**: why this cannot or should not be fixed
```

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

Launch **one Agent per subsystem** in parallel — one per row of the subsystem
mapping table in `review-core.md` Phase 1 (9 agents total; `encoding & common`
also covers `strata/src/lib.rs` and `strata/src/common/mod.rs`).

Agents are **stateless** — each prompt must be self-contained and include:
1. The exact file list for the subsystem (expand the directory paths from the mapping table)
2. The full content-or-path of the corresponding pattern guide from `.claude/docs/patterns/`
3. Instructions to read `technical-patterns.md` and `false-positive-guide.md`
4. The code style rules from Task 4, and the finding output format below

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
