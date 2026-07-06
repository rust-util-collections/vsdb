---
description: Deep regression review of VSDB changes (latest commit, N commits, hash, range, or full audit)
argument-hint: "[N | all | <hash> | <hash1>..<hash2>] [--fix]"
---

# Deep Regression Analysis for VSDB

You are performing a deep code review of changes to VSDB, a versioned key-value database built on mmdb.
This review combines VSDB-specific pattern analysis with Claude Code's multi-agent review architecture.

## Setup

1. **MANDATORY**: Read `.claude/docs/technical-patterns.md` first — your bug pattern reference.
2. Read `.claude/docs/review-core.md` — review methodology and subsystem mapping.
3. Read `.claude/docs/false-positive-guide.md` — consult before reporting any finding.

## Input

Arguments: `$ARGUMENTS`

Parse to determine scope; `--fix` flag means apply verified fixes after review.
Use the session's current effort level (no explicit override — review depth scales with it naturally).

| Input | Scope |
|-------|-------|
| *(empty)* | Latest commit |
| `N` (integer) | Last N commits |
| `all` | Full codebase audit |
| `<commit hash>` | Specific commit |
| `<hash1>..<hash2>` | Commit range |

Skip to **Full Audit Protocol** for `all`; otherwise use the Execution Protocol below.

## Execution Protocol (diff-based reviews)

### Phase 1: Context & Classification

1. Read the full diff (`git diff <range>`)
2. Identify ALL affected subsystems using the **subsystem mapping table in `review-core.md` Phase 1**
3. For each affected subsystem, read its pattern guide from `.claude/docs/patterns/`
4. Classify each change per `review-core.md` Phase 2 (COW, unsafe, control flow, error handling, etc.)

### Phase 2: Parallel Multi-Agent Review

Launch **4 review agents in parallel**, each focusing on a different dimension.
Each agent receives: the full diff, the PR/summary context, the list of affected subsystems,
and the relevant pattern guide excerpts.

**Agent 1 — Correctness Bugs** (deep context read):
Scan for bugs that require understanding surrounding code. Focus on:
- COW violations, ref-count imbalance, B+ tree invariants (technical-patterns.md Categories 1-2)
- Merkle proof errors, encoding round-trip breakage (Categories 3, 6)
- Prefix collision, shard routing mismatch (Category 4)
- Error handling gaps: partial failure leaving inconsistent state
- Concurrency: SWMR contract violations in unsafe/shadow code
- Only flag issues with concrete failure scenarios (see false-positive-guide.md)

**Agent 2 — Diff-Only Bugs** (diff surface scan):
Scan ONLY the diff lines without reading extra context. Flag:
- Syntax errors, type errors, missing imports (will not compile)
- Clear logic errors visible in the diff alone (inverted conditions, off-by-one)
- Unreachable code, dead branches introduced by the change
- Missing `// SAFETY:` comment on new unsafe blocks
- Ignore anything that requires surrounding code to validate

**Agent 3 — Cross-Cutting & Performance** (context-aware):
Check every change for:
- Crash safety: if `kill -9` hits at this line, is dirty flag set? Is state recoverable?
- Performance: does this add serialization/clone/allocation on hot paths (get, iter, B+ tree lookup)?
- API compatibility: does this change observable behavior for existing callers?
- Resource lifecycle: are prefixes, NodeIds, handles properly released on error paths?

**Agent 4 — Code Style & Conventions** (project rules):
Check changed files against:
- No `#[allow(...)]` — fix warnings, don't suppress
- Prefer imports over inline paths (3+ uses of same path in a file → add `use`)
- Import grouping: merge common prefixes (`use std::sync::{Arc, Mutex};`)
- Doc-code alignment: public API changes must update docs (CLAUDE.md, review-core.md, pattern guides)
- Unsafe audit: every `unsafe` block must have accurate `// SAFETY:` comment; `shadow()` callers must enforce SWMR

**CRITICAL: Only report HIGH SIGNAL issues.** Flag only:
- Code that will definitely fail to compile
- Code that will definitely produce wrong results
- Clear invariant violations from technical-patterns.md
- Concrete crash/leak/corruption scenarios

Do NOT flag: style preferences, "consider" suggestions without concrete downside, issues a linter catches, issues matching false-positive-guide.md patterns.

### Phase 3: Verification

For each finding from Phase 2 agents, launch a **verification agent** that:
1. Re-reads the reported code location with full context
2. Attempts to CONFIRM or REFUTE the finding against actual code
3. Cross-references with `false-positive-guide.md`
4. Returns only CONFIRMED findings with concrete evidence

Filter out any finding not confirmed by its verification agent.

### Phase 4: Audit Registry

1. Read `docs/audit.md` (create if absent)
2. **Prune**: Remove `## Open` entries that are 100% fixed in current code
3. **Merge**: Add confirmed findings under `## Open`, deduplicating against existing entries. Sort by severity (CRITICAL → HIGH → MEDIUM → LOW)
4. **Re-evaluate Won't Fix**: For each `## Won't Fix` entry, re-read the code. Promote to `## Open` if now fixable; remove if no longer applicable; keep if reason still holds
5. Write updated `docs/audit.md`. **Never include timestamps, dates, or time-based markers.**

Format:

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

### Phase 5: Report

Use the **ReportFindings** tool with the confirmed findings. Then output a terminal summary:

```
## Review Summary

**Scope**: <commits/diff description>
**Subsystems**: <list>
**Findings**: N (X critical, Y high, Z medium, W low)

## Findings
(one line per finding with severity and location)
```

If zero findings: `**Result**: LGTM — no regressions found. Coverage: <subsystems and invariants checked>.`

### Phase 6: Fix (if --fix)

If `--fix` was passed and findings exist:
1. Apply each fix to the working tree
2. Re-report findings via ReportFindings with `outcome` set (`fixed`, `skipped`, `no_change_needed`)

---

## Full Audit Protocol (for `all` mode)

### Strategy: Parallel Subsystem Audit

Launch **one Agent per subsystem** in parallel, using the subsystem mapping table in `review-core.md` Phase 1 (9 agents).

Each agent's prompt must be self-contained and include:
1. Exact file list for the subsystem (expand directory paths from the mapping table)
2. Full content of the corresponding pattern guide from `.claude/docs/patterns/`
3. Instructions to read `technical-patterns.md` and `false-positive-guide.md`
4. The code style rules from Agent 4
5. High-signal-only rule: flag only confirmed bugs, not style preferences

### Aggregation

After all agents complete:
1. Collect all findings
2. Launch verification agents for each finding (Phase 3)
3. Deduplicate cross-subsystem findings
4. Update audit registry (Phase 4)
5. Report with ReportFindings + terminal summary (Phase 5)
6. Fix if --fix (Phase 6)
