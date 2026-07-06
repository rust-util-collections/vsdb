---
description: Deep regression review of VSDB changes (latest commit, N commits, hash, range, or full audit)
argument-hint: "[N | all | <hash> | <hash1>..<hash2>] [--fix]"
---

# Deep Regression Analysis for VSDB

You are performing a deep code review of changes to VSDB, a versioned key-value database built on mmdb.
This review combines VSDB-specific pattern analysis with Claude Code's native
multi-agent review architecture: **dimensional review agents → adversarial
verification → completeness critic → structured report**.

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

---

## Unified Protocol

All modes follow the same 7-phase structure. Mode-specific adaptations are noted inline.

### Phase 1: Scope & Context

**All modes**:
1. Identify affected subsystems using the **subsystem mapping table in `review-core.md` Phase 1**
2. Load the pattern guide (`.claude/docs/patterns/`) for each affected subsystem

**Diff modes** (empty, N, hash, range):
3. Read the full diff (`git diff <range>`)
4. Classify each change per `review-core.md` Phase 2 (COW, unsafe, control flow, error handling, etc.)

**`all` mode**: all subsystems affected; load all pattern guides.

### Phase 2: Parallel Multi-Dimensional Review

This is the core of the review. Launch agents that cover distinct review *dimensions* —
different ways of seeing the same code, not just different files.

---

#### A. Diff modes (empty, N, hash, range)

Launch **4 agents in parallel**. Each receives: the full diff, affected subsystem list,
and relevant pattern guide excerpts.

**Agent 1 — Correctness Bugs** (reads changed files with full context):
- COW violations, ref-count imbalance, B+ tree invariants (technical-patterns.md Categories 1-2)
- Merkle proof errors, encoding round-trip breakage (Categories 3, 6)
- Prefix collision, shard routing mismatch (Category 4)
- Error handling gaps: partial failure leaving inconsistent state
- Concurrency: SWMR contract violations in unsafe/shadow code
- Reference: `technical-patterns.md`; pattern guide invariants

**Agent 2 — Diff-Only Bugs** (scans diff surface, no extra context):
- Syntax errors, type errors, missing imports (will not compile)
- Clear logic errors in the diff alone (inverted conditions, off-by-one)
- Unreachable code, dead branches introduced by the change
- Missing `// SAFETY:` comment on new unsafe blocks
- Ignore anything that requires surrounding code to validate

**Agent 3 — Cross-Cutting & Performance** (all changed files, context-aware):
- Crash safety: if `kill -9` hits at this line, is dirty flag set? Is state recoverable?
- Performance: does this add serialization/clone/allocation on hot paths (get, iter, B+ tree lookup)?
- API compatibility: does this change observable behavior for existing callers?
- Resource lifecycle: are prefixes, NodeIds, handles properly released on error paths?

**Agent 4 — Code Style & Conventions** (all changed files):
- No `#[allow(...)]` — fix warnings, don't suppress
- Prefer imports over inline paths (3+ uses of same path in a file → add `use`)
- Import grouping: merge common prefixes (`use std::sync::{Arc, Mutex};`)
- Doc-code alignment: public API changes must update docs (CLAUDE.md, review-core.md, pattern guides)
- Unsafe audit: every `unsafe` block must have accurate `// SAFETY:` comment; `shadow()` callers must enforce SWMR

---

#### B. `all` mode (full audit)

Full audit uses **two layers** — subsystem depth first, then cross-cutting breadth.

**Layer 1 — Subsystem Audit (parallel, agents per subsystem mapping table in `review-core.md`)**:

Launch one agent per subsystem, each with a self-contained prompt:
1. Exact file list for the subsystem
2. Full content of the corresponding pattern guide from `.claude/docs/patterns/`
3. Instructions to read `technical-patterns.md` and `false-positive-guide.md`
4. The code style rules from Agent 4
5. High-signal-only rule: flag only confirmed bugs, not style preferences

**Layer 2 — Cross-Cutting Review (2 agents, parallel, launched after Layer 1 completes)**:

Once all subsystem agents report, launch 2 agents that read **ALL source files** with a
global lens. These catch what subsystem-isolated agents miss.

**Agent A — Cross-Cutting & Performance** (all files):
- Crash safety: dirty flag consistency, state recoverability across kill-9 scenarios
- COW/ref-count invariants across ALL subsystems
- Merkle proof and encoding round-trip consistency
- Prefix collision and shard routing correctness globally
- Resource lifecycle: prefixes, NodeIds, handles released on all paths
- API consistency: naming and signatures consistent across subsystems

**Agent B — Code Style & Conventions** (all files):
- No `#[allow(...)]` suppressions; all warnings fixed
- Import grouping consistent across ALL files
- Every `unsafe` block has accurate `// SAFETY:` comment; SWMR contracts documented
- Doc-code alignment: CLAUDE.md, review-core.md, pattern guides match current code
- Public API documentation complete and accurate

---

**CRITICAL — High-signal gate (applies to ALL agents in ALL modes)**:

Only report findings with **concrete failure scenarios**:
- Code that will definitely fail to compile
- Code that will definitely produce wrong results on realistic input
- Clear invariant violations from `technical-patterns.md`
- Concrete crash / leak / corruption / data-loss scenarios

Do NOT flag: style preferences, "consider" suggestions without concrete downside,
linter-caught issues, or anything matching `false-positive-guide.md` patterns.

### Phase 3: Adversarial Verification

For each finding from Phase 2, launch **3 verification agents in parallel**. Each agent:
1. Re-reads the reported code location with **full context**
2. Is instructed to **try to REFUTE** the finding — find concrete reasons it is NOT a real bug
3. Cross-references with `false-positive-guide.md`
4. Returns: `{confirmed: bool, evidence: string}`

**Survival rule**: a finding is CONFIRMED only if **≥2 of 3** verification agents confirm
it as real. Findings with 0–1 confirmations are discarded.

This adversarial pattern prevents plausible-but-wrong findings from surviving.

If zero findings emerged from Phase 2, skip this phase.

### Phase 4: Completeness Critic

Launch **one final review agent** that audits the review itself:
- What subsystems, files, or functions were NOT examined?
- What invariants from the pattern guides were NOT verified?
- What edge cases (empty keys, boundary values, crash recovery paths) were NOT checked?
- What cross-subsystem interactions were missed?

If gaps are found, loop back to Phase 2 with the specific gap as new scope (launch
targeted agents for the missing coverage only). If no gaps remain, proceed.

If zero findings emerged from Phase 2 and the completeness critic finds no gaps,
skip directly to Phase 6 (no audit.md changes needed).

### Phase 5: Audit Registry

1. Read `docs/audit.md` (create if absent)
2. **Prune**: Remove `## Open` entries that are 100% fixed in current code
3. **Merge**: Add confirmed findings under `## Open`, deduplicating against existing entries.
   Sort by severity: CRITICAL → HIGH → MEDIUM → LOW
4. **Re-evaluate Won't Fix**: For each `## Won't Fix` entry, re-read the code.
   Promote to `## Open` if now fixable; remove if no longer applicable; keep if reason still holds
5. Write updated `docs/audit.md`. **Never include timestamps, dates, or time-based markers.**

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

### Phase 6: Report

Use the **ReportFindings** tool with the confirmed findings (empty array if none).
Then output a terminal summary:

```
## Review Summary

**Scope**: <commits/diff description or "full audit">
**Subsystems**: <list>
**Findings**: N (X critical, Y high, Z medium, W low)

## Findings
(one line per finding: severity, location, one-line summary)
```

If zero findings:
`**Result**: LGTM — no regressions found. Coverage: <subsystems and invariants checked>.`

### Phase 7: Fix (if --fix)

If `--fix` was passed and findings exist:
1. Apply each fix to the working tree
2. Re-report findings via ReportFindings with `outcome` set (`fixed`, `skipped`, `no_change_needed`)
