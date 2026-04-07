# VSDB Crash & Corruption Debugger

You are debugging a crash, data corruption, or incorrect behavior in VSDB.

## Setup

1. **MANDATORY**: Read `.claude/docs/technical-patterns.md` — your bug pattern reference.
2. Read `.claude/docs/review-core.md` — methodology for systematic analysis.
3. After initial analysis, load relevant subsystem patterns from `.claude/docs/patterns/`.

## Input

The user will provide one or more of:
- A panic/crash backtrace
- A failing test case or reproduction steps
- A description of incorrect behavior (e.g., "data from branch A appears on branch B")
- A corrupted database directory for analysis
- An error message or log output

## Execution Protocol

### Task 1: Symptom Classification

Map the symptom to a bug category from `technical-patterns.md`:

| Symptom | Likely Categories |
|---------|-------------------|
| Panic/crash | 5.x Unsafe, 1.x B+ Tree |
| Cross-branch data contamination | 1.1 COW Violation, 2.x Versioning |
| Deleted data reappears | 2.1 Ref-Count Imbalance, 2.4 Rollback |
| Merge loses data | 2.2 Three-Way Merge |
| Wrong Merkle root | 3.x Merkle Trie |
| Key lookup returns None for existing key | 4.2 Shard Mismatch, 6.1 Encoding |
| Unbounded disk growth | 1.3 Structural Sharing Leak, 2.1 Ref-Count |
| Proof verification fails | 3.1 Proof Mismatch, 3.3 SMT Default Hash |

### Task 2: Root Cause Investigation

Based on classification, investigate systematically:

**For B+ tree corruption:**
1. Check if the corrupted node was modified in-place (COW violation)
2. Check key ordering within the node and between parent/children
3. Check split/merge logic for the node's occupancy level
4. Trace the mutation path: which operation produced this node?

**For versioning bugs:**
1. Trace the commit DAG — are all parent pointers valid?
2. Check ref-counts for every commit on the affected branches
3. For merge bugs: reconstruct the three-way diff (base, source, target)
4. For GC bugs: check if the deleted commit was still reachable

**For Merkle trie bugs:**
1. Verify proof path matches the key's nibble/bit path
2. Check node serialization consistency between build and verify
3. For SMT: verify default hash constants
4. Check trie cache version against current commit

**For engine/shard bugs:**
1. Verify prefix computation for the affected data structure
2. Check shard routing: `prefix % 16` on both read and write
3. Check WriteBatch scope — does it cross shards?

**For encoding bugs:**
1. Test encode/decode round-trip for the affected type
2. Check if the type's serialization is deterministic
3. Check for postcard version changes in recent Cargo updates

### Task 3: Hypothesis Verification

For each hypothesis:
1. Write a mental test: "If hypothesis X is correct, then Y should be true"
2. Verify Y by reading code, running tests, or checking state
3. If Y is false, discard hypothesis and try next
4. If Y is true, look for additional confirming evidence

### Task 4: Fix Proposal

For the confirmed root cause:
1. Propose a minimal fix with exact code changes
2. Explain why the fix is correct (which invariant it restores)
3. Identify what tests should be added to prevent regression
4. Check if the same bug pattern exists elsewhere in the codebase

## Output Format

```
## Debug Report

**Symptom**: <one-line description>
**Root Cause**: <one-line description>
**Category**: <reference to technical-patterns.md pattern>
**Severity**: CRITICAL / HIGH / MEDIUM

### Investigation

<Step-by-step explanation of how you identified the root cause>

### Root Cause Detail

**Where**: file:line_range
**What**: <detailed explanation>
**Trigger**: <exact conditions that trigger the bug>

### Proposed Fix

<Code diff or detailed description>

### Regression Test

<Test case that would catch this bug>

### Related Code

<Other locations where the same pattern might exist>
```
