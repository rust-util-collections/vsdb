# VSDB Review Core Methodology

This document defines the systematic review protocol for VSDB code changes.

---

## Phase 1: Context Gathering

Before analyzing any change, gather context:

1. **Read the diff** — understand every changed line
2. **Identify affected subsystems** — map changes to:
   - `core/src/common/engine/` → engine, shard routing
   - `core/src/basic/mapx_raw/` → raw KV layer
   - `strata/src/basic/mapx/`, `mapx_ord/`, `mapx_ord_rawkey/` → typed collections
   - `strata/src/basic/persistent_btree/` → B+ tree
   - `strata/src/versioned/` → versioning, commit DAG, merge
   - `strata/src/trie/` → Merkle tries (MPT, SMT)
   - `strata/src/slotdex/` → slot indexing
   - `strata/src/dagmap/` → DAG collections
   - `strata/src/vecdex/` → vector index (HNSW)
   - `strata/src/common/ende.rs` → encoding/serialization
3. **Load subsystem patterns** — read the relevant `.claude/docs/patterns/<subsystem>.md`
4. **Check call sites** — use grep/LSP to find all callers of changed functions
5. **Check related tests** — identify which test files cover the changed code

## Phase 2: Change Classification

Classify each change into one or more categories:

| Category | Description | Risk Level |
|----------|-------------|------------|
| COW / structural sharing | Node allocation, NodeId creation, tree mutation | CRITICAL |
| Version DAG | Commit, branch, merge, rollback, GC | CRITICAL |
| Unsafe code | Any `unsafe {}` block, `shadow()`, `from_bytes()` | CRITICAL |
| Merkle proof | Proof generation, verification, hash computation | HIGH |
| Control flow | if/else, match, loop, early return changes | HIGH |
| Resource lifecycle | open/close, alloc/dealloc, prefix alloc/free | HIGH |
| Encoding | Key/value encode/decode, node serialization | HIGH |
| Shard routing | Prefix → shard mapping, MMDB integration | HIGH |
| Error handling | Result, Option, unwrap, expect, ? operator | MEDIUM |
| Configuration | Options, defaults, thresholds | LOW |
| Logging/metrics | tracing calls, stats updates | LOW |
| Test changes | New or modified test cases | LOW |

## Phase 3: Regression Analysis

For each HIGH or CRITICAL change, perform deep analysis:

### 3.1 Invariant Check
Identify the invariants that the changed code must maintain:
- **COW invariant**: Mutations allocate new nodes; old nodes are immutable
- **B+ tree ordering**: Keys sorted within each node; children partition the key space
- **Ref-count invariant**: Every live reference to a commit has a matching ref-count increment
- **Merge invariant**: Source-wins policy; no data loss on conflict
- **Prefix uniqueness**: No two data structures share a prefix
- **Shard consistency**: Read and write paths compute the same shard for the same prefix
- **Encoding round-trip**: `decode(encode(x)) == x` for all valid inputs
- **Proof soundness**: A valid proof must verify; an invalid proof must not
- **Dirty flag**: Set before mutation, cleared after completion

### 3.2 Boundary Condition Analysis
Check edge cases specific to the change:
- Empty collection / single entry
- B+ tree node at exact capacity (32 keys) — split trigger
- B+ tree node at minimum occupancy — merge trigger
- Single-branch repo vs multi-branch
- Merge with zero diffs (no-op merge)
- Merge where one branch is an ancestor of the other (fast-forward)
- 256-bit SMT path boundary (first/last bit)
- SlotDex at tier boundary
- Prefix = 0 or prefix = u64::MAX

### 3.3 Failure Path Analysis
For every new error path introduced:
- Does the error path clean up all acquired resources?
- Does partial failure leave the database in a consistent state?
- Is the dirty flag set correctly on error paths?
- Can the operation be retried safely after failure?
- Is the error propagated with sufficient context?

### 3.4 Concurrency Analysis
For changes touching shared state:
- Is the SWMR contract maintained? (single writer, multiple readers)
- Does `shadow()` usage have documented write exclusion?
- Is the MMDB singleton initialization safe?
- For cross-shard operations: is partial failure handled?

## Phase 4: Cross-Cutting Concerns

### 4.1 Crash Safety
If the change touches versioning, B+ tree mutation, or GC:
- What happens if the process crashes mid-operation?
- Is the dirty flag set before the operation begins?
- Are intermediate states recoverable?
- Can GC resume correctly after a crash?

### 4.2 Performance Regression
- Does this change add serialization on a hot path?
- Does this change the B+ tree depth or node fanout?
- Does this introduce unnecessary COW copies?
- Does this affect MMDB WriteBatch size or frequency?

### 4.3 API Contract
- Does the change alter observable behavior for existing users?
- Are new public APIs consistent with existing naming conventions?
- Do new options have sensible defaults?

### 4.4 Code Style Rules
These are enforced project conventions — violations are findings (severity LOW):
- **No lint suppression**: `#[allow(...)]` is forbidden. Warnings must be fixed, not silenced.
- **Prefer imports over inline paths**: Avoid `std::foo::Bar::new()` inline in function bodies when the same path appears 3+ times in a file; add a `use` import at file top instead. Function-body `use` statements (scoped imports) are fine. 1-2 inline uses of common `std::` items are acceptable.
- **Grouped imports**: Common prefixes must be merged — `use std::sync::{Arc, Mutex};` not two separate `use` lines.
- **Doc-code alignment**: Public API changes must have matching doc comment / README / CLAUDE.md updates. Stale docs are a finding. When a change adds, removes, or renames a public type, module, or subsystem path, also verify:
  - `CLAUDE.md` architecture table (paths, type names, dependency info)
  - `.claude/docs/review-core.md` subsystem path mappings
  - `.claude/commands/vs-review.md` full-audit subsystem partitioning table
  - `.claude/docs/patterns/` guides — referenced file lists and invariants

## Phase 5: Reporting

### Finding Format
For each finding, report:

```
[SEVERITY] subsystem: one-line summary

WHERE: file:line_range
WHAT: Description of the issue
WHY: Why this is a problem (reference invariant or pattern from technical-patterns.md)
FIX: Suggested fix (if clear) or questions to resolve
```

### Severity Levels
- **CRITICAL**: Data loss, corruption, undefined behavior, or cross-branch contamination
- **HIGH**: Incorrect results, resource leak, proof verification failure, or performance regression
- **MEDIUM**: Edge case bug, error handling gap, or minor performance issue
- **LOW**: Style, clarity, or non-functional improvement
- **INFO**: Observation or question, not necessarily a bug

### Quality Gate
Only report findings where you have **concrete evidence** from the code. Never report:
- Hypothetical issues without a specific triggering condition
- Style preferences not related to correctness
- "Consider" suggestions without a clear downside to the current code

Consult `.claude/docs/false-positive-guide.md` before finalizing any finding.
