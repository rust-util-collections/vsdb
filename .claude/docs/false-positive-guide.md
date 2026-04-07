# VSDB False Positive Guide

Before reporting any finding, check it against this guide. If a finding matches a false positive pattern below, either suppress it or explicitly note why it does NOT apply.

---

## FP-1: Rust Ownership System Already Prevents It

**Pattern**: Reporting a use-after-free, double-free, or dangling reference in safe Rust code.
**Rule**: If the code is safe Rust (no `unsafe` block), the borrow checker prevents these at compile time. Only report memory safety issues inside `unsafe` blocks or when raw pointers are involved.
**Exception**: Logical use-after-free (e.g., using a NodeId after the node has been GC'd) is still valid even in safe code.

## FP-2: SWMR Contract is Caller's Responsibility

**Pattern**: Reporting a data race in `shadow()` or mutable collection access.
**Rule**: VSDB explicitly documents that callers must enforce single-writer, multi-reader. If a `shadow()` call has a `// SAFETY:` comment citing SWMR enforcement, do not report it as a race. Only report if:
1. The safety comment's SWMR claim is contradicted by the call site (e.g., no lock visible)
2. Two shadow handles are used for concurrent writes without serialization

## FP-3: Prefix Isolation Makes Cross-Structure Interference Impossible

**Pattern**: Reporting that structure A's operations could affect structure B's data.
**Rule**: Each data structure has a unique u64 prefix. All keys are prefix-scoped. Cross-structure interference requires a prefix collision (Pattern 4.1), which is a separate finding. Do not report cross-structure issues unless you can demonstrate a concrete prefix collision.

## FP-4: Unwrap/Expect on Known-Valid State

**Pattern**: Reporting `unwrap()` or `expect()` as potential panics.
**Rule**: Many `unwrap()` calls are on states guaranteed by prior logic. Before reporting:
1. Check if the Option/Result is populated by a prior operation in the same scope
2. Check if the calling condition guarantees the value
3. Check if it's in test code (panics are acceptable in tests)
**When to report**: Only if you can construct a concrete scenario where the unwrap WILL fail in production.

## FP-5: Clippy Would Catch It

**Pattern**: Reporting a lint that `cargo clippy` with `#![deny(warnings)]` already enforces.
**Rule**: The CI runs clippy with deny-all-warnings. Do not duplicate clippy's work. Focus on semantic correctness.

## FP-6: "Consider" Without Concrete Downside

**Pattern**: Suggesting a refactor, adding error handling, or "defensive" code without identifying a specific failure scenario.
**Rule**: Every finding must have a concrete scenario where the current code produces a wrong result, crashes, or leaks resources.

## FP-7: Test-Only Code Held to Production Standards

**Pattern**: Reporting error handling, performance, or resource management issues in test code.
**Rule**: Test code has different standards. Tests may use `unwrap()`, hardcoded paths, single-threaded assumptions. Only report if the test itself is incorrect (testing the wrong thing).

## FP-8: Intentional Unsafe with Safety Comment

**Pattern**: Reporting an `unsafe` block that has a `// SAFETY:` comment explaining why it's sound.
**Rule**: Read the safety comment first. Only report if:
1. The safety comment's prerequisites are NOT actually guaranteed
2. A subsequent change has invalidated the safety comment's assumptions
3. The safety comment is missing or vague

## FP-9: Performance Issue Without Hot Path Evidence

**Pattern**: Reporting an allocation, clone, or serialization as a performance issue without evidence it's on a hot path.
**Rule**: VSDB has clear hot and cold paths:
- **Hot**: get(), iter next(), B+ tree point lookup, trie hash computation
- **Warm**: commit, merge inner loop, B+ tree split/merge
- **Cold**: branch create/delete, GC, initialization, rollback
Only report performance issues on hot/warm paths.

## FP-10: COW Allocation is By Design

**Pattern**: Reporting that a mutation "unnecessarily" allocates a new node.
**Rule**: COW requires new allocation on every mutation — this is the core design, not a bug. Only report if:
1. A mutation modifies a node in-place WITHOUT allocating a new NodeId (Pattern 1.1)
2. The same node is allocated twice for the same logical mutation (redundant COW)

## FP-11: Ref-Count Checked at Wrong Granularity

**Pattern**: Reporting a ref-count leak by analyzing a single function in isolation.
**Rule**: Ref-count operations often span multiple functions (increment in create, decrement in delete). Before reporting:
1. Trace the full lifecycle: creation → usage → destruction
2. Check both the increment AND decrement call sites
3. Verify whether rollback/error paths also handle the ref-count

## FP-12: Merkle Root Changes Are Expected After Mutations

**Pattern**: Reporting that the Merkle root changed after an insert/delete.
**Rule**: This is the entire point of a Merkle trie — the root hash changes when the underlying data changes. Only report if:
1. The root changes WITHOUT any underlying data change (spurious root update)
2. The root does NOT change when data DID change (missed update)
3. Two different datasets produce the same root (collision — catastrophic)
