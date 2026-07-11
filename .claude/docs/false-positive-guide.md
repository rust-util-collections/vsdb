# VSDB False Positive Guide

Before reporting any finding, check it against this guide. If a finding matches a false positive pattern below, either suppress it or explicitly note why it does NOT apply.

---

## FP-1: Rust Ownership System Already Prevents It

**Pattern**: Reporting a use-after-free, double-free, or dangling reference in safe Rust code.
**Rule**: If the code is safe Rust (no `unsafe` block), the borrow checker prevents these at compile time. Only report memory safety issues inside `unsafe` blocks or when raw pointers are involved.
**Exception**: Logical use-after-free (e.g., using a NodeId after the node has been GC'd) is still valid even in safe code.

## FP-2: Alias Exclusion Is Caller's Responsibility

**Pattern**: Reporting a data race in `shadow()` or mutable collection access.
**Rule**: VSDB documents exclusion at the affected key/operation granularity.
Plain map aliases may write disjoint keys concurrently; same-key writes are
forbidden. Structural multi-key operations may require one writer for the
whole structure. If a `shadow()` call's `// SAFETY:` comment establishes the
relevant exclusion, do not report a race. Only report if:
1. The safety comment's exclusion claim is contradicted by the call site
2. Writers overlap on the same key or on a structural operation that requires
   broader serialization

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
**Rule**: Test code may use `unwrap()` and isolated hardcoded scratch paths.
Tests run in parallel, so they may NOT assume process-global allocator,
registry, base-dir, or environment state is exclusive. Serialize
`vsdb_set_base_dir`/environment mutation and make global-state assertions
race-tolerant. Report a test only when it is incorrect or unsafe under this
documented parallel model.

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

## FP-13: Audit Disposition Skipped Without Relevant Re-evaluation

**Pattern**: Treating `Won't Fix` or `Rejected` as permanent, or requiring every
narrow review to re-audit every unrelated entry.
**Rule**: Re-evaluate an entry when its code, callers, assumptions, or subsystem
is in scope; a full audit checks all entries. A narrow review may leave
unrelated entries untouched.
**When to report**: An in-scope entry is carried forward without checking its
reason. Never add re-check dates/freshness markers to `docs/audit.md`.

## FP-14: Fresh Batch/Transaction Per Bulk Chunk

**Pattern**: Reporting a newly-created batch/transaction per chunk as needless
allocation.
**Rule**: `Mapx::clone_in` and chunked bulk mutation paths intentionally create
a fresh batch/transaction per bounded chunk. This bounds memory and makes each
chunk's commit independently atomic; reusing a committed transaction is not an
optimization.
**When to report**: The chunk size is unbounded, committed state leaks between
chunks, cleanup/retry semantics are wrong, or a documented whole-operation
atomicity promise is violated.

## FP-15: Namespace Close Holds the Registry Lock Through Teardown

**Pattern**: Reporting that `ns_close_impl` unnecessarily retains
`REGISTRY_LOCK` while `engine.close()` flushes and joins threads.
**Rule**: The separate `OPEN_NAMESPACES` table lock is released before teardown,
so unrelated cache hits proceed. `REGISTRY_LOCK` intentionally serializes
open/create/destroy/relocate until teardown completes, preventing the same
namespace root from being reopened while its old engine still owns locks/files.
**When to report**: A concrete deadlock cycle exists, the table lock spans
teardown, or a replacement protocol preserves same-id lifecycle exclusion with
a smaller critical section.
