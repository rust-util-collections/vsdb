# VSDB Review Core Methodology

This document defines the evidence standard and canonical subsystem mapping for
VSDB reviews.

## 1. Context and coverage

Before analysis:

1. Read the complete diff and surrounding functions.
2. Map every changed code file through the Subsystem Map.
3. Read all mapped guides, callers, and directly relevant tests.
4. For a full audit, build a tracked-file ledger first; do not rely on static
   enumerations, approximate sizes, or agent claims of coverage.

### Subsystem Map (single source of truth)

Commands/skills reference this table instead of duplicating mappings. Every
Rust source file has one primary row; unsafe, compatibility, and public-doc
checks are overlays.

| Subsystem | File patterns | Pattern guide(s) |
|-----------|---------------|------------------|
| engine/shard/prefix | `core/src/common/engine/**/*.rs`, `core/src/common/mod.rs`, `core/src/basic/mapx_raw/**/*.rs` | `patterns/engine.md`, `technical-patterns.md` |
| namespaces | `core/src/common/namespace.rs` | `patterns/engine.md`, `compatibility-policy.md` |
| typed collections | `strata/src/basic/mapx/**/*.rs`, `strata/src/basic/mapx_ord/**/*.rs`, `strata/src/basic/mapx_ord_rawkey/**/*.rs`, `strata/src/basic/orphan/**/*.rs` | `patterns/engine.md`, `technical-patterns.md` |
| persistent B+ tree | `strata/src/basic/persistent_btree/**/*.rs` | `patterns/btree.md` |
| versioning | `strata/src/versioned/**/*.rs` | `patterns/versioning.md` |
| Merkle tries | `strata/src/trie/**/*.rs` | `patterns/trie.md` |
| SlotDex | `strata/src/slotdex/**/*.rs` | `patterns/slotdex.md` |
| DagMap | `strata/src/dagmap/**/*.rs` | `patterns/dagmap.md` |
| VecDex | `strata/src/vecdex/**/*.rs` | `patterns/vecdex.md` |
| encoding/staged mutation | `strata/src/common/**/*.rs`, `core/src/common/error.rs` | `technical-patterns.md`, `compatibility-policy.md` |
| public/module/build boundaries | `core/src/lib.rs`, `core/src/basic/mod.rs`, `strata/src/lib.rs`, `strata/src/basic/mod.rs`, `Cargo.toml`, `core/Cargo.toml`, `strata/Cargo.toml`, `core/build.rs` | `compatibility-policy.md`, `technical-patterns.md` |

Guides live in `.claude/docs/patterns/`. Tests, benches, CI, README/CHANGELOG,
and `.claude/` are supporting surfaces: map them to the behavior they specify.

## 2. Risk classification

| Category | Examples | Default risk |
|----------|----------|--------------|
| COW/version DAG/unsafe | node replacement, refs, merge/rollback, shadow/raw casts | CRITICAL |
| Persisted format | metadata, tags, keys, node codecs, namespace layout | CRITICAL |
| Proof/routing/crash safety | Merkle proofs, prefix/shards, staged/dirty protocols | HIGH |
| Control/resource/API | branches, lifecycle, cleanup, public behavior | HIGH |
| Error handling | propagation, partial failure, retry/fail-stop | MEDIUM |
| Performance | serialization/allocation/locking on hot paths | context-dependent |
| Tests/docs/config | contract/coverage alignment | LOW unless behavior is wrong |

Classification allocates review effort; it is not itself a finding.

## 3. Evidence protocol

For each risky change:

1. **Invariant**: cite the mapped guide.
2. **Trigger**: construct realistic input, ordering, crash point, or old-data
   fixture.
3. **Trace**: follow callers, cleanup, cross-crate boundaries, and guards.
4. **Outcome**: state wrong value, data loss, corruption, panic, UB, leak,
   deadlock, compatibility rejection/misdecode, or quantified regression.
5. **Regression test**: identify the smallest test that fails before the fix.

### Boundary conditions

Check empty/single entry, B+ tree occupancy/split thresholds, ancestor/no-op
merges, proof path boundaries, tier/layer boundaries, prefix 0/u64::MAX,
namespace shard counts, malformed metadata, and partial I/O/commit failure.

### Concurrency and unsafe

Derive the real SWMR and lock protocol from current code. A `// SAFETY:` comment
is a claim to verify, not proof. Check alias lifetime, write exclusion,
process-global environment mutation, allocator locks/atomics, and lifecycle
serialization.

### Crash safety

Distinguish protocols:

- VerMap uses dirty-state detection/repair around multi-step operations.
- SlotDex/VecDex use one staged read-your-writes batch per mutation.
- B+ tree bulk load may flush internal node batches before returning; no root
  may escape while referenced nodes remain buffered.
- Namespace/allocator durable files require their documented sync/rename order.

Do not apply one subsystem's recovery model to another.

### Compatibility

For public or persisted changes, apply `compatibility-policy.md`. Prove old data
is preserved/rejected/migrated intentionally. Breaking changes require a major
version and concrete migration path.

### Performance

Require hot/warm-path evidence and quantify work. Cold initialization or
administrative micro-optimizations are not findings.

## 4. Deterministic and convention checks

Formatting, compilation, and Clippy belong to tools, not speculative agents.
Repository-specific conventions not enforced automatically remain LOW findings:

- no `#[allow(...)]`;
- import repeated inline paths and group all same-root imports;
- public API changes update docs, this map, and pattern guides;
- every unsafe operation has an accurate `// SAFETY:` contract.

## 5. Audit registry

Consult `docs/audit.md`:

- verify/prune relevant `Open` entries;
- re-evaluate in-scope `Won't Fix`/`Rejected` entries; full audit checks all;
- keep real disproportionate defects/debt under `Won't Fix` with reason;
- keep only recurring/material disproven claims under `Rejected`;
- remove resolved entries rather than accumulating history;
- never add dates or freshness markers.

### Finding format

```text
[SEVERITY] subsystem: summary
WHERE: file:line_range
TRIGGER: concrete input/order/crash/old-data state
OUTCOME: observable incorrect behavior
WHY: violated invariant and failed guard
FIX: minimal safe direction, regression test, migration impact
```

### Severity

- **CRITICAL**: data loss/corruption, UB, proof unsoundness, cross-structure
  contamination, or silent persisted-data misinterpretation.
- **HIGH**: incorrect results, deadlock, realistic crash/resource exhaustion,
  or material hot-path regression.
- **MEDIUM**: reachable edge bug, error-policy gap, or bounded leak.
- **LOW**: convention/documentation/clarity defect with concrete cost.

Observations/questions may appear in reports but never under `Open`.

## Quality gate

Retain only findings with concrete trigger and outcome. Refute against
`false-positive-guide.md`. Agent votes and pattern-name matches are not evidence.
