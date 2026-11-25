# VSDB Review Core

Shared evidence standard and source map for the four project skills.

This is a correctness review of the local Rust database. State findings in terms
of collection results, persisted state, resource lifetime, and documented API
contracts. Review depth describes reading effort; severity describes the
demonstrated consequence. Neither is an instruction to expand the task beyond
the selected repository scope.

## 1. Context

Start with the [maintenance scope](pragmatic-engineering.md), [worktree
policy](workflow-policy.md), [cross-cutting checks](technical-patterns.md) and
[false positive guide](false-positive-guide.md). For each in-scope change, read
the full diff, surrounding code, callers, tests and the mapped guide. Use the
[design checks](design-patterns.md) for multi-subsystem changes, design changes
or a full audit. Apply the [compatibility policy](compatibility-policy.md) to
public APIs, handles, persistent records and namespace layout. Skills that
create commits also use the [validation and release protocol](commit-protocol.md).

### Subsystem Map

| Subsystem | Implementation | Review guide |
| --- | --- | --- |
| Engine, universe and prefix allocator | [Engine and MMDB backend](../../core/src/common/engine), [universe headers and locks](../../core/src/common/universe.rs), [configuration, IDs and range bounds](../../core/src/common) | [Engine](patterns/engine.md) |
| Namespace lifecycle | [Registry, routing and teardown](../../core/src/common/namespace.rs) | [Engine](patterns/engine.md) · [Compatibility](compatibility-policy.md) |
| Raw and typed collections | [MapxRaw](../../core/src/basic/mapx_raw), [Mapx](../../strata/src/basic/mapx), [MapxOrd](../../strata/src/basic/mapx_ord), [raw-key maps](../../strata/src/basic/mapx_ord_rawkey), [Orphan](../../strata/src/basic/orphan) | [Engine](patterns/engine.md) · [Encoding and guards](technical-patterns.md) |
| Persistent B+ tree | [Pages, blobs, COW, diff, merge and runtime caches](../../strata/src/basic/persistent_btree) | [B+ tree](patterns/btree.md) |
| Versioning | [VerMap, snapshots, history, publications and repair](../../strata/src/versioned) | [Versioning](patterns/versioning.md) · [B+ tree](patterns/btree.md) |
| Merkle tries | [MPT, SMT, proofs, checkpoints and VerMap integration](../../strata/src/trie) | [Tries](patterns/trie.md) |
| SlotDex | [Slot codec, tier counts and pagination](../../strata/src/slotdex) | [SlotDex](patterns/slotdex.md) |
| DagMap | [Typed, raw-key and raw overlay maps](../../strata/src/dagmap) | [DagMap](patterns/dagmap.md) |
| VecDex | [HNSW, metric dispatch and read views](../../strata/src/vecdex) | [VecDex](patterns/vecdex.md) |
| Shared codecs, frames, readers, guards and staged writes | [Typed infrastructure](../../strata/src/common), [error types](../../core/src/common/error.rs) | [Technical checks](technical-patterns.md) · [Compatibility](compatibility-policy.md) |
| Offline migration | [Converters](../../migration/src), [fixtures and tests](../../migration/tests), [upgrade instructions](../../migration/README.md) | [Migration](patterns/migration.md) · [Compatibility](compatibility-policy.md) |
| Public exports and build | [Core exports](../../core/src/lib.rs), [typed exports](../../strata/src/lib.rs), [workspace and crate manifests](../../Cargo.toml) | [Compatibility](compatibility-policy.md) |

### Full-repository coverage

Start the ledger from `git ls-files` and assign **every tracked file** one
primary category: workspace source/tests/benches, build/CI, live documentation,
historical records, maintenance workflows, or generated/excluded with a reason.
Assign each Rust file one primary subsystem and map its tests and benches to the
behavior they exercise. Include all three crates, `AGENTS.md`, `docs/ai/`, and
each skill's `SKILL.md` and `agents/openai.yaml`.

Use the [documentation index](../README.md) to locate current API guides,
implemented design and historical evidence. Check the active v18 changelog
section against code. Within design records, distinguish implemented contracts,
measurements at named revisions and deferred proposals. Preserve the recorded
revision and workload when referencing an evaluation. For skill changes,
check the explicit invocation policy and the references used by that workflow.

## 2. Review depth (effort, not severity)

Depth sets how hard to look. Finding severity comes only from §5 outcomes.

| Class | Examples | Depth |
|-------|----------|-------|
| COW/DAG/unsafe | node replace, refs, merge/rollback, shadow/casts | deepest |
| Persisted format | meta, tags, keys, codecs, ns layout | deepest |
| Proof/routing/crash | Merkle, prefix/shards, staged/dirty | deep |
| Control/resource/API | lifecycle, cleanup, public behavior | deep |
| Errors | propagate, partial fail, retry | standard |
| Perf | serialization/alloc/locks on hot paths | quantify first |
| Tests/docs/config | coverage/alignment | light unless wrong |

## 3. Evidence

1. Name the invariant (mapped guide).
2. Realistic trigger (input, order, crash, old-data fixture).
3. Trace callers, cleanup, crate boundaries, guards.
4. Outcome: wrong value, loss, corruption, panic, UB, leak, deadlock,
   compatibility reject/misdecode, or **quantified** hot-path cost.
5. Smallest regression test that fails pre-fix.

Prefer an ordinary API input or operation sequence in the existing test suite.
For interrupted-write behavior, use a focused storage-error test double or an
isolated restart fixture. Keep any additional reproduction proportional to the
specific contract being checked, and record the difference between a traced
candidate and an actually reproduced failure.

**Boundaries:** empty/single entry, B+ occupancy/split, ancestor/no-op merge,
proof edges, tier/layer edges, prefix 0/`u64::MAX`, ns shard counts, bad meta,
partial I/O.

**Concurrency/unsafe:** Derive SWMR/locks from code. `// SAFETY:` is a claim.
Check alias life, write exclusion, process-global env, allocator, lifecycle serialize.

**Crash recovery.** Trace publication, synchronization and reopen across the
affected rows. VerMap uses one prefix with retirement and staging anchors.
SlotDex commits a batch of entries and tier counts together. VecDex can publish
several mutation chunks during one call. DagMap orders changes to co-located
node prefixes on one shard. The mapped guides describe each protocol and
its failure boundaries. For filesystem metadata, check the lock scope and the
order of file sync, rename and directory sync.

**Compatibility:** `compatibility-policy.md`. Old data preserve/reject/migrate
intentionally. A break needs explicit user acceptance before any major bump.

**Perf:** Hot/warm only; quantify. Cold init micro-opts are not findings.

**Design:** Locks, ownership, queues, multi-step install, degrade, public/on-disk
→ applicable D-\* in `design-patterns.md`; skip empty families.

**Incomplete implementation:**
`todo!` / `unimplemented!` / stand-in `unreachable!`; dummy returns where real
work is required; `// TODO|FIXME|HACK` for unfinished required behavior;
`if false` / `#[cfg(any())]` around incomplete required paths. Establish that a
normal supported call reaches the incomplete behavior, then assign severity
from its actual outcome under §5. A marker or macro name alone is not a defect.
Grep before calling dead code. Cleanup-only → LOW/skip.

## 4. Deterministic / style

fmt / compile / clippy → tools. Still LOW if tools miss:

- no `#[allow(...)]`
- import repeated paths; group same-root imports
- public docs + this map + guides stay aligned
- every unsafe has accurate `// SAFETY:`

## 5. Audit registry (`docs/audit.md`) — SSOT

Every skill that writes the registry uses these rules and this shape. State
meanings: `workflow-policy.md` §5. Decide from current code, not prior entry text.

1. Prune Open proven fixed/obsolete. Narrow scope: in-scope entries only; unrelated
   Open stays unless proven fixed. History → Git/CHANGELOG, never a Resolved section.
2. Add confirmed findings to Open; dedupe by root cause; order CRITICAL → LOW.
3. Re-check Won't Fix entries whose code, callers, or assumptions intersect
   the scope; full audit → all (FP-13). Prior refutations do not override new evidence.
4. Real but safe fix disproportionate → Won't Fix + Reason. Material claim
   disproven → remove the entry; keep useful refutation evidence in the review
   output or Git history. Routine noise → no entry.
5. No dates, freshness markers, or “last reviewed”. Separate entries with `---`.

```markdown
## Open
### [SEVERITY] subsystem: summary
- **Where**: `path` (`fn`) — add lines only when the fn is ambiguous
- **What**: defect + realistic trigger (input/order/crash/old data)
- **Why**: observable outcome; invariant; why guards fail
- **Suggested fix**: direction + regression test + migration impact

## Won't Fix
### [SEVERITY] subsystem: summary
- **Where** / **What** / **Reason**
```

Severity:

- **CRITICAL**: loss/corruption, UB, unsound proof, cross-structure contamination, silent persisted misread
- **HIGH**: wrong results, deadlock, realistic crash/exhaustion, material hot-path hit
- **MEDIUM**: edge bug, error-policy gap, bounded leak
- **LOW**: convention/docs with real cost

Observations ≠ Open.

## Quality gate

Concrete trigger + outcome only. Refute via `false-positive-guide.md`. Agent
agreement and pattern IDs are not proof.
