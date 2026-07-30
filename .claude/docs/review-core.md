# VSDB Review Core

Evidence standard and subsystem map. Apply `pragmatic-engineering.md`: only
findings/process that remove a concrete failure mode.

## 1. Context

1. Full diff + surrounding functions.
2. Map each changed code file via Subsystem Map; load guides, callers, tests.
3. Design-shaped / multi-subsystem diffs → also `design-patterns.md`.
4. Public API / serialized meta / durable KRT / ns layout / format markers →
   `compatibility-policy.md`.
5. Full audit → tracked-file ledger first (no static/size guesses).

### Subsystem Map

One primary row per Rust file. Unsafe, compatibility, public-doc checks are overlays.

| Subsystem | Files | Guides |
|-----------|-------|--------|
| engine/shard/prefix | `core/.../engine/**`, `core/.../mod.rs`, `core/.../mapx_raw/**` | `patterns/engine.md`, `technical-patterns.md` |
| namespaces | `core/.../namespace.rs` | `patterns/engine.md`, `compatibility-policy.md` |
| typed collections | `strata/.../mapx/**`, `mapx_ord/**`, `mapx_ord_rawkey/**`, `orphan/**` | `patterns/engine.md`, `technical-patterns.md` |
| B+ tree | `strata/.../persistent_btree/**` | `patterns/btree.md` |
| versioning | `strata/.../versioned/**` | `patterns/versioning.md` |
| tries | `strata/.../trie/**` | `patterns/trie.md` |
| SlotDex | `strata/.../slotdex/**` | `patterns/slotdex.md` |
| DagMap | `strata/.../dagmap/**` | `patterns/dagmap.md` |
| VecDex | `strata/.../vecdex/**` | `patterns/vecdex.md` |
| encoding/staged | `strata/.../common/**`, `core/.../error.rs` | `technical-patterns.md`, `compatibility-policy.md` |
| public/build | both `lib.rs`/`basic/mod.rs`, root+crate `Cargo.toml`, `core/build.rs` | `compatibility-policy.md`, `technical-patterns.md` |

Guides under `.claude/docs/patterns/`. Tests/benches/CI/README/CHANGELOG/`.claude`
→ map to the behavior they cover.

## 2. Risk (effort, not a finding)

| Class | Examples | Default |
|-------|----------|---------|
| COW/DAG/unsafe | node replace, refs, merge/rollback, shadow/casts | CRITICAL |
| Persisted format | meta, tags, keys, codecs, ns layout | CRITICAL |
| Proof/routing/crash | Merkle, prefix/shards, staged/dirty | HIGH |
| Control/resource/API | lifecycle, cleanup, public behavior | HIGH |
| Errors | propagate, partial fail, retry | MEDIUM |
| Perf | serialization/alloc/locks on hot paths | context |
| Tests/docs/config | coverage/alignment | LOW unless wrong |

## 3. Evidence

1. Name the invariant (mapped guide).
2. Realistic trigger (input, order, crash, old-data fixture).
3. Trace callers, cleanup, crate boundaries, guards.
4. Outcome: wrong value, loss, corruption, panic, UB, leak, deadlock,
   compatibility reject/misdecode, or **quantified** hot-path cost.
5. Smallest regression test that fails pre-fix.

**Boundaries:** empty/single entry, B+ occupancy/split, ancestor/no-op merge,
proof edges, tier/layer edges, prefix 0/`u64::MAX`, ns shard counts, bad meta,
partial I/O.

**Concurrency/unsafe:** Derive SWMR/locks from code. `// SAFETY:` is a claim.
Check alias life, write exclusion, process-global env, allocator, lifecycle serialize.

**Crash:** Do not mix recovery models —
VerMap dirty flag; SlotDex/VecDex one staged batch; B+ bulk_load may flush internal
batches (no root escapes while nodes buffered); ns/allocator need documented
sync/rename order.

**Compatibility:** `compatibility-policy.md`. Old data preserve/reject/migrate
intentionally. Breaks → major + concrete migration.

**Perf:** Hot/warm only; quantify. Cold init micro-opts are not findings.

**Design:** Locks, ownership, queues, multi-step install, degrade, public/on-disk
→ applicable D-\* in `design-patterns.md`; skip empty families.

**Placeholder (CRITICAL in non-test prod when it ships behavior):**
`todo!` / `unimplemented!` / stand-in `unreachable!`; dummy returns where real
work is required; `// TODO|FIXME|HACK` for unfinished required behavior;
`if false` / `#[cfg(any())]` around incomplete required paths. Grep before
calling dead code. Cleanup-only → LOW/skip.

## 4. Deterministic / style

fmt / compile / clippy → tools. Still LOW if tools miss:

- no `#[allow(...)]`
- import repeated paths; group same-root imports
- public docs + this map + guides stay aligned
- every unsafe has accurate `// SAFETY:`

## 5. Audit (`docs/audit.md`)

- Prune fixed in-scope Open (history → Git/CHANGELOG, not a Resolved section).
- Re-check intersecting Won't Fix / Rejected; full audit → all.
- Real but disproportionate → Won't Fix + reason.
- Material disproven → Rejected (no severity). Drop routine noise.
- No dates/freshness markers.

```text
[SEVERITY] subsystem: summary
WHERE: file:line_range
TRIGGER: input/order/crash/old-data
OUTCOME: observable wrong behavior
WHY: invariant + why guards fail
FIX: minimal direction + regression + migration impact
```

- **CRITICAL**: loss/corruption, UB, unsound proof, cross-structure contamination, silent persisted misread
- **HIGH**: wrong results, deadlock, realistic crash/exhaustion, material hot-path hit
- **MEDIUM**: edge bug, error-policy gap, bounded leak
- **LOW**: convention/docs with real cost

Observations ≠ Open.

## Quality gate

Concrete trigger + outcome only. Refute via `false-positive-guide.md`. Agent
agreement and pattern IDs are not proof.
