# Migration (`vsdb_migrate`) Review Patterns

[Offline converters](../../../migration/src) · [Migration tests and fixtures](../../../migration/tests) · [Upgrade guide](../../../migration/README.md)

**Arch:** Standalone offline generation staging for v17/v4 and v18/v4, plus v17 collection converters. Check staged external paths, a complete ordered-row digest, writable geometry, source preservation and exclusive publication. Runtime `vsdb_core` and `vsdb` accept only v18 formats (`VSUNI018`, `VSENG018`, `VSNREG02`, `VSMAPX02`, `VSTYPE04`); all historical envelope/row decoders and resumable conversion journals live exclusively in `vsdb_migrate`.

Evaluate offline conversion by recoverability, data integrity, and feasible time and
memory use on real datasets. Keep legacy decoders, journals and migration-specific
optimizations in `vsdb_migrate`. Changes to runtime collection or engine paths
need an independent runtime requirement.

## MMDB v4 to v5 generation staging

Use `stage_universe` (CLI `vsdb-stage-v5`) for a quiesced v17/v4 or
v18/v4 universe. `migrate_universe` upgrades VSDB metadata on a v4 working
copy. Application-specific v17 handle and collection conversions operate
on the staged generation. Review the two operations at their own boundaries.
[Physical stager](../../../migration/src/universe/staging.rs) · [Operator procedure](../../../migration/README.md)

- Acquire all available universe and shard locks, recheck the registry, then
  take the file inventory. Include canonical external placements. A closing
  writer's final WAL or application attachment must appear on retry.
- Validate and export the same private image. Check ordered length-framed
  row digests and counts, including an SST-only row, replacement from a
  residual WAL, deletion and empty value. Reopen v5 writable with the live
  shard format. Exercise a saved collection handle through the v18 API.
  [Shard fixture](../../../migration/tests/staged_generation.rs) · [Collection fixture](../../../migration/tests/collections_generation.rs)
- Before publication, check the exclusive allocator ceiling against visible
  physical prefixes and canonical instance metadata filenames, including
  empty maps and populated pending shards. v17 reconciles a higher legacy
  shard-zero floor. In v18 the existing file is authoritative. Application
  payloads are opaque when validating that file. Missing-ceiling recovery
  also searches embedded handle frames.
- Preserve source files and any competing destination. An exclusive rename
  followed by a failed parent directory sync can leave a complete visible
  generation. The application keeps the previous generation selected until
  it receives a durability acknowledgement. [Staging and interruption tests](../../../migration/src/universe/staging.rs)

## Logical v17 conversion invariants

**M1 Source intact until target validates** — never wipe legacy rows or retire component prefixes (`remove_old_nodes`, `batch_wiped`, `Phase::Retire`) until the target ceiling, header, and v18 handle deserialize and pass structural validation. Clear pre-publication staging keys (`[0x10]..[0x13]`, scratch prefixes) before retrying an uncommitted conversion.

**M2 Historical envelope coverage** — `v17.0.21` opened and mutated `v16` datasets in place, so valid v17 databases contain both `VSTYPE03` and create-time `VSTYPE02` envelopes (where `BranchId`/`CommitId` hashed `"u64"`), plus 16-byte and 24-byte `VSMAPX01` raw frames. Converters must accept all v17-valid wire forms.

**M3 Nominal tag translation (`legacy_tag_for`)** — translate every library type whose module path or type-alias expansion changed between v17 and v18 (`BranchId`, `CommitId`, `VecDexDyn`), including heterogeneous saved `DagMap` descendants (`V != T`).

**M4 Exact v17 crash states & ceiling reservation** — `migrate_universe` must accept every crash state v17 could durably produce (including `{root}/mmdb` created before `__SYSTEM__/__initializing__.tmp` → `__initializing__`, on both `ns == 0` and `Pending` `ns != 0`) while failing closed on unanchored `shard_*` dirs. Multi-prefix conversion journals (`Record::Running`) must embed a current `VSMAPX02` frame for their highest allocated target/scratch prefix so missing-ceiling recovery never reissues it.

**M5 Exclusive lock & fallible open** — `migrate_universe` holds the exclusive universe lock at `<base>/__SYSTEM__/LOCK` and shared locks on the discovered shards through header publication. Those shard locks exclude v17 writers. Collection converters open namespaces via `Namespace::open(id.ns.unwrap_or_default())?` (never panicking `Namespace::default_ns()`) and reject read-only mode before mutating storage.

**M6 Persistence surface parity** — cover handles saved via `__instance_meta__` (`save_meta`) as well as serialized handle bytes stored in `__CUSTOM__/` or parent collections (`serde`), rejecting trailing bytes via `postcard::take_from_bytes`.

## Bugs

**Premature source wipe** — deleting 8-byte v17 B+ tree nodes before `PersistentBTree::check_ceiling` / `Header::get` succeeds, stranding unrecoverable data on retry.
**`VSTYPE02` / `VecDexDyn` reject** — hardcoding `VSTYPE03` or missing a v17→v18 type-alias rename in `legacy_tag_for` / `saved_tag`.
**Unreachable crash branch** — checking `!mmdb.exists()` before handling `__initializing__.tmp`, when v17 created `mmdb/` first.
**Default-engine panic** — calling `Namespace::default_ns()` on an unmigrated or read-only namespace-only root instead of propagating `Result`.

## Checklist

- [ ] Legacy rows/prefixes wiped only after v18 handle + ceiling + header validate
- [ ] Pre-publication staging ranges/scratch prefixes cleared on retry; retries idempotent
- [ ] Both `VSTYPE02` and `VSTYPE03` (and 16B/24B `VSMAPX01`) accepted
- [ ] `legacy_tag_for` covers all moved/aliased library types (`BranchId`, `CommitId`, `VecDexDyn`) and mixed-type DAG children
- [ ] `migrate_universe` handles interrupted v17 init (`mmdb/` empty ± `__initializing__.tmp`) and locks universe + shards
- [ ] Journals expose highest scratch/target prefix to ceiling recovery and verify checksums
- [ ] `Namespace::open` used (no `default_ns()` panic); read-only rejected cleanly
- [ ] Frozen v17 fixtures + interrupted subprocess/retry tests verify data and identity preservation
