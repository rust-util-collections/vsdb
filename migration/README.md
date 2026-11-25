# Offline migration to VSDB v18 and MMDB v5

The runtime opens v18 collection data on MMDB v5. The separate
`vsdb_migrate` crate reads MMDB 4.3.15 and can stage an entire universe
before the application's first v5 write.

| Source | Operation |
|---|---|
| Fresh database | Configure v18 with a new base directory. |
| v18 collections on v4 shards | Stage the source with `vsdb-stage-v5`. Verify the application can reopen its existing handles. |
| v17 collections on v4 shards | Stage with the same command. Then apply the typed v17 conversions below to the new base and the matching application-owned handles. |
| v18 collections on v5 shards | Configure v18 against the existing complete generation. |

## Stage the engine generation

1. Stop processes using the base, its explicitly placed namespaces, and
   external application state. Take one consistent backup of all of these
   paths. An offline copy of a registry with absolute paths still refers
   to the original namespace directories. Supply the quiesced, internally
   consistent source tree and keep its referenced placements available
   for the full conversion.
2. Run the command from the workspace, with an existing destination parent
   and an absent final generation. The destination and every source root
   must be disjoint. Place the destination on a filesystem supporting an
   exclusive directory rename and directory fsync.

   ```sh
   cargo +stable run --release -p vsdb_migrate --bin vsdb-stage-v5 -- /data/old /archive/generation-v5
   ```

   The command copies regular files and directories into a private staging
   directory. It rejects symlinks within the copied trees and special
   files. The preflight checks directory identities across source placements
   and the destination parent, including aliases through mount points. The target needs space for the working copy and the verified v5
   shards. The v4 data remains available for a retry. The tool takes shared
   locks on existing universe and shard locks; immutable snapshots may omit
   those files. Quiescence also covers external application files.

   The new base is `/archive/generation-v5/base`. Derived namespace paths
   stay relative to that base. Explicit placements become siblings under
   `/archive/generation-v5/placements/<16-hex-namespace-id>`. Source
   placements can span filesystems. The generated placements and base
   publish together on the destination filesystem. Keep the published
   generation at its chosen absolute path so its registry remains valid.
   On Unix the generation directory is private (mode 0700). Stage as the
   intended service account, or have the operator arrange ownership and
   access for that account before selecting the new generation. Source
   roots and external namespaces can have different access policies.
   When the destination parent is a symlink, the CLI prints the resolved
   physical base and the registry binds the corresponding physical placements.

3. The converter checks established and initializing shard layouts. Empty
   or uncreated pending shards retain their lifecycle. For each anchored
   shard, a v4 read-only scan of that same private copy includes recovered WAL rows and their deletions.
   The tool ingests the sorted visible rows into fresh bottom-level v5 SSTs
   using the live adapter's 16 KiB blocks and Zstd compression. It checks both
   iterators for errors and compares row counts and Keccak-256 digests of
   length-framed keys and values after a writable reopen with seven levels
   and eight-byte prefixes. A v5 verification error retains its typed cause.
   MMDB 4.3.15 exposes terminal iterator failures as diagnostic strings;
   these carry the VSDB storage classification `Other`. A new v5 sequence
   history starts at export.
   Non-shard metadata, allocator floors, saved handles and the contents of
   `__CUSTOM__` travel with their universe. Separately stage any application
   files outside these trees.
4. If v17 schemas are present, run the conversions below on a copy of your
   application state configured with the new base. Verify externally saved
   handles, embedded values, branch and tree roots, DAG descendants, proofs
   and vector queries before switching traffic. On success, select the
   complete new generation and its application files together. Before new
   writes, rollback selects the original files and their matching binary.
   After accepting writes, rollback requires replay or reconciliation.

A failure before the exclusive rename leaves the final name available,
unless another process claimed it. A killed process can leave a private
`.vsdb-stage-*` sibling. Retry from the protected source using an absent
destination. If the final parent-directory sync fails after the rename,
the complete directory can already be visible, with durability unconfirmed.
A retry preserves any existing destination. Stage to another absent name
and verify the returned generation before selecting it.

The library entry point is `vsdb_migrate::stage_universe(source, generation)`.
Run it in the offline program before configuring VSDB. Its output can be used
for application-specific conversions:

```rust,no_run
use std::path::Path;
use vsdb::{InstanceId, Mapx, VsdbOptions, vsdb_configure};
use vsdb_migrate::{migrate_basic, stage_universe};

fn upgrade(source: &Path, generation: &Path, saved: InstanceId) -> vsdb::Result<()> {
    stage_universe(source, generation)?;
    vsdb_configure(VsdbOptions::new(generation.join("base")))?;
    let map = migrate_basic::<Mapx<String, u64>>(saved)?;
    assert_eq!(map.instance_id(), saved);
    Ok(())
}
```

## Existing v4 logical conversion programs

A program that converts v17 collection rows on MMDB v4 first can still
build the complete v4-backed converter. Check out VSDB commit
`6ddaed901901910537d0cf280141218380c5665d` separately and link the
application's offline converter against that checkout's `vsdb_migrate`.
On an isolated **working copy** with external paths rebound to its working
placements, run its `migrate_universe`, configure that checkout's v18/v4
runtime there, complete typed conversions, verify them and close the process.
Then pass the resulting v18/v4 universe to the staging command above. In
the current crate `migrate_universe` still upgrades VSDB metadata on a v4
working copy. It leaves anchored shards in v4 format.
[Frozen v4-backed migration guide](https://github.com/rust-util-collections/vsdb/blob/6ddaed901901910537d0cf280141218380c5665d/migration/README.md)

## Choose a converter

Use the saved-metadata entry point for an ID produced by `save_meta()`.
For a postcard-serialized handle (including a handle stored only in an
application record), use the byte-slice entry point and store its result.
Composite converters also update the backing collection. The conversions
preserve each collection's primary prefix.

| Data | Saved `__instance_meta__` conversion | Serialized byte-slice handle conversion |
| --- | --- | --- |
| Raw map | `migrate_raw(id)` | `raw_handle(&bytes)` |
| Mapx, MapxOrd, MapxOrdRawKey, Orphan | `migrate_basic::<T>(id)` | `basic_handle::<T>(&bytes)` |
| SlotDex (`u32`, `u64`, `u128` slots) | `slotdex::migrate_v17::<S, K>(id)` | `slotdex::migrate_handle_v17::<S, K>(&bytes)` |
| SlotDex (verified custom slot codec) | `slotdex::migrate_custom_v17::<S, K>(id, cap)` | `slotdex::migrate_custom_handle_v17::<S, K>(&bytes, cap)` |
| VecDex, including runtime metrics | `vecdex::migrate_v17::<K, M, S>(id)` | `vecdex::migrate_handle_v17::<K, M, S>(&bytes)` |
| VerMap | `versioned::migrate_v17::<K, V>(id)` | `versioned::migrate_handle_v17::<K, V>(&bytes)` |
| Standalone PersistentBTree | `versioned::migrate_tree_v17(id, &live_roots)` | `versioned::migrate_tree_handle_v17(&bytes, &live_roots)` |
| DagMapRaw root | `dagmap::migrate_v17(id)` | `dagmap::migrate_handle_v17(&bytes)` |
| DagMapRawKey root | `dagmap::migrate_rawkey_v17::<V>(id)` | `dagmap::migrate_rawkey_handle_v17::<V>(&bytes)` |
| Versioned trie checkpoints | Wrap the converted `VerMap` with `VerMapWithProof::from_map`. A root query rebuilds the trie from its rows. | — |

## Application-owned roots, keys and handles

- **Standalone B+ tree.** Supply every live v17 root, repeating a root when several owners hold it. The pool has no persisted root table. Replace each application's root ID with the corresponding returned ID. Keep the original list for retries.
- **DAG.** Convert the root first and apply its returned old-to-new ID map to external descendant IDs. For serialized root or child handles, load `dagmap::DagHandleConverter::open(root_id)` once and pass each handle to `raw_handle(bytes)` or `rawkey_handle::<V>(bytes)`. This preserves its independently stored value type and the saved metadata at the node. To re-tag an existing child metadata file containing a renamed library type, use `dagmap::migrate_child_v17::<V>(child_id)` with its relocated ID (or its original ID when absent from the mapping).
- **DAG edge tokens.** Include the universe-wide v17 `__SYSTEM__/dag_id_ceiling` (and the older `id_num` counter if present) in the backup. Conversion seeds parent allocators above all previously reserved edge tokens. When upgrading an already converted DAG, rerun its root converter offline before adding children. The converter raises existing allocation floors and retains any higher v18 allocations. If a retired v17 token was already reused for a live v18 child, reconcile references using the original backup before replaying old prune operations.
- **SlotDex.** For the custom converter, supply the original capacity and verify that the old codec always used the current fixed width and bucket alignment. The converter reconciles entries, counts and tier floors. Rebuild an ambiguous variable-width dataset from authoritative application records.
- **VecDex.** The converter preserves graph edges and node IDs. Use `MetricKind` as the metric type for a v17 runtime-selected metric.
- **Keys.** Ordered `usize` and `isize` keys require an explicit integer width chosen for the source architecture. Float and unordered hash-collection keys need an application-defined canonical replacement.
- **Nested handles.** Decode the surrounding application schema, convert the serialized handle bytes, then re-encode the value or sidecar. For ordinary raw-map values, `rewrite_values` applies a callback in bounded batches. Return `None` for values already converted so a retry can rescan the prefix. Use a schema-aware conversion for versioned values as well. Type tags include the fully qualified Rust type name. Preserve the original application names at conversion time or explicitly translate renamed types.

## Interrupted initialization and allocator recovery

The universe upgrade binds interrupted engine initializers to their original
namespace so v18 can complete shard creation. An orphan established record
or populated namespace directory requires recovery of the missing registration.
When the allocator file is missing, reconstruction includes the default shard's
historical ceiling, namespace data, instance metadata filenames and embedded
handle bytes.
