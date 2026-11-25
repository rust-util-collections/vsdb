# Migrating a v17 database

`vsdb_migrate` is a separate Rust library. Applications running v18 normally depend on `vsdb`. Link `vsdb_migrate` in the offline upgrade program.

1. Stop processes using the universe, including those using explicit namespace directories. Back up the base directory, every explicitly placed namespace and application-owned handle files together. Rollback restores that complete backup with the v17 binary. Keep downgraded processes on the restored v17 path. A v17 binary can open the shards of a fresh v18 default engine and damage its contents.
2. Call `migrate_universe(base)` **before** configuring v18 in the upgrade process. It converts the registry, authenticates each engine with its namespace and universe IDs, and retains the prefix allocator floor. An interrupted run uses the durable upgrade identifier on retry.
3. Configure v18 and convert each saved handle using its original key/value types. Preserve the returned identity mappings and convert embedded handles before starting application traffic.

| Data | Saved `__instance_meta__` conversion | Serialized byte-slice handle conversion |
| --- | --- | --- |
| Raw map | `migrate_raw(id)` | `raw_handle(&bytes)` |
| Mapx, MapxOrd, MapxOrdRawKey, Orphan | `migrate_basic::<T>(id)` | `basic_handle::<T>(&bytes)` |
| SlotDex | `slotdex::migrate_v17::<S, K>(id)` | `slotdex::migrate_handle_v17::<S, K>(&bytes)`, `slotdex::migrate_custom_handle_v17::<S, K>(&bytes, cap)` |
| VecDex, including runtime metrics | `vecdex::migrate_v17::<K, M, S>(id)` | `vecdex::migrate_handle_v17::<K, M, S>(&bytes)` |
| VerMap | `versioned::migrate_v17::<K, V>(id)` | `versioned::migrate_handle_v17::<K, V>(&bytes)` |
| Standalone PersistentBTree | `versioned::migrate_tree_v17(id, &live_roots)` | `versioned::migrate_tree_handle_v17(&bytes, &live_roots)` |
| DagMapRaw, DagMapRawKey | `dagmap::migrate_v17(id)`, `dagmap::migrate_rawkey_v17::<V>(id)`, `dagmap::migrate_child_v17::<V>(child_id)` | `dagmap::migrate_handle_v17(&bytes)`, `dagmap::migrate_rawkey_handle_v17::<V>(&bytes)` |
| Trie checkpoints | Rebuild from the converted version store. | — |

```rust,no_run
use vsdb::{InstanceId, Mapx, VsdbOptions, vsdb_configure};
use vsdb_migrate::{migrate_basic, migrate_universe};

fn upgrade(base: &std::path::Path, saved: InstanceId) -> vsdb::Result<()> {
    migrate_universe(base)?;
    vsdb_configure(VsdbOptions::new(base))?;
    let map = migrate_basic::<Mapx<String, u64>>(saved)?;
    assert_eq!(map.instance_id(), saved);
    Ok(())
}
```

Collection-specific converters retain the primary prefix. The DAG converter also returns relocated child identities. VecDex preserves the graph edges and node IDs. A saved v17 SlotDex with an ambiguous variable-width slot codec requires a rebuild from application records. Ordered pointer-width keys require conversion to an explicit integer width using the source architecture. Float and unordered hash-collection keys require an application-defined canonical replacement.

For a handle inside a user value or application sidecar (including handles persisted without `save_meta()`), decode the surrounding application schema, pass its serialized handle bytes to `basic_handle::<T>`, `raw_handle`, or the collection's `migrate_*handle_v17` helper, then re-encode the value. `rewrite_values` applies an application-supplied conversion to one prefix in bounded batches. Its callback should recognize values it has already converted, so a retry can resume safely. Use the same schema-aware approach for application sidecars and for versioned values containing handles. Type tags include the fully qualified Rust type name. Preserve the original application names at conversion time or supply an explicit schema migration for renamed types.

The universe upgrade checks the v17 shard locks and namespace lifecycle records
before publishing an identity. It binds interrupted engine initializers to their original namespace so that v18 can complete shard creation safely. An orphan established record or populated namespace
directory requires recovery of the missing registration. When the allocator file
is missing, reconstruction includes the default shard's historical ceiling,
namespace data, instance metadata filenames and embedded handle bytes.
