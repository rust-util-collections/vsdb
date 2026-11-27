# Open a VSDB universe read-only

Use `VsdbOptions::read_only(path)` to query an existing universe after the
writer exits, or from a consistent immutable filesystem snapshot. The open
mode applies to every namespace in the reader process. To read alongside a
writer **in the same process**, use a reader handle and a captured view.
[Captured reads](storage.md#captured-reads)

## Save the handle in the writer

Create the collection while writable and keep the `InstanceId` returned by
`save_meta()` in application storage. The token identifies the collection and
its owning namespace.

```rust,no_run
use vsdb::{Mapx, VsdbOptions, vsdb_configure};

fn main() -> vsdb::Result<()> {
    vsdb_configure(VsdbOptions::new("/srv/my-app/vsdb"))?;

    let mut map = Mapx::<String, String>::new();
    map.insert(&"answer".into(), &"42".into());
    let id = map.save_meta()?;

    // Store `id` in application configuration or another bootstrap record.
    map.try_sync_wal()?;
    println!("{id}");
    Ok(())
}
```

## Restore the handle in the reader

Start a separate reader process. Configure its base and open mode before
opening a collection or requesting a VSDB directory. For a filesystem
snapshot, include the base and its registered explicit namespace roots.
Registered absolute paths must resolve to the snapshot data in the reader's
filesystem. Pass the token printed by the writer as the first argument.

```rust,no_run
use vsdb::{
    InstanceId, Mapx, OpenMode, VsdbOptions, vsdb_configure, vsdb_open_mode,
};

fn main() -> vsdb::Result<()> {
    vsdb_configure(VsdbOptions::read_only("/srv/my-app/vsdb"))?;
    assert_eq!(vsdb_open_mode(), OpenMode::ReadOnly);

    let token = std::env::args().nth(1).expect("pass the saved InstanceId");
    let id: InstanceId = token.parse()?;
    let map = Mapx::<String, String>::from_meta(id)?;

    assert!(map.namespace().is_read_only());
    for (key, value) in map.iter() {
        println!("{key}: {value}");
    }
    Ok(())
}
```

A raw-map application uses `vsdb_core::{MapxRaw, VsdbOptions, vsdb_configure}`
with the same writer and reader configuration. Save a raw map with
`map.save_meta()?` and restore its token with `MapxRaw::from_meta(id)?`.
Both `from_meta` and serde handle deserialization open the owning namespace.
[Raw map examples](../core/docs/api.md#mapxraw)

## Operation behavior

| Operation | Read-only behavior |
| --- | --- |
| Map reads, range scans, snapshots, history, slot pages and vector queries | Read existing data. Namespace open/list, shard properties and closing a non-default namespace are also available. |
| Merkle roots and proofs | `VerMapWithProof` can rebuild its trie in memory. Saving a checkpoint returns `VsdbError::ReadOnly`. |
| `save_meta`, `clone_in`, `clone_colocated`, committing a raw or basic typed map batch, and VSDB atomic file writes | Return `VsdbError::ReadOnly`. |
| Create, relocate or destroy a namespace, create a valid SlotDex or VecDex, mutate a VerMap, or prune a DagMap | Fallible writes return `VsdbError::ReadOnly`. |
| Infallible collection creation or mutation | A constructor, deep-copying `Clone`, or a direct `insert`, `remove` or `clear` that attempts a write panics. A call that exits before writing can be a no-op. |
| WAL fences, `vsdb_flush`, `Namespace::flush`, `VerMap::gc`, or deferred deletes | Skip maintenance writes. `VerMap::flush()` requests a writable checkpoint and returns `VsdbError::ReadOnly`. |

Use `collection.namespace().is_read_only()` or `vsdb_open_mode()` before
dispatching requests to infallible mutation APIs.

## Snapshot and locking rules

VSDB takes a shared, non-blocking lock on an existing `__SYSTEM__/LOCK`
for the process lifetime. Several readers can hold it together. A writer
requires exclusive access, including for namespace administration. On Unix,
MMDB also takes a shared lock on each existing shard `LOCK` as it opens
that shard. An immutable copy may omit lock files. Keep such a copy
byte-stable until all readers exit.

A reader that initially finds no universe lock file checks again after
attempting to read the header.
If a writer holds the new lock, the reader gets `Io(WouldBlock)` even before
the writer publishes the header. If the reader acquires the new lock, it
rereads the header under that lock.

## Open checks and recovery

Read-only engine open checks the universe and engine identities, shard-ready
marker and per-shard layout. A completed dataset may retain a stale
initialization sentinel. A namespace registered as `Pending` requires a
writable recovery step before a reader can open it.

MMDB replays committed residual WAL records in memory. VSDB can also
reconstruct a missing prefix allocator ceiling in memory. Neither operation
changes the copy. For an interrupted initialization, finish recovery on a
writable staging copy with the explicit namespace roots available there.
An established engine whose identity header survives but whose ready marker
and initializer are both absent reports `VsdbError::Corrupt` with the
required record's path. A matching initializer can allow writable
initialization to resume. Once the ready marker is durable and its engine
layout is valid, the initializer is optional.

Both open modes check the shard layout. VerMap can validate its reference graph in memory
with `verify_and_gc()`. Restoring a branch-name index that needs writable
repair returns `VsdbError::Corrupt`. [Initialization and format checks](namespaces.md#initialization-and-format-checks) · [VerMap recovery](../strata/docs/versioned.md#durability-and-recovery)
