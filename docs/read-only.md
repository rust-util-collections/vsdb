# Open a VSDB universe read-only

Use `VsdbOptions::read_only(path)` to query an existing VSDB universe.
The setting applies to every namespace opened by the process. Configure it
once, before opening a collection or requesting a VSDB directory.
`vsdb_configure` leaves the process environment intact.

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

After the writer exits, start a separate reader process, or give the reader a
consistent immutable filesystem snapshot. Include the base directory and each
explicitly placed namespace used by the application. Registered absolute
paths must resolve to the snapshot data in the reader's filesystem.

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
For reads alongside a live writer in one process, use a reader handle in
read-write mode. [Raw map examples](../core/docs/api.md#mapxraw) · [Captured reads](storage.md#captured-reads)

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
requires exclusive access, including for namespace administration. A reader
that initially finds no lock file checks again after reading the header.
If a writer holds the new lock, the reader gets `Io(WouldBlock)` even before
the writer publishes the header. If the reader acquires the new lock, it rereads
the header under that lock. On Unix,
MMDB also takes a shared lock on each existing shard `LOCK` as it opens
that shard. An immutable copy may omit lock files. Keep such a copy
byte-stable until all readers exit.

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

VSDB preflights every existing anchored shard's v5 header for seven levels
and an eight-byte prefix. VerMap can validate its reference graph in memory
with `verify_and_gc()`. Restoring a branch-name index that needs writable
repair returns `VsdbError::Corrupt`.
