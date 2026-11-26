# Read-only mode

Use `VsdbOptions::read_only` to query a v18 universe from an immutable
filesystem snapshot or a read-only mount. One open mode covers the default
namespace and every namespace restored by the process.

A v18-on-v4 development universe needs offline physical conversion before
opening with this runtime, including read-only use. The runtime preflights
the v5 level count and eight-byte prefix of every existing anchored shard.
A read-only MMDB open by itself adopts the stored geometry. [Upgrade guide](../../migration/README.md)

## Save a handle while writable

`save_meta` stores the handle in its owning namespace. Keep the returned
`InstanceId` with the application. A token identifies rows in that universe.

```rust,no_run
use vsdb_core::{MapxRaw, VsdbOptions, vsdb_configure};

fn main() -> vsdb_core::Result<()> {
    vsdb_configure(VsdbOptions::new("/srv/my-app/vsdb"))?;
    let mut map = MapxRaw::new();
    map.insert(b"answer", b"42");
    map.try_sync_wal()?;
    let id = map.save_meta()?;
    println!("{id}");
    Ok(())
}
```

## Open an existing universe

Start a separate reader process after the writer exits, or give the reader
a consistent immutable snapshot. Include the base directory and every
explicitly placed namespace the application uses. Explicit roots are recorded
as absolute paths and must resolve to the snapshot data in the reader's
filesystem.

```rust,no_run
use vsdb_core::{InstanceId, MapxRaw, VsdbOptions, vsdb_configure};

fn main() -> vsdb_core::Result<()> {
    vsdb_configure(VsdbOptions::read_only("/srv/my-app/vsdb"))?;
    let token = std::env::args().nth(1).expect("pass the writer's InstanceId");
    let map = MapxRaw::from_meta(token.parse::<InstanceId>()?)?;
    assert!(map.namespace().is_read_only());
    assert_eq!(map.get(b"answer"), Some(b"42".to_vec()));
    Ok(())
}
```

Configure before opening a collection or requesting a VSDB directory.
`from_meta` opens the namespace named by the token. Serde deserialization
can also restore a previously serialized handle.

## Operation behavior

| Operation | Read-only behavior |
| --- | --- |
| Point reads, ranges, read views, shard properties, namespace open/list/close | Available on existing data. Close applies to non-default namespaces. |
| `save_meta`, `clone_in`, `clone_colocated`, batch `commit` and `commit_sync` | Return `VsdbError::ReadOnly`. |
| Namespace creation, destruction, relocation and VSDB atomic file writes | Return `VsdbError::ReadOnly`. |
| Constructors, deep-copying `Clone`, direct `insert`, `remove` and `clear` | Panic on the attempted creation or write. |
| WAL fences, flushes and deferred-delete registration | Perform no writes. |

For a reader that must follow a live writer **inside the same process**, use
`MapxRaw::reader()` in read-write mode. It shares the original map's identity.
See [immutable iteration alongside writes](api.md#immutable-iteration-alongside-writes).

## Snapshot and locking rules

VSDB takes a shared, non-blocking lock on an existing `__SYSTEM__/LOCK`
for the process lifetime. Several readers can hold it together. A writer
requires exclusive access, including for namespace administration. On Unix,
MMDB also takes a shared lock on each existing shard `LOCK` as it opens
that shard. An immutable copy may omit lock files. Keep such a copy
byte-stable until all readers exit.

Read-only engine open checks the universe and engine identities, shard-ready
marker and per-shard layout. A completed dataset may retain a stale
initialization sentinel. A namespace registered as `Pending` requires a
writable recovery step before a reader can open it.

MMDB replays committed residual WAL records in memory. VSDB can also
reconstruct a missing prefix allocator ceiling in memory. Neither operation
changes the copy. For an interrupted v18 initialization, finish recovery on a
writable staging copy with the explicit namespace roots available there.
An established engine whose identity header survives but whose ready marker
and initializer are both absent reports `VsdbError::Corrupt` with the
required record's path. A matching initializer can allow writable
initialization to resume. Once the ready marker is durable and its engine
layout is valid, the initializer is optional.

For v17 data, run the [offline v18 migration](../../migration/README.md)
before taking the snapshot for readers.
