# Read-only mode

VSDB can open an existing database universe without changing any file in it.
This is useful for immutable snapshots, offline inspection, replicas built from
filesystem snapshots, and processes whose database tree is mounted read-only.

Read-only mode is process-wide. It applies to the default namespace and every
non-default namespace opened by that process; a process cannot mix writable and
read-only VSDB namespaces.

## Basic use

Configure the process before the first VSDB access, then restore handles that a
writer saved earlier:

```rust,no_run
use vsdb::{
    InstanceId, Mapx, OpenMode, VsdbOptions, vsdb_configure, vsdb_open_mode,
};

fn main() -> vsdb::Result<()> {
    vsdb_configure(VsdbOptions::read_only("/srv/my-app/vsdb"))?;
    assert_eq!(vsdb_open_mode(), OpenMode::ReadOnly);

    // Persist this token in the writer process with `map.save_meta()`.
    let id: InstanceId = "40960000".parse()?;
    let map = Mapx::<String, String>::from_meta(id)?;

    assert!(map.namespace().is_read_only());
    for (key, value) in map.iter() {
        println!("{key}: {value}");
    }
    Ok(())
}
```

Normal writable programs configure with `vsdb_configure(VsdbOptions::new(path))`.

`vsdb_configure` is one-shot. Call it before any database, namespace, or
derived-directory function is used. It never touches the process environment.

## Preparing handles in the writer

Read-only processes open existing handles; they do not create collections or
save new metadata. Create and save each application root while writable:

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

An `InstanceId` includes the owning namespace. `from_meta` and serde handle
deserialization automatically open that namespace in the process-wide mode, so
no separate read-only namespace option is needed.

## Collection behavior

| Request in read-only mode | Outcome |
|---|---|
| Restore an existing handle with `from_meta` or serde | Opens the owning namespace in read-only mode. |
| Read maps, snapshots, history, slot pages or vectors | Point, range and index queries run normally. |
| Compute a Merkle root or proof | `VerMapWithProof` can rebuild its trie in memory. Explicit checkpoint saves return `VsdbError::ReadOnly`. |
| Save metadata, clone persistent data with `clone_in`, or commit a nonempty write batch | Returns `VsdbError::ReadOnly`. |
| Create a valid SlotDex or VecDex, mutate a VerMap, or prune a DagMap | Fallible write operations return `VsdbError::ReadOnly`. |
| Use infallible collection creation or mutation | A constructor, deep-copying `Clone`, or attempted engine write panics. A call that exits before writing can be a no-op. |
| Call `vsdb_flush`, `Namespace::flush` or `VerMap::gc` | Skips maintenance writes. Deferred `lazy_delete` registration is also skipped. |

Use `collection.namespace().is_read_only()` or `vsdb_open_mode()` when
dispatching requests that can reach an infallible write API.

## Opening an existing universe

Preserve the whole v18 base directory and any externally rooted namespaces
your saved handles reference. Read-only open can
replay residual shard WAL records in memory. VerMap can likewise validate its
reference graph in memory. `VerMap::verify_and_gc()` can verify pages in this
mode, while `VerMap::flush()` requests a writable checkpoint and returns
`VsdbError::ReadOnly`.

Readers take shared locks where lock files exist. A writer needs exclusive
access to the universe. When reading an immutable filesystem copy that omits
lock files, keep the copy byte-stable for the readers' lifetime. The core guide
describes the universe and shard locks, initialization checks and WAL recovery.
[Core read-only mode](../../core/docs/read-only.md)

For v17 data, convert a backup or staging copy offline with `vsdb_migrate`,
including saved collection handles and any application-owned embedded handles.
Validate the converted data before taking the v18 snapshot for readers.
[Migration from v17](../../migration/README.md)
