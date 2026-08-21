# Read-only mode

`vsdb_core` can restore existing raw collection handles without changing the
database tree. The mode is intended for immutable snapshots, offline
inspection, and processes whose VSDB directory is mounted read-only.

## Open an existing universe

Configure the process before any database, namespace, or derived-directory API
is used, then restore an `InstanceId` saved by a writable process:

```rust,no_run
use vsdb_core::{
    InstanceId, MapxRaw, OpenMode, VsdbOptions, vsdb_configure, vsdb_open_mode,
};

# fn main() -> vsdb_core::Result<()> {
vsdb_configure(VsdbOptions::read_only("/srv/my-app/vsdb"))?;
assert_eq!(vsdb_open_mode(), OpenMode::ReadOnly);

let id: InstanceId = "40960000".parse()?;
let map = MapxRaw::from_meta(id)?;
assert!(map.namespace().is_read_only());
assert_eq!(map.get(b"answer"), Some(b"42".to_vec()));
# Ok(())
# }
```

`vsdb_configure` is one-shot and process-wide. It covers the default namespace
and every non-default namespace restored in that process. Different modes need
separate processes. Writable callers may continue to use
`vsdb_set_base_dir(path)`; its options-based equivalent is
`vsdb_configure(VsdbOptions::new(path))`.

Call configuration during single-threaded startup. Like `vsdb_set_base_dir`,
it publishes `VSDB_BASE_DIR` for child processes.

## Prepare the handle while writable

```rust,no_run
use vsdb_core::{MapxRaw, VsdbOptions, vsdb_configure, vsdb_flush};

# fn main() -> vsdb_core::Result<()> {
vsdb_configure(VsdbOptions::new("/srv/my-app/vsdb"))?;
let mut map = MapxRaw::new();
map.insert(b"answer", b"42");
let id = map.save_meta()?;
println!("{id}");
vsdb_flush();
# Ok(())
# }
```

A reader restores existing handles with `from_meta` or serde. It must not call
collection constructors.

## Operation behavior

Point reads, ranges, iterators, namespace inspection, and in-memory recovery of
committed residual MMDB WAL records are supported. Opening a saved handle whose
`InstanceId` names a non-default namespace automatically opens that namespace
read-only.

Fallible write operations reject the request before side effects:

- `save_meta`, `clone_in`, and batch `commit` return
  `VsdbError::ReadOnly`;
- namespace creation, destruction, and relocation return the same error;
- raw VSDB-managed atomic file writes return the same error.

Legacy direct mutations such as `MapxRaw::insert`, `remove`, and `clear` are
infallible APIs, so they panic in read-only mode. Creating a collection and its
deep-copying `Clone` implementation also panic. Check `vsdb_open_mode()` or
`Namespace::is_read_only()` before entering generic code that uses them.

`vsdb_flush`, `Namespace::flush`, and deferred `lazy_delete` registration are
no-ops because they are maintenance operations.

## Snapshot and locking rules

The database must already be complete. Read-only open does not create a base
directory, shard, format marker, allocator file, lifecycle record, metadata
directory, WAL, or SST. Missing, partial, pending, and unsupported datasets are
rejected rather than initialized or repaired.

On Unix, MMDB takes a shared non-blocking lock for each existing shard `LOCK`
file. Multiple readers can coexist, while a cooperating writer's exclusive
lock makes a reader fail to open. Stop the writer before opening the same live
tree read-only.

Snapshots without `LOCK`, and platforms without Unix `flock`, are effectively
unlocked. Keep their files byte-stable until all readers close. A read-only
mount or read-only permissions are supported; the process still needs read and
directory-traversal access.

MMDB can replay valid residual WAL data in memory. VSDB format markers,
allocator migrations, namespace lifecycle metadata, and other durable repair
state are never changed by a read-only open. If a dataset needs migration or
repair, perform one writable open on a backup or staging copy and take a fresh
immutable snapshot afterward.
