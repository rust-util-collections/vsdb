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

# fn main() -> vsdb::Result<()> {
vsdb_configure(VsdbOptions::read_only("/srv/my-app/vsdb"))?;
assert_eq!(vsdb_open_mode(), OpenMode::ReadOnly);

// Persist this token in the writer process with `map.save_meta()`.
let id: InstanceId = "40960000".parse()?;
let map = Mapx::<String, String>::from_meta(id)?;

assert!(map.namespace().is_read_only());
for (key, value) in map.iter() {
    println!("{key}: {value}");
}
# Ok(())
# }
```

Normal writable programs may keep using `vsdb_set_base_dir(path)`. The
equivalent options-based form is `vsdb_configure(VsdbOptions::new(path))`.

`vsdb_configure` is one-shot. Call it at the start of `main`, before spawning
threads and before any database, namespace, or derived-directory function is
used. It also publishes `VSDB_BASE_DIR` to child processes, matching
`vsdb_set_base_dir`.

## Preparing handles in the writer

Read-only processes open existing handles; they do not create collections or
save new metadata. Create and save each application root while writable:

```rust,no_run
use vsdb::{Mapx, VsdbOptions, vsdb_configure, vsdb_flush};

# fn main() -> vsdb::Result<()> {
vsdb_configure(VsdbOptions::new("/srv/my-app/vsdb"))?;

let mut map = Mapx::<String, String>::new();
map.insert(&"answer".into(), &"42".into());
let id = map.save_meta()?;

// Store `id` in application configuration or another bootstrap record.
println!("{id}");
vsdb_flush();
# Ok(())
# }
```

An `InstanceId` includes the owning namespace. `from_meta` and serde handle
deserialization automatically open that namespace in the process-wide mode, so
no separate read-only namespace option is needed.

## What is supported

Read-only mode supports:

- restoring saved raw and typed collection handles with `from_meta` or serde;
- point reads, iteration, ranges, version/history queries, SlotDex queries, and
  VecDex searches;
- opening and listing existing namespaces, reading shard properties, and
  closing non-default namespaces;
- computing Merkle roots and proofs. `VerMapWithProof` rebuilds a missing or
  stale trie cache in memory and does not save it;
- MMDB WAL recovery in memory, so committed records that have not reached an
  SST remain readable without rotating, truncating, or flushing the WAL.

The following fallible operations return `VsdbError::ReadOnly` before a side
effect:

- metadata saves and raw atomic file writes managed by VSDB;
- explicit MPT/SMT cache saves;
- namespace create, destroy, and relocate;
- engine batch commits and `clone_in`;
- fallible mutations on `VerMap`, SlotDex, VecDex, and DAG pruning.

Existing collection APIs such as `Mapx::insert`, `remove`, and `clear` predate
fallible mutation results. They fail fast with a panic in read-only mode, as
they do for other fatal engine write failures. Creating a new collection and
the deep-copying `Clone` implementations also panic. Restore an existing
handle instead, and check the mode before entering code that uses these
infallible write APIs.

```rust,no_run
use vsdb::{OpenMode, vsdb_open_mode};

if vsdb_open_mode() == OpenMode::ReadOnly {
    // Route this request to read-only behavior.
}
```

`vsdb_flush`, `Namespace::flush`, deferred `lazy_delete` registration,
`VerMap::gc`, and automatic trie-cache saves are no-ops. Explicit MPT/SMT
cache saves return `VsdbError::ReadOnly`. These operations are maintenance
only; skipping them does not change logical read results.

## Filesystem and process rules

The database must already be complete. Read-only open never creates a base
directory, shard, format marker, allocator file, namespace lifecycle record,
metadata directory, WAL, SST, or cache file. A missing, partial, pending, or
unsupported dataset is rejected instead of being initialized or repaired.

On Unix, each MMDB shard takes a shared, non-blocking lock when its existing
`LOCK` file is present. Multiple read-only processes can coexist, but a
cooperating writable process holds the exclusive lock and makes read-only open
fail. Stop the writer before opening the same live tree read-only.

An immutable filesystem snapshot may omit `LOCK`; MMDB then opens it unlocked.
Keep such a snapshot byte-stable for the lifetime of every reader. Platforms
without Unix `flock` must also be treated as unlocked. Never point an unlocked
reader at files that a writer can change concurrently.

Mounting the tree read-only or applying read-only permissions is supported and
recommended when operationally convenient. The process still needs ordinary
read and directory-traversal permission.

## Recovery and upgrades

Read-only MMDB recovery replays valid residual WAL records into memory without
changing disk. VSDB-level migrations and durable repair passes are different:
they require a writable open and are not performed in read-only mode. In
particular, read-only mode does not write format markers, allocator migrations,
namespace lifecycle sidecars, versioned-map reference-count repairs, or
garbage-collection registrations.

After an upgrade or a crash that left VSDB maintenance metadata pending, open a
backup or staging copy writable once, let normal recovery finish, validate it,
then take the immutable snapshot used by readers. Never make the only copy of a
dataset the target of an untested migration.

## Operational checklist

1. Stop the writer or take a storage-level consistent snapshot.
2. Preserve the whole default base directory and every explicitly rooted
   namespace referenced by saved handles.
3. Start a separate process and call
   `vsdb_configure(VsdbOptions::read_only(path))` before any other VSDB API.
4. Restore existing `InstanceId` values or serialized handles; do not call
   collection constructors.
5. Treat `VsdbError::ReadOnly` as a capability error, not a retryable I/O
   failure.
6. Keep unlocked snapshots immutable until all readers close.
