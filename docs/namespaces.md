# Namespace placement and lifecycle

Use a namespace to place collections on a particular volume or give them a
separate memory budget. Each namespace belongs to one universe and owns
its root directory, MMDB shards, WALs and workers. The universe allocates
map prefixes across all namespaces. [Storage architecture](architecture.md)

## Create, capture and restore

The default namespace has ID zero and 16 shards. `Namespace::create()`
allocates a new numeric ID and creates a four-shard namespace under the
base directory. Use `create_with` to choose an absolute root, shard count
or memory budget. Creation clamps the requested shard count to 1–64 and
persists it for subsequent opens.

`new_in(&ns)` places a new collection in the supplied namespace.
`ns.scope(|| ...)` selects it for new handles on the current thread for the
duration of the closure. Scopes nest and restore the previous scope on
unwind. Reads, writes and restores use the namespace recorded in the handle.

```rust,no_run
use vsdb::{Mapx, Namespace, NamespaceOpts};

type Archive = Mapx<u64, String>;

let archive = Namespace::create_with(NamespaceOpts {
    path: Some("/mnt/fast/archive".into()),
    shards: 8,
    mem_budget_mb: Some(1024),
}).unwrap();
let id = Archive::new_in(&archive).save_meta().unwrap();
let restored = Archive::from_meta(id).unwrap();
assert_eq!(restored.namespace().id(), archive.id());
```

`InstanceId` carries the map prefix and namespace. `42@7` selects map 42
in namespace 7. Both `42` and `42@0` select map 42 in the default namespace.
A raw `VSMAPX02` handle stores the prefix and namespace in 24 bytes.
Typed handles use `VSTYPE04` and check the fully qualified Rust type name,
including generic arguments. A type alias preserves the original type's
identity. A changed type name can change the tag even if the value encodes
to the same bytes. Applications version their own value schemas.
[Type frames](../strata/src/common/frame.rs)

DagMap children inherit the parent's namespace and shard. `VerMapWithProof`
uses the namespace of its wrapped map. Standalone `MptCalc` and `SmtCalc`
hold their nodes in memory.

### Place raw maps on the same shard

Two maps created with `new_in(&ns)` can land on different shards, each with
its own WAL. Use `new_colocated` for a fresh raw map on an existing map's
shard. Use `clone_in` to deep-copy a collection to a fresh identity. Large
raw copies publish in bounded chunks and attempt to clear the destination
on failure.

```rust
use vsdb_core::MapxRaw;

let orders = MapxRaw::new();
let payments = orders.new_colocated();
assert!(orders.is_colocated_with(&payments));
```

## Directory layout and recovery

```text
<base>/
  __SYSTEM__/
    LOCK                        universe lock
    format_version              VSDB format marker (ASCII 18)
    universe_header             VSUNI018, format version and universe ID
    __namespaces__              VSNREG02, next ID and registry records
    __prefix_ceiling__          exclusive prefix reservation ceiling
    __instance_meta__/          saved default-namespace handles
    .shards_ready               default engine ready marker
  mmdb/__SYSTEM__/engine_header default engine VSENG018 identity
  mmdb/shard_00/ ...            default shards, created on first use
  __NAMESPACES__/<hex id>/      derived namespace root
    __SYSTEM__/engine_header    universe ID, namespace ID and shard count
    __SYSTEM__/format_version   VSDB format marker (ASCII 18)
    __SYSTEM__/.shards_ready    ready marker
    __SYSTEM__/__instance_meta__/  saved handles for this namespace
    mmdb/shard_00/ ...          namespace shards
```

A derived root moves with the base. An explicit root must be absent or
empty at creation. Its resolved path must be disjoint from the base and
every other registered root. The registry persists the ID, optional path
and budget, shard count, creation time and pending or established state.
Back up the base, all explicit roots, and application-owned files from
the same quiesced generation. Copying the base preserves the registered
absolute paths to explicit roots. [Upgrade an existing universe](upgrade.md)

### Initialization and format checks

Configure the base and open mode before using directory, database or
namespace APIs. Writable processes take an exclusive universe lock. Each
read-only opener takes a shared lock when the lock file exists; the caller
must exclude writers when using a lockless immutable copy.
`vsdb_configure` durably publishes the registry and allocator ceiling
before the universe header. Namespace creation reserves an ID with a
pending record, creates and verifies the engine, then establishes the
record. A writable opener can complete an interrupted initialization.
If creation rolls back after populating an explicit root, VSDB retains its
ownership record until cleanup is durable. After a filesystem error clears,
retry `Namespace::create_with` at the same path to finish cleanup and create
the namespace.
An established root must contain its registered shards. Writable open
creates a missing `format_version` marker. Read-only open leaves it untouched.
Either mode rejects a conflicting marker as `VsdbError::Corrupt`. Before
opening shards, VSDB checks each existing anchored shard's `CURRENT` and
format 5 MANIFEST header for seven levels and an eight-byte prefix. MMDB
checks the remaining manifest records, WALs and table files as it opens
each shard.
[Universe and engine validation](../core/src/common/universe.rs) · [Read-only operation](read-only.md)

### Prefix recovery

Prefixes are unique across the universe. The allocator persists the
exclusive ceiling before issuing a new window of 8,192 IDs to a thread.
Unused reservations and co-location can leave gaps. When the ceiling
is missing, recovery examines shard keys and values, saved instance
metadata and regular files under `__CUSTOM__` for issued IDs.
[Prefix allocation and recovery](../core/src/common/engine/mmdb.rs)

## Close, reopen, relocate and destroy

`Namespace::list()` lists registered non-default namespaces. The open
namespace table retains one strong reference to each open non-default engine.
Namespace clones, collection handles, independent readers and owned VerMap
snapshots can keep it open. Ordinary iterators borrow their source handle.
A detached MMDB range retains its captured data sources independently and
may still yield old rows after close. Finish such iterators before moving
or removing the underlying directory. The default engine stays open for
the process lifetime once initialized. [Close implementation](../core/src/common/namespace.rs)

| Operation | Effect |
| --- | --- |
| `Namespace::close_by_id(id)` | Refuses while clients retain the engine. On success closes its shards and leaves the registry entry available to reopen. |
| `ns.close()` | Consumes the caller's handle. A refusal returns `Err((Some(ns), error))` and preserves that handle. A teardown error returns `Err((None, error))` after taking the engine out of the open table. |
| `Namespace::open(id)` | Reuses the cached engine if open, or verifies the registered root and opens its shards. |
| `Namespace::relocate(id, path)` | Updates the closed namespace's registration after the caller moves its files. The destination must match its recorded identity, shard count and complete data set. |
| `Namespace::destroy(id)` | Requires a closed, non-default namespace. Verifies ownership, durably removes the registry entry, then removes the contents. An adopted mount point that cannot be removed is emptied and kept. A later I/O error can leave an unregistered directory. |

```rust
use vsdb_core::Namespace;

let archive = Namespace::create().unwrap();
let id = archive.id();
archive.close().unwrap();

let reopened = Namespace::open(id).unwrap();
assert_eq!(reopened.id(), id);
```

Close checks for other owners under the registry and open-table locks. It
releases the table lock before I/O and holds the registry lock through
teardown so an opener waits for close to finish. Writable close attempts all
shard closes and returns the first error. Read-only close releases resources
without flushing.

### Destruction and interrupted cleanup

Destroy empties and synchronizes the directory through the same handle it
used to verify ownership. After removing the registered name, it synchronizes
the parent. If the root was already absent, destroy tries to synchronize
the nearest surviving ancestor before unregistering the namespace. An
inaccessible ancestor or one that cannot be synchronized on a read-only
filesystem can still permit registry removal. A symlink at the registered
path must resolve to a verified namespace directory.

For final removal, destroy moves the name into a private sibling directory
and checks its identity there. If a replacement was moved, destroy restores
it without overwriting an occupied name. If restoration fails, the error
identifies the retained entry under `.vsdb-destroy-*/entry`. An interruption
can also leave an entry there. [Destruction implementation](../core/src/common/namespace/destroy.rs)
