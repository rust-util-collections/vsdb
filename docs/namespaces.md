# Namespace placement and lifecycle

A VSDB process selects one universe. Its registry and prefix allocator
are shared across all namespaces. Each namespace has its own root directory,
MMDB engine, WALs, workers and memory sizing. Placing a collection with
`new_in(&ns)` selects the engine. Its prefix selects one shard within that
engine by `prefix % shard_count`. VerMap, VecDex and SlotDex publish each
mutation chunk under a single prefix. DagMap co-locates parent and child
prefixes on one shard for atomic child creation. [Sharded storage operations](storage.md) · [Namespace implementation](../core/src/common/namespace.rs)

## Create, capture and restore

`Namespace::create()` creates an anonymous namespace with a derived path.
`create_with(NamespaceOpts { path, shards, mem_budget_mb })` accepts an
explicit absolute root, shard count and budget. A namespace ID is a
universe-wide, monotonically allocated `u64`. The default namespace has
ID zero and 16 shards. Named namespaces default to four shards. Creation
clamps the chosen shard count to 1–64 and persists it for subsequent opens.

`ns.scope(|| ...)` changes the ambient namespace for new handles on the
current thread. Scopes nest and restore the previous scope on unwind.
Explicit placement selects the namespace for a new root. A DagMap child
always inherits its parent's namespace and shard. Reads, writes and
restores use the namespace recorded in the handle.

```rust
use vsdb::{Mapx, Namespace};

let ns = Namespace::create().unwrap();
let map = Mapx::<u64, String>::new_in(&ns);
let token = map.save_meta().unwrap();
drop(map);
let restored = Mapx::<u64, String>::from_meta(token).unwrap();
assert_eq!(restored.namespace().id(), ns.id());
```

`InstanceId` carries the map prefix and namespace. A bare `u64` identifies
a map in the default namespace. Parsing `42@7` selects map 42 in namespace
7, while `42@0` canonicalizes to the default-namespace token. A raw
`VSMAPX02` handle stores the prefix and namespace in 24 bytes.
`VSTYPE04` adds a tag derived from the fully qualified Rust type name and
collection-specific metadata. Applications version their own value
schemas. `clone_in(&target)` deep-copies the collection to a fresh identity.
A large raw map copy publishes bounded chunks and attempts to clear its
destination on failure. [Collection and handle API](../core/docs/api.md#namespaces) · [Type frames](../strata/src/common/frame.rs)

## Directory layout and recovery

```text
<base>/
  __SYSTEM__/
    LOCK                        universe lock
    universe_header             VSUNI018, format version and universe ID
    __namespaces__              VSNREG02, next ID and registry records
    __prefix_ceiling__          exclusive prefix reservation ceiling
    __instance_meta__/          saved default-namespace handles
    .shards_ready               default engine ready marker
  mmdb/__SYSTEM__/engine_header default engine VSENG018 identity
  mmdb/shard_00/ ...            default shards, created on first use
  __NAMESPACES__/<hex id>/      derived namespace root
    __SYSTEM__/engine_header    universe ID, namespace ID and shard count
    __SYSTEM__/.shards_ready    ready marker
    mmdb/shard_00/ ...          namespace shards
```

A derived root moves with the base. An explicit root must be absent or
empty at creation. Its resolved path must be disjoint from the base and
every other registered root. The registry persists the ID, optional path
and budget, shard count, creation time and pending or established state.
Back up the base, all explicit roots, and application-owned files from
the same quiesced generation. [Offline migration and backup](../migration/README.md)

Configure the base and open mode before using directory, database or
namespace APIs. Writable processes take an exclusive universe lock. Each
read-only opener takes a shared lock when the lock file exists; the caller
must exclude writers when using a lockless immutable copy.
`vsdb_configure` durably publishes the registry and allocator ceiling
before the universe header. Namespace creation reserves an ID with a
pending record, creates and verifies the engine, then establishes the
record. A writable opener can complete an interrupted initialization.
An established root must contain its registered shards. Both open modes
check the anchored v5 MANIFEST geometry before opening the shards.
[Universe and engine validation](../core/src/common/universe.rs) · [Read-only operation](../core/docs/read-only.md)

Prefixes are unique across the universe. The allocator persists the
exclusive ceiling before issuing a new window of 8,192 IDs to a thread.
Unused reservations and co-location can leave gaps. When the ceiling
is missing, recovery examines shard keys and values, saved instance
metadata and regular files under `__CUSTOM__` for issued IDs.
[Prefix allocation and recovery](../core/src/common/engine/mmdb.rs)

## Close, reopen, relocate and destroy

The open namespace table retains one strong reference to a named engine.
Namespace clones, collection handles, independent readers and owned VerMap
snapshots can keep it open. Ordinary iterators borrow their source handle.
A detached MMDB range retains its captured data sources independently and
may still yield old rows after close. Finish such iterators before moving
or removing the underlying directory. The default engine stays open for
the process lifetime once initialized. [Close implementation](../core/src/common/namespace.rs)

| Operation | Effect |
| --- | --- |
| `Namespace::close_by_id(id)` | Refuses while clients retain the named engine. On success closes its shards and leaves the registry entry available to reopen. |
| `ns.close()` | Consumes the caller's handle. A refusal returns `Err((Some(ns), error))` and preserves that handle. A teardown error returns `Err((None, error))` after taking the engine out of the open table. |
| `Namespace::open(id)` | Reuses the cached engine if open, or verifies the registered root and opens its shards. |
| `Namespace::relocate(id, path)` | Updates the closed namespace's registration after the caller moves its files. The destination must match its recorded identity, shard count and complete data set. |
| `Namespace::destroy(id)` | Requires a closed, non-default namespace. Registry removal is made durable before recursive directory removal. A later I/O error can leave an unregistered directory for the caller to clean up. |

Close proves exclusivity under the registry and open-table locks. It
releases the table lock before I/O and holds the registry lock through
teardown to serialize reopen against close. In writable mode it attempts
all shard closes and returns the first error. Read-only teardown releases
resources without flushing. [Administration examples](../core/docs/api.md#closing-relocating-and-destroying)
