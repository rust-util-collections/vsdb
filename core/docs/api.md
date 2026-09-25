# vsdb_core API Examples

This document provides examples for the public APIs in the `vsdb_core` crate.

## MapxRaw

`MapxRaw` is a raw key-value store.

```rust
use vsdb_core::MapxRaw;
use vsdb_core::common::{VsdbOptions, vsdb_configure, vsdb_get_base_dir};

// It's recommended to set a base directory for the database.
// vsdb_configure(VsdbOptions::new("/tmp/vsdb_core_test")).unwrap();

let mut map = MapxRaw::new();

// Insert raw bytes
map.insert(b"key1", b"value1");
map.insert(b"key2", b"value2");

// Get raw bytes
assert_eq!(map.get(b"key1").as_deref(), Some(&b"value1"[..]));

// Check for existence
assert!(map.contains_key(b"key2"));

// Remove a key
map.remove(b"key1");
assert!(!map.contains_key(b"key1"));
```

Ordinary writes are WAL-backed but are not individually fsynced against power
loss. `map.try_sync_wal()?` makes prior successful writes to the owning shard durable
without forcing a memtable flush. Other shards require their own fences; this
does not create a cross-shard transaction. The call is a no-op in read-only mode.
`map.sync_wal()` retains the panic-on-error form. `ns.try_sync_wal()?` fences all
shards of one namespace; it returns the first error and does not make the
namespace's collections transactional. Neither form forces a memtable flush.

## Utility Functions

Example for getting and setting the base directory.

```rust
use vsdb_core::{VsdbOptions, vsdb_configure, vsdb_get_base_dir};

// Configure before the first VSDB access.
// This selects the path without changing the process environment.
vsdb_configure(VsdbOptions::new("/tmp/my_vsdb_data")).unwrap();

// Get the current base directory
let dir = vsdb_get_base_dir();
assert_eq!(dir.to_str().unwrap(), "/tmp/my_vsdb_data");

// Pass the selected path explicitly when launching a child process.
let mut child = std::process::Command::new("my-worker");
child.env("VSDB_BASE_DIR", &dir);
```

## Read-only mode

Read-only access is configured once for the whole process, before any other
VSDB API. Restore a handle saved by an earlier writable process; do not create
a new collection:

```rust,no_run
use vsdb_core::{
    InstanceId, MapxRaw, OpenMode, VsdbError, VsdbOptions, vsdb_configure,
    vsdb_open_mode,
};

# fn main() -> vsdb_core::Result<()> {
vsdb_configure(VsdbOptions::read_only("/srv/my-app/vsdb"))?;
assert_eq!(vsdb_open_mode(), OpenMode::ReadOnly);

let id: InstanceId = "40960000".parse()?;
let mut map = MapxRaw::from_meta(id)?;
assert!(map.namespace().is_read_only());
let _value = map.get(b"key");

// Batches expose the capability failure without panicking.
let mut batch = map.batch();
batch.insert(b"key", b"new value");
assert!(matches!(
    batch.commit(),
    Err(VsdbError::ReadOnly { .. })
));
# Ok(())
# }
```

The mode automatically applies to non-default namespaces opened while
restoring handles. Point reads, ranges, iterators, properties, and in-memory
WAL recovery are supported. Metadata saves, namespace administration writes,
cloning, and batch commits return `VsdbError::ReadOnly`; infallible direct
mutation APIs panic; flush and deferred-delete maintenance calls are no-ops.

See the [complete read-only mode guide](read-only.md) for process
locking, immutable snapshots, filesystem permissions, and upgrade/recovery
constraints.

## Memory budget

The default namespace uses a fixed 2 GiB sizing input. Configure it before
the first VSDB access; an explicit nonzero value takes precedence over
`VSDB_MEM_BUDGET_MB`:

```rust,no_run
use vsdb_core::{VsdbOptions, vsdb_configure};

vsdb_configure(VsdbOptions::new("/data/vsdb").with_mem_budget_mb(8192)).unwrap();
```

Non-default namespaces use `NamespaceOpts::mem_budget_mb` and default to
512 MiB each. All MB-named options use binary MiB; there is no host-RAM or
cgroup detection. Budgets size caches and write buffers with floors and caps;
they do not bound process RSS. Allow headroom for pinned blocks, metadata,
and application allocations, and measure workload performance when tuning.
See the [memory design](../../docs/proposals/shared-mem-pool.md).

## Namespaces

`vsdb_core` provides the namespace subsystem — independently-rooted engine
instances that coexist in one process, each with its own base dir, mmdb
shards, and memory budget.  Persistent collections such as `MapxRaw` expose
`new_in` for explicit placement and `namespace()` for querying ownership.
Placement in one namespace does not guarantee placement on the same shard.

```rust,no_run
use vsdb_core::{InstanceId, MapxRaw, Namespace, NamespaceOpts};

// Create a namespace — parameterless, gives a fresh anonymous placement group.
let ns = Namespace::create().unwrap();

// Create with explicit config (opt-in: custom path, shard count, memory budget).
let ns2 = Namespace::create_with(NamespaceOpts {
    path: Some("/mnt/fast/db".into()),
    shards: 8,
    mem_budget_mb: Some(1024),
}).unwrap();

// Create a MapxRaw in an explicit namespace.
let mut map = MapxRaw::new_in(&ns);
map.insert(b"k", b"v");
assert_eq!(map.namespace().id(), ns.id());

// Persist and recover via InstanceId.
let id: InstanceId = map.save_meta().unwrap();
// id.to_string() => e.g. "42@1" (map 42 in namespace 1)
let restored = MapxRaw::from_meta(id).unwrap();
let ns_id = ns.id();

// Admin tier: list, close, destroy, relocate.
let all = Namespace::list().unwrap();
for info in &all {
    println!("ns {} at {:?}, {} shards", info.id, info.path, info.shards);
}

// Close: flush and release resources (engine threads, LOCK files).
// Requires all client handles dropped — reopen is restart-equivalent.
drop((restored, map, ns));
Namespace::close_by_id(ns_id).unwrap();

// Consuming form: the handle itself is accounted for; refusal hands
// it back so a live namespace is never invalidated.
let ns3 = Namespace::create().unwrap();
match ns3.close() {
    Ok(()) => {}                       // closed
    Err((Some(ns3), _e)) => { /* refused — `ns3` is still usable */ }
    Err((None, _e)) => { /* closed, but teardown reported an error */ }
}

// Cross-namespace deep copy: the counterpart of `Clone` that picks
// the target namespace (chunked; never whole-map in memory).
let src = MapxRaw::new_in(&ns2);
let copy = src.clone_in(&Namespace::default_ns()).unwrap();

// Destroy: remove the whole directory tree without per-key traversal.
// Filesystem work scales with the number of files/directories.
// Requires the namespace be not-open.
Namespace::destroy(ns_id).unwrap();

// Relocate: re-point a namespace at a new root directory.
// Data movement is the operator's job; the target must hold
// an initialized dataset (format marker + per-shard CURRENT anchors).
let ns2_id = ns2.id();
let old_root = ns2.path().to_owned();
drop((src, ns2));
Namespace::close_by_id(ns2_id).unwrap();
std::fs::rename(old_root, "/mnt/archive/db").unwrap();
Namespace::relocate(ns2_id, "/mnt/archive/db").unwrap();

// Per-shard engine telemetry (mmdb property names), one reading per
// shard in shard order — e.g. cache hit/miss counters. Each engine's
// shards share one block-cache pool, so a hot collection (one
// collection = one shard) can use the engine's whole cache slice.
let ns4 = Namespace::create().unwrap();
let hits = ns4.shard_properties("stats.block_cache_hits");
let misses = ns4.shard_properties("stats.block_cache_misses");
assert_eq!(hits.len(), misses.len());
```
