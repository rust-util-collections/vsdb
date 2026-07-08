# vsdb_core API Examples

This document provides examples for the public APIs in the `vsdb_core` crate.

## MapxRaw

`MapxRaw` is a raw key-value store.

```rust
use vsdb_core::MapxRaw;
use vsdb_core::common::{vsdb_set_base_dir, vsdb_get_base_dir};

// It's recommended to set a base directory for the database.
// vsdb_set_base_dir("/tmp/vsdb_core_test").unwrap();

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

## Utility Functions

Example for getting and setting the base directory.

```rust
use vsdb_core::{vsdb_set_base_dir, vsdb_get_base_dir};

// Set a custom base directory.
// Call this early in main, before spawning any threads — it mutates
// the process environment (`VSDB_BASE_DIR`).
vsdb_set_base_dir("/tmp/my_vsdb_data").unwrap();

// Get the current base directory
let dir = vsdb_get_base_dir();
assert_eq!(dir.to_str().unwrap(), "/tmp/my_vsdb_data");
```

## Namespaces

`vsdb_core` provides the namespace subsystem — independently-rooted engine
instances that coexist in one process, each with its own base dir, mmdb
shards, and memory budget.  All collection types (`MapxRaw` included) gain
`new_in` for explicit placement and `namespace()` for querying ownership.

```rust
use vsdb_core::{
    MapxRaw,
    common::{
        Namespace, NamespaceOpts, InstanceId, DEFAULT_NS_ID,
        vsdb_ns_list, vsdb_ns_close, vsdb_ns_destroy, vsdb_ns_relocate,
    },
};

// Create a namespace — parameterless, gives a fresh anonymous placement group.
let ns = Namespace::create().unwrap();

// Create with explicit config (opt-in: custom path, shard count, memory budget).
let ns2 = Namespace::create_with(NamespaceOpts {
    path: Some("/mnt/fast/db".into()),
    shards: 8,
    mem_budget_mb: 1024,
}).unwrap();

// Create a MapxRaw in an explicit namespace.
let map = MapxRaw::new_in(&ns);
map.insert(b"k", b"v");
assert_eq!(map.namespace().id(), ns.id());

// Persist and recover via InstanceId.
let id: InstanceId = map.save_meta().unwrap();
// id.to_string() => e.g. "42@1" (map 42 in namespace 1)
let restored = MapxRaw::from_meta(id).unwrap();

// Admin tier: list, close, destroy, relocate.
let all = vsdb_ns_list().unwrap();
for info in &all {
    println!("ns {} at {:?}, {} shards", info.id, info.path, info.shards);
}

// Close: flush and release resources (engine threads, LOCK files).
// Requires all client handles dropped — reopen is restart-equivalent.
vsdb_ns_close(ns.id()).unwrap();

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

// Destroy: O(1) bulk reclaim of the entire directory tree.
// Requires the namespace be not-open.
vsdb_ns_destroy(ns.id()).unwrap();

// Relocate: re-point a namespace at a new root directory.
// Data movement is the operator's job; the target must hold
// an initialized dataset (format marker + per-shard CURRENT anchors).
vsdb_ns_relocate(ns2.id(), "/mnt/archive/db").unwrap();

// Per-shard engine telemetry (mmdb property names), one reading per
// shard in shard order — e.g. cache hit/miss counters. Each engine's
// shards share one block-cache pool, so a hot collection (one
// collection = one shard) can use the engine's whole cache slice.
let ns4 = Namespace::create().unwrap();
let hits = ns4.shard_properties("stats.block_cache_hits");
let misses = ns4.shard_properties("stats.block_cache_misses");
assert_eq!(hits.len(), misses.len());
```
