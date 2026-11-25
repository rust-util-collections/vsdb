# vsdb_core API Examples

## Configure the universe

Configure once, before opening a collection or requesting a VSDB directory.
`vsdb_configure` resolves relative paths against the current working directory
and selects the open mode for every namespace in this process.

```rust,no_run
use vsdb_core::{VsdbOptions, vsdb_configure, vsdb_get_base_dir};

vsdb_configure(VsdbOptions::new("/srv/my-app/vsdb")).unwrap();
let dir = vsdb_get_base_dir();

// A child process selects its own configuration.
let mut child = std::process::Command::new("my-worker");
child.env("VSDB_BASE_DIR", dir);
```

The default writable path comes from `VSDB_BASE_DIR`, then `$HOME/.vsdb`.
If neither variable is available, VSDB falls back to a per-process temporary
path. For an existing read-only dataset, use
`VsdbOptions::read_only(path)`. See the [read-only deployment guide](read-only.md).

Writable configuration protects the universe from v17 writers by publishing
`__SYSTEM__/format_version` with value `18` before first use, including when
only non-default namespaces will be opened. Placed engines publish the same
marker before creating shards. Writable reopen backfills an absent marker on
an existing v18 root. Keep v17 processes offline while reopening an early v18
dataset with a missing marker. A conflicting marker (including one written by
an older writer) requires investigation against a trusted backup. Read-only
reopen preserves the original files.

## MapxRaw

Raw maps store arbitrary byte strings. Iteration uses lexicographic key order.

```rust
use vsdb_core::MapxRaw;

let mut map = MapxRaw::new();
map.insert(b"key1", b"value1");
map.insert(b"key2", b"value2");
assert_eq!(map.get(b"key1").as_deref(), Some(&b"value1"[..]));
assert!(map.contains_key(b"key2"));
map.remove(b"key1");
assert!(!map.contains_key(b"key1"));

let id = map.save_meta().unwrap();
drop(map);
let restored = MapxRaw::from_meta(id).unwrap();
assert_eq!(restored.get(b"key2").as_deref(), Some(&b"value2"[..]));
```

Keep the `InstanceId` in application storage for the next process. The
token selects the namespace and the metadata file. Both `from_meta` and
serde deserialization restore access to the original rows. Coordinate writes
through multiple restored handles to exclude simultaneous writes to the same key.

A new raw map reserves its prefix on first use. `try_instance_id()` returns
that identity and reports allocator I/O errors. `save_meta()` also propagates
reservation errors, including on an empty map.

## Atomic batches and durability

Ordinary writes use the shard WAL and may become visible before synchronization.
Choose the boundary your application needs.

| Operation | Scope and guarantee |
| --- | --- |
| `batch.commit()` | Publishes the staged edits atomically under one collection prefix. |
| `batch.commit_sync()` | Synchronizes the shard WAL before publishing the staged edits. An empty batch performs no fence. |
| `map.try_sync_wal()` | Fences prior successful writes on the assigned shard. An untouched map with no assigned shard returns immediately. `sync_wal()` is the panic-on-error form. |
| `ns.try_sync_wal()` | Fences the namespace's shards in order. An error can leave earlier shards synchronized. |

A batch created with `batch_wiped()` starts with a deletion covering the whole map.
`delete_range(start, end)` stages a half-open deletion. Later inserts in the
same batch take precedence. Both commit methods consume the batch. After an
I/O failure, check the recovered state before retrying because the WAL may
contain the write. WAL synchronization leaves memtable flushing independent.
A batch covers one map even when several maps share a shard.

```rust
use vsdb_core::MapxRaw;

let mut map = MapxRaw::new();
let mut batch = map.batch();
batch.insert(b"version", b"2");
batch.insert(b"status", b"ready");
batch.commit_sync().unwrap();
assert_eq!(map.get(b"status").as_deref(), Some(&b"ready"[..]));
```

## Immutable iteration alongside writes

Use `MapxRaw::read_view()` for several queries against one committed state of
a map. It supports `get`, `contains_key`, `iter` and `range`. A fatal engine
error panics. Each independently captured view may see a different state.
Release views and iterators when done so compaction can reclaim old versions.

To continue writing while a view is alive, capture it through a reader.

```rust
use vsdb_core::MapxRaw;
let mut map = MapxRaw::new();
map.insert(b"key", b"before");
let reader = map.reader();
let view = reader.read_view();
map.insert(b"key", b"after");
assert_eq!(view.get(b"key").unwrap(), b"before");
assert_eq!(reader.get(b"key").unwrap(), b"after");
```

`MapxRawReader::clone` shares read-only access, while `MapxRaw::clone` still
deep-copies. A reader retains namespace ownership. Closing that namespace is
refused until all readers and other owning handles are dropped. Views borrow
their map or reader. Their ordinary `iter` and `range` iterators borrow the view.
Ranges accept owned or borrowed byte bounds such as `b"a"..b"z"` and `..`.

An immutable `iter` or `range` retains one committed view captured when the
iterator is created. Writes, clears and flushes through another handle do not
change that view. Separate queries can see different states. The `shadow`
contract permits these reads while retaining its exclusion of competing
writers to the same key. Mutable iteration counts as a write.

Use `range_detached` on a raw map or its view to keep iterating after the
borrow ends. The iterator can retain its source files and memory even after
the owning namespace closes.

```rust
use vsdb_core::MapxRaw;
let mut map = MapxRaw::new();
map.insert(b"key", b"before");
let mut view = map.range_detached(..);
map.insert(b"key", b"after");
assert_eq!(view.next().unwrap().1, b"before");
```

## Memory budget

The default namespace uses a 2 GiB sizing input unless overridden.
A nonzero `with_mem_budget_mb` setting takes precedence over `VSDB_MEM_BUDGET_MB`.

```rust,no_run
use vsdb_core::{VsdbOptions, vsdb_configure};

vsdb_configure(VsdbOptions::new("/data/vsdb").with_mem_budget_mb(8192)).unwrap();
```

Non-default namespaces use `NamespaceOpts::mem_budget_mb` and default to
512 MiB each. All MB-named options use binary MiB. Budgets size caches and
write buffers with floors and caps. Process RSS also includes pinned blocks,
metadata and application allocations.
See the [memory design](../../docs/proposals/shared-mem-pool.md).

## Namespaces

`MapxRaw::new_in(&ns)` selects an engine. `ns.scope(|| MapxRaw::new())` selects
the same engine for constructors on the current thread. Existing handles always
route to their recorded namespace. For a fresh map on the **same shard** as an
existing map, use `new_colocated`.

```rust
use vsdb_core::{MapxRaw, Namespace};

let ns = Namespace::create().unwrap();
let map = MapxRaw::new_in(&ns);
let colocated = map.new_colocated();
assert!(map.is_colocated_with(&colocated));

let target = Namespace::default_ns();
let copy = map.clone_in(&target).unwrap();
assert_eq!(copy.namespace().id(), target.id());
```

### Placement and telemetry

`Namespace::create()` derives the root under `__NAMESPACES__` and defaults to
four shards. `create_with` accepts an explicit root, shard count (clamped to
1–64) and memory sizing. The registry persists those choices. Each engine
shares a single block cache across its shards.

```rust,no_run
use vsdb_core::{Namespace, NamespaceOpts};

let ns = Namespace::create_with(NamespaceOpts {
    path: Some("/mnt/fast/db".into()),
    shards: 8,
    mem_budget_mb: Some(1024),
}).unwrap();

let hits = ns.shard_properties("stats.block_cache_hits");
let misses = ns.shard_properties("stats.block_cache_misses");
assert_eq!(hits.len(), misses.len());
```

### Closing, relocating and destroying

`Namespace::list()` lists registered non-default namespaces. Drop owning
collections, readers and other namespace handles before closing one. On a
writable close, the engine flushes and synchronizes its shards, releases its
locks and leaves the registry entry available for `Namespace::open(id)`.

```rust
use vsdb_core::Namespace;

let ns = Namespace::create().unwrap();
let id = ns.id();
match ns.close() {
    Ok(()) => {
        let reopened = Namespace::open(id).unwrap();
        assert_eq!(reopened.id(), id);
    }
    Err((Some(ns), error)) => eprintln!("namespace {} still open: {error}", ns.id()),
    Err((None, error)) => eprintln!("namespace closed with a teardown error: {error}"),
}
```

`close_by_id(id)` takes an ID and requires all owning handles to have been
dropped. The default namespace stays open for the lifetime of the process.

To move a closed non-default namespace, move the complete directory first,
then call `Namespace::relocate(id, new_path)` to update the registry. VSDB
checks the destination's universe and namespace IDs, shard count, completion
marker and shard anchors. To delete the namespace permanently, call
`Namespace::destroy(id)` while it is closed. Destruction removes its registry
entry and the whole directory tree.
