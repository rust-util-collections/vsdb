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
`VsdbOptions::read_only(path)`. See the [read-only deployment guide](../../docs/read-only.md).

Writable configuration protects the universe from v17 writers by publishing
`__SYSTEM__/format_version` with value `18` before first use, including when
only non-default namespaces will be opened. Placed engines publish the same
marker before creating shards. Writable reopen backfills an absent marker on
an existing v18 root. Keep v17 processes offline while reopening an early v18
dataset with a missing marker. A conflicting marker (including one written by
an older writer) requires investigation against a trusted backup. Read-only
reopen preserves the original files.

A namespace verifies each existing shard's `CURRENT` and v5 MANIFEST
header before it opens any of its shards for writable recovery. Seven
levels and an eight-byte prefix are required on read-only and writable
opens. MMDB checks the remaining manifest records, WALs and table files
as it opens each shard.

For named engines, see [namespace placement and lifecycle](../../docs/namespaces.md).
For configuration, sizing formulas and per-shard telemetry, see
[memory sizing](../../docs/storage.md#memory-sizing).

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

A batch created with `batch_wiped()` starts with a deletion covering the whole map.
`delete_range(start, end)` stages a half-open deletion. Later inserts in the
same batch take precedence. Commit consumes the batch. Use `commit_sync()` when this
write needs a synchronized shard WAL.

```rust
use vsdb_core::MapxRaw;

let mut map = MapxRaw::new();
let mut batch = map.batch();
batch.insert(b"version", b"2");
batch.insert(b"status", b"ready");
batch.commit_sync().unwrap();
assert_eq!(map.get(b"status").as_deref(), Some(&b"ready"[..]));
```

To handle admission pressure, match
`VsdbError::Storage { kind: StorageErrorKind::Busy, .. }` and rebuild the
refused batch. The storage guide describes which errors can leave an
ambiguous recovery outcome and how to fence earlier writes.
[Batch visibility and durability](../../docs/storage.md#batch-visibility-and-durability)

## Immutable iteration alongside writes

Use `MapxRaw::read_view()` for several queries against one committed state of
a map. It supports `get`, `contains_key`, `iter` and `range`. A fatal engine
error panics. Each independently captured view may see a different state.
Release views and iterators when done so compaction can reclaim old versions.

Use `try_get_with(key, decode)` on a map, reader or captured view to decode
from borrowed bytes while MMDB holds the backing allocation. The decoder
runs for a stored empty value and is skipped for a missing or deleted key.
A view's `try_visit_many(keys, visit)` resolves up to 32 keys at the same
snapshot, then visits every input slot in order, including duplicates and
misses. All returned backing allocations stay pinned until visiting ends,
including early return on a visitor error. The limit of 32 counts keys;
different SST blocks or memtable arenas can keep substantially more memory
alive than the requested value bytes. A storage failure precedes the first
callback. A first read of a fresh raw map can reserve its identity and
return a prefix-allocation error. `try_range` and its cursor's `try_step_next` and
`try_step_prev` expose typed storage errors at open or during iteration.
Each step lends the raw key suffix and value until the next cursor operation.

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
