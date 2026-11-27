# Raw maps and process configuration

## Configure the universe

Configure once, before opening a collection or requesting a VSDB directory.
`vsdb_configure` resolves relative paths against the current working directory
and selects the open mode for every namespace in this process.

```rust,no_run
use vsdb_core::{VsdbOptions, vsdb_configure};

vsdb_configure(VsdbOptions::new("/srv/my-app/vsdb")).unwrap();
```

Without explicit configuration the process opens for writing. The base path
comes from `VSDB_BASE_DIR`, then `$HOME/.vsdb`.
If neither variable is available, VSDB falls back to a per-process temporary
path. For an existing read-only dataset, use
`VsdbOptions::read_only(path)`. See the [read-only deployment guide](../../docs/read-only.md).

Use `VsdbOptions::with_mem_budget_mb` on the options for the default
namespace. Additional namespaces take their own path, shard count and
budget at creation. [Memory sizing](../../docs/storage.md#memory-sizing) · [Namespace placement](../../docs/namespaces.md#create-capture-and-restore)

## MapxRaw

Raw maps store arbitrary byte strings. Iteration uses lexicographic key order.

```rust
use vsdb_core::MapxRaw;

let mut settings = MapxRaw::new();
settings.insert(b"theme", b"dark");

let id = settings.save_meta().unwrap();
let restored = MapxRaw::from_meta(id).unwrap();
assert_eq!(restored.get(b"theme").unwrap(), b"dark");
```

Keep the `InstanceId` in application storage for the next process. In the
same universe, it selects the namespace and saved metadata file.
Deserializing a previously serialized raw handle also reopens the original
rows. Across restored handles, exclude simultaneous writes to the same key.

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
assert_eq!(map.get(b"status").unwrap(), b"ready");
```

To let the caller retry when the shard would stall on admission, finish the
batch with `commit_no_slowdown()` or `commit_sync_no_slowdown()`.
`VsdbError::Storage { kind: StorageErrorKind::Busy, .. }` means this batch
was refused before WAL append. Rebuild it to retry. An I/O failure after
WAL append starts can leave a recoverable batch.
[Batch visibility and durability](../../docs/storage.md#batch-visibility-and-durability)

## Immutable iteration alongside writes

Use `MapxRaw::read_view()` for several queries against one committed state of
a map. It supports `get`, `contains_key`, `iter` and `range`. A fatal engine
error panics. Capture the view through a reader to keep writing through the map.

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

Each new view or immutable iterator captures the state at creation. Later
writes, clears and flushes leave that state intact. Release views and iterators
when done so compaction can reclaim old versions. The same reads are valid
through `shadow`. Mutable iteration counts as a write and requires excluding
other writers to the same keys.

### Detached ranges

Use `range_detached` on a raw map or its view to keep iterating after the
borrow ends. The iterator can retain its source files and memory even after
the owning namespace closes.

```rust
use vsdb_core::MapxRaw;

let mut map = MapxRaw::new();
map.insert(b"key", b"before");
let mut rows = map.range_detached(..);
map.insert(b"key", b"after");
assert_eq!(rows.next().unwrap().1, b"before");
```

### Borrowed values and fallible cursors

- `try_get_with(key, decode)` on a map, reader or captured view lends the
  value bytes to the decoder. The decoder runs for a stored empty value. A
  missing or deleted key returns `Ok(None)` and skips the callback. The
  first read of a fresh raw map can return a prefix-allocation error.
- A view's `try_visit_many(keys, visit)` resolves up to 32 input slots at
  one sequence before the first callback. It then visits them in input
  order, including duplicates and misses, until completion or a visitor
  error. Values can pin whole SST blocks or memtable arenas throughout
  the visit. A storage error during resolution precedes all callbacks.
- `try_range` reports errors opening a cursor. Its `try_step_next` and
  `try_step_prev` methods report errors encountered during iteration. Each
  successful step lends a key suffix and value until the next cursor operation.
