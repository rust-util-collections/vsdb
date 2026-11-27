# Maps and values

Use `Mapx` for exact lookups, `MapxOrd` for ordered ranges,
`MapxOrdRawKey` for byte keys and `Orphan` for a single value. Each handle
opens a persistent collection in one namespace. [Placement and handle restoration](../../docs/namespaces.md#create-capture-and-restore)

## Mapx

`Mapx<K, V>` looks up keys by their encoded bytes. Its iterator visits
encoded-key order, which can differ from the Rust key type's sort order.

```rust
use vsdb::Mapx;

let mut counts = Mapx::<String, u64>::new();
counts.insert(&"pears".into(), &3);
assert_eq!(counts.get("pears"), Some(3));
```

Borrowed lookups use the stored key type's encoding. `String` accepts `str`.
Byte keys (`Vec<u8>`, `Box<[u8]>`, `[u8; N]`) accept byte slices for `get`,
`get_mut`, `contains_key`, and `remove`. An array key matches only a slice
of the same length with the same contents.

```rust
use vsdb::Mapx;

let mut scores = Mapx::<[u8; 2], u64>::new();
scores.insert(b"GB", &7);
assert_eq!(scores.get(b"GB".as_slice()), Some(7));
assert_eq!(scores.get(b"G".as_slice()), None);
```

Implement `KeyLookup<Q>` or `OrderedKeyLookup<Q>` on the owning key type to
add a borrowed exact-match form. Equal keys must have a canonical encoding. The
blanket implementation for serde-encoded equality keys requires `Eq + Hash`.

## MapxOrd

`MapxOrd<K, V>` uses an order-preserving key encoding. Iteration, ranges,
`first()` and `last()` follow that order. Maps with `String` keys also
accept string-slice bounds such as `map.range("a".."z")`.

```rust
use vsdb::MapxOrd;

let mut counts = MapxOrd::<i32, u64>::new();
counts.insert(&3, &30);
counts.insert(&1, &10);
counts.insert(&2, &20);

assert_eq!(counts.range(1..3).collect::<Vec<_>>(), [(1, 10), (2, 20)]);
assert_eq!(counts.last(), Some((3, 30)));
```

## Single values and byte keys

Use `Orphan<T>` for a standalone value and `MapxOrdRawKey<V>` for a typed
value indexed by ordered application-defined bytes.

```rust
use vsdb::{MapxOrdRawKey, Orphan};

let mut visits = Orphan::new(0_u64);
visits.update(|count| *count += 1).unwrap();
assert_eq!(visits.get_value(), 1);

let mut status = MapxOrdRawKey::new();
status.insert(b"/index", &200_u16);
assert_eq!(status.get(b"/index"), Some(200));
```

## Basic collection read views

Use `map.reader()` to share access while the original handle keeps writing.
The reader retains the namespace. Each call to `read_view()` captures one
committed state for lookups and iteration. Ordered views also support
ranges. An `OrphanReader` reads the current value on each `get()`.

```rust
use vsdb::Mapx;

let mut balances = Mapx::<u32, u64>::new();
balances.insert(&1, &5);
let reader = balances.reader();
let before = reader.read_view();

balances.update(&1, |balance| *balance += 2).unwrap();
assert_eq!(before.get(&1), Some(5));
assert_eq!(reader.read_view().get(&1), Some(7));
```

## Edit a value

Mutable value guards write back after a mutable dereference or a vacant
insertion. Use `update` or `guard.commit()` to receive encoding and storage
errors. Mutate a `RefCell` via the guard's mutable dereference. Debug builds
diagnose interior changes made through a shared dereference.

## Durability fences

Use `commit_sync()` for a durable batch. On `Mapx`, `MapxOrd`,
`MapxOrdRawKey` and `Orphan`, `try_sync_wal()` fences earlier writes on
the collection's shard. [Empty batches, admission and recovery](../../docs/storage.md#batch-visibility-and-durability)

```rust
use vsdb::Orphan;

let mut last_event = Orphan::new(0_u64);
last_event.set_value(&42);
last_event.try_sync_wal().unwrap();
```

Use `collection.namespace().try_sync_wal()?` to fence all shards of a
namespace, including those used by a composite index such as `SlotDex`.
The basic collections' `sync_wal()` convenience method panics on errors.
