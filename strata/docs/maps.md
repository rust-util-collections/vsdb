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
counts.insert(&"pears".to_owned(), &3);
assert_eq!(counts.get("pears"), Some(3));
counts.remove("pears");
assert!(!counts.contains_key("pears"));
```

Borrowed lookups use the stored key type's encoding. `String` accepts `str`.
Byte keys (`Vec<u8>`, `Box<[u8]>`, `[u8; N]`) accept byte slices for `get`,
`get_mut`, `contains_key`, and `remove`. An array key matches only a slice
of the same length with the same contents.

```rust
use vsdb::Mapx;

let mut map = Mapx::<[u8; 2], u64>::new();
map.insert(&[1, 0], &7);
assert_eq!(map.get(&[1, 0][..]), Some(7));
assert_eq!(map.get(&[0][..]), None);
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

let mut latest = Orphan::new(40_u64);
latest.update(|v| *v += 2).unwrap();
let mut by_path = MapxOrdRawKey::<u64>::new();
by_path.insert(b"/index", &latest.get_value());
assert_eq!(by_path.get(b"/index"), Some(42));
```

## Basic collection read views

Use `map.reader()` to share access while the original handle keeps writing.
The reader retains the namespace. Each call to `read_view()` captures one
committed state for lookups and iteration. Ordered views also support
ranges. An `OrphanReader` reads the current value on each `get()`.

```rust
use vsdb::MapxOrd;
let mut m = MapxOrd::<String, u64>::new();
m.insert(&"balance".to_owned(), &5);
let reader = m.reader();
let before = reader.read_view();
m.update("balance", |v| *v += 2).unwrap();
assert_eq!(before.get("balance"), Some(5));
assert_eq!(reader.read_view().get("balance"), Some(7));
```

## Edit a value

Mutable value guards write back after a mutable dereference or a vacant
insertion. Use `update` or `guard.commit()` to receive encoding and storage
errors. Mutate a `RefCell` via the guard's mutable dereference. Debug builds
diagnose interior changes made through a shared dereference.

## Durability fences

Use `commit_sync()` to synchronize a typed map batch or `try_sync_wal()`
to fence earlier writes on its shard. For empty batches, admission refusal
and recovery after an I/O error, see
[batch visibility and durability](../../docs/storage.md#batch-visibility-and-durability).

`Mapx`, `MapxOrd`, `MapxOrdRawKey`, and `Orphan` expose `try_sync_wal()`.

```rust
use vsdb::MapxOrd;
fn main() -> vsdb::Result<()> {
    let mut state = MapxOrd::<u64, String>::new();
    state.insert(&1, &"accepted".to_owned());
    state.try_sync_wal()?;
    Ok(())
}
```

Use `collection.namespace().try_sync_wal()?` to fence all shards of a
namespace, including those used by a composite index such as `SlotDex`.
The basic collections' `sync_wal()` convenience method panics on errors.
