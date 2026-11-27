# SlotDex pagination and counts

`SlotDex<S, K>` indexes unique `(slot, key)` pairs. A timestamp is one
possible slot. A key can appear in several slots and contributes one entry
in each. `SlotDex64<K>` uses `u64` slots.

## Insert and query entries

```rust
use vsdb::{SlotDex64, slotdex::Order};

let mut events = SlotDex64::<u64>::new(8, false).unwrap();
events.insert_batch([(100, 2), (100, 1), (200, 3), (300, 4)]).unwrap();
assert_eq!(events.count(100..300), 3);

assert_eq!(events.page(.., 2, 0, Order::Desc), [4, 3]);
assert_eq!(events.page(.., 2, 1, Order::Desc), [1, 2]);
```

Use `page(slots, page_size, page_index, order)` to seek to a zero-based page,
`count(slots)` to count pairs, and `iter(slots, order)` to stream keys.
Each call captures one committed state. A single iterator keeps that state
across the scan. Separate page requests can observe intervening writes.

## Slot encoding and scan order

Slots implement `SlotType` with a fixed-width encoding. The first argument
to `new` is the tier capacity, a `u32` of at least two. Tiers store counts
over successively larger ranges. Built-in signed slot types align their
buckets relative to the type's minimum value.

The second argument, `swap_order`, reverses the physical order of slots.
`Order::Asc` and `Order::Desc` always refer to logical slot order. Keys within
each slot stay in ascending key order. A scan in reverse physical order buffers
up to 256 keys and 16 KiB of raw key data across adjacent slots. Larger
groups stream individually. [Slot queries](../src/slotdex/query.rs)

## Batch writes and restored handles

`insert_batch` publishes the whole call in one ordinary shard batch. Entries,
counts and the writer generation change together. For power-loss durability
after a successful write, call `events.namespace().try_sync_wal()`.
[Batch visibility and durability](../../docs/storage.md#batch-visibility-and-durability)

Serialize writers that share an index. If another handle has advanced the
generation, a stale writer returns an error, including on `clear`. Restore a
fresh handle before writing again. Readers through restored handles can run
concurrently. Reopen checks the widths and slot encodings of tier rows.
`clone()` and `clone_in()` deep-copy the index to an independent prefix.
