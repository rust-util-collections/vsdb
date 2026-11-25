# Collection examples

| Task | Examples |
|---|---|
| Store and edit typed data | [Mapx](#mapx), [MapxOrd](#mapxord), [single values and byte keys](#single-values-and-byte-keys) |
| Share or place collections | [Read views](#basic-collection-read-views), [namespaces](#namespaces), [read-only processes](read-only.md) |
| Make writes durable | [Batch commits and WAL fences](#durability-fences) |
| Track versions and prove state | [VerMap](#vermap), [Merkle tries](#mptcalc--smtcalc-merkle-trie), [VerMapWithProof](#vermapwithproof) |
| Index slots, overlays or vectors | [SlotDex](#slotdex), [DagMap](#dagmap), [VecDex](vecdex.md) |

## Mapx

`Mapx` is a hash map-like data structure that stores key-value pairs.

```rust
use vsdb::Mapx;

let mut map: Mapx<String, String> = Mapx::new();

// Insert some key-value pairs
map.insert(&"key1".to_string(), &"value1".to_string());
map.insert(&"key2".to_string(), &"value2".to_string());

// Get a value
assert_eq!(map.get("key1"), Some("value1".to_string()));

// Check if a key exists
assert!(map.contains_key("key2"));

// Iterate over the key-value pairs
for (key, value) in map.iter() {
    println!("{}: {}", key, value);
}

// Remove a key-value pair
map.remove("key1");
```

Borrowed lookups use the stored key type's encoding. `String` accepts `str`;
byte keys (`Vec<u8>`, `Box<[u8]>`, `[u8; N]`) accept byte slices for `get`,
`get_mut`, `contains_key`, and `remove`. Array lookups validate the full slice,
including its length:

```rust
use vsdb::Mapx;

let mut map = Mapx::<[u8; 2], u64>::new();
map.insert(&[1, 0], &7);
assert_eq!(map.get(&[1, 0][..]), Some(7));
assert_eq!(map.get(&[0][..]), None);
```

Implement `KeyLookup<Q>` or `OrderedKeyLookup<Q>` on the owning key type to
add a borrowed exact-match form. Equal keys must have a canonical encoding. The
serde blanket for equality keys requires `Eq + Hash`. Ordered maps with string
keys accept string-slice bounds such as `map.range("a".."z")`.

## MapxOrd

`MapxOrd` is a B-tree map-like data structure that stores key-value pairs in a sorted order.

```rust
use vsdb::MapxOrd;

let mut map: MapxOrd<i32, String> = MapxOrd::new();

// Insert some key-value pairs
map.insert(&3, &"three".to_string());
map.insert(&1, &"one".to_string());
map.insert(&2, &"two".to_string());

// Get a value
assert_eq!(map.get(&1), Some("one".to_string()));

// Iterate over the key-value pairs in sorted order
for (key, value) in map.iter() {
    println!("{}: {}", key, value);
}

// Get the first and last key-value pairs
assert_eq!(map.first(), Some((1, "one".to_string())));
assert_eq!(map.last(), Some((3, "three".to_string())));
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

Obtain a cheap, cloneable `MapReader` with `map.reader()`. A reader retains the namespace independently of the writer. Call `reader.read_view()` or `map.read_view()` to capture a single sequence for point queries and iteration. The ordered views also support ranges. An `OrphanReader` reads its latest value with `get()`.

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

Mutable value guards write back after a mutable dereference or a vacant insertion. Use `update` or `guard.commit()` to receive encoding and storage errors. Mutate a `RefCell` via the guard's mutable dereference. Debug builds diagnose interior changes made through a shared dereference.

## Namespaces

Persistent collections support namespaces — independently-rooted engine instances
for physical placement and whole-directory removal without per-key traversal.
In-memory `MptCalc` / `SmtCalc` need no namespace; `VerMapWithProof` inherits
placement from its wrapped `VerMap`. A creation scope selects a namespace for ordinary constructors.

```rust,no_run
use vsdb::{Mapx, MapxOrd, Namespace, NamespaceOpts};

let cold = Namespace::create().unwrap();

// Ambient placement: ordinary new() calls inside scope() use `cold`.
// Explicit new_in() and DagMap children use their explicit/parent namespace.
let archive: Mapx<u64, String> = cold.scope(|| Mapx::new());

// Same namespace placement (not necessarily the same shard).
let index = Mapx::<u64, u64>::new_in(&archive.namespace());
assert_eq!(archive.namespace().id(), index.namespace().id());

// Save the handle and restore the same collection.

let id = archive.save_meta().unwrap();
// The token displays as "<map_id>@<namespace_id>" for this namespace.
let restored: Mapx<u64, String> = Mapx::from_meta(id).unwrap();

// A default-namespace v18 handle can also be looked up by its numeric map ID.
let local: Mapx<u64, String> = Mapx::new();
let numeric_id = local.save_meta().unwrap().map_id;
let from_id: Mapx<u64, String> = Mapx::from_meta(numeric_id).unwrap();

// Place this namespace on an explicit volume with its own shard count and budget.

let fast = Namespace::create_with(NamespaceOpts {
    path: Some("/mnt/nvme/vsdb".into()),
    shards: 8,
    mem_budget_mb: Some(2048),
}).unwrap();
let hot: MapxOrd<i32, String> = MapxOrd::new_in(&fast);

// Deep-copy the entries into a fresh instance in the default namespace.
let archive_copy = archive.clone_in(&Namespace::default_ns()).unwrap();
```

Typed handles restored by serde or `from_meta` check a tag of the fully
qualified Rust type name, including generic parameters. Unrelated types with
the same short name are rejected. Moving or renaming a persisted type requires
an explicit migration; a type alias referring to the original type is fine.
Application field changes under the same name require explicit schema versioning.
v18 typed handles use `VSTYPE04`. Convert v17 handles offline using the
[migration guide](../../migration/README.md).

### Placement and memory

- `Mapx::new()` uses the current creation scope, or the default namespace outside one.
- Two `new_in(&ns)` calls can route to different shards; namespace placement alone does not provide a shared WAL.
- A composite structure (`VerMap`, `SlotDex`, …) always lives wholly inside one namespace.
- Cross-namespace atomic transactions do not exist (separate WALs).
- Reads, writes, and deserialization always route via the handle's own namespace —
  ambient scope affects creation only.
- Memory sizing defaults to 2 GiB for the default namespace and 512 MiB for
  each non-default namespace, without host-RAM/cgroup detection.
  A nonzero `VsdbOptions::with_mem_budget_mb` (then `VSDB_MEM_BUDGET_MB`) configures the
  default engine; `NamespaceOpts::mem_budget_mb` configures others. Units are
  binary MiB. These inputs are not hard RSS limits: floors, caps, pinned
  blocks, and runtime metadata affect actual memory use. See the
  [memory design](../../docs/proposals/shared-mem-pool.md).

## Durability fences

`Mapx`, `MapxOrd`, and `MapxOrdRawKey` batches also expose `commit_sync()`.
This opt-in operation synchronizes the shard WAL before atomic publication;
ordinary `commit()` keeps its existing asynchronous default. An empty batch is
not a fence for earlier writes. No cross-collection transaction is implied.
An I/O failure may have left recoverable WAL data, so avoid automatic retries.

SlotDex and VecDex use ordinary shard batches for mutations. Preflight
rejection leaves that batch unapplied, and runtime caches update only after
successful commits. A persistence error can still leave a complete WAL batch
that recovery replays; an error does not establish that recovered disk state
is unchanged. Related rows recover together as one atomic batch.

`Mapx`, `MapxOrd`, `MapxOrdRawKey`, and `Orphan` expose `try_sync_wal()`:

```rust
use vsdb::MapxOrd;
fn main() -> vsdb::Result<()> {
    let mut state = MapxOrd::<u64, String>::new();
    state.insert(&1, &"accepted".to_owned());
    state.try_sync_wal()?;
    Ok(())
}
```

The fence makes prior successful writes on the collection's shard durable
without forcing a memtable flush; it returns synchronization errors. Other
shards need their own fences. Use `collection.namespace().try_sync_wal()?` to
cover every shard of that namespace, including composite indexes such as
`SlotDex`. Shards synchronize sequentially, so an error can leave earlier
shards synchronized. These calls do not provide cross-collection transactions.
They are no-ops in read-only mode. The collection-level `sync_wal()` form
panics on errors; ordinary inserts and batch commits do not fsync individually.

## VerMap

`VerMap` provides Git-style versioned storage with branching, commits, merge, and rollback (`rollback_to`).

The version-store guide explains working roots, merge rules and durable publication.
[VerMap architecture and recovery](versioned.md)

```rust
use vsdb::versioned::map::VerMap;
use vsdb::versioned::BranchId;

// 1. Create an empty versioned map (starts with a "main" branch).
let mut m: VerMap<u32, String> = VerMap::new();
let main = m.main_branch();

// 2. Write on the main branch and commit a snapshot.
m.insert(main, &1, &"hello".into()).unwrap();
m.commit(main).unwrap();

// 3. Fork a feature branch — cheap, no data copied.
let feat: BranchId = m.create_branch("feature", main).unwrap();
m.insert(feat, &1, &"updated".into()).unwrap();
m.commit(feat).unwrap();

// 4. Branches are isolated.
assert_eq!(m.get(main, &1).unwrap(), Some("hello".into()));
assert_eq!(m.get(feat, &1).unwrap(), Some("updated".into()));

// 5. Three-way merge: feature → main (source wins on conflict).
m.merge(feat, main).unwrap();
assert_eq!(m.get(main, &1).unwrap(), Some("updated".into()));

// 6. Clean up: deleting the branch reclaims unreachable commits and
//    B+ tree nodes automatically — no manual gc() call required
//    (gc() also supports an explicit full node sweep).
//    Physical disk reclamation waits for compaction and is best-effort.
m.delete_branch(feat).unwrap();
```

## MptCalc / SmtCalc (Merkle Trie)

`MptCalc` and `SmtCalc` hold their trie nodes in memory. Applications can use them
to compute a root or prove a key. `VerMapWithProof` connects either backend to
a persistent, versioned data set.

```rust
use vsdb::trie::MptCalc;

// Build a trie
let mut mpt = MptCalc::new();
mpt.insert(b"key1", b"value1").unwrap();
mpt.insert(b"key2", b"value2").unwrap();
// MPT keys must be at most 1024 bytes (MAX_MPT_KEY_LEN); insert,
// batch_update, and from_entries return an error for longer keys.
// SMT keys are hashed to a fixed 256-bit path and have no length limit.

// Compute the 32-byte Merkle root hash
let root = mpt.root_hash();
assert_eq!(root.len(), 32);

// Lookup
assert_eq!(mpt.get(b"key1").unwrap(), Some(b"value1".to_vec()));

// Remove
mpt.remove(b"key1").unwrap();
assert_eq!(mpt.get(b"key1").unwrap(), None);

// Batch update
mpt.batch_update(&[
    (b"k1".as_ref(), Some(b"v1".as_ref())),
    (b"k2".as_ref(), None),  // remove
]).unwrap();

// Save and restore a standalone trie cache.
let cache_root = std::env::temp_dir().join(format!("vsdb-trie-example-{}", std::process::id()));
let cache_dir = cache_root.as_path();
std::fs::create_dir(cache_dir).unwrap();
let cache_id = 1;
let sync_tag = 0;
mpt.save_cache(cache_dir, cache_id, sync_tag).unwrap();
let (loaded, tag, hash) = MptCalc::load_cache(cache_dir, cache_id).unwrap();
assert_eq!(tag, sync_tag);
assert_eq!(loaded.root_hash(), hash);
// VerMapWithProof loads a cache automatically; saving requires save_cache(commit).
std::fs::remove_dir_all(cache_dir).unwrap();
```

### SmtCalc with Proofs

```rust
use vsdb::trie::SmtCalc;

let mut smt = SmtCalc::new();
smt.insert(b"alice", b"100").unwrap();
smt.insert(b"bob", b"200").unwrap();

let root32 = smt.root_hash();

// Membership proof
let proof = smt.prove(b"alice").unwrap();
assert_eq!(proof.value(), Some(b"100".as_ref()));
assert!(SmtCalc::verify_proof(&root32, b"alice", &proof).unwrap());

// Non-membership proof
let proof = smt.prove(b"charlie").unwrap();
assert_eq!(proof.value(), None);
assert!(SmtCalc::verify_proof(&root32, b"charlie", &proof).unwrap());
```

### Proof transport

Both `MptProof` and `SmtProof` support serde and a versioned postcard transport:

```rust
use vsdb::{MptCalc, MptProof};
let mut trie = MptCalc::new();
trie.insert(b"account", b"10").unwrap();
let root: [u8; 32] = trie.root_hash();
let wire = trie.prove(b"account").unwrap().to_bytes().unwrap();
let received = MptProof::from_bytes(&wire).unwrap();
assert!(MptCalc::verify_proof(&root, b"account", &received).unwrap());
```

`to_bytes` is equivalent to `postcard::to_allocvec`; `from_bytes` also rejects
trailing bytes. Version-1 field order is `(format, key, value, nodes)` for MPT
and `(format, key_hash, leaf, siblings)` for SMT. The eight-byte format tags are
`VSMPTP01` and `VSSMTP01`; other kinds or versions are rejected. A successfully
decoded proof still needs verification against the expected root and key.
Trie cache files and application-defined older proof encodings are separate
formats and are not accepted by these decoders.

## VerMapWithProof

Integrates `VerMap` with MPT or SMT for versioned Merkle root computation.
Use `prove_at(branch, &key)` to synchronize and return a `ProofWithRoot` in one
call, including uncommitted changes. `prove_at_commit(commit, &key)` selects
committed history instead and returns an error for reclaimed commits. Both
backends use the same method names and ordered key codec. The wrapper's
`save_meta` and `from_meta` persist its typed single-prefix handle. The type
tag binds the trie backend as well as the key and value types. Use `new_in` to
select a namespace and `clone_in` to deep-copy the graph and retained history.

```rust
use vsdb::{SmtCalc, VerMapWithProof};
let mut state = VerMapWithProof::<u64, String, SmtCalc>::new();
let main = state.map().main_branch();
state.map_mut().insert(main, &7, &"value".to_owned()).unwrap();
let result = state.prove_at(main, &7).unwrap();
assert!(VerMapWithProof::<u64, String, SmtCalc>::verify_key_proof(
    &result.root_hash, &7, &result.proof,
).unwrap());
```

Earlier root queries and `save_cache` calls do not determine these methods'
proof context. The `prove` and `prove_key` methods use the last synchronized trie; they remain useful when proving several keys
against a root the caller has already selected. `ProofWithRoot` supports serde;
its embedded proof uses the versioned representation above.

```rust
use vsdb::trie::{MptCalc, VerMapWithProof};

let mut vmp: VerMapWithProof<Vec<u8>, Vec<u8>, MptCalc> = VerMapWithProof::new();
let main = vmp.map().main_branch();

// Write data and commit
vmp.map_mut().insert(main, &b"key1".to_vec(), &b"val1".to_vec()).unwrap();
let commit = vmp.map_mut().commit(main).unwrap();

// Compute the Merkle root (incrementally maintained)
let root = vmp.merkle_root(main).unwrap();
assert_eq!(root.len(), 32);

// Optional application checkpoint: serializes the entire trie, so do this
// periodically rather than on every block. Errors are returned to the caller.
vmp.save_cache(commit).unwrap();
// Construction automatically loads an available cache; missing/stale caches
// are rebuilt/caught up. Root calculation and Drop never save cache files.
```

Trie clones share immutable subtrees. Each batch publishes its candidate root
after all operations succeed. Hashes and proofs can be computed through a
shared reference. v18 preserves the v17 commitment and proof wire formats.

MPTC v2 and SMTC v4 checkpoints stream post-order nodes and omit cached
node hashes. Loading verifies the checksum, canonical tree shape, root
commitment, and, for versioned maps, the universe, map, commit and B+ tree
root IDs. The loader bounds node count, pending subtrees, value sizes, and
total decoded memory. The encoded size limit is 512 MiB including the checksum. An oversized save returns an error and preserves the previous file.

A branch root and its proof use one captured working state, including edits
that were uncommitted when the query began.

`save_cache(commit)` synchronizes proofs to the selected committed state,
excluding dirty changes. Call `merkle_root(branch)` again before proving the
branch working state. Historical root queries return `CommitNotFound` for
reclaimed commits, even if a matching trie cache remains.

## SlotDex

`SlotDex<S, K>` indexes unique (slot, key) pairs and maintains tiered counts for
offset pagination across slot ranges. A timestamp can serve as the slot.

```rust
use vsdb::{SlotDex64, slotdex::Order};  // SlotDex<u64, K> alias — slot type is u64

let mut db = SlotDex64::<String>::new(10u32, false).unwrap(); // tier_capacity must be >= 2

// Insert entries into slots (e.g., timestamps)
db.insert(100, "entry_a".to_string()).unwrap();
db.insert(100, "entry_b".to_string()).unwrap();
db.insert(200, "entry_c".to_string()).unwrap();
db.insert(300, "entry_d".to_string()).unwrap();

assert_eq!(db.total(), 4);

// Paged queries
let page = db.page(.., 2, 0, Order::Desc);  // any slot range, page_size=2, page_index=0
assert_eq!(page, vec!["entry_d".to_string(), "entry_c".to_string()]);

// Slot-range queries
let entries = db.page(100..=200, 10, 0, Order::Asc);
assert_eq!(entries.len(), 3);
assert_eq!(db.count(100..300), 3);

// Remove
db.remove(100, &"entry_a".to_string()).unwrap();
assert_eq!(db.total(), 3);

// Bulk insertion (amortizes engine writes; ideal for imports/rebuilds)
db.insert_batch([(400u64, "entry_e".to_string()), (400, "entry_f".to_string())])
    .unwrap();
assert_eq!(db.total(), 5);
```

`Order` controls slot order; keys within a slot retain ascending key order.
For a complete range scan or aggregation, use `iter(slots, order)`:

```rust
use vsdb::{SlotDex64, slotdex::Order};
let mut index = SlotDex64::<u64>::new(8, false).unwrap();
index.insert_batch((0..70_000).map(|key| (100, key))).unwrap();
assert_eq!(index.iter(100..=100, Order::Asc).count(), 70_000);
let first_ten: Vec<_> = index.iter(.., Order::Desc).take(10).collect();
assert_eq!(first_ten, (0..10).collect::<Vec<_>>());
```

Each iterator reads one committed snapshot and decodes keys on demand. A
descending scan buffers up to 256 keys and 16 KiB of raw key data across
adjacent slots. Larger groups stream in ascending key order within the slot.
Use `page` to seek to an offset and `count` to sum a slot range.

Coordinates implement `SlotType` with a fixed byte array encoding. Built-in
signed coordinates align tier buckets relative to their minimum value. The
fan-out is a `u32` with a minimum of two.

Each mutation commits entries, counts and a writer generation in one engine
batch. Readers through restored handles can run concurrently. Serialize all
writers across aliases. A stale writer returns an error, including on `clear`.
Deserialize a fresh handle to resume writing. `clone` and `clone_in` deep-copy
the index to an independent prefix.

For built-in v17 coordinates (`u32`, `u64`, `u128`), the offline
`vsdb_migrate::slotdex::migrate_v17` converter keeps the prefix
and index rows and upgrades the total and handle metadata. Legacy capacities
must fit in `u32`. For a custom fixed-width codec, `migrate_custom_v17` checks
the supplied capacity and every entry and tier count. A variable-width custom
slot needs to rebuild its index from application records.

## DagMap

`DagMap<K, V>` stores local overrides in a single-parent tree. Reads walk up
to the first matching override. Removing a key masks the inherited value.
`DagMapRawKey<V>` accepts byte keys and `DagMapRaw` stores bytes for both keys
and values. Empty byte strings and unit values round-trip.

```rust
use vsdb::DagMap;
let mut root = DagMap::<u64, String>::new(None);
root.insert(&1, &"original".into());
let mut edit = DagMap::new(Some(&mut root));
edit.insert(&1, &"edited".into());
assert_eq!(root.get(&1).as_deref(), Some("original"));
let merged = edit.prune().unwrap();
assert_eq!(merged.instance_id(), root.instance_id());
assert_eq!(root.get(&1).as_deref(), Some("edited"));
```

Serialize structural changes to each connected graph. Every node has one
tagged map on the root's shard. A successful prune removes ancestor side branches, folds the mainline,
reparents surviving head children,
clears consumed nodes and fences that
shard's WAL. Retrying on the consumed head finishes any interrupted cleanup.
`clone` copies the connected graph. `clone_in` places the copy in another
namespace and preserves each parent's child tokens.

The offline converters `vsdb_migrate::dagmap::migrate_v17` and
`migrate_rawkey_v17::<V>` preserve the root ID. They return the converted
root and the mapping for children that needed a new co-located prefix.
Update any saved application-side child IDs with that mapping.

## VecDex

See the [VecDex guide](vecdex.md) for runnable examples, runtime metric
selection, filtered search, and recovery/ownership rules.
