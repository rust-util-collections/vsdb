# In-process namespace close — current design

Implemented in v16.1.0; current public APIs are `Namespace::close_by_id(id)`
and consuming `Namespace::close(self)`. The earlier RFC's free-function names
and ownership migration plan are historical. See the
[namespace design](namespaces.md) and
[implementation](../../core/src/common/namespace.rs).

## Purpose

Epoch rotation needs to release the old engine's workers, WAL handles, and
cache resources before deleting its directory. Previously leaked engines
could only be released by process exit. The accepted design owns engines
normally and proves no namespace-owning client handles remain before teardown.
It does not invalidate handles or make every collection operation check a
closed flag.

## Ownership

```text
OPEN_NAMESPACES[id] ──┐
Namespace clones ────┼── Arc<NsInner>
collection handles ─┘      └── Engine
                                └── MmDB { dbs: Box<[DB]> }
```

`NsInner` owns its engine inline; `Namespace::engine()` returns a borrow of
that handle. Shard DBs are owned, not leaked. Iterators that need namespace
liveness borrow a handle or retain a namespace clone. The default namespace
uses the same ownership shape behind a static `LazyLock` and lives for the
process lifetime; closing it is refused.

MMDB detached snapshot iterators own their sources through internal refcounts.
They do not retain a namespace reference and may outlive close, yielding a
consistent, stale snapshot. Close therefore releases engine ownership but
cannot promise all memory/file resources disappear while detached snapshots
still retain them. Finish using those snapshots before removing or moving the
underlying tree; do not use close as an iterator invalidation signal.

## Protocol and locking

`ns_close_impl` is shared by both public forms:

1. Reject default namespace close. Acquire `REGISTRY_LOCK`, then the
   `OPEN_NAMESPACES` table lock.
2. Account for the table's strong reference, plus the consumed caller handle
   if present. Refuse if any other namespace-owning references remain.
3. Drop the accounted caller handle under the table lock and remove the entry.
   It is now exclusively owned and can be unwrapped safely.
4. Release the table lock before slow teardown. Keep `REGISTRY_LOCK` through
   teardown so a concurrent open cannot acquire this engine midway through
   close. Cache hits for unrelated open namespaces need only the table lock.
5. In writable mode, close every shard, flush active memtables, sync WALs, and
   release engine resources. Attempt every shard even when one fails and
   return the first error. Read-only close performs no maintenance writes.

The registry is unchanged: a later open reconstructs the engine from persisted
state, just as after a restart. Opens, creates, destroys, and relocations that
need the registry lock wait for teardown. There is no force-close path.

## Errors

| Result from `ns.close()` | State |
|-------------------------|-------|
| `Ok(())` | Closed; registered and reopenable |
| `Err((Some(ns), error))` | Refused before teardown; returned handle remains usable |
| `Err((None, error))` | Teardown ran and reported a flush/sync failure; engine is no longer open |

`close_by_id` returns `Result<()>` without returning a handle. Its errors can
mean either a precondition refusal (no teardown) or a teardown failure. A
not-open ID is an error, so close is not an idempotent success API. A teardown
failure must be handled before destroying storage; it does not promise the
same durability as a successful close.

## Rotation example

```rust
use vsdb::{Mapx, Namespace};

let ns = Namespace::create().unwrap();
let ns_id = ns.id();
let mut map = Mapx::<u64, String>::new_in(&ns);
map.insert(&1, &"archived".to_owned());

// Finish all collection/iterator users, then consume the final client handle.
drop(map);
ns.close().unwrap();

// Registry removal precedes filesystem removal. No per-key scan is needed,
// but directory deletion still scales with the files/directories removed.
Namespace::destroy(ns_id).unwrap();
```

For non-consuming close, drop every collection, ordinary iterator, and
`Namespace` clone, then call `Namespace::close_by_id(id)`. To reopen instead
of destroy, use `Namespace::open(id)` or recover a saved collection handle.

## Design alternatives and invariants

The original alternatives were a closed flag on a leaked shell, or reclaiming
leaked pointers with `Box::from_raw`. The former retained shell leaks and
introduced stale-handle checks; the latter required a new unsafe lifecycle
proof. Owned engines and borrowed access remove both leak sites and let Rust
lifetimes carry the normal access proof.

- No engine reference outlives the namespace ownership anchoring it.
- Exclusivity is checked while both locks prevent open-table cloning races.
- Refusal preserves the handle and open-table entry.
- Teardown does not hold the table lock through slow I/O.
- Detached snapshots have their own source lifetime, not a live-engine promise.
- Default close, force-close, and live-handle relocation are unsupported.

Closing does not change handle bytes, prefix identities, shard routing, or the
registry layout. The resource-ownership change is implemented; remaining
future memory-pool work is documented separately in
[shared-mem-pool.md](shared-mem-pool.md).
