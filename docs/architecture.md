# VSDB storage architecture

The `vsdb` crate defines collections and their key and value encodings.
`vsdb_core` stores their rows on MMDB 6 using shard format 5. A process opens
one **universe**, identified by its base directory and universe header.

| Term | What it identifies |
| --- | --- |
| Namespace | A storage root and its group of shards. The universe registry records additional namespaces and their placement. |
| Shard | One MMDB database in a namespace, with its own write-ahead log (WAL) and read sequence. |
| Prefix | An eight-byte map identifier allocated by the universe. All rows with that prefix live on the same shard. |
| `InstanceId` | A map ID and its namespace in this universe. With saved handle metadata, it lets the application reopen the collection. |

## From a collection handle to a shard

`MapxOrd` encodes ordered keys and their values. VerMap encodes tree pages,
blobs and branch records. Both address those rows through
`vsdb_core::MapxRaw`. [Typed collections](../strata/README.md#choosing-a-collection) · [Raw map implementation](../core/src/basic/mapx_raw/mod.rs)

```mermaid
flowchart TD
    newMap["Create a persistent map"] --> place["Capture the explicit or ambient namespace"]
    place --> allocate["Obtain a universe-wide prefix when needed"]
    restore["Restore a saved map"] --> resolve["Open the recorded namespace and reuse the saved prefix"]
    allocate --> handle["MapxRaw identity: namespace and prefix"]
    resolve --> handle
    handle --> engine["Select the namespace's MMDB engine"]
    engine --> shard["Select shard: numeric prefix modulo shard count"]
    shard --> row["Address a row: eight prefix bytes followed by the collection-local key"]
```

Two prefixes in the same namespace can route to different shards. DagMap
deliberately allocates a child prefix on its parent's shard so it can publish
the child and the parent edge together. [Namespace layout and lifecycle](namespaces.md) · [MMDB adapter](../core/src/common/engine/mmdb.rs)

## Batch publication and captured reads

For a nonempty map batch, the shard is the unit of atomic visibility.
The two commit modes differ in when the writer can rely on the WAL for
power-loss recovery.

```mermaid
flowchart TD
    batch["Stage inserts and deletes in a map batch"] --> mode{"How does the caller finish the batch?"}
    mode -->|Drop| discard["Discard staged edits"]
    mode -->|commit| asyncWal["Append to the shard WAL"]
    mode -->|commit_sync| syncWal["Append and sync the shard WAL"]
    asyncWal --> publish["Publish all batch edits together"]
    syncWal --> publish
    publish --> view["A new read view can see the complete batch"]
```

A `read_view()` captured before publication continues to read its original
sequence for both point lookups and scans. After an ordinary commit,
`try_sync_wal()` on the map can make earlier WAL-backed writes durable
without publishing another edit. DagMap's `insert_colocated` path checks
that the two raw handles share an engine and shard before submitting one
batch across their prefixes. A namespace-wide fence visits each shard
individually. [Read views, fence scope and write errors](storage.md)

## VerMap working edits and history

A branch records both a working tree root and the ID of its latest history
commit. Writers through aliases of the same store must serialize their operations.

```mermaid
flowchart TD
    edit["Lease the working root in edit_working"] --> overlay["Read staged edits first, then the leased tree"]
    overlay -->|Callback error| dropEdits["Discard the overlay"]
    overlay -->|Success with edits| check{"Does the branch still have the leased root?"}
    check -->|No| stale["Return WorkingRootChanged"]
    check -->|Yes| pages["Copy changed B+ tree paths and reuse untouched subtrees"]
    pages --> working["Publish new pages and changed working root in one ordinary shard batch"]
    working -->|snapshot| lease["Lease the working root for a reader"]
    working -->|Separate commit call| history["Sync one batch recording the root and advancing the branch head"]
```

The history entry links to the previous head when one exists. A reader can
open a retained commit with `at(commit)`. Its root lease keeps that tree
readable across later edits and rollbacks. [Working edits and publication](../strata/src/versioned/map.rs) · [Branches, snapshots and recovery](../strata/docs/versioned.md)
