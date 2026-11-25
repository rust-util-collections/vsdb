# VSDB v18 design and implementation status

The workspace implements the v18 format. The changelog lists v18.0.0 as unreleased. This record uses the original subsystem numbers so that the design decisions and measurements in §10 can be read together. [Release notes](../../CHANGELOG.md)

**Upgrade boundary.** Stop v17 processes and convert the universe offline. The runtime opens current-format storage. The independent `vsdb_migrate` library converts legacy envelopes, row layouts, saved metadata and serialized handles. Applications identify handles embedded in their own schemas. Converters preserve primary collection identities and return a remapping for relocated DAG children. [Migration procedure and converter table](../../migration/README.md)

The original proposal compared alternatives on v17.0.21, including in-place readers, lazy upgrades and sparse persisted reference counts. The full proposal and its original validation gates are preserved at their historical revision, also available locally with `git show fba65bb:docs/proposals/v18-roadmap.md`. [Original roadmap at `fba65bb`](https://github.com/rust-util-collections/vsdb/blob/fba65bb8becf8f3aa3917b0d20ada5fedb6ed7d3/docs/proposals/v18-roadmap.md)

## Implemented design

### 1. SlotDex

Each slot has a fixed-width, order-preserving byte representation. Entry rows concatenate slot and key bytes, so the index knows their boundary without delimiters. Tier capacity is a separate `u32` of at least two. Queries capture a read view for counts, tiers and entries. Physical descending iteration groups bounded runs and streams oversized slots individually. Keys within a slot stay in ascending order. [Slot codec](../../strata/src/slotdex/slot_type.rs) · [Query implementation](../../strata/src/slotdex/query.rs)

The total row stores the total and writer generation. An edit checks the stored generation before staging its batch. A stale handle can read by reconstructing tiers from its captured view. Applications serialize writers to the same index. [Mutation and hydration](../../strata/src/slotdex/mod.rs)

### 2. Key and value codecs

`KeyEnDe` and `ValueEnDe` support encoding into a caller's buffer. The blanket postcard key implementation requires equality and hashing. Stored key equality must match encoded byte equality. Exact probes use `KeyLookup<Q>` or `OrderedKeyLookup<Q>`, so the stored type selects the encoding for a borrowed probe. Ordered integer keys use fixed-width types and order-preserving big-endian bytes with a sign adjustment for signed integers. Converting legacy ordered pointer-width keys requires knowing the source architecture. [Codecs and bounds](../../strata/src/common/ende.rs) · [Migration guide](../../migration/README.md)

### 3. Prefix routing and scans

Each raw map has a universe-wide prefix and routes to one shard. The default namespace uses 16 shards. Other namespaces persist their creation-time count. Short engine keys fit the prefix and encoded user key in a stack buffer. Ranges capture a sequence, apply prefix and caller bounds in MMDB, and strip the prefix from each owned result in place. Raw batches can delete a half-open range within their prefix. [MMDB integration](../../core/src/common/engine/mmdb.rs) · [Raw batch](../../core/src/basic/mapx_raw/mod.rs)

### 4. Basic collection access

Occupied and vacant entries borrow the collection safely. A typed value guard marks a mutable dereference for write-back. A vacant value also persists when the guard completes. Use `update` for a fallible edit. Readers share the collection identity, and a read view captures one state for repeated queries. `OrphanReader::get` reads the single stored value. [Value guards](../../strata/src/common/guard.rs) · [Batch and reader contracts](durable-batches-and-read-views.md)

### 5. Universe, engine identity and handles

`VSUNI018` is the universe bootstrap commit point. `VSNREG02` tracks the registry's next namespace ID and pending or established records. Each ready engine has a `VSENG018` header binding its universe, namespace and shard count. A universe-level advisory lock covers explicitly placed namespaces and the default engine. [Namespace design and directory layout](namespaces.md)

`VSMAPX02` carries a prefix and namespace ID. `VSTYPE04` adds a nominal type tag and collection-specific metadata. Both formats place the prefix at a fixed offset so missing-ceiling recovery can recognize embedded handles. Public storage errors use `VsdbError`. Engine failures carry a stable classification and source. [Typed frame](../../strata/src/common/frame.rs) · [Error definitions](../../core/src/common/error.rs)

### 6. VerMap and PersistentBTree

Version metadata and immutable tree objects share one prefix. Slotted pages live under `0x10`, with values larger than 1 KiB in separate `0x12` blobs. Owned snapshots pin their roots independently of the writer. Working edits publish a new root, and history operations use synchronous batches. Large builds can stage chunks before publication. Recovery uses retired-root records (`0x05`), staging records (`0x06`) and verified reference checkpoints (`0x07`). [Durability and recovery](../../strata/docs/versioned.md#durability-and-recovery) · [Tag definitions](../../strata/src/versioned/store.rs)

The selected reference model uses in-memory counts and a verified checkpoint (Mode B). Persisting per-object reference changes on each commit added too much cost in the retained-history workload. Clean reopen pays to load and validate the checkpoint. [Reference persistence experiment](../evaluations/v18-versioned.md)

### 7. Merkle tries

`MptCalc` and `SmtCalc` use copy-on-write nodes with shared cached hashes. Roots and proof transport retain their v17 cryptographic encodings. Map-bound checkpoints (`MPTC` v2 and `SMTC` v4) identify the universe, map, commit and tree root. Applications rebuild disposable checkpoints from the version store after conversion. `VerMapWithProof::save_cache` explicitly persists a checkpoint for a committed state. [Checkpoint codecs](../../strata/src/trie/cache.rs) · [Proof wrapper](../../strata/src/trie/proof.rs)

### 8. DAG overlays

Each node owns a raw prefix on the graph's shard. Its metadata binds a DAG identity and local edge IDs to its parent and children. Separate tags identify metadata, edges and user data, including empty values. Pruning folds data and reparents surviving children before marking consumed nodes for clearing. Co-location preserves phase ordering on the shard WAL. [DAG lifecycle](../../strata/src/dagmap/raw/mod.rs) · [Typed wrapper](../../strata/src/dagmap/typed.rs)

### 9. VecDex

The HNSW graph stores little-endian vectors, adjacency lists, key mappings, a 34-byte state row and a schema binding under one prefix. Graph operations read persisted state, including the next node ID, which survives clear and advances as compaction rebuilds nodes. A search uses one snapshot for traversal and result keys. `VecDexDyn` is the runtime-metric specialization of `VecDex`, with the selected metric persisted in metadata. [VecDex guide](../../strata/docs/vecdex.md) · [Graph storage](../../strata/src/vecdex/mod.rs)

## 10. Post-Implementation Structural Performance Evaluation and Follow-Up Designs

The first v18 comparison used the v17.0.21 code at `da9a325`. Its four reports were recorded in `cc2f41c`. Supplementary measurements below also used `cc2f41c`, isolated universes, release builds, Rust 1.100.0 and MMDB 4.3.15. Later commits changed several measured paths. The figures apply to the earlier implementation.

### 10.1 Initial benchmark coverage

| Report | Workloads measured |
| --- | --- |
| [Basic maps](../evaluations/v18-basic.md) | Point hits, range scans, key allocations and untouched value guards |
| [SlotDex and DagMap](../evaluations/v18-slotdex-dagmap.md) | Descending pages and iterators, batch growth, DAG prune and destroy |
| [VerMap](../evaluations/v18-versioned.md) | Retained-history blocks, checkpoint reopen, point reads and branch operations |
| [Tries and VecDex](../evaluations/v18-trie-vecdex.md) | Root hashing, batch updates, checkpoint size, RSS and unfiltered vector search |

### 10.2 Large B+ tree values

On the original 1,024-key tree, a copy-on-write edit materialized and rewrote unchanged sibling blobs. The boundary between inline bytes and blob descriptors explains the jump from 1,024 to 1,025 bytes.

| Value size | Leaf class | `VerMap::insert` median (p99) | 40-edit `diff_commits` | `BTree` 1-key overwrite | `BTree` 1-key `diff` | Underflow `remove` |
|---:|---|---:|---:|---:|---:|---:|
| `1,024 B` | Inline `LEAF` (`0b01`) | `66.1 µs` (`95.5 µs`) | `440.1 µs` | `48.8 µs` | `19.9 µs` | `30.3 µs` |
| `1,025 B` | `BLOB_LEAF` (`0b10`) | `101.8 µs` (`131.1 µs`) | `742.3 µs` | `85.7 µs` | `51.9 µs` | `28.2 µs` |
| `16,384 B` | `BLOB_LEAF` (`0b10`) | `583.9 µs` (`16,641.4 µs`) | `4,467.1 µs` | `982.7 µs` | `209.1 µs` | `229.2 µs` |
| `65,536 B` | `BLOB_LEAF` (`0b10`) | `2,049.4 µs` (`53,275.9 µs`) | `55,968.7 µs` | `2,028.7 µs` | `439.7 µs` | `891.8 µs` |

`9b3b709` introduced descriptor-preserving edits and diffs. Equal blob IDs can be skipped. Different IDs still need byte comparison so reverting a value compares cleanly. Page-header reads handle height and occupancy without loading sibling values. Direct splicing of encoded page slots on single-key edits remains a proposal. [Leaf layout](../../strata/src/basic/persistent_btree/nodes.rs) · [Diff implementation](../../strata/src/basic/persistent_btree/diff.rs)

### 10.3 VerMap point reads and reference state

The baseline point-read path fetched a branch head and allocated a root lease per lookup. This experiment compared it with reads on an already-open snapshot.

| Tree keys (`N`) | Height | `Snapshot::get` | `VerMap::get` | Per-lookup head + lease overhead | `flush()` (`[0x07]`) | Warm `from_meta` |
|---:|---:|---:|---:|---:|---:|---:|
| `5,000` | 2 | `452.4 ns` | `628.5 ns` | `+176.1 ns` (`+39%`) | `1.95 ms` | `0.20 ms` |
| `100,000` | 3 | `885.2 ns` | `1,313.2 ns` | `+428.0 ns` (`+48%`) | `2.18 ms` | `0.70 ms` |
| `500,000` | 4 | `1,178.5 ns` | `1,657.8 ns` | `+479.3 ns` (`+41%`) | `4.04 ms` | `3.20 ms` |

`da21703` added synchronized branch-head caching, stack-pinned one-shot reads and branch creation using the name index. `71588f2` added an eight-shard clock cache for immutable internal pages. Its page-byte budget per tree runtime is the namespace budget divided by 64, clamped to 64 KiB–16 MiB. Multiple active trees can each have a cache. [Point reads](../../strata/src/versioned/read.rs) · [Tree runtime](../../strata/src/basic/persistent_btree/mod.rs)

A shared-only refcount representation (Mode B+) and persisted `0x08` delta log remain proposals. They need a way to distinguish live singly-owned objects from dead or newly unowned objects and an explicit format boundary for a different checkpoint representation.

### 10.4 Working-state Merkle roots

The baseline used 20,000 keys and 32-byte values. Repeated edits on a dirty branch repeatedly diffed from the committed state.

| Workload | Measured time (`cc2f41c`) | Behavior & bottleneck |
|---|---:|---|
| Full streaming `MptCalc::from_entries` + `root_hash` (`20,000` keys) | `15.49 ms` | $O(\text{height})$ B+ tree cursor memory; single bottom-up hash pass |
| `merkle_root_at_commit` incremental diff: `0.5%` (`100` keys) | `1.01 ms` | `15.3×` faster than full rebuild |
| `merkle_root_at_commit` incremental diff: `20%` (`4,000` keys) | `11.37 ms` | Approaches full-rebuild cost while allocating `Vec<TreeDiff>` |
| `merkle_root_at_commit` incremental diff: `100%` (`20,000` keys) | `16.17 ms` | Slower than `from_entries` **and** materializes all `20,000` `TreeDiff` + `ops` slices in RAM |
| Working-state `1 insert + merkle_root(br)` with `2,000` prior dirty keys | `1.69 ms / step` | Clones `committed_trie` and **re-diffs all `2,001` dirty keys on every call** ($O(K^2)$ across a block) |

`8a05514` added a working-branch cursor, streaming diff chunks of at most 4,096 entries and a full rebuild when accumulated diffs exceed `max(estimated target entries / 2, 512)`. The wrapper releases its working lease before `flush` or `close` so the lease does not hold up retirement during checkpointing. [Root synchronization](../../strata/src/trie/proof.rs)

### 10.5 Vector construction, filtering and compaction

On 2,000 128-dimensional `f32` vectors with L2 distance, serial insertion took 818.5 ms and batched insertion 566.4 ms. Compacting 1,500 live vectors took 341.6 ms. With `k = 10` and `ef_search = 50`, search took 140.7 µs unfiltered, 277.1 µs at 50% selectivity, 1,131.4 µs at 10%, and 4,610.0 µs at 1%. Each result comes from the baseline graph.

`b31360a` carried the bounded vector cache across 64-item build chunks and removed an extra adjacency read on neighbor append. `5b4f4cb` added typed and raw filters on read views and raw filters on the index. Raw predicates see encoded key bytes and avoid decoding rejected keys. Selective predicates can still visit much of the graph. [Construction](../../strata/src/vecdex/mod.rs) · [Read-view queries](../../strata/src/vecdex/reader.rs)

In-place `compact()` still stages the rebuilt graph for one atomic replacement. An application can build a separate index in chunks, durably publish its new identity, then retire the old index. An internal generation switch with crash cleanup would require a new graph layout. [Compaction guidance](../../strata/docs/vecdex.md)

### 10.6 SlotDex batch growth

The early `wide_then_hot` benchmark grew 1,025 distinct slots and repeatedly touched the same buckets. The v17 and initial v18 estimates were 65.15 ms and 79.15 ms, with substantial variance (`p = 0.06`). `f87ad4c` subsequently coalesced level-zero and tier counts during staging. Tier promotion sees the counts accumulated so far and the batch still publishes atomically. [Baseline and methodology](../evaluations/v18-slotdex-dagmap.md) · [Batch implementation](../../strata/src/slotdex/mod.rs)

### 10.7 Engine write-buffer sizing

This baseline wrote 1,500 batches of 40 KiB into one prefix. The dedicated namespace's buffer held the entire burst. A sustained workload that fills that buffer also pays for its synchronous flush and WAL replay on reopen.

| Placement | Per-shard `write_buffer_size` | Flushes triggered during 60 MiB burst | Total time | Median batch | p99 batch | Max batch latency |
|---|---:|---:|---:|---:|---:|---:|
| Default namespace (`16` shards, `2 GiB` budget) | `16 MiB` | `3–4` sync flushes | `127.4 ms` | `42.0 µs` | `110.8 µs` | **`26,624.9 µs` (`26.6 ms`)** |
| Dedicated namespace (`1` shard, `2 GiB` budget) | `256 MiB` | `0` (stays in active memtable) | `87.4 ms` (`-31%`) | `53.9 µs` | `216.9 µs` | **`306.0 µs` (`0.31 ms`)** |

`8d15889` corrected the sizing rationale for MMDB 4.3. The write leader freezes and flushes a full memtable synchronously. Default and non-default engines still use the same per-shard sizing formula. Changing the default remains subject to a sustained ingest comparison across one and 16 shards over at least three buffer capacities. The original gate calls for p99 and maximum flush stalls, reopen latency, L0 file counts, cache counters and peak RSS under cgroup pressure. [Sizing and memory accounting](shared-mem-pool.md)

### 10.8 Deferred designs

The remaining proposals have different compatibility and measurement requirements.

| Candidate | Proposed change and evidence needed |
| --- | --- |
| B+ tree slotted page splice | For a single-key edit that fits one page, copy payload slices into a new page and adjust the shifted slot offsets. Preserve blob IDs and compare the bytes and allocations with the existing COW path. This uses the v18 page format. |
| Shared-only references (Mode B+) | Store extra owners in memory and track newly allocated, unowned nodes separately. A sparse on-disk checkpoint or generation-bound `0x08` delta log needs a new format. Verify liveness on reopen, including shared blobs, and bound replay before adopting the format. |
| Generation-based VecDex compaction | Proposed v19 layout: build into an inactive generation, switch the state row atomically, then delete the old generation's ranges. Recovery would need to find interrupted builds. v18 compaction publishes a single replacement batch. |
| SlotDex paged tier hydration and cursor pagination | Measure tier-cache memory on large histories before introducing on-demand bucket reads. Cursor pagination would require a separate public query contract. |
| VerMap sweeping and working transactions | Stream unreachable-object deletion if retaining all dead keys during recount becomes a memory bottleneck. A working transaction would group read-modify-write operations before one branch-head publication. |
| Disk-backed Merkle nodes | Measure retained tries that exceed the current in-memory COW representation before designing persistent node storage. |
| MMDB v5 adapter port | The v5 engine now supplies a native WAL fence, pinned and bounded grouped reads, prefixed batch mutations and pool/member memory gauges. Adopting them in this v4-backed workspace requires the staged shard conversion and caller-level validation. Per-shard prefix reservations still require crash-safe allocator recovery. [MMDB implementation and evidence](https://github.com/rust-util-collections/mmdb/blob/master/docs/proposals/mmdb-v5-execution-plan.md) |
| Broader memory pooling | Measure cache and write-buffer contention across engines before sharing their budgets. [Memory pool proposals](shared-mem-pool.md) |

Changes to persistent formats follow the compatibility policy. The original roadmap records the proposed validation gates and candidate v19 layouts. [Compatibility policy](../ai/compatibility-policy.md) · [Deferred memory designs](shared-mem-pool.md) · [Historical specification](https://github.com/rust-util-collections/vsdb/blob/fba65bb8becf8f3aa3917b0d20ada5fedb6ed7d3/docs/proposals/v18-roadmap.md)
