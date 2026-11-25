# VerMap v18 reference persistence and benchmark results

The release uses Mode B, compact in-memory counts with a verified checkpoint. On a retained million-key tree, persisting sparse refcount changes pushed a 1,000-key block beyond the proposal's 10% throughput tolerance. The v17 comparison used commit `da9a325`, MMDB 4.3.15, Rust stable 1.100.0 and release optimization on an x86-64 Linux host with an AMD EPYC 9474F. Each mode used a separate database.

## Block workload

Bulk-load integer keys 0 through 999,999 and commit. For sixteen consecutive blocks, modify 1,000 keys and retain each new commit. Key `j` in block `b` is `((1000 * b + j) * 48271) % 1000000` and its new value is `key + b`. This distributes updates across the tree and leaves older versions reachable.

The v17 prototype measured individual working edits plus the final commit. A second run on the same implementation added sparse refcount persistence. It captured the actual net edge-count changes between successive blocks outside the timer. For each changed ID it issued a point read and a batched replacement or deletion of the extra-reference row. Around 23,000 such rows changed per late block.

| v17 block experiment | Middle block time |
|---|---:|
| In-memory ownership | 38 ms |
| Sparse persisted ownership prototype | 78 ms |

On the v18 slotted layout with range-grouped edits, the corresponding times were 22.5 ms with the checkpoint design and 60.4 ms with the sparse RMW prototype. Timing included `write_batch`, the final commit and, for the sparse mode, the actual reads and batched writes. The prototype retained RAM counts to generate its trace. Its sparse row updates used an additional ordinary batch on the same WAL before the final synchronous history commit.

A repeatable Criterion workload fixes the retained history at sixteen blocks, measures a seventeenth update and synchronous commit, and rolls it back outside the timer. The ten-sample result for the production Mode B implementation was **24.54 ms** (interval **23.85–25.58 ms**).

```sh
cargo +stable bench -p vsdb --bench versioned -- 'retained/1000-key block'
```

## Reopen tradeoff

The first open ran in a fresh process on each dataset. The next three opens ran in the same process after dropping each handle. The OS file cache stayed enabled. RSS is the process's `VmRSS` immediately after open, including MMDB and the allocator.

| Layout | First open | Median subsequent open | First-open RSS | Last-open RSS |
|---|---:|---:|---:|---:|
| Sparse prototype, metadata verification | 121.9 ms | 0.30 ms | 15.0 MiB | 15.0 MiB |
| Verified Mode B checkpoint | 118.1 ms | 30.19 ms | 24.7 MiB | 34.0 MiB |

The sparse open prototype measured metadata and four allocator-ceiling probes, with node ownership available from point reads during later mutations. The Mode B run deserialized and verified its checkpoint, including the checksum and per-object references, on every open. Its warm-open latency and resident memory pay for cheaper mutations throughout the lifetime of the writer.

## Existing Criterion workloads

The entries below are point estimates from separate v17 and v18 runs with isolated directories. The versioned benchmark's history grows during several of these measurements. Treat filesystem sync latency and differences in retained sample sizes as part of these numbers.

| Operation | v17 | v18 |
|---|---:|---:|
| Point read, hit | 2.33 µs | 1.10 µs |
| Point read, miss | 2.57 µs | 0.82 µs |
| Ten individual inserts and commit | 4.16 ms | 4.01 ms |
| Branch, ten inserts, commit, merge and branch deletion | 11.72 ms | 16.02 ms |
| Merge with 100 changed keys on each side | 31.67 ms | 26.69 ms |

The branch lifecycle workload was slower in this disk-backed run. It includes four separately durable history changes. The single-prefix implementation makes each change synchronous at publication.
