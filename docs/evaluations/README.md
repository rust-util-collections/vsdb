# VSDB performance evaluations

## MMDB v4 to v5 with VSDB v18

The following comparisons used x86_64 Linux. Upgrade figures compare the
same logical workload on v18 with its v4 and v5 backends. Percentages are
median changes within matched pairs. Baseline variation is measured by
running the control against itself.

| Workload | Result |
| --- | --- |
| Writes to one hot shard | Saturated throughput rose **59.53%**. At a fixed offered load, foreground small-put p99 fell **95.88%**. [Adapter measurements](v18-adapter/README.md) |
| All 16 shards active | Paced small-put p99 fell **73.64%**. At saturation, foreground small-put completions/s fell **31.63%** in all five pairs. Aggregate byte throughput fell 2.26%, within 14.75% baseline variation. [Adapter measurements](v18-adapter/README.md) |
| DagMap child creation in the default namespace | One write submission replaces two. Operation p99 fell **33.38%** against v18/v4 and **31.05%** against the two-write v5 control. [Publication measurements](v18-publication/README.md) |
| Warm standalone B+ tree on 32,768 keys | Point-hit p99 increased **12.03%**, miss p99 **11.83%**. For a 128-row scan the measured 32.83% p99 increase and 16.82% throughput loss fell within their respective baseline spreads. [Stock tree comparison](v18-tree-v5/stock/README.md) · [Tree implementation decision](v18-tree-v5/README.md#tree-implementation-decision) |
| Warm VecDex search on the same 8,192-node graph | The median run p99 rose from **184.57 to 253.38 µs**. The paired regression was **37.48%** across five slower pairs, with identical ordered IDs and distances. [Complete query comparison](v18-vecdex-v5/README.md) |

## VerMap working edits on MMDB v5

Four dependent VerMap updates performed as individual
operations took 70.54 µs at p99, compared with 31.72 µs in a bounded
working edit at `2ae6149`. The paired improvement was **52.51%**. Engine
write submissions fell from four to one. Use `write_batch` when final
values are already known. [Working-edit measurements](v18-publication/README.md)

## v17 to v18 collection layouts on MMDB v4

These measurements isolate collection-version changes before the MMDB v5
port. Both sides used MMDB 4.3.15 and Rust 1.100.0 on x86_64 Linux.
The comparison used v17.0.21 code at `da9a325` and the v18 evaluation at
`cc2f41c`.

| Operation | v17 | v18 | Fixture |
|---|---:|---:|---|
| Ordered map point hit | 154.20 ns | 133.09 ns | Sequential hits on one logical CPU |
| Ordered map scan | 77.60 µs | 78.11 µs | 1,000 keys, overlapping 95% intervals |
| SlotDex descending iterator | 140.93 µs | 22.32 µs | First 128 keys of 8,192 sparse slots |
| DagMap prune | 669.28 ms | 6.00 ms | Nine-child mainline with eight side branches, concurrent writer on a separate prefix |
| MPT checkpoint size | 14,279,048 B | 7,045,478 B | 100,000 entries with 32-byte values |
| SMT checkpoint size | 16,828,069 B | 10,428,139 B | Same entries |

The untouched 64 KiB typed value guard used 3 allocator requests
(131,091 bytes) on v18, compared with 35 (720,910 bytes) on v17. The
SlotDex batch-growth comparison did not establish a general speedup. The
v18 branch lifecycle workload, including four separately durable history
changes, measured 16.02 ms against 11.72 ms on v17. [Basic workload](../../strata/benches/basic.rs) · [SlotDex batch workload](../../strata/benches/slotdex_batch.rs) · [DAG workload](../../strata/benches/dagmap.rs) · [Trie workload](../../strata/benches/trie_bench.rs)

**VerMap keeps reference counts in memory with a verified checkpoint.**
On the v18 slotted layout, updating and committing 1,000 distributed keys
in a retained million-key tree took 22.5 ms with a checkpoint and 60.4 ms
with sparse persisted counts. On subsequent opens with a warm OS cache,
checkpoint validation took 30.19 ms versus 0.30 ms for the prototype's
metadata-only open, which deferred owner lookups. The chosen design favors
the cost of repeated updates. Frozen v17 fixtures verify trie roots and
proof bytes independently of timing. [Versioned workload](../../strata/benches/versioned.rs) · [Trie tests](../../strata/src/trie/test.rs)
