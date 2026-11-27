# VSDB performance evaluations

These reports describe specific VSDB v18 revisions and workloads on Linux
x86_64. For the current runtime's configuration, use the [memory sizing guide](../storage.md#memory-sizing).

## MMDB v4 to v5 with VSDB v18

The upgrade comparisons ran the same logical workload on the v4 and v5
backends. Each report gives paired measurements, the control runs and the
instructions for reproducing its workload.

| Measurement | Report and comparison |
| --- | --- |
| Write latency, throughput and memory | One hot shard and all 16 shards, under paced and saturated load. [Adapter measurements](v18-adapter/README.md) |
| DagMap and VerMap write submissions | One- and two-write DagMap child creation, and four dependent VerMap updates submitted separately or in a single working edit. [Publication measurements](v18-publication/README.md) |
| Ordered-tree lookup and scan latency | Warm reads on 32,768 keys, with separate stock-runtime and prototype controls. [Stock tree comparison](v18-tree-v5/stock/README.md) · [Tree implementation decision](v18-tree-v5/README.md#tree-implementation-decision) |
| End-to-end VecDex queries | Warm searches on an identical 8,192-node graph, including verification of ordered IDs and distances. [Complete query comparison](v18-vecdex-v5/README.md) |

## Collection measurements on VSDB v18

At `cc2f41c`, the early v18 layout ran on MMDB 4.3.15 with Rust 1.100.0
on x86_64 Linux. The measurements below describe that revision and its
fixtures. [Original report](https://github.com/rust-util-collections/vsdb/blob/4c15817f4bf26513386c9a6ce1d375dca33043dd/docs/evaluations/README.md)

| Operation | v18 observation | Fixture |
| --- | ---: | --- |
| Ordered map point hit | 133.09 ns | Sequential hits on one logical CPU |
| Ordered map scan | 78.11 µs | 1,000 keys |
| SlotDex descending iterator | 22.32 µs | First 128 keys of 8,192 sparse slots |
| DagMap prune | 6.00 ms | Nine-child mainline with eight side branches and a concurrent writer on a separate prefix |
| MPT checkpoint size | 7,045,478 B | 100,000 entries with 32-byte values |
| SMT checkpoint size | 10,428,139 B | Same entries |

The untouched 64 KiB typed value guard used three allocator requests for
131,091 bytes. The branch lifecycle workload with four separately durable
history changes took 16.02 ms. [Basic workload](../../strata/benches/basic.rs) · [DAG workload](../../strata/benches/dagmap.rs) · [Trie workload](../../strata/benches/trie_bench.rs)

## VerMap reference counts on the v18 slotted layout

**VerMap keeps reference counts in memory with a verified checkpoint.**
On the v18 slotted layout, updating and committing 1,000 distributed keys
in a retained million-key tree took 22.5 ms with a checkpoint and 60.4 ms
with sparse persisted counts. On subsequent opens with a warm OS cache,
checkpoint validation took 30.19 ms versus 0.30 ms for the prototype's
metadata-only open, which deferred owner lookups. The chosen design favors
the cost of repeated updates. [Versioned workload](../../strata/benches/versioned.rs)
