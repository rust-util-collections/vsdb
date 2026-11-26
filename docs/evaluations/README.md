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
