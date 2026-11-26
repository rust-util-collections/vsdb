# v18 collection layouts compared with v17

These measurements isolate collection-version changes before the MMDB v5
port. Both sides used MMDB 4.3.15 and Rust 1.100.0 on x86_64 Linux.
The comparison used v17.0.21 code at `da9a325` and the v18 evaluation at
`cc2f41c`. For the separate v18/v4 versus v18/v5 upgrade results, see
[the v18 release description](../v18.md).

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
