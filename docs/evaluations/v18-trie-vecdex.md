# v18 trie and VecDex measurements

This comparison was recorded at `cc2f41c` against the v17.0.21 code at `da9a325`. Later implementation changes are tracked in the [v18 design record](../proposals/v18-roadmap.md#10-post-implementation-structural-performance-evaluation-and-follow-up-designs).

Measured on x86-64 Linux with an AMD EPYC 9474F, Rust stable 1.100.0
and mmdb 4.3.15.
Each database run used a fresh directory. Criterion ran the public APIs in
release mode. Results include changes to the underlying VSDB engine.
These are local measurements on a shared host.

## Time per operation

| Workload | v17 estimate | v18 estimate | Criterion change, 95% interval |
|---|---:|---:|---:|
| MPT cold root, 1,000 keys | 555.68 µs | 475.21 µs | −17.7% to −11.8% |
| MPT cached root | 74.35 ns | 3.03 ns | −96.0% to −95.8% |
| MPT batch update, 1,000 keys | 354.37 µs | 239.88 µs | −33.2% to −30.7% |
| SMT cold root, 1,000 keys | 931.59 µs | 917.31 µs | −3.3% to +0.8% |
| SMT cached root | 35.31 ns | 3.06 ns | −91.7% to −91.2% |
| SMT batch update, 1,000 keys | 802.76 µs | 659.51 µs | −21.8% to −16.2% |
| VecDex L2 search, 1,000 vectors, 10 neighbors | 503.55 µs | 304.47 µs | −40.0% to −38.5% |

Cold-root timings on both versions used an independently built, unhashed
trie for every measurement, with construction outside the timing window.
This matters because v18 clones share their cached node hashes. The trie
benches used 15 samples, 1 s warmup and 2 s measurement per workload.
The vector bench used its normal 20 samples and 5 s measurement, with
128-dimensional random f32 vectors, random queries and search beam 50.
Its graphs and queries vary between runs.

```sh
cargo +stable bench -p vsdb --bench trie_bench -- \
  '((mpt|smt)_(batch_update_1000|root_hash_1000_(cold|warm)))$' \
  --sample-size 15 --warm-up-time 1 --measurement-time 2
cargo +stable bench -p vsdb --bench vecdex -- 'vecdex/search/k=10/1000$'
```

## Resident memory and checkpoint size

Each process built one trie with 100,000 entries. Key `n` was
`Keccak256(n.to_be_bytes())` for `n: u64` from 0 through 99,999. Every
value contained 32 bytes set to 1. The RSS delta measures Linux `VmRSS`
after computing the root against `VmRSS` before generating entries.
Each process then called `save_cache` and measured the resulting file.

| Trie | v17 RSS delta | v18 RSS delta | v17 checkpoint | v18 checkpoint |
|---|---:|---:|---:|---:|
| MPT | 47,096 KiB | 26,804 KiB | 14,279,048 B | 7,045,478 B |
| SMT | 35,952 KiB | 35,940 KiB | 16,828,069 B | 10,428,139 B |

The MPT used about 57% of the reference RSS on this dataset. RSS includes
allocator effects. The file sizes include headers and checksums.

## Correctness evidence

The frozen v17 proof fixture records both root commitments and serialized
membership and absence proofs for a trie containing a value at the empty key,
odd-length nibble prefixes and a compressed SMT path. The v18 test regenerates
these bytes and compares them in full. Checkpoint tests also exercise a
1,024-byte prefix chain on a 2 MiB worker stack, malformed branches,
path inconsistencies, resource limits, interrupted or oversized writes,
map replacement and reclaimed commits. [Trie tests](../../strata/src/trie/test.rs)
and [checkpoint validation](../../strata/src/trie/cache.rs).

The offline VecDex migration test starts from v17 metadata and all raw rows
of a saved graph. It compares migrated vector bytes, retained graph and key
rows, numeric state and the recorded v17 search result. A simulated interrupted vector chunk and a repeated metadata publication also preserve those results. Runtime tests cover
serialized writers, captured reads through clear/compact, independent deep
copies, parameterized metric identity, and runtime metric round trips.
[Converter fixture test](../../migration/tests/vecdex.rs) and [VecDex tests](../../strata/src/vecdex/test.rs).
