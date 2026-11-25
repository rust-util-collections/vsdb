# Borrowed leaf projection for tree point reads

**Keep the existing v5 tree point-read path.** On the same warm
standalone B+ tree that exhibited the upgrade regression, the prototype
substantially reduced allocations by inspecting the selected leaf through
`try_get_with`. The paired p99 improvements for point hits and misses
were 3.37% and 4.36%, below the predeclared 10% end-to-end benefit.
Against the separately measured v4 reference, projected point hits and
misses still took 6.39% and 6.44% longer at p99.

## Caller latency

| Request | v4 p99 µs | v5 control p99 µs | Projection p99 µs | Paired projection gain (min to max) | Max v5 A/A | Favorable | Throughput change |
|---|---:|---:|---:|---:|---:|---:|---:|
| Get hit | 2.660 | 2.970 | 2.830 | +3.37% (+3.08% to +7.07%) | 3.31% | 5/5 | +10.46% |
| Get miss | 2.640 | 2.970 | 2.830 | +4.36% (+2.75% to +7.41%) | 2.69% | 5/5 | +12.32% |
| Contains hit | 2.640 | 2.900 | 2.790 | +3.82% (+1.72% to +6.06%) | 1.38% | 5/5 | +8.46% |
| Contains miss | 2.640 | 2.900 | 2.800 | +3.82% (+1.37% to +6.04%) | 1.73% | 5/5 | +8.19% |
| 128-row scan | 9.690 | 12.630 | 12.450 | +1.43% (−21.13% to +3.75%) | 6.61% | 3/5 | +3.56% |

Each cell is the median of the within-process p99s. Comparisons use the
median of five within-pair deltas. Positive gains mean lower p99 or
higher successful queries/s as appropriate. The maximum absolute
scalar/scalar change from the separate A/A pairs describes repeatability.
Each process executes the full workload on CPU 4. Every point class supplied
250,000 deterministic queries per process. Every scan consumed 128 rows
and checked every key and value, with 10,000 complete requests per process.
Each run used a new database and warmed the 32,768-key, 32-byte-value
dataset before timing. The scan uses the production iterator in both v5
arms. [Full raw process observations](trials.csv) · [Trace and host](host.json)

The original version used VSDB v18 with MMDB v4.3.15. The v5 scalar and
candidate each used MMDB `255015a`. Each seed included an independent
second v4 process to estimate v4 A/A spread.

| Request | v4 → v5 scalar p99 gain | v4 → projection p99 gain | Max v4 A/A | v4 → v5 scalar throughput | v4 → projection throughput |
|---|---:|---:|---:|---:|---:|
| Get hit | −11.88% | −6.39% | 3.65% | −27.74% | −20.20% |
| Get miss | −12.50% | −6.44% | 4.40% | −28.62% | −20.45% |
| Contains hit | −11.54% | −6.34% | 1.94% | −28.95% | −22.97% |
| Contains miss | −11.15% | −6.95% | 1.93% | −28.59% | −22.68% |
| 128-row scan | −30.20% | −28.48% | 14.65% | −18.38% | −17.70% |

The two get classes were co-primary. Each needed a paired p99 gain of
at least 10%, a gain larger than control A/A and four favorable pairs.
The protected scan throughput floor was 95% of v5 scalar, and scan and
contains p99 could rise at most 5%. Calling the v4 regression closed
would additionally require a candidate p99 within 105% of v4 for both
get classes, with control noise small enough to resolve that margin.
The observations apply to this warm one-shard workload and the two
adapters' normal configurations under a 2 GiB VSDB budget.

## Returned data and allocations

A published leaf is immutable. The prototype uses the existing
`MapxRaw::try_get_with` or `MapxRawReader::try_get_with` callback to
locate the slot while MMDB owns its backing bytes. It copies a selected
inline value into the API's owned result or extracts the blob ID and releases
the pin before reading that blob. Existence queries only compare keys.
An unpublished writer-owned leaf still comes from the pending object map.
The immutable internal-page cache, mutable root refresh, iterator and
root leases retain their existing behavior. The earlier [retained-header
experiment](../README.md) was a separate prototype.

The diagnostic binaries counted allocation and reallocation requests
on the calling thread only. Each class checked the same 8,192 results
after warmup. Values are averages per query. The timed binaries used their
normal allocators. [Raw counts and checksums](allocations.csv)

| Operation | v4 allocations / bytes | v5 control allocations / bytes | Projection allocations / bytes |
|---|---:|---:|---:|
| get-hit | 5.01 / 1,791.2 | 2.02 / 1,741.8 | 1.02 / 77.0 |
| get-miss | 4.01 / 1,763.5 | 1.02 / 1,711.3 | 0.02 / 46.8 |
| contains-hit | 4.01 / 1,760.2 | 1.02 / 1,708.2 | 0.02 / 43.5 |
| contains-miss | 4.01 / 1,763.5 | 1.02 / 1,709.8 | 0.02 / 44.5 |

All 678 x86_64 strata library tests passed against the prototype, including
a test of pending and durable inline/empty/blob slots, independently owned
results, missing keys and a leased root surviving owner release. The 83-test
tree subset and 12 snapshot tests also passed separately.

## Reproduction

Run from a complete VSDB checkout with an empty scratch directory.
Each arm builds in a separate Cargo target directory, and the runner checks
three distinct executable hashes. Five independent seeds contribute six
fresh processes each, in alternating order. The scripts retain all sample
counts and sources. The [replayable prototype](point-projection.patch)
applies to `8ce01f0`.

```sh
mkdir /tmp/vsdb-tree-projection
bash docs/evaluations/v18-tree-v5/projection/reproduce.sh "$PWD" /tmp/vsdb-tree-projection
python3 -B docs/evaluations/v18-tree-v5/projection/summarize.py docs/evaluations/v18-tree-v5/projection/trials.csv
```
