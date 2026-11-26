# Stock v18 B+ tree reads on MMDB v4 and final v5 defaults

VSDB `6854273` on MMDB v5 took **12.03% longer for a warm point hit and
11.83% longer for a warm miss at p99** than v18 on MMDB v4, measured as
the median within-pair change. Both operations slowed in all five
pairs. These results cover an application reading one immutable
standalone `PersistentBTree` root on the measured x86_64 Linux host.

| Request | Median run p99, v4 → v5 | Paired p99 cost (min to max) | Largest v4 A/A p99 difference | Paired v5 throughput change | Largest v4 A/A throughput difference |
|---|---:|---:|---:|---:|---:|
| Point hit | 2.630 → 2.960 µs | +12.03% (+10.23% to +14.07%) | 3.45% | −26.56% | 11.47% |
| Point miss | 2.620 → 2.940 µs | +11.83% (+9.85% to +22.78%) | 3.77% | −27.30% | 2.51% |
| 128-row range | 10.100 → 13.070 µs | +32.83% (+5.57% to +74.97%) | 41.18% | −16.82% | 23.08% |

All five paired ranges took longer under v5. Their control-to-control
spread exceeded the median difference for both p99 and throughput, so
the magnitude of the scan regression remains uncertain. The two
point-read regressions exceeded their corresponding v4 spreads.
Each binary uses the production tree implementation for its revision.
[Complete paired runs](trials.csv) · [Host, revisions and binary fingerprints](host.json)

## Workload and controls

The stock v4 baseline used VSDB `6ddaed9` and MMDB 4.3.15. The final v5
build used VSDB `6854273` and MMDB `255015a`. On that build an independent
call to `Namespace::memory_usage()` confirmed 16 shards, 16,777,216 bytes
per write buffer, a limit of four immutable generations per shard and
268,435,456 bytes for each shared cache store. The caller set a 2 GiB
VSDB budget. One tree's storage prefix routed its pages to one shard.
[Geometry check](geometry.rs)

Both binaries used the [original probe](../probe.rs). Each process created
a fresh local database, loaded 32,768 evenly spaced eight-byte keys with
32-byte inline values, flushed the namespace and warmed the entire tree.
The probe timed 60,000 successful point hits, 60,000 confirmed misses and
10,000 bounded forward ranges of 128 consecutive rows. The scan consumes
and verifies every key and value inside the measured request. Point
verification follows each timed call. Throughput includes verification in
each class's full wall time. Both arms used CPU 4 and the same NVMe Btrfs
directory with compression explicitly disabled. Device reads were not
required for this warm comparison.

Five independent seeds each ran two fresh v4 processes for the A/A check
and a separate fresh v4 plus final v5 process for the A/B comparison.
Order reversed on alternating rounds. A shared lock serialized measured
processes on the host. Table p99s are medians of the five within-run A/B
percentiles. Paired costs use `100 × (v5 / v4 − 1)` within each round.
A/A spread is the maximum absolute change from its designated first
reference. These are full-stack comparisons at the two recorded revisions.

## Reproduction

Recompute the published comparison from the recorded trials. The analyzer
checks binary fingerprints, process order, seeds and complete operation counts.

```sh
python3 -B docs/evaluations/v18-tree-v5/stock/release.py summarize \
    docs/evaluations/v18-tree-v5/stock/trials.csv docs/evaluations/v18-tree-v5/stock/host.json
```

For fresh measurements, the [original build script and Cargo locks](https://github.com/rust-util-collections/vsdb/tree/4c15817f4bf26513386c9a6ce1d375dca33043dd/docs/evaluations/v18-tree-v5/stock)
recreate both historical v18 builds.
