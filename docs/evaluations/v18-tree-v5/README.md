# Retaining a checked B+ tree leaf header

**Keep the current production tree read path.** The candidate improved
128-row scan throughput by 12.70% and scan p99 in all five pairs. Its
paired median p99 gain of 9.40% fell below the 10% promotion target and
the 28.66% maximum difference between matched control runs. The source
and mechanism oracle are preserved as patches for future workload-specific
evaluation.

The candidate keeps a leaf's checked slot count and type with the
immutable page bytes in a range iterator. A point hit consumes its value
before returning from the lookup, while the already-checked leaf view is
still borrowed. Both changes reuse the same public operations, existing
page cache, blob reader, root refresh and lease lifetime. Page encoding,
ordering, cache capacity and key ownership stay the same.

On a 96-key tree with both overflow and inline values, an independent
test-only counter observed two leaf-header parses per point hit, one
per point miss and 101 while consuming 95 rows across three leaves.
The candidate reduced these counts to one, one and three respectively.
The oracle verifies the exact large and small values and ordered keys at
an excluded lower and included upper bound. Additionally, 83 tree tests
and 12 snapshot tests passed on the candidate, covering empty and unbounded
ranges, internal cache eviction, mutation across aliases, blob-backed
values, and pin retention after dropping a snapshot.

## Measured requests

| Operation | Control p99 µs | Candidate p99 µs | Paired median gain | Paired gain range | Max A/A | Favorable pairs | Throughput change |
|---|---:|---:|---:|---:|---:|---:|---:|
| 128-row range | 12.760 | 11.560 | +9.40% | +6.38% to +14.56% | 28.66% | 5/5 | +12.70% |
| Point hit | 2.950 | 3.020 | −2.39% | −4.25% to +2.03% | 3.99% | 1/5 | −3.04% |
| Point miss | 2.950 | 2.970 | −0.68% | −1.71% to +2.71% | 6.65% | 2/5 | −0.81% |

The candidate's iterator retained the same owned leaf buffer and one
internal page per cursor level. Its fixed `BTreeIter` state grew from 120
to 136 bytes on x86_64. Every returned key and value retained the existing
ownership semantics. This experiment did not add a cache shared by separate
queries.

| Upgrade reference on the same trace | v4 scalar p99 µs | v5 scalar p99 µs | Paired v5 p99 gain | Paired v5 throughput change |
|---|---:|---:|---:|---:|
| 128-row range | 11.209 | 12.760 | −10.45% | −12.77% |
| Point hit | 2.640 | 2.950 | −11.74% | −26.46% |
| Point miss | 2.620 | 2.950 | −12.69% | −26.77% |

For this standalone workload the ported engine's ordinary point calls and
bounded scan regressed compared with the v4-backed implementation. This
comparison uses each adapter's normal sizing at the same caller budget.
Changing the engine changes its read path and internal memory geometry.
The experiment establishes the observed caller cost and does not isolate
the cause. Each p99 cell is the median of five within-run percentiles;
paired gains are computed before taking the median.

Each fresh release process bulk-loaded 32,768 evenly spaced eight-byte
big-endian keys with deterministic 32-byte inline values, flushed the
default 16-shard namespace and warmed every row. The driver used a 2 GiB
VSDB budget and the checkout's own engine options. Each process then timed
60,000 hits, 60,000 misses and 10,000 bounded forward scans of 128 rows.
Each class used a deterministic pseudorandom trace. Point p99 includes the
public call returning the owned result. Scan p99 includes construction of
the iterator, reading the rows, validating keys and values, and exhausting
the bounded range. Throughput uses successful requests divided by the
per-class wall time. These are single-threaded warm observations.

A/A pairs used separate processes of the v5 control. A/B pairs used a
separate v5 scalar process and a process running the experimental code.
The v4-backed release ran once in the middle of each five-process round
for a separate upgrade comparison. Every arm used the same per-round key
trace. The ordering reverses every other round. The primary was the
scan p99. Promotion required at least a 10% paired median improvement
above the largest absolute A/A difference, four favorable pairs, at least
95% of scan throughput and no more than a 5% median point-p99 regression.

The [raw process measurements](trials.csv) include the query count, within-run
p99, class wall time and executable fingerprint. The [host record](host.json)
includes toolchain, CPU, filesystem and revisions. Separate Cargo target
directories and a three-fingerprint check prevent accidental reuse of the
same workspace artifact for multiple arms. The final measurements use
only these verified binaries.

## Reproduction

From a full VSDB checkout on Linux, use the current evidence directory,
two available paths and CPU 4. The script creates three detached worktrees
under the supplied empty scratch directory, uses independent Cargo outputs,
and creates a fresh local database for every trial. If other benchmarks share
the host, pass the same optional `--lock` path to the Python driver for each.

```sh
mkdir /tmp/vsdb-tree-trial
bash docs/evaluations/v18-tree-v5/reproduce.sh "$PWD" /tmp/vsdb-tree-trial
python3 -B docs/evaluations/v18-tree-v5/summarize.py docs/evaluations/v18-tree-v5/trials.csv
```

The [oracle patch](oracle.patch) applies to `0ecb57d`. The [candidate
patch](leaf-header.patch) applies on top of that instrumented control. To
repeat the deterministic mechanism check inside each checkout, set
`VSDB_BASE_DIR` to a fresh test directory and run
`cargo +stable test -p vsdb --lib checked_leaf_header_reused_for_hits_and_cursor_rows`.
The original branch fails with the repeated-parse counts above. The
experimental branch passes. All header counters compile only in unit tests.
