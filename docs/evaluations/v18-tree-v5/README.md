# B+ tree reads on MMDB v5

The stock upgrade comparison measured **12.03% higher p99 for point hits
and 11.83% higher p99 for misses** on a warm standalone tree of 32,768
keys. The measured scan latency and throughput changes were smaller than
the corresponding control variation. The final stock comparison uses a
16 MiB shard write buffer with four immutable generations.
[Production v4/v5 measurements and replay](stock/README.md)

## Tree implementation decision

The production tree retains its existing point lookup and range iterator.
Two independently measured alternatives reduced work on the same warm
key distribution, but their primary end-to-end p99 gains were below the
10% adoption threshold.

| Alternative and primary operation | Scalar p99 | Candidate p99 | Paired improvement | Largest v5 control-to-control change |
| --- | ---: | ---: | ---: | ---: |
| Cache the checked leaf header, 128-row scan | 12.760 µs | 11.560 µs | 9.40% | 28.66% |
| Inspect a pinned leaf, point hit | 2.970 µs | 2.830 µs | 3.37% | 3.31% |
| Inspect a pinned leaf, point miss | 2.970 µs | 2.830 µs | 4.36% | 2.69% |

Column medians are per-run p99s. Each improvement is the median of five
within-pair changes. Each v5 control and candidate pair used the same
deterministic queries and verified the result. The host ran Linux/x86_64.

- **Retained header.** The iterator held the checked leaf's slot count
  and type beside its immutable bytes. A 95-row scan over three leaves
  parsed three leaf headers instead of 101. Timed 128-row scan throughput
  rose 12.70%. The iterator grew from 120 to 136 bytes. Each process
  ran 10,000 bounded scans and 60,000 each of point hits and misses.
  [Measurements](trials.csv) · [Configuration](host.json) · [Header-count test](oracle.patch) · [Measured change](leaf-header.patch)
- **Borrowed leaf projection.** The point lookup searched the leaf while
  its MMDB payload was pinned. It returned an owned inline value or an
  independently read blob. Throughput rose 10.46% for hits and 12.32%
  for misses. Caller-thread allocation requests per hit fell from
  1,741.8 to 77.0 bytes and per miss from 1,711.3 to 46.8 bytes.
  Each timing process ran 250,000 queries per point class. Allocations
  were measured separately over 8,192 verified calls per class.
  [Measurements](projection/trials.csv) · [Allocation counts](projection/allocations.csv) · [Configuration](projection/host.json) · [Measured change](projection/point-projection.patch)

## Reproduction

From a full VSDB checkout on Linux, recompute the candidate comparisons
from the recorded CSVs:

```sh
d=docs/evaluations/v18-tree-v5
python3 -B "$d/summarize.py" "$d/trials.csv"
python3 -B "$d/projection/summarize.py" "$d/projection/trials.csv"
```

To rerun either experiment, supply a new scratch directory. The scripts
build the recorded revisions in separate worktrees and Cargo target
directories, pin trials to CPU 4 and use fresh databases.

```sh
scan_dir=$(mktemp -d)
bash "$d/reproduce.sh" "$PWD" "$scan_dir"
point_dir=$(mktemp -d)
bash "$d/projection/reproduce.sh" "$PWD" "$point_dir"
```
