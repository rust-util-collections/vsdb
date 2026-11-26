# VecDex search with MMDB v5

**Warm end-to-end search p99 rose 37.48% on the measured v4 → v5 upgrade.**
The stock v18 adapter on MMDB 4.3.15 (`6ddaed9`) had a median run p99 of
184.57 µs. With the selected v5 policy (`6854273`) it was 253.38 µs. All
five pairs were slower on v5, by 34.83–69.45%. Maximum control/control
variation was 22.19% on v4 and 11.98% on v5. Both returned the same
ordered IDs and distance bit patterns, with zero observed storage reads in
the warm timing window. The counters in v5 include index-leaf acquisitions
as well as data blocks, so their difference alone cannot explain the
upgrade cost. [Stock upgrade pairs](results/policy.csv) · [Host, binary and fixture identities](results/metadata.json)

**The production query uses scalar pinned decoding.** On the same graph,
grouping vectors reduced block acquisitions at width 32 from 309.56 to
170.99 per query. It failed to clear the 10% complete-query p99 threshold
and matched control variation. Width eight and a fixed per-query distance
cache also fell short.

| Query comparison | Scalar p99 | Compared p99 | Median paired improvement | Maximum A/A difference |
|---|---:|---:|---:|---:|
| Warm, 32 vectors | 262.95 µs | 281.00 µs | −5.75% | 239.94% |
| Warm, 8 vectors, 30,000 queries per run | 253.18 µs | 230.11 µs | +9.15% | 19.29% |
| Device reads, 32 vectors | 9.815 ms | 8.960 ms | +8.65% | 26.30% |
| Warm, 128-entry distance cache | 254.14 µs | 257.00 µs | −0.48% | 7.75% |

All 200,000 complete queries in the device pairs recorded at least 4 KiB of
Linux storage reads. Average physical bytes per query were 1,197,844 for
scalar and 1,233,076 for grouped reads. A separate width-eight disk
diagnostic observed 8.838 ms p99 without a paired latency control.
[Warm trials](results/warm.csv) · [Width eight](results/width8-repeat.csv) · [Device trials](results/device.csv) · [Disk diagnostic](results/device-width8.csv) · [Distance cache](results/distance-cache.csv)

In instrumented runs with 1,000 identical queries, each strategy decoded
49,426,944 vector payload bytes. Peak simultaneous SST allocation capacity
was 16,739 bytes at width one, 133,911 at width eight and 535,627 at width
32. Process RSS was measured separately: the stock warm v5 run reached
13,324 KiB in the probe's sampled high-water reading. Some cold GNU time
readings include the file-eviction helper. [Pin measurements at width one](results/pins-1.txt) · [Width eight](results/pins-8.txt) · [Width 32](results/pins-32.txt) · [RSS controls](results/rss.csv) · [Stock RSS](results/rss-policy.csv)

## Workload and reproduction

The graph contained 8,192 nodes with 96-dimensional f32 vectors, `m=16`,
base degree limit 32, construction beam 64 and search beam 12. The timed
operation was a public L2 search for five neighbors, including snapshot
acquisition. The warm fixture used a single shard, a 512 MiB namespace
budget and one SST. A fresh v5 import of all 41,493 logical rows produced
the same 4,428,811-byte SST and empty WAL at the selected buffer setting.
Trials used CPU 4 on the recorded Ryzen 9 5900X. Each latency comparison
used five independent A/A and A/B pairs. The singleton control stayed
within its baseline variation. [Singleton trials](results/singleton.csv)

Run `python3 summarize.py results` from this directory to verify the
retained cells, trace equality and I/O coverage. To collect the trials
again from the VSDB root, use a fresh Linux scratch area on the target
filesystem. The builder creates separate release worktrees. The
`final` executable is the pre-policy control used in `policy.csv`.

```sh
d=docs/evaluations/v18-vecdex-v5
s=$(mktemp -d /tmp/vsdb-vecdex.XXXXXXXX)
mkdir "$s/results"
python3 "$d/build.py" . "$s/bin" 6ddaed9 v4
for width in 1 8 32; do python3 "$d/build.py" . "$s/bin" dd07a00 "$width"; done
python3 "$d/build.py" . "$s/bin" 1300ad2 shipped
python3 "$d/build.py" . "$s/bin" 1300ad2 cached
python3 "$d/build.py" . "$s/bin" 3b220e7 final
python3 "$d/build.py" . "$s/bin" 6854273 policy
"$s/bin/v4" create "$s/builder" "$s/graph.rows"
"$s/bin/v4" import "$s/v4-warm" "$s/graph.rows" 512
"$s/bin/32" import "$s/v5-warm" "$s/graph.rows" 512
"$s/bin/32" import "$s/v5-device" "$s/graph.rows" 8
"$s/bin/1" singleton "$s/v5-singleton"
for regime in warm device device-width8 distance-cache; do
    python3 "$d/trials.py" "$s/bin" "$s" "$s/results/$regime.csv" "$regime" --cpus 4
done
python3 "$d/trials.py" "$s/bin" "$s" "$s/results/singleton.csv" singleton --samples 100000 --cpus 4
python3 "$d/trials.py" "$s/bin" "$s" "$s/results/width8-repeat.csv" width8-repeat --samples 30000 --seed 361126 --cpus 4
python3 "$d/trials.py" "$s/bin" "$s" "$s/results/policy.csv" policy --seed 561126 --cpus 4
python3 "$d/rss.py" "$s/bin" "$s" "$s/results/rss-policy.csv" --policy-warm-only --cpus 4 --lock "$s/timing.lock"
python3 "$d/rss.py" "$s/bin" "$s" "$s/results/rss.csv" --cpus 4 --lock "$s/timing.lock"
python3 "$d/summarize.py" "$s/results"
for width in 1 8 32; do
    python3 "$d/build.py" . "$s/bin" dd07a00 "$width" --pin-diagnostics
    taskset -c 4 "$s/bin/$width-pins" run "$s/v5-warm" warm 261127 1000
done
```
