# VecDex v18 search on MMDB v5

**Retain scalar pinned scoring for HNSW queries.** On the warm primary
graph, fetching up to 32 neighbor vectors together reduced block
acquisitions per query from 309.56 to 170.99. The median run p99 was
262.95 µs for scalar pinned reads and 281.00 µs for the grouped trial.
The median paired gain was −5.75%, with two of five pairs favoring the
grouped candidate (range −51.64% to +14.63%). The largest scalar/scalar
difference was 239.94%. Those results support deferring the grouped
strategy on this workload. The host variation limits estimates of its
absolute cost. The selected implementation checks graph rows and storage
errors, and decodes vector and adjacency bytes directly while each pin
is live. [Warm paired runs](results/warm.csv) · [Binary and fixture identities](results/metadata.json)

Separate instrumented runs projected 49,426,944 bytes of vector payload in
1,000 queries for both strategies. The maximum distinct backing SST
capacity retained by a vector lookup rose from 16,739 bytes for a scalar
lookup to 535,627 bytes for a group. Both runs returned the same ordered
keys, distance bits and 868 first-place self matches. These byte counts
describe the graph query's vector reads. They exclude other query
allocations and the engine's evictable cache charge. [Scalar pin diagnostics](results/pins-1.txt) · [Grouped pin diagnostics](results/pins-32.txt)

## Verified storage reads

All 200,000 queries in the five scalar/scalar and five scalar/grouped
pairs charged at least 4 KiB of Linux storage reads per request. The
full-request median p99 was 9.815 ms for scalar pinned reads and 8.960 ms
for width 32. The paired median gain was 8.65% (2.93–13.99%, five favorable
pairs), below the 10% target and the 26.30% maximum A/A difference.
Acquisitions fell from 309.56 to 170.99 per query while kernel-accounted
read bytes averaged 1,197,844 and 1,233,076 per query respectively (median
over the five A/B runs of each arm). Grouping reduced repeated probes,
with a similar amount of physical data transferred on this fixture.
[All physical-read pairs](results/device.csv)

A separate five-process width-eight diagnostic returned the same trace
and finished all 50,000 queries with observed storage reads. Median
run p99 was 8.838 ms, with 184.01 acquisitions per query. This arm has
independent process timings and no paired storage-latency promotion
decision. [Width-eight disk observations](results/device-width8.csv)

## Process memory

Each cell is the largest of three independent resource runs in KiB.
The first value is GNU time's OS maximum for the command. The second
is the largest directly observed high-water mark in the VecDex process's
`/proc/PID/status`. The cold command includes a Python file-advisory helper;
its peak can exceed the probe process's. These are process-lifetime
observations on the fixture, and include allocation outside the query window.

| Release arm | Warm GNU max / probe VmHWM (KiB) | Device GNU max / probe VmHWM (KiB) |
|---|---:|---:|
| v4 stock (`6ddaed9`) | 13,096 / 13,060 | — |
| v5 scalar pinned (`dd07a00`) | 13,128 / 13,300 | 12,076 / 7,244 |
| v5 width eight (`dd07a00`) | 13,280 / 13,376 | 12,080 / 7,472 |
| v5 width 32 (`dd07a00`) | 13,148 / 13,336 | 12,148 / 7,444 |
| v5 retained scalar (`1300ad2`) | 13,088 / 13,324 | 12,080 / 7,304 |
| v5 final corrected scalar (`3b220e7`) | 13,228 / 13,312 | 12,084 / 7,268 |

The warm processes each ran 1,000 queries after warming the larger
engine cache. Each device process ran 500 on the smaller cache with file
eviction, including a qualifying storage read on every query. The paired
latency datasets above use the longer 10,000-query traces. Returned
SST pin capacity and OS process RSS have separate owners and timing.
[27 original RSS processes](results/rss.csv) · [Six final-process RSS observations](results/rss-final.csv)

## Warm upgrade comparison

Using the same logical graph and query traces, the actual public release
build at `1300ad2` took 254.77 µs in median query p99, compared with
187.24 µs for the v18 collections on MMDB 4.3.15 at `6ddaed9`. The
within-pair median regression was **37.28%** (34.44–42.21%, all five
pairs). The v4/v4 paired noise was at most 22.75%. Both physical fixtures
had one SST, the same configured 16 KiB block target, compression policy
and 512 MiB namespace sizing input. Both reported zero Linux storage reads
during the timed queries. Each run returned exactly the same ID/distance
digest and known-nearest count as its counterpart. Scalar/scalar v5
controls had a largest paired difference of 7.87%. The two v5 scalar
implementations differed by a paired median of 0.46% in this series.
[Actual v4 and v5 release comparisons](results/landed.csv)

The block counters reported 154.78 acquisitions per v4 query and 309.56
per retained v5 query. The v4 SST reader loads its index when opening the
file. V5 lookups acquire both an index leaf and a data block. These
counter definitions and the differing SST encodings limit comparisons
of work per acquisition. The query timing includes all read and compute
steps, and this measurement does not isolate the time of the format change.
A separate width-eight run showed a 10.42% paired gain against the
experimental pinned-scalar control. The independent confirmation below uses the retained production scalar
source as its reference. [Intermediate-width and upgrade observations](results/secondary.csv)

On a singleton graph, five independent pairs with 100,000 queries per
process gave a −0.50% paired change for the width-32 candidate, with a
1.68% maximum A/A difference. [Singleton controls](results/singleton.csv)

## Query and decision rule

The primary workload is an ordinary warm `VecDex<u64, L2>::search` requesting
five neighbors from a persisted 8,192-node HNSW graph (96-dimensional f32,
`m=16`, base degree limit 32, construction beam 64, search beam 12). Half
the vector clusters have nearby node IDs, half have interleaved IDs. Score
existing stored vectors against deterministic queries drawn from the dataset.
Record the nearest-self count and a digest of every ordered result ID and
distance bit pattern. Construction, warm-up, and query generation are outside
the latency window. Time the full public search call, including its snapshot.
A separate freshly opened, smaller-cache workload samples actual Linux
`read_bytes`. Read-ahead, OS-cache hits and engine-cache hits can coexist in a
query. Record the fraction of requests with at least 4 KiB of charged reads.

Compare scalar pinned decoding with fetching up to 32 vectors from each
eligible neighbor list. Use the same graph rows, layout, key traces,
release compiler and affinity within each pair. Alternate order and interleave
five independent A/A and five A/B pairs. Each process issues 10,000 queries.
Summarize the median of within-pair p99 gains. Promote the default strategy
when the primary gain reaches 10%, exceeds the largest absolute A/A difference,
and at least four of five pairs favor it. Protect ordinary reads and singleton
lists against a 5% median regression. If the first primary comparison has
a gain of at least 10% but cannot clear control variation, run one longer
warm comparison with 30,000 queries per process and independent seeds. An
ambiguous single-key guard can be repeated at 300,000 queries per process. Record the eight-key-width intermediate
and MMDB v4 adapter as separate comparisons. The application graph and its
vectors stay identical across preparations; an engine-format conversion is
unnecessary for this isolated workload. All versioned data migration tests
remain independent of the benchmark.

## Two targeted follow-ups

After observing the warm upgrade regression, a query-local cache trial
stored up to 128 (node ID, distance) entries. Reuse applied only to the
three built-in metrics. It could avoid rereading a vector revisited at a
different level. All entries expired at the end of the query and custom
metrics continued to receive every callback. The same 10%/A/A/four-of-five
rule applies to this separate [prototype at the retained revision](distance-cache.patch).
Its five matched pairs measured 254.14 µs versus 257.00 µs median run p99
and a paired median gain of −0.48% (−1.49% to +0.57%, two favorable pairs,
7.75% maximum A/A difference). It cut acquisitions from 309.56 to 302.26
per query and did not reach the latency threshold. Keep the ordinary
single-view scoring path. [Distance-cache comparison](results/distance-cache.csv)

The width-eight confirmation used fresh seeds 361127–361131, 30,000
queries per process, the retained production scalar reference and five
independent A/A pairs. All five A/B pairs improved. Median run p99 was
253.18 µs for scalar and 230.11 µs for width eight. The 9.15% paired
median gain (5.18–14.63%) fell below the 10% target and the 19.29%
maximum A/A difference. Peak backing capacity for one vector group
was 133,911 bytes. Retain scalar reads for this release. [Confirmation trials](results/width8-repeat.csv) · [Width-eight pin diagnostics](results/pins-8.txt)

## Reproduction on Linux

Use an empty scratch area on the target filesystem. Each build command
creates a detached worktree under the binary directory. The v4 builder
substitutes a deterministic level generator while exercising the normal
VecDex insertion/linking code. Persist the logical key/value stream once
and load it into the warm v4, warm v5 and constrained-cache v5 stores. A
single namespace and shard avoid cross-shard timing differences. The warm
store uses a 512 MiB namespace sizing input. The device store uses 8 MiB.
The disk-backed arm issues file-local page eviction requests outside every
timed search, then counts Linux `read_bytes` per request; qualifying runs
need at least 4 KiB on 95% of requests. The engine cache stays enabled.

Run the commands from the VSDB root. The example assumes CPU 4 is available.
Pin diagnostics use MMDB's optional returned-block allocation identity on a
separate executable. It sums each SST allocation capacity once per group
and records the largest simultaneous sum. It separately counts the vector
payload bytes passed to the decoder; scratch scalar conversion is required
in both arms. Timing executables omit pin diagnostics.

```sh
d=docs/evaluations/v18-vecdex-v5
s=$(mktemp -d /tmp/vsdb-vecdex.XXXXXXXX)
mkdir "$s/results"
python3 "$d/build.py" . "$s/bin" 6ddaed9 v4
for width in 1 8 32; do python3 "$d/build.py" . "$s/bin" dd07a00 "$width"; done
python3 "$d/build.py" . "$s/bin" 1300ad2 shipped
python3 "$d/build.py" . "$s/bin" 1300ad2 cached
python3 "$d/build.py" . "$s/bin" 3b220e7 final
"$s/bin/v4" create "$s/builder" "$s/graph.rows"
"$s/bin/v4" import "$s/v4-warm" "$s/graph.rows" 512
"$s/bin/32" import "$s/v5-warm" "$s/graph.rows" 512
"$s/bin/32" import "$s/v5-device" "$s/graph.rows" 8
"$s/bin/1" singleton "$s/v5-singleton"
for regime in warm secondary device device-width8 landed distance-cache; do
    python3 "$d/trials.py" "$s/bin" "$s" "$s/results/$regime.csv" "$regime" --cpus 4
done
python3 "$d/trials.py" "$s/bin" "$s" "$s/results/singleton.csv" singleton --samples 100000 --cpus 4
python3 "$d/trials.py" "$s/bin" "$s" "$s/results/width8-repeat.csv" width8-repeat --samples 30000 --seed 361126 --cpus 4
python3 "$d/trials.py" "$s/bin" "$s" "$s/results/final.csv" final --seed 461126 --cpus 4
python3 "$d/rss.py" "$s/bin" "$s" "$s/results/rss-final.csv" --final-only --cpus 4 --lock "$s/timing.lock"
python3 "$d/rss.py" "$s/bin" "$s" "$s/results/rss.csv" --cpus 4 --lock "$s/timing.lock"
python3 "$d/summarize.py" "$s/results"
for width in 1 8 32; do
    python3 "$d/build.py" . "$s/bin" dd07a00 "$width" --pin-diagnostics
    taskset -c 4 "$s/bin/$width-pins" run "$s/v5-warm" warm 261127 1000
done
```

The separate RSS runs launch the same release executables and fixtures
under GNU time. It records maximum resident set size in KiB for the
whole process lifetime. The collector polls the probe's own `VmHWM` as
a second kernel observation, independent of the cold-arm advisory helper.
These shorter resource traces use three separate seeds, 1,000 warm or
500 cold queries each. RSS includes the database, cache, harness, query
materialization and warm-up. Returned SST allocation capacity has its
own diagnostic runs.

The warm-up performs scalar lookups over every graph row. Result digests
include the ordered keys and IEEE-754 distance bits, and nearest-self counts
compare the first result against an independently known zero-distance
neighbor. Report storage reads and block acquisitions separately. An
acquisition includes an engine-cache hit. A lower count can leave similar
device traffic when two keys arrive in different blocks.
