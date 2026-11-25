# VecDex v18 search on MMDB v5

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
"$s/bin/v4" create "$s/builder" "$s/graph.rows"
"$s/bin/v4" import "$s/v4-warm" "$s/graph.rows" 512
"$s/bin/32" import "$s/v5-warm" "$s/graph.rows" 512
"$s/bin/32" import "$s/v5-device" "$s/graph.rows" 8
"$s/bin/1" singleton "$s/v5-singleton"
for regime in warm secondary device device-width8 landed; do
    python3 "$d/trials.py" "$s/bin" "$s" "$s/results/$regime.csv" "$regime" --cpus 4
done
python3 "$d/trials.py" "$s/bin" "$s" "$s/results/singleton.csv" singleton --samples 100000 --cpus 4
python3 "$d/summarize.py" "$s/results"
for width in 1 32; do
    python3 "$d/build.py" . "$s/bin" dd07a00 "$width" --pin-diagnostics
    taskset -c 4 "$s/bin/$width-pins" run "$s/v5-warm" warm 261127 1000
done
```

The warm-up performs scalar lookups over every graph row. Result digests
include the ordered keys and IEEE-754 distance bits, and nearest-self counts
compare the first result against an independently known zero-distance
neighbor. Report storage reads and block acquisitions separately. An
acquisition includes an engine-cache hit. A lower count can leave similar
device traffic when two keys arrive in different blocks.
