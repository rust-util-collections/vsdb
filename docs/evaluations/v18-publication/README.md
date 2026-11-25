# Publication and foreground write profile

Use the same Rust driver against the v4-backed adapter at `6ddaed9`, the
native v5 adapter before atomic DAG registration at `504630f`, and the
candidate with bounded VerMap edits at `0481462`. All three resolve the
same value codecs. Build the standalone Cargo manifest in this directory
with `cargo +stable build --release` in each worktree, copying this
directory into earlier revisions. Enable `--features working-edit` for
the candidate. Name the saved executables `v4`, `v5-previous` and
`v5-pinned`. Build the last two again with the additional `counters`
feature, saving them as `diag-previous` and `diag-pinned`.
Diagnostic runs count engine submissions on the issuing thread. The ordinary
latency binaries use the regular dependency feature set.

The child case creates 10,000 children on one existing parent. The
dependent operation reads and increments two keys twice, either serially
or inside one bounded working edit. A precomputed write_batch sets the
same final values. Each process verifies all final values or the graph
size and last edge. History commit time is outside those measurements.

For placement, one writer issues 10,000 128-byte map updates against a
paced clock. A second writer changes 1,024 256-byte values in VerMap for
each 128 foreground arrivals. Both runs start with 4,096 seeded entries
per collection. The namespace has two shards. Choosing the small map's
prefix controls whether the writers share a shard. Wait for both streams
to finish. Record arrival-to-ack and service p99, throughput, process RSS
and L0 at both ends of the timed interval. The predeclared offer is 3,000
foreground puts/s, with floor/ceil checks for the outstanding foreground
and bulk requests at half and end (at most 100 and two respectively).
Rows failing that qualification are descriptive only.

Use a fresh output directory and uncompressed scratch on the target device:

```sh
python3 run.py /path/to/binaries /path/to/output --cpus 4-7 --scratch /path/to/scratch --lock /tmp/vsdb-profile.lock
python3 summarize.py /path/to/output/trials.csv
```

Each paired p99 uses 10,000 complete logical operations. The largest
absolute A/A change measures run-to-run variation. Throughput measures
the complete operation loop. An idle or empty working edit is covered by
the generation-unchanged unit test. Root generation is checked directly
by the versioned tests; the separately instrumented submissions measure
calls actually received by the engine.

## Results on x86_64 Linux

The Ryzen 9 5900X runs used four pinned cores on NVMe with Btrfs
compression disabled in the benchmark directory. All 180 timed processes
completed and the five diagnostics verified the engine submission counts.
The source worktrees and binary fingerprints are retained with the trials.
[Host and executables](results/host.txt) · [All process observations](results/trials.csv) · [Paired statistics](results/summary.csv)

| Logical operation | Control p99 | Compared p99 | Median paired improvement | Largest A/A change | Favorable pairs |
|---|---:|---:|---:|---:|---:|
| DagMap child, v5 two writes versus one atomic batch | 12.79 µs | 8.28 µs | 31.38% | 25.39% | 5/5 |
| Four dependent VerMap edits, individual versus working session | 63.67 µs | 26.13 µs | 56.18% | 26.95% | 5/5 |
| Two known final values, write_batch versus a session performing four reads and edits | 23.76 µs | 26.35 µs | −5.64% | 14.45% | 0/5 |
| DagMap child, v4-backed v18 versus v5-backed v18 | 13.13 µs | 9.09 µs | 34.19% | 11.41% | 5/5 |

Column medians describe the individual arms. The gain is calculated inside
each matched pair. The new DagMap path made 1,000 engine submissions for
1,000 children; the previous v5 implementation made 2,000. Individual
VerMap updates made 4,000 submissions, the working overlay and precomputed
batch each made 1,000. Use a working session for dependent updates. A
caller with precomputed edits already has the same publication count and
can continue using `write_batch`.

All small puts and bulk operations met the matched-load screens for the
mixed workload. Routing a foreground map away from VerMap reduced median
run p99 from 1,075.88 to 79.54 µs on v5. The 85.71% median paired gain
was smaller than the 240.50% control variation, with four favorable pairs.
The v4/v5 same-shard and split-shard comparisons also had wide control
variation. This trace establishes neither a guaranteed tail reduction nor
a reason to change persistent shard routing. The CSV retains the individual
arrival/service p99s, L0 debt, bulk row counts and RSS. On equivalent
individual VerMap calls and precomputed batches, v4/v5 median p99 changes
also stayed inside control variation.
