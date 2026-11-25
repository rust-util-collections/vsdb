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
