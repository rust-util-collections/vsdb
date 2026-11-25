# SlotDex and DagMap v18 validation

Measured on an AMD EPYC 9474F with stable Rust 1.100.0, mmdb 4.3.15, optimized
Cargo builds and separate scratch universes. Both revisions used the same
benchmark bodies. The v17 baseline came from `da9a325`. Runs used four pinned
logical CPUs (82–85) and the default 16-shard namespace. Criterion used ten
samples, a one-second warm-up and a three-second target measurement window.

## SlotDex

Each query index held 8,192 keys with tier capacity 16. Dense slots were
consecutive and had one key each. Sparse slots were spaced by 1,000,000. Hot
slots had 128 keys each. All rows used physical ascending storage. The
iterator consumed the first 128 keys in descending slot order. The page
retrieved 128 keys at page index 16. Times are Criterion point estimates.

| Workload | v17 | v18 |
|---|---:|---:|
| Dense descending iterator | 132.19 µs | 22.12 µs |
| Sparse descending iterator | 140.93 µs | 22.32 µs |
| Hot descending iterator | 12.59 µs | 11.01 µs |
| Dense descending page | 36.40 µs | 24.81 µs |
| Sparse descending page | 76.40 µs | 23.56 µs |
| Hot descending page | 20.96 µs | 14.45 µs |
| Batch growth, wide then hot | 65.15 ms | 79.15 ms |
| Batch growth, sparse floors | 161.80 ms | 160.75 ms |

The batch measurements varied substantially. Criterion reported p = 0.06 for
wide-then-hot and p = 0.94 for sparse floors. The wide-then-hot point estimate
increased by 14 ms in this run.

The iterator groups sparse L1 buckets using their actual entry counts. Large
fan-outs use bounded forward scans over slot coordinates, with forward probes
across empty spans. A group retains at most 256 raw entry keys totaling 16 KiB.
Individual larger keys and high-population slots stream on demand.

## DagMap

Each repetition created a fresh root, a mainline of nine children and eight
side branches. Another thread updated and read one of 128 unrelated keys in
the same namespace and slept for 50 µs between operations. Timings exclude
graph construction and the cleanup following a successful prune.

| Operation | v17 Criterion time | v18 Criterion time | v17 p99 | v18 p99 |
|---|---:|---:|---:|---:|
| Prune the mainline | 669.28 ms | 6.00 ms | 605.79 ms | 3.85 ms |
| Destroy the root | 34.33 ms | 7.33 ms | 65.67 ms | 5.39 ms |

The p99 measurements came from a separate run of the same workload. We
timed 104 operations individually, discarded four warm-up samples, and
reported the 99th ordered sample from the remaining 100. Absolute latency
varied between runs. Each completed v18 operation issues one WAL fence on
the DAG's shard. Child, parent and data mutations remain ordered on that WAL.

Reproduce the Criterion workloads with:

```sh
VSDB_BASE_DIR="$(mktemp -d)" cargo +stable bench -p vsdb --bench slotdex_batch
VSDB_BASE_DIR="$(mktemp -d)" cargo +stable bench -p vsdb --bench dagmap
```

## Correctness and migration

- SlotDex tests compare signed endpoint queries, odd strides, both physical
  orders, bounded key decoding, stale writers, generation exhaustion and
  snapshots retained across a clear. The compile-fail test exercises a custom
  `SlotType` whose byte type is `Vec<u8>`.
- DagMap tests cover 5,000 consecutive child nodes, cross-namespace deep copies,
  foreign registry entries, local edge collisions during reparenting, recovery
  from partial marker and cleanup passes and repeated pruning on consumed
  nodes. Subprocess tests exit without destructor flushes between WAL phases.
- The independent migration crate restores captured v17 handle bytes and
  component rows into isolated roots. Tests check that v18 rejects the old
  handle, then verify retained prefixes, slot counts, edge tokens, typed
  child metadata and logical data after conversion. Additional tests resume
  the DAG converter with partially overwritten destinations and partially
  retired source prefixes.
