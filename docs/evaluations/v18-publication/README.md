# DagMap and VerMap publication costs

**A DagMap child now takes one engine write submission.** In the default
16-shard namespace, child-creation p99 fell **33.38%** against the v4-backed
v18 runtime. On v5, atomically inserting the child's metadata, the parent
edge and the advanced allocator reduced p99 **31.05%** against the prior
two-write implementation. All five pairs favored the atomic batch. The probe
also checked the edge and child count. [Raw trials](results/default-namespace/trials.csv) · [Paired summary](results/default-namespace/summary.csv)

| Operation | Control p99 | Compared p99 | Median paired p99 change | Max control variation |
|---|---:|---:|---:|---:|
| DagMap child, v4 → v5 | 13.48 µs | 9.13 µs | −33.38% | 7.68% |
| DagMap child on v5, two writes → one | 13.30 µs | 9.17 µs | −31.05% | 7.41% |
| Four dependent VerMap updates on v5, individual → working edit | 70.54 µs | 31.72 µs | −52.51% | 39.58% |
| Two known final values on v5, precomputed batch → working edit with four reads and writes | 27.22 µs | 32.45 µs | +19.21% | 44.60% |

**Use a VerMap working edit when later writes depend on earlier reads.**
On the checked implementation at `2ae6149`, each group of four dependent
updates incremented two keys twice and produced one working-root
publication. Instrumented builds counted four write submissions for the
individual updates, one for the working edit, and one for the precomputed
batch. A read-only callback submitted zero writes or fences. History
commit was outside the measurement. [Raw trials and operation counts](results/verified-leaf/trials.csv) · [Paired summary](results/verified-leaf/summary.csv)

## Foreground writes during bulk publication

One foreground producer issued 10,000 128-byte map writes at 3,000/s. One
VerMap producer changed 1,024 values of 256 bytes per 128 foreground
arrivals. Both placements sustained the offered load. On v5, using a
second shard reduced median run foreground arrival-to-ack p99 from
1,075.88 µs to 79.54 µs. The paired improvement of 85.71% was within the
240.50% maximum difference between repeated controls. Call-only p99 was
22.24 µs versus 14.51 µs, with a 32.25% paired improvement against
99.31% variation. The fixture did not resolve a repeatable latency benefit
from changing shard placement. [Placement and upgrade trials](results/trials.csv) · [Summary](results/summary.csv)

## Reproduce

All comparisons used x86_64 Linux on a Ryzen 9 5900X, cores 4–7, independent
fresh processes, and matching seeds inside each pair. The table gives medians
of the five within-run p99s and the median change calculated within the five
pairs. Each process completed 10,000 logical operations. The CSVs record
separate control/control pairs. [Placement binaries](results/host.txt) · [DagMap binaries](results/default-namespace/host.txt) · [VerMap binaries](results/verified-leaf/host.txt)

To recompute the tables from this directory:

```sh
python3 summarize.py results/trials.csv
python3 summarize.py results/default-namespace/trials.csv --default-dag
python3 summarize.py results/verified-leaf/trials.csv --final-edits
```

To collect new observations, copy this directory into detached worktrees for
the measured revisions. Build its Cargo manifest with `cargo +stable build --release` using a separate target directory for each build. Save ordinary
binaries as `v4` (`6ddaed9`), `v5-previous` (`504630f`),
`v5-pinned` (`0481462`, feature `working-edit`), and `v5-final`
(`2ae6149`, feature `working-edit`). Also build the three v5 binaries with
the additional `counters` feature, naming them `diag-previous`, `diag-pinned`
and `diag-final`. The [runner](run.py) takes a binary directory, fresh output
directory, CPU list, scratch directory and timing lock, e.g.:

```sh
python3 run.py /path/to/bin /path/to/new-output --cpus 4-7 --scratch /path/to/scratch --lock /tmp/vsdb-profile.lock --default-dag
```

Use `--final-edits` for VerMap and omit both workload flags for the mixed
placement trace. Confirm the result with the corresponding summarizer command.
