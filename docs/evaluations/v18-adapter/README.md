# VSDB v18 adapter measurements

These Linux/x86_64 trials exercise ordinary VSDB raw maps, snapshot views and
one namespace with 16 live MMDB shards. The historical runtime is VSDB
`6ddaed9` on MMDB 4.3.15. The native adapter is VSDB `b5f3153` on MMDB
`255015a`. An isolated alternate build changes only its buffer threshold from
6 MiB to 16 MiB at this budget. All three runs configure a 2 GiB VSDB memory
budget, a 256 MiB limit in each evictable pool store, and VSDB's normal
16 KiB SST blocks with per-level LZ4/Zstd. Both v5 builds use an immutable
limit of four. The v4 writer flushes synchronously.

## Workloads

The allocation probe warms 2,048 existing 4,096-byte values, then counts
successful operations on that same snapshot. Five fresh processes per adapter
report caller-thread allocation requests and requested bytes. The counted
work includes lookup and result consumption. The scan includes cursor
construction. Existence returns a boolean, decode consumes one byte and the
scan checks the key length and a byte of each value. The retained-value
control requests an owned copy in both adapters. The memory binaries use
the ordinary system allocator without the counting wrapper.

The memory probe opens a fresh universe per process. Sixteen writers each
own a separate map and a seventeenth map serves a foreground scalar writer.
On the hot fixture all 17 maps have distinct prefixes on the same shard.
On the all-active fixture the bulk writers cover the 16 distinct shards
exactly once. Each map preloads 4,096 rows with deterministic 512-byte
values and then the namespace is flushed. Scalar timed operations write
128-byte payloads. Each bulk operation commits 32 values of 512 bytes in
one WAL-enabled asynchronous batch. Byte throughput includes each
eight-byte user key. The commands record RSS and the LSM's starting and
ending state before the process closes.

For peak throughput, each worker issues another operation immediately after
its acknowledgment until a shared four-second deadline. The denominator
includes operations in flight at the deadline. For tail latency, schedule
10,000 scalar arrivals and 1,250 arrivals per bulk writer, counting elapsed
time from each scheduled arrival through acknowledgment. The initial rate
estimate uses saturated byte throughput. Verify the fixed mix at descending
rates with all three builds, then use 70% of a jointly sustainable rate.
A qualified half/end cut allows the greater of two, the contributing stream
count, or 1% of the full per-class schedule to remain outstanding. Five
seeds run independent control/control, control/candidate and v4/v5 pairs
in alternating order. Paired effects use the median of the within-seed
ratios. Check variation using the maximum absolute change between the two
16 MiB v5 control runs.

The 1 GiB RSS envelope, at least 95% of control byte throughput and at most
105% of control foreground p99 must hold in both placements for a common
profile to pass. Retain a numerical outcome as inconclusive if matched
load fails or the control spread obscures the latency margin. Compare L0
files, outstanding generations, flushes and compactions alongside
throughput. Whole-process RSS includes the harness. A raw-map trial
does not instantiate the higher-level per-tree page caches.

## Reproduce

Use a full VSDB clone on Linux and separate worktrees. The scripts require
Python 3, a stable Rust toolchain, `taskset`, `findmnt`, and Btrfs utilities.
Select a fresh local scratch directory with adequate capacity.

```sh
git worktree add --detach /tmp/adapter-v4 6ddaed9
git worktree add --detach /tmp/adapter-v5 b5f3153
git worktree add --detach /tmp/adapter-v5-16m b5f3153
git -C /tmp/adapter-v5-16m apply "$PWD/docs/evaluations/v18-adapter/control-16m.patch"

d=docs/evaluations/v18-adapter
work=$(mktemp -d)
mkdir "$work/scratch"
btrfs property set "$work/scratch" compression none
export CARGO_TARGET_DIR="$work/target"
python3 "$d/build.py" /tmp/adapter-v5 "$work/build-v5" "$work/bin/v5" --v5
python3 "$d/build.py" /tmp/adapter-v5-16m "$work/build-v5-16m" "$work/bin/v5-16m" --v5
python3 "$d/build.py" /tmp/adapter-v4 "$work/build-v4" "$work/bin/v4"
python3 "$d/build.py" /tmp/adapter-v4 "$work/build-v4-alloc" "$work/bin/v4-alloc" --allocations
python3 "$d/build.py" /tmp/adapter-v5 "$work/build-v5-alloc" "$work/bin/v5-alloc" --v5 --allocations
python3 "$d/run.py" "$work/bin" "$work/runs.jsonl.gz" --scratch "$work/scratch" --lock "$work/timing.lock" --cpus 4-7
python3 "$d/summarize.py" "$work/runs.jsonl.gz" > "$work/summary.csv"
```
