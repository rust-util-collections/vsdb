# VSDB v18 adapter measurements

These Linux/x86_64 trials exercise ordinary VSDB raw maps, snapshot views and
one namespace with 16 live MMDB shards. The historical runtime is VSDB
`6ddaed9` on MMDB 4.3.15. The native adapter is VSDB `b5f3153` on MMDB
`255015a`. An isolated alternate build changes only its buffer threshold from
6 MiB to 16 MiB at this budget. All three runs configure a 2 GiB VSDB memory
budget, a 256 MiB limit in each evictable pool store, and VSDB's normal
16 KiB SST blocks with per-level LZ4/Zstd. Both v5 builds use an immutable
limit of four. The v4 writer flushes synchronously.

## Allocation results

The final live/snapshot allocation comparison uses VSDB `d9f00f4`. Each
cell is the median of five fresh-process per-call readings for a 4 KiB
value. Callers kept the same data and returned the same checksums in all
arms. [Raw runs](allocations.jsonl.gz) · [Counts and requested bytes](allocations.csv) · [Binary and source fingerprints](builds.json)

| Operation | v4 requested bytes/call | v5 requested bytes/call |
|---|---:|---:|
| Live existence | 4,189.6 | 42.5 |
| Live borrowed decode | 4,191.0 | 44.6 |
| Snapshot existence | 4,185.6 | 40.6 |
| Snapshot borrowed decode | 4,185.3 | 40.6 |
| Snapshot owned hit | 4,191.6 | 4,140.5 |
| Snapshot scan, consuming each row | 4,127.6 | 7.5 |

V5 removes the intermediate value copy from both existence checks and
both immediate decoders. Each owned hit still allocates space for the
result. The v5 scan lends the current key and value until it advances.

To verify the archived packet against the saved build fingerprints, run
`python3 summarize.py allocations.jsonl.gz --allocations-only --builds builds.json`
from this directory. A local rebuild can supply its own receipt for that check.

## Workloads

The allocation probe warms 2,048 existing 4,096-byte values, then counts
successful live and snapshot operations against the same unchanged rows. Five fresh processes per adapter
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

The saturation table reports successful scalar puts/s and successful batch
commits/s alongside total logical MiB/s. To compare the delivered work at
the same demand ratio, also count `min(scalar/s, batches/s ÷ 2)` complete
shares of one 136-byte put and two 16,640-byte batches. This discounts work
in excess of either stream's fixed-mix demand. Saturated workers still
issue independently. Foreground p99 comes from the separate fixed arrival
schedule. A gain in aggregate bytes can coincide with fewer foreground
completions. The synthetic evidence test halves the scalar rate and raises
the bulk rate to demonstrate that distinction.

The 1 GiB RSS envelope, at least 95% of control byte throughput and at most
105% of control foreground p99 must hold in both placements for a common
profile to pass. Retain a numerical outcome as inconclusive if matched
load fails or the control spread obscures the latency margin. Keep all recorded pairs in the descriptive medians and ranges. The decision
gain is empty when any scheduled-load pair fails. All observed process
high-water marks still contribute to the independent absolute RSS ceiling.
A matched control spread greater than 5% leaves the guard inconclusive.
Compare L0 files, outstanding generations, flushes and compactions alongside
throughput. Whole-process RSS includes the harness. A raw-map trial
does not instantiate the higher-level per-tree page caches.

## Populated shared-cache pressure check

The write-only matrix exercises memtables and compaction. Its observed RSS
does not include a heavily populated evictable store or reads from a
collection page cache. A separate all-active check preloads distinct
32,768-row maps on all 16 shards (512-byte values), then flushes and warms
the shared store through actual raw-map reads. Each process must begin
with at least 128 MiB of segmented charge. It retains 16 read handles and
issues 10,000 scheduled reads across those shards against known, unmodified
values while replaying the same 10,000 small and 20,000 batch writes.

The three interleaved 6 MiB/16 MiB pairs use identical traces and a
50 ms sampler for active/immutable memory, L0 pins and both evictable
stores. Raw start/end member lists contain active bytes, immutable count,
immutable bytes and pin bytes in shard order. A sample across shards and
the separate file-count observations need not be simultaneous. Report the
lowest sampled segmented charge, whole-process RSS and remaining L0 and
immutable generations. The separate 1 GiB resource check uses every process
high-water mark. This short study does not qualify a query latency gain.

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
git worktree add --detach /tmp/adapter-v5-live d9f00f4
python3 "$d/build.py" /tmp/adapter-v5-live "$work/build-v5-live-alloc" "$work/bin/v5-alloc" --v5 --allocations
python3 "$d/run.py" "$work/bin" "$work/allocations.jsonl.gz" --scratch "$work/scratch" --lock "$work/timing.lock" --allocations-only
python3 "$d/summarize.py" "$work/allocations.jsonl.gz" --allocations-only
python3 "$d/cache_trials.py" run "$work/warm-cache.jsonl.gz" --small "$work/bin/v5" --large "$work/bin/v5-16m" --rate 4928 --revision b5f3153 --scratch "$work/scratch" --lock "$work/timing.lock" --cpus 4-7
python3 "$d/cache_trials.py" summarize "$work/warm-cache.jsonl.gz" --summary "$work/warm-cache.csv"
```
