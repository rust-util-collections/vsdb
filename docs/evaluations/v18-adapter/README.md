# VSDB v18 adapter measurements

These Linux/x86_64 trials exercise ordinary VSDB raw maps, snapshot views and
one namespace with 16 live MMDB shards. The historical runtime is VSDB
`6ddaed9` on MMDB 4.3.15. The sizing comparison uses VSDB `b5f3153` on MMDB `255015a` with
a 6 MiB buffer and an isolated build changing only that threshold to 16 MiB. All three runs configure a 2 GiB VSDB memory
budget, a 256 MiB limit in each evictable pool store, and VSDB's normal
16 KiB SST blocks with per-level LZ4/Zstd. Both v5 builds use an immutable
limit of four. The v4 writer flushes synchronously.

## Buffer selection

VSDB selected **16 MiB per shard** for its 2 GiB, 16-shard configuration
at `6854273`, with up to four immutables per shard. With the same pool and
caller trace, reducing the threshold to 6 MiB lost 27.60% of hot-shard
saturation throughput against a 1.44% A/A band. All five pairs lost
26.46–31.52%, exceeding the 5% throughput-regression limit. Both profiles
stayed below the 1 GiB engine scenario in every sampled process. The recorded
2 GiB input, cache capacities and possible buffer generations do not enforce
a hard RSS ceiling. [Complete original packet](runs.jsonl.gz) · [Paired metrics and guard status](summary.csv)

| Placement | Buffer | Small-put p99 at matched offer, ms | Saturated MiB/s | Saturated puts/s | Saturated batches/s | Complete-mix MiB/s | Maximum RSS MiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| Hot | 16 MiB | 1.787 | 297.67 | 1,640 | 18,744 | 52.25 | 270.62 |
| Hot | 6 MiB | 2.403 | 216.65 | 1,238 | 13,643 | 39.47 | 101.85 |
| All active | 16 MiB | 2.792 | 492.86 | 12,499 | 30,956 | 398.32 | 677.50 |
| All active | 6 MiB | 8.363 | 441.85 | 9,163 | 27,765 | 292.00 | 447.88 |

Each performance column is the median of five independent v5 A/B runs.
RSS is the largest process high-water mark over both load modes and all
appearances of that binary at each placement. All matched schedules
qualified. The independent A/A spread for small-put p99 was 10.47% hot
and 1,034.63% all-active. Those latency observations alone cannot certify
a 5% guard. The saturation bulk rates in the hot arm (−27.59%, A/A 1.47%)
identify most of the drop in total bytes with the smaller buffer. The
common-mix calculation discounts surplus bulk completions.

| Saturation endpoint | L0 before → after | Immutables after | Completed flushes / compactions |
|---|---:|---:|---:|
| Hot, 16 MiB | 3 → 5 | 1 | 79 / 19 |
| Hot, 6 MiB | 7 → 6 | 1 | 154 / 32 |
| All active, 16 MiB | 16 → 27 | 15 | 111 / 25 |
| All active, 6 MiB | 16 → 40 | 35 | 276 / 64 |

The same logical preload produced different initial hot-shard file layouts.
On the all-active fixed offer, the 16 MiB arm finished with 32 L0 files,
16 flushes and zero compactions during the interval. The 6 MiB arm
finished with zero L0 files after 48 flushes and 16 compactions. Both had
zero outstanding immutables. These are per-arm median file and operation
counts. File sizes differ with the buffer setting. With all 16 shards full,
one active and four immutable 16 MiB thresholds per shard total 1,280 MiB
before caches and overshoot. The raw writes exercised a few MiB of segmented
charge and zero overflow; the populated-cache check below exercises a reader.

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

All six processes completed the 10,000 reads, 10,000 puts and 20,000
batches at the specified offers. All passed the writer backlog screen and
stayed below 1 GiB RSS. The following maxima and charge ranges come from the
three processes for each profile. Component maxima can occur at different
times. [Full process records](warm-cache.jsonl.gz) · [Per-process summary](warm-cache.csv)

| Buffer / shard | Initial segmented MiB | Lowest sampled segmented MiB | Max peak RSS MiB | Max active / immutable MiB | L0 start → end | End immutable |
|---|---:|---:|---:|---:|---:|---:|
| 6 MiB | 247.17–248.44 | 15.44 | 518.44 | 106.94 / 104.94 | 45 → 33 | 0 |
| 16 MiB | 248.20–249.46 | 248.20 | 588.08 | 241.94 / 254.94 | 32 → 48 | 0 |

Every 6 MiB trial completed 48 flushes and 15 compactions during the trace.
Its shared segmented store ended at 76–77 MiB. The 16 MiB arm completed
16 flushes and zero compactions, retaining 252–253 MiB of evictable charge.
The latter saved foreground work in this finite window while leaving more
L0 files for later compaction. Peak sampled overflow charge was zero in both.
Component and process observations describe the measured data, read mix
and write schedule. Higher occupancy in the independent overflow store,
caller-held pins, old views or application page caches changes the budget.

## Direct comparison with the selected v5 runtime

The final paired upgrade probe uses the normal VSDB runtime at `6854273`
and the original v4 executable. It reuses the two calibrated offers and
the same seeds. Each of the five rounds has a fresh v4/v4 pair, independent
from a v4/selected-v5 pair in the same placement and load. The verifier
checks the executable fingerprint and actual buffer size in the recorded
roles. The operations, qualification, nominal budget and endpoint
definitions are the same as the original matrix. Each placement completed
all of the offered work in all five independent A/A and A/B pairs. Stock
v5 peaked at 664.28 MiB RSS in this direct write-only comparison.
[All 80 processes](upgrade.jsonl.gz) · [Computed paired outcomes](upgrade.csv)

| Placement and metric | v4 median | Stock v5 median | Median paired change | Within-pair range | Max v4 A/A difference |
|---|---:|---:|---:|---:|---:|
| Hot paced put p99, ms | 44.737 | 1.824 | 95.88% lower | 26.76–96.16% lower | 12.78% |
| Hot saturated MiB/s | 183.37 | 294.33 | +59.53% | +57.35 to +76.35% | 6.90% |
| Hot saturated small puts/s | 771 | 2,108 | +172.81% | +53.92 to +553.81% | 72.52% |
| All-active paced put p99, ms | 22.704 | 6.905 | 73.64% lower | 8.33% higher to 89.41% lower | 37.53% |
| All-active saturated MiB/s | 507.29 | 499.24 | −2.26% | −38.15 to −1.39% | 14.75% |
| All-active saturated small puts/s | 13,310 | 11,575 | −31.63% | −46.44 to −10.03% | 21.77% |
| All-active saturated batches/s | 31,861 | 31,365 | −2.23% | −38.13 to −1.26% | 14.77% |
| All-active complete-mix MiB/s | 424.15 | 368.86 | −27.22% | −46.44 to −10.03% | 21.77% |

The hot-shard put p99 and total saturation throughput improved by more
than their respective A/A bands and in all five matched trials. At
all-active saturation the small writer completed fewer puts in all five
pairs. Its median regression and the loss in complete-mix rate both exceed
the observed control spread, while the median change in gross byte throughput
falls inside its wider A/A band. A/A variability also limits precision of
any 5% no-regression claim on this host. On this write mix, batching and
WAL/flush behavior can affect the work share of competing classes.

At the end of all-active saturation v5 had a median of 25 L0 files and
17 pending immutable generations, compared with 31 L0 files on v4's
synchronous flush path. V5 performed 112 flushes and 26 compactions during
the timed window against 169 and 38 on v4. Both started with 16 L0 files.
At the all-active fixed offer, both ended with 32 L0 files after 16 flushes
and zero compactions. V5 finished those paced runs with zero pending
immutables. Saturation throughput uses the end of the worker window;
background work can continue afterward.

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
git worktree add --detach /tmp/adapter-selected 6854273
python3 "$d/build.py" /tmp/adapter-selected "$work/build-selected" "$work/bin/selected" --v5
python3 "$d/upgrade.py" run "$work/runs.jsonl.gz" "$work/upgrade.jsonl.gz" --v4 "$work/bin/v4" --selected "$work/bin/selected" --buffer 16 --revision "$(git -C /tmp/adapter-selected rev-parse HEAD)" --scratch "$work/scratch" --lock "$work/timing.lock"
python3 "$d/upgrade.py" summarize "$work/runs.jsonl.gz" "$work/upgrade.jsonl.gz" --summary "$work/upgrade.csv"
```
