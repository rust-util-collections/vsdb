# VSDB v18 adapter measurements

VSDB v18 with MMDB v5 uses **16 MiB per write buffer** and up to four
immutable memtables per shard in the 2 GiB, 16-shard configuration. On
one hot shard the upgraded adapter delivered 59.53% more saturated write
throughput. With all shards busy, the small writer lost 31.63% of its
completions/s. Both figures compare the stock v18/v4 and v18/v5 adapters.
[Storage configuration](../../storage.md#memory-sizing) · [Build and source fingerprints](builds.json)

## Direct comparison with the selected v5 runtime

The control used VSDB `6ddaed9` with MMDB 4.3.15. The v5 adapter used
VSDB `6854273` with MMDB `255015a`. Both exposed one namespace with 16
shards, 16 KiB SST data blocks and per-level LZ4/Zstd. Sixteen bulk writers
each owned a map. On the hot fixture all maps shared the small writer's
shard. On the all-active fixture the bulk writers covered every shard.

Each map preloaded 4,096 deterministic 512-byte values before flush.
At the fixed offer, one writer issued 10,000 asynchronous 128-byte puts
and the 16 other writers issued 20,000 asynchronous batches in total,
each updating 32 values of 512 bytes. The offer was 2,103 small puts/s
hot and 4,928 all-active. P99 runs from scheduled arrival to acknowledgement.
At saturation each worker issued its next request upon acknowledgement
through a common four-second window. Logical throughput includes keys.
The complete-mix rate counts shares of one put and two batch completions,
discounting surplus work in either class.

Each placement and load used five independent v4/v4 (A/A) pairs and five
v4/v5 pairs in fresh Linux/x86_64 processes, with the same seed in each
pair. Changes below are medians of within-pair changes. The A/A column
gives the largest absolute within-pair variation. Every fixed-offer run
completed its trace and qualified at the halfway and final backlog checks.
[All 80 runs](upgrade.jsonl.gz) · [Paired results, ranges and eligibility](upgrade.csv)

| Placement and metric | v4 median | v5 median | Paired change | Max v4 A/A |
|---|---:|---:|---:|---:|
| Hot, paced small-put p99 | 44.737 ms | 1.824 ms | 95.88% lower | 12.78% |
| Hot, saturated MiB/s | 183.37 | 294.33 | +59.53% | 6.90% |
| Hot, saturated small puts/s | 771 | 2,108 | +172.81% | 72.52% |
| All active, paced small-put p99 | 22.704 ms | 6.905 ms | 73.64% lower | 37.53% |
| All active, saturated MiB/s | 507.29 | 499.24 | −2.26% | 14.75% |
| All active, saturated small puts/s | 13,310 | 11,575 | −31.63% | 21.77% |
| All active, saturated batches/s | 31,861 | 31,365 | −2.23% | 14.77% |
| All active, complete-mix MiB/s | 424.15 | 368.86 | −27.22% | 21.77% |

At all-active saturation, each of five v5 runs completed fewer foreground
puts than its v4 comparison. Aggregate byte throughput stayed within
control variation. Both arms started with 16 L0 files. V5 ended with a
median of 25 L0 files and 17 pending immutables. V4, which flushes
synchronously, ended with 31 L0 files. Stock v5 peak process RSS across
the direct upgrade trials was 664.28 MiB.

## Buffer selection

The 6 MiB and 16 MiB v5 profiles used the same logical trace, a depth of
four and two shared evictable stores capped at 256 MiB each. The acceptance
criteria required both placements to retain at least 95% of control
saturation throughput and at most 105% of control small-put p99 while
staying within a measured 1 GiB RSS budget. The 6 MiB profile lost
**27.60%** of hot throughput (all five paired losses, 26.46–31.52%, A/A
1.44%) and 10.10% of all-active throughput. Small-put p99 control
variation exceeded the 5% margin on both placements.
[Buffer trials and calibrations](runs.jsonl.gz) · [Paired metrics](summary.csv)

| Placement | Buffer | Paced p99, ms | Saturation, MiB/s | Peak RSS, MiB | Saturation L0 start → end | Immutables at end |
|---|---:|---:|---:|---:|---:|---:|
| Hot | 16 MiB | 1.787 | 297.67 | 270.62 | 3 → 5 | 1 |
| Hot | 6 MiB | 2.403 | 216.65 | 101.85 | 7 → 6 | 1 |
| All active | 16 MiB | 2.792 | 492.86 | 677.50 | 16 → 27 | 15 |
| All active | 6 MiB | 8.363 | 441.85 | 447.88 | 16 → 40 | 35 |

A separate three-pair check populated the shared cache by reading 32,768
rows per shard, then interleaved 10,000 reads with the same all-active
write trace. The 16 MiB arms started with 248.20–249.46 MiB of segmented
charge, peaked at **588.08 MiB RSS**, and ended with 48 L0 files from
32 after 16 flushes and zero compactions. The 6 MiB arms peaked at
518.44 MiB RSS, dropped as low as 15.44 MiB of segmented charge, and
ended with 33 L0 files from 45 after 48 flushes and 15 compactions.
All six runs met the writer backlog check and finished with zero pending
immutables. No overflow charge was observed. The larger buffer left
more compaction work after the fixed-offer window. One active plus four
full 16 MiB buffers on each of 16 shards would account for 1,280 MiB
in nominal thresholds alone. Callers' retained pins, the overflow store,
per-tree caches and application data can increase process RSS.
[Populated-cache runs](warm-cache.jsonl.gz) · [Observed charges and endpoints](warm-cache.csv)

## Allocation results

For a 4 KiB SST value, the median caller-thread allocation in bytes per
successful operation was:

| Operation | v4 | v5 |
|---|---:|---:|
| Live existence | 4,189.6 | 42.5 |
| Live immediate decode | 4,191.0 | 44.6 |
| Snapshot existence | 4,185.6 | 40.6 |
| Snapshot immediate decode | 4,185.3 | 40.6 |
| Snapshot owned hit | 4,191.6 | 4,140.5 |
| Snapshot scan, consuming each row | 4,127.6 | 7.5 |

Five fresh processes per adapter each warmed 2,048 values and checked the
same results. The v5 build at `d9f00f4` decodes from pinned bytes and
lends scan rows until the next cursor advance. Owned results still require
value storage. These numbers measure allocation requests on the calling
thread. [Allocation records](allocations.jsonl.gz) · [Counts and bytes](allocations.csv)

## Reproduce

To regenerate the four final tables and verify the recorded build
fingerprints, run these commands in this directory. Each `diff` should
produce empty output.

```sh
out=$(mktemp -d)
python3 -B summarize.py runs.jsonl.gz --builds builds.json > "$out/buffers.csv"
diff -u summary.csv "$out/buffers.csv"
python3 -B summarize.py allocations.jsonl.gz --allocations-only --builds builds.json > "$out/allocations.csv"
diff -u allocations.csv "$out/allocations.csv"
python3 -B cache_trials.py summarize warm-cache.jsonl.gz --builds builds.json --summary "$out/cache.csv"
diff -u warm-cache.csv "$out/cache.csv"
python3 -B upgrade.py summarize runs.jsonl.gz upgrade.jsonl.gz --builds builds.json --summary "$out/upgrade.csv"
diff -u upgrade.csv "$out/upgrade.csv"
```

For fresh measurements start from a full VSDB clone on Linux with Python 3,
stable Rust, `taskset`, `findmnt` and Btrfs utilities. Run from the VSDB
repository root with CPUs 4–7 and a fresh scratch directory on local Btrfs.
Keep each revision's Cargo target directory separate.

```sh
d="$PWD/docs/evaluations/v18-adapter"
work=$(mktemp -d)
mkdir "$work/scratch"
btrfs property set "$work/scratch" compression none
git worktree add --detach "$work/v4" 6ddaed9
git worktree add --detach "$work/v5" b5f3153
git worktree add --detach "$work/v5-16m" b5f3153
git -C "$work/v5-16m" apply "$d/control-16m.patch"
git worktree add --detach "$work/live" d9f00f4
git worktree add --detach "$work/selected" 6854273
build() {
    checkout=$1
    bin=$2
    shift 2
    CARGO_TARGET_DIR="$work/build-$checkout-$bin/target" python3 "$d/build.py" \
        "$work/$checkout" "$work/build-$checkout-$bin" "$work/bin/$bin" "$@"
}
build v4 v4
build v5 v5 --v5
build v5-16m v5-16m --v5
build v4 v4-alloc --allocations
build v5 v5-alloc --v5 --allocations
python3 "$d/run.py" "$work/bin" "$work/runs.jsonl.gz" --scratch "$work/scratch" --lock "$work/timing.lock" --cpus 4-7
build live v5-alloc --v5 --allocations
python3 "$d/run.py" "$work/bin" "$work/allocations.jsonl.gz" --allocations-only --scratch "$work/scratch" --lock "$work/timing.lock"
python3 "$d/cache_trials.py" run "$work/cache.jsonl.gz" --small "$work/bin/v5" --large "$work/bin/v5-16m" --rate 4928 --revision b5f3153 --scratch "$work/scratch" --lock "$work/timing.lock" --cpus 4-7
build selected selected --v5
python3 "$d/upgrade.py" run "$work/runs.jsonl.gz" "$work/upgrade.jsonl.gz" --v4 "$work/bin/v4" --selected "$work/bin/selected" --buffer 16 --revision "$(git -C "$work/selected" rev-parse HEAD)" --scratch "$work/scratch" --lock "$work/timing.lock"
```
