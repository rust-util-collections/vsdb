# v18 basic collections

This comparison was recorded at `cc2f41c` against the v17.0.21 code at `da9a325`. Later implementation changes are tracked in the [v18 design record](../proposals/v18-roadmap.md#10-post-implementation-structural-performance-evaluation-and-follow-up-designs).

Criterion ran the workspace's `basic` bench on stable Rust 1.100.0 with mmdb 4.3.15, using separate data directories. The two optimized binaries ran consecutively on the same logical CPU with 3 seconds of warmup and 10 samples per case. The v17 ordered-map benchmark used a native 64-bit integer on this host. The v18 version uses an explicit `u64`.

| Operation | v17 estimate (95% interval) | v18 estimate (95% interval) |
| --- | --- | --- |
| Mapx sequential point hit | 513.76 ns (513.20–514.92) | 480.50 ns (477.88–484.03) |
| MapxOrd sequential point hit | 154.20 ns (154.08–154.47) | 133.09 ns (132.09–135.36) |
| MapxOrd scan, 1,000 keys | 77.60 µs (77.46–77.72) | 78.11 µs (77.26–80.65) |

Point hits improved in both benchmarks. The scan intervals overlap. The engine's SST range tests exercise excluded endpoints, bounded reverse seeks, snapshot visibility, and error propagation after a failed block read.

```sh
cargo bench -p vsdb --bench basic -- 'read \(hit\)|range \[1000' --noplot
```

## Allocations

A separate release-mode harness counted system allocator requests on the caller's thread, including reallocations and MMDB's own allocations. It warmed each operation 64 times, then repeated it 1,024 times. Point queries missed on a populated map. The guard test acquired and dropped an untouched `Orphan<Vec<u8>>` guard containing 65,536 bytes.

| Operation | v17 requests / requested bytes per call | v18 requests / requested bytes per call |
| --- | ---: | ---: |
| Raw eight-byte key miss | 2 / 40 | 1 / 24 |
| Ordered u64 miss | 3 / 48 | 1 / 24 |
| Mapx borrowed three-byte slice miss | 6 / 59 | 1 / 20 |
| Untouched 64 KiB value guard | 35 / 720,910 | 3 / 131,091 |

Short-key misses retain the engine's allocation. The typed guard still owns the bytes read from storage and the decoded user value.
