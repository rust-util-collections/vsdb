//! Q1 gate for the engine-level block-cache pool (shared-mem-pool RFC
//! §10 Q1): measures SST-backed random point-reads under a bounded
//! budget in the two load shapes that matter —
//!
//! - **skew**: one hot map = one hot shard (`prefix % shards` routing
//!   puts a collection entirely inside one shard). Working set ~10 MB
//!   vs a 4 MB private per-shard cache: the pool should improve this
//!   (the hot shard can use the whole engine slice).
//! - **uniform**: 16 maps spread across shards, per-shard working set
//!   below the private capacity: both configurations fully cache, so
//!   any delta is pure pool overhead/contention — the regression gate.
//!
//! A/B protocol (two builds, criterion baselines):
//!   1. set `block_cache: None` in `mmdb_open` (private split), run
//!      `cargo bench -p vsdb_core --bench cache_pool -- --save-baseline private`
//!   2. restore `block_cache: Some(pool.clone())`, run
//!      `cargo bench -p vsdb_core --bench cache_pool -- --baseline private`

use criterion::{Criterion, criterion_group, criterion_main};
use rand::RngExt;
use std::{hint::black_box, thread, time::Duration};
use vsdb_core::{MapxRaw, Namespace};

const VAL: usize = 256;
/// ~10 MB of incompressible values, all in ONE shard.
const SKEW_N: u64 = 40_000;
/// ~0.64 MB per map × 16 maps — every shard warm, none over capacity.
const UNI_PER_MAP: u64 = 2_500;
/// Point-reads per timed iteration (per thread).
const READS: usize = 8_192;

fn setup() -> (MapxRaw, Vec<MapxRaw>) {
    // Pin the budget so cache capacity is the differentiator, not the
    // host's RAM. SAFETY: executed at bench startup, before the first
    // engine touch and before any thread exists — the same contract
    // `vsdb_set_base_dir` documents.
    unsafe { std::env::set_var("VSDB_MEM_BUDGET_MB", "64") };
    let dir = format!("/tmp/vsdb_bench_cache_pool_{}", rand::rng().random::<u64>());
    vsdb_core::vsdb_set_base_dir(&dir).unwrap();

    let mut rng = rand::rng();
    let mut val = [0u8; VAL];

    let mut skew = MapxRaw::new();
    for i in 0..SKEW_N {
        rng.fill(&mut val[..]);
        skew.insert(i.to_be_bytes(), val);
    }

    let mut uni: Vec<MapxRaw> = (0..16).map(|_| MapxRaw::new()).collect();
    for m in uni.iter_mut() {
        for i in 0..UNI_PER_MAP {
            rng.fill(&mut val[..]);
            m.insert(i.to_be_bytes(), val);
        }
    }

    // Drain memtables so the timed reads are SST reads through the
    // block cache, not memtable hits.
    Namespace::default_ns().flush();
    (skew, uni)
}

fn cache_pool(c: &mut Criterion) {
    let (skew, uni) = setup();

    let mut rng = rand::rng();
    let skew_keys: Vec<[u8; 8]> = (0..READS)
        .map(|_| rng.random_range(0..SKEW_N).to_be_bytes())
        .collect();
    let uni_keys: Vec<(usize, [u8; 8])> = (0..READS)
        .map(|_| {
            (
                rng.random_range(0..uni.len()),
                rng.random_range(0..UNI_PER_MAP).to_be_bytes(),
            )
        })
        .collect();

    let mut group = c.benchmark_group("vsdb_core::cache_pool");
    group
        .measurement_time(Duration::from_secs(5))
        .sample_size(10);

    group.bench_function(format!("skew read 1t x{READS}"), |b| {
        b.iter(|| {
            for k in &skew_keys {
                black_box(skew.get(k));
            }
        })
    });

    group.bench_function(format!("skew read 8t x{READS}"), |b| {
        b.iter(|| {
            thread::scope(|s| {
                for _ in 0..8 {
                    let keys = &skew_keys;
                    let m = &skew;
                    s.spawn(move || {
                        for k in keys {
                            black_box(m.get(k));
                        }
                    });
                }
            })
        })
    });

    group.bench_function(format!("uniform read 1t x{READS}"), |b| {
        b.iter(|| {
            for (mi, k) in &uni_keys {
                black_box(uni[*mi].get(k));
            }
        })
    });

    group.bench_function(format!("uniform read 8t x{READS}"), |b| {
        b.iter(|| {
            thread::scope(|s| {
                for _ in 0..8 {
                    let keys = &uni_keys;
                    let maps = &uni;
                    s.spawn(move || {
                        for (mi, k) in keys {
                            black_box(maps[*mi].get(k));
                        }
                    });
                }
            })
        })
    });

    group.finish();
}

criterion_group!(benches, cache_pool);
criterion_main!(benches);
