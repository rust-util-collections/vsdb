//! Measures SST-backed random point reads with a bounded engine-level cache pool.
//! See `docs/storage.md#memory-sizing` for v18 cache sizing (`B / 8` per pool
//! store). At `mem_budget_mb = 128`, each shared pool store has 16 MiB capacity
//! versus 1 MiB per shard under an equal 16-shard private split:
//!
//! - **skew**: one hot map = one hot shard (`prefix % shards` routing
//!   puts a collection entirely inside one shard). Working set ~10.8 MiB
//!   fits inside the 16 MiB shared pool store while exceeding a 1 MiB
//!   private per-shard split.
//! - **uniform**: 16 maps spread across shards (~0.68 MiB per map):
//!   per-shard working set stays below the 1 MiB private split so both
//!   configurations cache the working set and any delta reflects pool
//!   overhead/contention.
//!
//! A/B protocol (two builds, criterion baselines):
//!   1. replace `.with_block_cache(Some(Arc::clone(pool)))` in `mmdb_open` with
//!      `.with_block_cache(None).with_block_cache_capacity(sizing.pool_store_capacity() / shards as u64)`
//!      (equal private split) and add `let _ = pool;` to its body so the
//!      unused parameter still builds under `deny(warnings)`, then run
//!      `cargo bench -p vsdb_core --bench cache_pool -- --save-baseline private`
//!   2. restore `.with_block_cache(Some(Arc::clone(pool)))`, then run
//!      `cargo bench -p vsdb_core --bench cache_pool -- --baseline private`

#[path = "../tests/common/scratch.rs"]
mod scratch;

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
    // host's RAM.
    let dir = scratch::database_dir();
    vsdb_core::vsdb_configure(vsdb_core::VsdbOptions::new(&dir).with_mem_budget_mb(128))
        .unwrap();

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
