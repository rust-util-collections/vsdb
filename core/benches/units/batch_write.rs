use criterion::{Criterion, black_box, criterion_group};
use std::time::Duration;
use vsdb_core::MapxRaw;

// Benchmark single insert operations
fn single_inserts(c: &mut Criterion) {
    let mut group = c.benchmark_group("** RocksDB Write Performance **");
    group
        .measurement_time(Duration::from_secs(10))
        .sample_size(50);

    group.bench_function(" single inserts (1000 ops) ", |b| {
        b.iter(|| {
            let mut db = MapxRaw::new();
            for i in 0usize..1000 {
                let key = i.to_be_bytes();
                let value = (i * 2).to_be_bytes();
                db.insert(&key, &value);
            }
            black_box(db);
        })
    });

    group.finish();
}

// Benchmark mixed read/write workload
fn mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("** RocksDB Mixed Workload **");
    group
        .measurement_time(Duration::from_secs(10))
        .sample_size(50);

    group.bench_function(" 80% read / 20% write (1000 ops) ", |b| {
        b.iter(|| {
            let mut db = MapxRaw::new();
            // Pre-populate
            for i in 0usize..800 {
                let key = i.to_be_bytes();
                let value = i.to_be_bytes();
                db.insert(&key, &value);
            }

            // Mixed workload
            for i in 0usize..1000 {
                if i % 5 == 0 {
                    // 20% writes
                    let key = (800 + i / 5).to_be_bytes();
                    let value = i.to_be_bytes();
                    db.insert(&key, &value);
                } else {
                    // 80% reads
                    let key = (i % 800).to_be_bytes();
                    black_box(db.get(&key));
                }
            }
            black_box(db);
        })
    });

    group.finish();
}

// Benchmark range scans
fn range_scans(c: &mut Criterion) {
    let mut group = c.benchmark_group("** RocksDB Range Scans **");
    group
        .measurement_time(Duration::from_secs(10))
        .sample_size(50);

    let mut db = MapxRaw::new();
    // Pre-populate with 10000 entries
    for i in 0u64..10000u64 {
        let key = i.to_be_bytes();
        let value = i.to_be_bytes();
        db.insert(&key, &value);
    }

    group.bench_function(" scan 100 entries ", |b| {
        b.iter(|| {
            let count = db.iter().take(100).count();
            black_box(count);
        })
    });

    group.bench_function(" scan 1000 entries ", |b| {
        b.iter(|| {
            let count = db.iter().take(1000).count();
            black_box(count);
        })
    });

    group.finish();
}

criterion_group!(benches, single_inserts, mixed_workload, range_scans);
