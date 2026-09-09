use criterion::{Criterion, criterion_group};
use rand::RngExt;
use std::{
    hint::black_box,
    sync::atomic::{AtomicUsize, Ordering},
};
use vsdb_core::MapxRaw;

const READ_ENTRIES: usize = 5_000;

fn read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("vsdb_core::mapx_raw / sequential");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    let i = AtomicUsize::new(0);
    let mut db = MapxRaw::new();
    group.bench_function(" write ", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::SeqCst);
            let val = n.to_be_bytes();
            db.insert(val, val);
        })
    });

    group.bench_function(" read ", |b| {
        let mut read_db = MapxRaw::new();
        for n in 0..READ_ENTRIES {
            read_db.insert(n.to_be_bytes(), n.to_be_bytes());
        }
        let mut read_n = 0usize;
        b.iter(|| {
            let n = read_n;
            read_n = (read_n + 1) % READ_ENTRIES;
            black_box(read_db.get(n.to_be_bytes()));
        })
    });
    group.finish();
}

fn random_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("vsdb_core::mapx_raw / random");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    let mut rng = rand::rng();
    let mut db = MapxRaw::new();
    group.bench_function(" random write ", |b| {
        b.iter(|| {
            let n = rng.random::<u64>() as usize;
            let key = n.to_be_bytes();
            db.insert(key, key);
        })
    });

    group.bench_function(" random read ", |b| {
        let mut read_db = MapxRaw::new();
        let keys: Vec<_> = (0..READ_ENTRIES)
            .map(|_| {
                let key = (rng.random::<u64>() as usize).to_be_bytes();
                read_db.insert(key, key);
                key
            })
            .collect();
        b.iter(|| {
            let index = rng.random_range(0..keys.len());
            black_box(read_db.get(keys[index]));
        })
    });
    group.finish();
}

criterion_group!(benches, read_write, random_read_write);
