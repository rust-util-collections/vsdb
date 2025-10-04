use criterion::{Criterion, criterion_group};
use rand::Rng;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use vsdb_core::MapxRaw;

fn read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::basic::mapx_raw::MapxRaw **");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    let i = AtomicUsize::new(0);
    let mut db = MapxRaw::new();
    group.bench_function(" write ", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::SeqCst);
            let val = n.to_be_bytes();
            db.insert(&val, &val);
        })
    });

    group.bench_function(" read ", |b| {
        b.iter(|| {
            let n = i.fetch_sub(1, Ordering::SeqCst);
            db.get(&n.to_be_bytes());
        })
    });
    group.finish();
}

fn random_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::basic::mapx_raw::MapxRaw **");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    let mut rng = rand::thread_rng();
    let mut db = MapxRaw::new();
    let mut keys = vec![];
    group.bench_function(" random write ", |b| {
        b.iter(|| {
            let n = rng.random::<u64>() as usize;
            let key = n.to_be_bytes();
            db.insert(&key, &key);
            keys.push(key);
        })
    });

    group.bench_function(" random read ", |b| {
        b.iter(|| {
            let index: usize = rng.random_range(0..keys.len());
            keys.get(index).map(|key| db.get(key));
        })
    });
    group.finish();
}

criterion_group!(benches, read_write, random_read_write);
