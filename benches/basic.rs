use criterion::{criterion_group, criterion_main, Criterion};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

fn bench(c: &mut Criterion) {
    let i = AtomicUsize::new(0);
    let mut db = vsdb::Vecx::new();

    let mut group = c.benchmark_group("** Cache DB Benchmark **");
    group
        .measurement_time(Duration::from_secs(8))
        .sample_size(12);

    group.bench_function("vecx_write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.push(vec![n; 512]);
        })
    });

    group.bench_function("vecx_rw", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.push(vec![n; 512]);
            db.get(n);
        })
    });

    let i = AtomicUsize::new(0);
    let mut db = vsdb::MapxOC::new();

    group.bench_function("mapx_write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.set_value(n, vec![n; 512]);
        })
    });

    group.bench_function("mapx_rw", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.set_value(n, vec![n; 512]);
            db.get(&n);
        })
    });

    group.bench_function("mapx_mut_back", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.set_value(n, vec![n; 512]);
            db.get_mut(&n);
        })
    });

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
