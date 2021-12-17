use criterion::{criterion_group, criterion_main, Criterion};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

fn bench(c: &mut Criterion) {
    let i = AtomicUsize::new(0);
    let mut db = vsdb::Vecx::new();

    let mut group = c.benchmark_group("** VsDB Benchmark **");
    group
        .measurement_time(Duration::from_secs(8))
        .sample_size(12);

    group.bench_function("vecx_write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.push(vec![n; 128]);
        })
    });

    group.bench_function("vecx_rw", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.push(vec![n; 128]);
            db.get(n);
        })
    });

    let i = AtomicUsize::new(0);
    let mut db = vsdb::Mapx::new();

    group.bench_function("mapx_write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.set_value([n; 32], vec![n; 128]);
        })
    });

    group.bench_function("mapx_rw", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.set_value([n; 32], vec![n; 128]);
            db.get(&[n; 32]);
        })
    });

    group.bench_function("mapx_rw_write_back", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.set_value([n; 32], vec![n; 128]);
            *db.get_mut(&[n; 32]).unwrap() = vec![n; 1];
        })
    });

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
