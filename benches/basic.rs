use criterion::{criterion_group, criterion_main, Criterion};
use std::{
    sync::atomic::{AtomicU8, AtomicUsize, Ordering},
    time::Duration,
};

fn bench(c: &mut Criterion) {
    let i = AtomicUsize::new(0);
    let mut db = vsdb::Vecx::new();

    let mut group = c.benchmark_group("** VsDB Benchmark **");
    group
        .measurement_time(Duration::from_secs(8))
        .sample_size(12);

    group.bench_function("  Vecx write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.push(vec![n; 128]);
        })
    });

    group.bench_function("  Vecx read_write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.push(vec![n; 128]);
            db.get(n);
        })
    });

    let i = AtomicUsize::new(0);
    let mut db = vsdb::Mapx::new();

    group.bench_function("  Mapx write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.set_value([n; 2], vec![n; 128]);
        })
    });

    group.bench_function("  Mapx read_write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.set_value([n; 2], vec![n; 128]);
            db.get(&[n; 2]);
        })
    });

    let i = AtomicU8::new(0);
    let mut db = vsdb::MapxRawVersioned::new();
    db.version_create(b"benchmark").unwrap();

    group.bench_function("  VERSIONED Mapx write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.insert_ref(&[n; 2], &[n; 128]).unwrap();
        })
    });

    group.bench_function("  VERSIONED Mapx read_write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.insert_ref(&[n; 2], &[n; 128]).unwrap();
            db.get(&[n; 2]);
        })
    });

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
