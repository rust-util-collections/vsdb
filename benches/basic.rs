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

    group.bench_function("vecx_write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.push(&vec![n; 128]);
        })
    });

    group.bench_function("vecx_rw", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.push(&vec![n; 128]);
            db.get(n);
        })
    });

    let i = AtomicUsize::new(0);
    let mut db = vsdb::Mapx::new();

    group.bench_function("mapx_write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.set_value([n; 2], &vec![n; 128]);
        })
    });

    group.bench_function("mapx_rw", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.set_value([n; 2], &vec![n; 128]);
            db.get(&[n; 2]);
        })
    });

    group.bench_function("mapx_rw_write_back", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.set_value([n; 2], &vec![n; 128]);
            *db.get_mut(&[n; 2]).unwrap() = vec![n; 1];
        })
    });

    let i = AtomicU8::new(0);
    let mut db = vsdb::MapxRawVersioned::new();
    db.version_create(b"benchmark").unwrap();

    group.bench_function("versioned_mapx_write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.insert(vec![n; 2], vec![n; 128]).unwrap();
        })
    });

    group.bench_function("versioned_mapx_rw", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.insert(vec![n; 2], vec![n; 128]).unwrap();
            db.get(&[n; 2]);
        })
    });

    group.bench_function("versioned_mapx_rw_write_back", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.insert(vec![n; 2], vec![n; 128]).unwrap();
            *db.get_mut(&[n; 2]).unwrap() = vec![n; 1];
        })
    });

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
