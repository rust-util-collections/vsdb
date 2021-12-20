use criterion::{criterion_group, criterion_main, Criterion};
use std::{
    sync::atomic::{AtomicU8, AtomicUsize, Ordering},
    time::Duration,
};
use vsdb::{versioned, Mapx, Vecx, VersionName};

fn bench(c: &mut Criterion) {
    let i = AtomicUsize::new(0);
    let mut db = Vecx::new();

    let mut group = c.benchmark_group("** VSDB **");
    group
        .measurement_time(Duration::from_secs(8))
        .sample_size(12);

    group.bench_function(" Vecx write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.push(vec![n; 128]);
        })
    });

    group.bench_function(" Vecx read_write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.push(vec![n; 128]);
            db.get(n);
        })
    });

    let i = AtomicUsize::new(0);
    let mut db = Mapx::new();

    group.bench_function(" Mapx write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.set_value([n; 2], vec![n; 128]);
        })
    });

    group.bench_function(" Mapx read_write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.set_value([n; 2], vec![n; 128]);
            db.get(&[n; 2]);
        })
    });

    let i = AtomicU8::new(0);
    let mut db = versioned::mapx_raw::MapxRawVersioned::new();

    group.bench_function(" VERSIONED Mapx write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.version_create(VersionName(&n.to_be_bytes())).unwrap();
            db.insert(&[n; 2], &[n; 128]).unwrap();
        })
    });

    group.bench_function(" VERSIONED Mapx read_write", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            db.version_create(VersionName(&n.to_be_bytes())).unwrap();
            db.insert(&[n; 2], &[n; 128]).unwrap();
            db.get(&[n; 2]);
        })
    });

    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
