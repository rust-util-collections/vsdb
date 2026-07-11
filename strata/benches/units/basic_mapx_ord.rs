use criterion::{Criterion, criterion_group};
use rand::RngExt;
use std::{
    hint::black_box,
    ops::Bound,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use vsdb::basic::mapx_ord::MapxOrd;

fn read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("vsdb::mapx_ord / sequential");
    group
        .measurement_time(Duration::from_secs(3))
        .sample_size(10);

    let i = AtomicUsize::new(0);
    let mut db: MapxOrd<usize, usize> = MapxOrd::new();

    group.bench_function(" write ", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::SeqCst);
            db.insert(&n, &n);
        })
    });

    group.bench_function(" read ", |b| {
        b.iter(|| {
            let n = i.fetch_sub(1, Ordering::SeqCst);
            black_box(db.get(&n));
        })
    });

    // Pre-populate for remove
    let base = i.load(Ordering::SeqCst);
    for n in base..(base + 5000) {
        db.insert(&n, &n);
    }
    i.store(base + 5000, Ordering::SeqCst);

    group.bench_function(" remove ", |b| {
        let rm = AtomicUsize::new(i.load(Ordering::SeqCst));
        b.iter(|| {
            let n = rm.fetch_sub(1, Ordering::SeqCst);
            db.remove(&n);
        })
    });

    group.finish();
}

fn random_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("vsdb::mapx_ord / random");
    group
        .measurement_time(Duration::from_secs(3))
        .sample_size(10);

    let mut rng = rand::rng();
    let mut db: MapxOrd<usize, usize> = MapxOrd::new();
    let mut keys = vec![];
    group.bench_function(" random write ", |b| {
        b.iter(|| {
            let n = rng.random::<u64>() as usize;
            db.insert(&n, &n);
            keys.push(n);
        })
    });

    group.bench_function(" random read ", |b| {
        b.iter(|| {
            let index: usize = rng.random_range(0..keys.len());
            black_box(keys.get(index).map(|key| db.get(key)));
        })
    });

    group.finish();
}

fn ordered_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("vsdb::mapx_ord / ordered");
    group
        .measurement_time(Duration::from_secs(3))
        .sample_size(10);

    // Pre-populate with 10000 sparse keys (0, 3, 6, 9, ...)
    let mut db: MapxOrd<usize, usize> = MapxOrd::new();
    for n in 0..10_000usize {
        let key_val = n * 3;
        db.insert(&key_val, &key_val);
    }

    group.bench_function(" get_le ", |b| {
        let mut i = 1usize;
        b.iter(|| {
            // Query keys that fall between sparse entries
            i = (i + 7) % 30_000;
            black_box(db.get_le(&i));
        })
    });

    group.bench_function(" get_ge ", |b| {
        let mut i = 1usize;
        b.iter(|| {
            i = (i + 7) % 30_000;
            black_box(db.get_ge(&i));
        })
    });

    group.bench_function(" range [1000, 2000) (1k keys) ", |b| {
        let lo = 3000usize; // key_val 3000 => index 1000
        let hi = 6000usize; // key_val 6000 => index 2000
        assert_eq!(
            db.range((Bound::Included(lo), Bound::Excluded(hi))).count(),
            1000
        );
        b.iter(|| {
            let count = db.range((Bound::Included(lo), Bound::Excluded(hi))).count();
            black_box(count);
        })
    });

    group.bench_function(" iter full (10k keys) ", |b| {
        b.iter(|| {
            let count = db.iter().count();
            black_box(count);
        })
    });

    group.finish();
}

criterion_group!(benches, read_write, random_read_write, ordered_ops);
