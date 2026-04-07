use criterion::{Criterion, black_box, criterion_group};
use rand::{Rng, RngExt};
use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use vsdb::{ValueEnDe, basic::mapx_ord::MapxOrd};

fn read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("vsdb::mapx_ord / sequential");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    let i = AtomicUsize::new(0);
    let mut db = MapxOrd::new();

    group.bench_function(" write ", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::SeqCst);
            let key = <usize as ValueEnDe>::encode(&n);
            db.insert(&key, &n);
        })
    });

    group.bench_function(" read ", |b| {
        b.iter(|| {
            let n = i.fetch_sub(1, Ordering::SeqCst);
            let key = <usize as ValueEnDe>::encode(&n);
            black_box(db.get(&key));
        })
    });

    // Pre-populate for remove
    let base = i.load(Ordering::SeqCst);
    for n in base..(base + 5000) {
        let key = <usize as ValueEnDe>::encode(&n);
        db.insert(&key, &n);
    }
    i.store(base + 5000, Ordering::SeqCst);

    group.bench_function(" remove ", |b| {
        let rm = AtomicUsize::new(i.load(Ordering::SeqCst));
        b.iter(|| {
            let n = rm.fetch_sub(1, Ordering::SeqCst);
            let key = <usize as ValueEnDe>::encode(&n);
            db.remove(&key);
        })
    });

    group.finish();
}

fn random_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("vsdb::mapx_ord / random");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    let mut rng = rand::rng();
    let mut db = MapxOrd::new();
    let mut keys = vec![];
    group.bench_function(" random write ", |b| {
        b.iter(|| {
            let n = rng.random::<u64>() as usize;
            let key = <usize as ValueEnDe>::encode(&n);
            db.insert(&key, &n);
            keys.push(key);
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
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    // Pre-populate with 10000 sparse keys (0, 3, 6, 9, ...)
    let mut db = MapxOrd::new();
    for n in 0..10_000usize {
        let key_val = n * 3;
        let key = <usize as ValueEnDe>::encode(&key_val);
        db.insert(&key, &key_val);
    }

    group.bench_function(" get_le ", |b| {
        let mut i = 1usize;
        b.iter(|| {
            // Query keys that fall between sparse entries
            i = (i + 7) % 30_000;
            let key = <usize as ValueEnDe>::encode(&i);
            black_box(db.get_le(&key));
        })
    });

    group.bench_function(" get_ge ", |b| {
        let mut i = 1usize;
        b.iter(|| {
            i = (i + 7) % 30_000;
            let key = <usize as ValueEnDe>::encode(&i);
            black_box(db.get_ge(&key));
        })
    });

    group.bench_function(" range [1000, 2000) (1k keys) ", |b| {
        let lo = <usize as ValueEnDe>::encode(&3000usize); // key_val 3000 => index 1000
        let hi = <usize as ValueEnDe>::encode(&6000usize); // key_val 6000 => index 2000
        b.iter(|| {
            let count = db
                .range((Bound::Included(lo.clone()), Bound::Excluded(hi.clone())))
                .count();
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
