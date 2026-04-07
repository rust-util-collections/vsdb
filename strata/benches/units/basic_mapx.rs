use criterion::{Criterion, black_box, criterion_group};
use rand::{Rng, RngExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use vsdb::{ValueEnDe, basic::mapx::Mapx};

fn read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("vsdb::mapx / sequential");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    let i = AtomicUsize::new(0);
    let mut db = Mapx::new();

    group.bench_function(" write ", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::SeqCst);
            db.insert(&[n; 2], &vec![n; 128]);
        })
    });

    group.bench_function(" read ", |b| {
        b.iter(|| {
            let n = i.fetch_sub(1, Ordering::SeqCst);
            black_box(db.get(&[n; 2]));
        })
    });

    group.bench_function(" batch write (100 items) ", |b| {
        b.iter(|| {
            let mut batch = db.batch_entry();
            for _ in 0..100 {
                let n = i.fetch_add(1, Ordering::SeqCst);
                batch.insert(&[n; 2], &vec![n; 128]);
            }
            batch.commit().unwrap();
        })
    });

    // Pre-populate for contains_key / remove / iter
    let base = i.load(Ordering::SeqCst);
    for n in base..(base + 5000) {
        db.insert(&[n; 2], &vec![n; 128]);
    }
    i.store(base + 5000, Ordering::SeqCst);

    group.bench_function(" contains_key ", |b| {
        let max = i.load(Ordering::SeqCst);
        let mut k = 0usize;
        b.iter(|| {
            k = (k + 1) % max;
            black_box(db.contains_key(&[k; 2]));
        })
    });

    group.bench_function(" iter (5k entries) ", |b| {
        b.iter(|| {
            let count = db.iter().count();
            black_box(count);
        })
    });

    group.bench_function(" remove ", |b| {
        let rm = AtomicUsize::new(i.load(Ordering::SeqCst));
        b.iter(|| {
            let n = rm.fetch_sub(1, Ordering::SeqCst);
            db.remove(&[n; 2]);
        })
    });

    group.finish();
}

fn random_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("vsdb::mapx / random");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    let mut rng = rand::rng();
    let mut db = Mapx::new();
    let mut keys = vec![];
    group.bench_function(" random write ", |b| {
        b.iter(|| {
            let n = rng.random::<u64>() as usize;
            let key = [n; 2];
            db.insert(&key, &vec![n; 128]);
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

criterion_group!(benches, read_write, random_read_write);
