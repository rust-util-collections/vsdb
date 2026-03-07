use criterion::{Criterion, criterion_group};
use rand::Rng;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use vsdb::{ValueEnDe, basic::mapx::Mapx};

fn read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::basic::mapx::Mapx **");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    let i = AtomicUsize::new(0);
    let mut db = Mapx::new();

    group.bench_function(" write ", |b| {
        b.iter(|| {
            let n = i.fetch_add(1, Ordering::SeqCst);
            db.set_value(&[n; 2], &vec![n; 128]);
        })
    });

    group.bench_function(" read ", |b| {
        b.iter(|| {
            let n = i.fetch_sub(1, Ordering::SeqCst);
            db.get(&[n; 2]);
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

    group.finish();
}

fn random_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::basic::mapx::Mapx **");
    group
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(10);

    let mut rng = rand::thread_rng();
    let mut db = Mapx::new();
    let mut keys = vec![];
    group.bench_function(" random write ", |b| {
        b.iter(|| {
            let n = rng.random::<u64>() as usize;
            let key = [n; 2];
            db.set_value(&key, &vec![n; 128]);
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
