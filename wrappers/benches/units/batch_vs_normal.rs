use criterion::{Criterion, criterion_group};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use vsdb::basic::mapx::Mapx;

fn batch_vs_normal_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** Batch vs Normal Write (100 items) **");
    group
        .measurement_time(Duration::from_secs(10))
        .sample_size(50);

    let i = AtomicUsize::new(0);
    let mut db = Mapx::new();

    // Case 1: Normal write 100 items sequentially
    group.bench_function(" normal write 100 ", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let n = i.fetch_add(1, Ordering::Relaxed);
                db.insert(&n, &n);
            }
        })
    });

    // Case 2: Batch write 100 items (entry struct)
    group.bench_function(" batch entry 100 ", |b| {
        b.iter(|| {
            let mut batch = db.batch_entry();
            for _ in 0..100 {
                let n = i.fetch_add(1, Ordering::Relaxed);
                batch.insert(&n, &n);
            }
            batch.commit().unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, batch_vs_normal_write);
