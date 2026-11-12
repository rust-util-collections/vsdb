use criterion::{BenchmarkId, Criterion, Throughput, criterion_group};
use std::{
    hint::black_box,
    time::{Duration, Instant},
};
use vsdb::SlotDex64;

fn batch_growth(c: &mut Criterion) {
    let mut group = c.benchmark_group("slotdex/batch_growth");
    group
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3))
        .sample_size(10);

    let capacity = 1024u64;
    // The first 1025 distinct slots create a full 1024-floor top tier.
    // Touch every floor inside the same batch, then keep adding keys to one
    // hot slot without changing the number of floors or promoting again.
    let wide_then_hot: Vec<_> = std::iter::once((0, 0))
        .chain((0..capacity).map(|n| (n * capacity + 1, n + 1)))
        .chain((0..capacity).map(|n| (n * capacity + 1, capacity + 1 + n)))
        .chain((0..10_000).map(|n| (0, 2 * capacity + 1 + n)))
        .collect();
    let capacity = 16u64;
    let wide_floors: Vec<_> = (0..10_000).map(|n| (n * capacity.pow(3), n)).collect();

    for (name, capacity, items) in [
        ("wide_then_hot", 1024, wide_then_hot),
        ("wide_floors", 16, wide_floors),
    ] {
        let mut db = SlotDex64::<u64>::new(capacity, false).unwrap();
        group.throughput(Throughput::Elements(items.len() as u64));
        group.bench_function(BenchmarkId::new(name, capacity), |b| {
            b.iter_custom(|iters| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    db.insert_batch(black_box(items.iter().copied())).unwrap();
                    elapsed += start.elapsed();
                    black_box(db.total());
                    // Keep the data bounded and exclude cleanup from timing.
                    db.clear().unwrap();
                }
                elapsed
            });
        });
    }
    group.finish();
}

criterion_group!(benches, batch_growth);

#[path = "units/legacy_budget.rs"]
mod legacy_budget;

fn main() {
    let dir = format!("/tmp/vsdb_bench_slotdex_batch_{}", rand::random::<u128>());
    let mut opts = vsdb::VsdbOptions::new(&dir);
    if let Some(mb) = legacy_budget::budget_mb() {
        opts = opts.with_mem_budget_mb(mb);
    }
    vsdb::vsdb_configure(opts).unwrap();
    benches();
    Criterion::default().configure_from_args().final_summary();
}
