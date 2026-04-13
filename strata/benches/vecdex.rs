#![allow(warnings)]

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::random;
use std::hint::black_box;
use vsdb::vecdex::{HnswConfig, VecDex, distance::L2};

const DIM: usize = 128;

fn random_vec() -> Vec<f32> {
    (0..DIM).map(|_| random::<f32>()).collect()
}

fn build_index(n: u64) -> VecDex<u64, L2> {
    let cfg = HnswConfig {
        dim: DIM,
        m: 16,
        m_max0: 32,
        ef_construction: 100,
        ef_search: 50,
    };
    let mut idx = VecDex::new(cfg);
    for i in 0..n {
        idx.insert(&i, &random_vec()).unwrap();
    }
    idx
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("vecdex/insert");
    group
        .measurement_time(std::time::Duration::from_secs(5))
        .sample_size(10);

    for &n in &[1_000u64, 5_000, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_with_setup(
                || {
                    let cfg = HnswConfig {
                        dim: DIM,
                        m: 16,
                        m_max0: 32,
                        ef_construction: 100,
                        ef_search: 50,
                    };
                    VecDex::<u64, L2>::new(cfg)
                },
                |mut idx| {
                    for i in 0..n {
                        idx.insert(&i, &random_vec()).unwrap();
                    }
                    idx.clear();
                },
            );
        });
    }

    group.finish();
}

fn bench_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("vecdex/search");
    group
        .measurement_time(std::time::Duration::from_secs(5))
        .sample_size(20);

    for &n in &[1_000u64, 5_000, 10_000] {
        let idx = build_index(n);
        group.bench_with_input(BenchmarkId::new("k=10", n), &idx, |b, idx| {
            b.iter(|| {
                let q = random_vec();
                black_box(idx.search(&q, 10).unwrap());
            });
        });
        group.bench_with_input(BenchmarkId::new("k=10,ef=100", n), &idx, |b, idx| {
            b.iter(|| {
                let q = random_vec();
                black_box(idx.search_ef(&q, 10, 100).unwrap());
            });
        });
    }

    group.finish();
}

fn bench_filtered_search(c: &mut Criterion) {
    let mut group = c.benchmark_group("vecdex/filtered_search");
    group
        .measurement_time(std::time::Duration::from_secs(5))
        .sample_size(20);

    let idx = build_index(5_000);
    // 50% selectivity
    group.bench_function("n=5000,k=10,50%", |b| {
        b.iter(|| {
            let q = random_vec();
            black_box(
                idx.search_with_filter(&q, 10, |k: &u64| k % 2 == 0)
                    .unwrap(),
            );
        });
    });
    // 10% selectivity
    group.bench_function("n=5000,k=10,10%", |b| {
        b.iter(|| {
            let q = random_vec();
            black_box(
                idx.search_with_filter(&q, 10, |k: &u64| k % 10 == 0)
                    .unwrap(),
            );
        });
    });

    group.finish();
}

criterion_group!(benches, bench_insert, bench_search, bench_filtered_search);
criterion_main!(benches);
