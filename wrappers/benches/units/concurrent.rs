use criterion::{Criterion, criterion_group};
use std::sync::{Arc, Barrier};
use std::time::Instant;
use vsdb::basic::mapx::Mapx;

fn concurrent_independent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::concurrent::independent_writes **");
    group.sample_size(10);

    let thread_counts = [2, 4, 8, 16];

    for &num_threads in &thread_counts {
        group.bench_function(format!("{} threads", num_threads), |b| {
            b.iter_custom(|iters| {
                let iters_per_thread = (iters / num_threads) + 1;
                let barrier = Arc::new(Barrier::new(num_threads as usize + 1));
                let mut handles = vec![];

                for _ in 0..num_threads {
                    let b = barrier.clone();
                    handles.push(std::thread::spawn(move || {
                        let mut db = Mapx::new();
                        b.wait(); // 等待主线程发令
                        for j in 0..iters_per_thread {
                            let k = j as u64;
                            db.insert(&k.to_be_bytes(), &vec![0u8; 64]);
                        }
                        b.wait(); // 同步结束
                    }));
                }

                barrier.wait(); // 全部子线程就绪
                let start = Instant::now();
                barrier.wait(); // 全部子线程完成
                let elapsed = start.elapsed();

                for h in handles {
                    h.join().unwrap();
                }

                elapsed
            })
        });
    }

    group.finish();
}

fn concurrent_hotspot_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::concurrent::hotspot_writes **");
    group.sample_size(10);

    let thread_counts = [2, 4, 8, 16];

    for &num_threads in &thread_counts {
        group.bench_function(format!("{} threads", num_threads), |b| {
            b.iter_custom(|iters| {
                let iters_per_thread = (iters / num_threads) + 1;
                let shared_db = Mapx::new();
                let barrier = Arc::new(Barrier::new(num_threads as usize + 1));
                let mut handles = vec![];

                for i in 0..num_threads {
                    let mut db = shared_db.clone();
                    let b = barrier.clone();
                    handles.push(std::thread::spawn(move || {
                        b.wait();
                        for j in 0..iters_per_thread {
                            let k = (i * iters_per_thread + j) as u64;
                            db.insert(&k.to_be_bytes(), &vec![0u8; 64]);
                        }
                        b.wait();
                    }));
                }

                barrier.wait();
                let start = Instant::now();
                barrier.wait();
                let elapsed = start.elapsed();

                for h in handles {
                    h.join().unwrap();
                }

                elapsed
            })
        });
    }

    group.finish();
}

fn concurrent_reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::concurrent::reads **");
    group.sample_size(10);

    let mut db = Mapx::new();
    let num_items = 10_000;
    for i in 0..num_items {
        let k = i as u64;
        db.insert(&k.to_be_bytes(), &vec![0u8; 64]);
    }

    let thread_counts = [2, 4, 8, 16];

    for &num_threads in &thread_counts {
        group.bench_function(format!("{} threads", num_threads), |b| {
            b.iter_custom(|iters| {
                let iters_per_thread = (iters / num_threads) + 1;
                let barrier = Arc::new(Barrier::new(num_threads as usize + 1));
                let mut handles = vec![];

                for _ in 0..num_threads {
                    let db_clone = db.clone();
                    let b = barrier.clone();
                    handles.push(std::thread::spawn(move || {
                        b.wait();
                        for j in 0..iters_per_thread {
                            let k = (j % num_items) as u64;
                            let _ = db_clone.get(&k.to_be_bytes());
                        }
                        b.wait();
                    }));
                }

                barrier.wait();
                let start = Instant::now();
                barrier.wait();
                let elapsed = start.elapsed();

                for h in handles {
                    h.join().unwrap();
                }

                elapsed
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    concurrent_independent_writes,
    concurrent_hotspot_writes,
    concurrent_reads
);
