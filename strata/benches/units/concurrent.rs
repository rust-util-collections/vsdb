use criterion::{Criterion, criterion_group};
use std::sync::{Arc, Barrier};
use std::time::Instant;
use vsdb::basic::mapx::Mapx;

#[inline(always)]
fn xorshift64(s: &mut u64) -> u64 {
    let mut x = *s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *s = x;
    x
}

fn concurrent_independent_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::concurrent::independent_writes **");
    group.sample_size(10);

    // A single shared map, with per-thread disjoint keyspaces.
    // We use `shadow()` to obtain per-thread mutable handles while
    // upholding the "no concurrent writes to the same key" requirement.
    let db: Mapx<[u8; 8], Vec<u8>> = Mapx::new();

    let thread_counts = [2, 4, 8, 16];

    for &num_threads in &thread_counts {
        group.bench_function(format!("{} threads", num_threads), |b| {
            b.iter_custom(|iters| {
                // Each thread does `iters` operations.
                // Total work = num_threads * iters; criterion divides elapsed by iters,
                // so reported time = wall-clock for one "round" of all threads doing 1 op.
                // Perfect scaling: reported time stays constant as threads increase.
                let barrier = Arc::new(Barrier::new(num_threads as usize + 1));
                let mut handles = vec![];

                for tid in 0..num_threads {
                    let mut db_shadow = unsafe { db.shadow() };
                    let bar = barrier.clone();
                    handles.push(std::thread::spawn(move || {
                        let value = vec![0u8; 64];
                        bar.wait();

                        // Disjoint key ranges by thread id.
                        for j in 0..iters {
                            let k = ((tid as u64) << 48) | (j as u64);
                            db_shadow.insert(&k.to_be_bytes(), &value);
                        }

                        bar.wait();
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

fn concurrent_hotspot_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::concurrent::hotspot_writes **");
    group.sample_size(10);

    let thread_counts = [2, 4, 8, 16];

    for &num_threads in &thread_counts {
        group.bench_function(format!("{} threads", num_threads), |b| {
            b.iter_custom(|iters| {
                let shared_db: Mapx<[u8; 8], Vec<u8>> = Mapx::new();
                let barrier = Arc::new(Barrier::new(num_threads as usize + 1));
                let mut handles = vec![];

                for i in 0..num_threads {
                    let mut db = shared_db.clone();
                    let bar = barrier.clone();
                    handles.push(std::thread::spawn(move || {
                        let value = vec![0u8; 64];
                        bar.wait();
                        for j in 0..iters {
                            let k = ((i as u64) << 48) | (j as u64);
                            db.insert(&k.to_be_bytes(), &value);
                        }
                        bar.wait();
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

fn concurrent_shadow_hotspot_writes(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::concurrent::shadow_hotspot_writes **");
    group.sample_size(10);

    let thread_counts = [2, 4, 8, 16];

    for &num_threads in &thread_counts {
        group.bench_function(format!("{} threads", num_threads), |b| {
            b.iter_custom(|iters| {
                let db: Mapx<[u8; 8], Vec<u8>> = Mapx::new();
                let barrier = Arc::new(Barrier::new(num_threads as usize + 1));
                let mut handles = vec![];

                for i in 0..num_threads {
                    let mut db_shadow = unsafe { db.shadow() };
                    let bar = barrier.clone();
                    handles.push(std::thread::spawn(move || {
                        let value = vec![0u8; 64];
                        bar.wait();
                        for j in 0..iters {
                            let k = ((i as u64) << 48) | (j as u64);
                            db_shadow.insert(&k.to_be_bytes(), &value);
                        }
                        bar.wait();
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

    let mut db: Mapx<[u8; 8], Vec<u8>> = Mapx::new();
    let num_items: u64 = 100_000;

    // Build a larger steady-state dataset once (outside timing).
    let value = vec![0u8; 64];
    for i in 0..num_items {
        db.insert(&i.to_be_bytes(), &value);
    }

    let thread_counts = [2, 4, 8, 16];

    for &num_threads in &thread_counts {
        group.bench_function(format!("{} threads", num_threads), |b| {
            b.iter_custom(|iters| {
                let barrier = Arc::new(Barrier::new(num_threads as usize + 1));
                let mut handles = vec![];

                for tid in 0..num_threads {
                    let db_shadow = unsafe { db.shadow() };
                    let bar = barrier.clone();
                    handles.push(std::thread::spawn(move || {
                        let mut rng = 0x9E3779B97F4A7C15_u64 ^ (tid as u64);
                        bar.wait();
                        for _ in 0..iters {
                            let k = xorshift64(&mut rng) % num_items;
                            let _ = db_shadow.get(&k.to_be_bytes());
                        }
                        bar.wait();
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

fn concurrent_mixed_read_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("** vsdb::concurrent::mixed_read_write **");
    group.sample_size(10);

    let mut db: Mapx<[u8; 8], Vec<u8>> = Mapx::new();
    let num_items: u64 = 100_000;

    let value = vec![0u8; 64];
    for i in 0..num_items {
        db.insert(&i.to_be_bytes(), &value);
    }

    let configs: &[(usize, usize)] = &[(1, 1), (2, 2), (4, 4), (8, 8)];

    for &(num_readers, num_writers) in configs {
        let total = num_readers + num_writers;
        group.bench_function(
            format!("{} readers + {} writers", num_readers, num_writers),
            |b| {
                b.iter_custom(|iters| {
                    let barrier = Arc::new(Barrier::new(total + 1));
                    let mut handles = vec![];

                    for tid in 0..num_readers {
                        let db_shadow = unsafe { db.shadow() };
                        let bar = barrier.clone();
                        handles.push(std::thread::spawn(move || {
                            let mut rng = 0xD1B54A32D192ED03_u64 ^ (tid as u64);
                            bar.wait();
                            for _ in 0..iters {
                                let k = xorshift64(&mut rng) % num_items;
                                let _ = db_shadow.get(&k.to_be_bytes());
                            }
                            bar.wait();
                        }));
                    }

                    for wid in 0..num_writers {
                        let mut db_shadow = unsafe { db.shadow() };
                        let bar = barrier.clone();
                        handles.push(std::thread::spawn(move || {
                            let value = vec![0u8; 64];
                            bar.wait();
                            for j in 0..iters {
                                let k = ((wid as u64 + num_readers as u64) << 48)
                                    | (j % (num_items * 4));
                                db_shadow.insert(&k.to_be_bytes(), &value);
                            }
                            bar.wait();
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
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    concurrent_independent_writes,
    concurrent_hotspot_writes,
    concurrent_shadow_hotspot_writes,
    concurrent_reads,
    concurrent_mixed_read_write,
);
