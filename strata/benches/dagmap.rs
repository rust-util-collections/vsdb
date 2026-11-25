//! Time structural operations after initializing each graph. A background
//! writer uses an unrelated prefix in the same namespace.
use criterion::{Criterion, criterion_group};
use std::{
    hint::black_box,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};
use vsdb::DagMapRaw;
use vsdb_core::MapxRaw;

fn structural(c: &mut Criterion) {
    let mut group = c.benchmark_group("dagmap");
    group
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    let running = AtomicBool::new(true);
    thread::scope(|scope| {
        scope.spawn(|| {
            let mut unrelated = MapxRaw::new();
            let mut seq = 0u64;
            while running.load(Ordering::Relaxed) {
                let k = (seq % 128).to_le_bytes();
                unrelated.insert(k, seq.to_le_bytes());
                black_box(unrelated.get(k));
                seq = seq.wrapping_add(1);
                thread::sleep(Duration::from_micros(50));
            }
            unrelated.clear();
        });
        for op in ["prune", "destroy"] {
            group.bench_function(op, |b| {
                b.iter_custom(|n| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..n {
                        let mut root = DagMapRaw::new(None);
                        root.insert(b"root", b"v");
                        let mut head = DagMapRaw::new(Some(&mut root));
                        for i in 0..8u8 {
                            let mut side = DagMapRaw::new(Some(&mut head));
                            side.insert(b"side", [i]);
                            head = DagMapRaw::new(Some(&mut head));
                            head.insert([i], [i]);
                        }
                        let start = Instant::now();
                        if op == "prune" {
                            let mut merged = head.prune().unwrap();
                            elapsed += start.elapsed();
                            black_box(merged.get([7]));
                            merged.destroy();
                        } else {
                            root.destroy();
                            elapsed += start.elapsed();
                        }
                    }
                    elapsed
                })
            });
        }
        running.store(false, Ordering::Relaxed);
    });
    group.finish();
}
criterion_group!(benches, structural);
fn main() {
    let base = std::env::var_os("VSDB_BASE_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = base.join(format!("vsdb-dagmap-bench-{}", rand::random::<u128>()));
    vsdb::vsdb_configure(vsdb::VsdbOptions::new(dir)).unwrap();
    benches();
    Criterion::default().configure_from_args().final_summary();
}
