// Temporary verification-only test (deleted before session end).
// Measures wall-clock cost of Namespace::create_with(shards=64) and
// whether vsdb_ns_list()/destroy()/relocate() are blocked for that
// entire duration by a concurrent create/open on an unrelated namespace.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use vsdb_core::common::namespace::{Namespace, NamespaceOpts, vsdb_ns_list};

#[test]
fn lock_timing_probe() {
    let base = std::env::var("VSDB_BASE_DIR").expect("set VSDB_BASE_DIR");
    println!("base dir = {base}");

    // Baseline: vsdb_ns_list() cost with nothing concurrent.
    let t0 = Instant::now();
    let _ = vsdb_ns_list().unwrap();
    println!("baseline vsdb_ns_list(): {:?}", t0.elapsed());

    // Time a single 64-shard create from cold (fresh id, fresh dirs).
    let t1 = Instant::now();
    let _ns64 = Namespace::create_with(NamespaceOpts {
        shards: 64,
        ..Default::default()
    })
    .unwrap();
    let create64_dur = t1.elapsed();
    println!("create_with(shards=64) cold: {:?}", create64_dur);

    // Now: start a second 64-shard create on a background thread, and
    // from the main thread hammer vsdb_ns_list() concurrently, recording
    // the max single-call latency observed while the create is running.
    let done = Arc::new(AtomicBool::new(false));
    let done2 = done.clone();
    let creator = std::thread::spawn(move || {
        let t = Instant::now();
        let ns = Namespace::create_with(NamespaceOpts {
            shards: 64,
            ..Default::default()
        })
        .unwrap();
        done2.store(true, Ordering::SeqCst);
        (ns, t.elapsed())
    });

    // Give the creator a head start so it's inside open_record_locked.
    std::thread::sleep(Duration::from_micros(50));

    let mut max_list_latency = Duration::ZERO;
    let mut samples = 0u32;
    while !done.load(Ordering::SeqCst) {
        let t = Instant::now();
        let _ = vsdb_ns_list().unwrap();
        let d = t.elapsed();
        if d > max_list_latency {
            max_list_latency = d;
        }
        samples += 1;
        if samples > 200_000 {
            break;
        }
    }
    let (_ns, create_dur2) = creator.join().unwrap();
    println!(
        "concurrent create_with(shards=64): {:?}, max vsdb_ns_list() latency observed during it: {:?} ({} samples)",
        create_dur2, max_list_latency, samples
    );
}
