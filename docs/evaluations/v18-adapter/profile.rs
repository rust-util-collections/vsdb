//! Standalone Linux probe. Its only interface to storage is the public VSDB API.
use std::{
    env, fs,
    hint::black_box,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};
use vsdb_core::{MapxRaw, Namespace, VsdbOptions, vsdb_configure};

const SHARDS: usize = 16;
const ROWS: usize = 4096;
const BATCH: usize = 32;
const VALUE: usize = 512;
const RATIO: usize = 8;

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn corpus(seed: u64) -> Vec<[u8; VALUE]> {
    (0..ROWS)
        .map(|i| {
            let mut value = [0; VALUE];
            for (j, chunk) in value.chunks_exact_mut(8).enumerate() {
                let id = (i * VALUE / 8 + j) as u64;
                chunk.copy_from_slice(&mix(id ^ seed).to_le_bytes());
            }
            value
        })
        .collect()
}

fn rss_kib(field: &str) -> usize {
    fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix(field))
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

fn prop(ns: &Namespace, key: &str) -> usize {
    ns.shard_properties(key)
        .into_iter()
        .map(|v| v.unwrap().parse::<usize>().unwrap())
        .sum()
}

fn report_shape(ns: &Namespace, stage: &str) {
    let l0 = prop(ns, "num-files-at-level0");
    let l1 = prop(ns, "num-files-at-level1");
    let flushes = prop(ns, "stats.flushes_completed");
    let compactions = prop(ns, "stats.compactions_completed");
    print!(
        " {stage}_l0={l0} {stage}_l1={l1} {stage}_flushes={flushes} {stage}_compactions={compactions}"
    );
    #[cfg(feature = "v5")]
    {
        let g = ns.memory_usage();
        print!(
            " {stage}_active={} {stage}_immutable_bytes={} {stage}_immutable_count={} {stage}_pins={} {stage}_segmented={} {stage}_overflow={}",
            g.shards
                .iter()
                .map(|s| s.active_memtable_bytes)
                .sum::<usize>(),
            g.shards
                .iter()
                .map(|s| s.immutable_memtable_bytes)
                .sum::<usize>(),
            g.shards
                .iter()
                .map(|s| s.immutable_memtable_count)
                .sum::<usize>(),
            g.shards.iter().map(|s| s.l0_pin_bytes).sum::<u64>(),
            g.pool.segmented_weighted_bytes,
            g.pool.overflow_weighted_bytes
        );
        assert_eq!(g.pool.segmented_capacity_bytes, 256 << 20);
        assert_eq!(g.pool.overflow_capacity_bytes, 256 << 20);
        print!(" {stage}_buffer_size={}", g.write_buffer_bytes_per_shard);
    }
}

fn worker_maps(foreground: &MapxRaw, ns: &Namespace, hot: bool) -> Vec<MapxRaw> {
    if hot {
        return (0..SHARDS).map(|_| foreground.new_colocated()).collect();
    }
    let mut by_shard: Vec<Option<MapxRaw>> = (0..SHARDS).map(|_| None).collect();
    while by_shard.iter().any(Option::is_none) {
        let map = MapxRaw::new_in(ns);
        let shard = map.instance_id().map_id as usize % SHARDS;
        by_shard[shard] = Some(map);
    }
    by_shard.into_iter().map(Option::unwrap).collect()
}

#[derive(Default)]
struct Calls {
    completed: usize,
    bytes: usize,
    half_debt: usize,
    end_debt: usize,
    latencies_ns: Vec<u64>,
    end_ns: u64,
}

struct Workload {
    values: Vec<[u8; VALUE]>,
    seed: u64,
    foreground_rate: usize,
    foreground_ops: usize,
    capacity_ms: u64,
}

// All writers use one common clock. Each stream has a separate map prefix.
// A hit at the deadline counts toward bytes and its final acknowledgment.
fn run_writer(mut map: MapxRaw, lane: usize, w: &Workload, start: Instant) -> Calls {
    let foreground = lane == SHARDS;
    let period = if foreground { 1 } else { RATIO };
    let limit = w.foreground_ops / period;
    let paced = w.foreground_rate > 0;
    let mut finished = Vec::with_capacity(if paced { limit } else { 0 });
    let mut result = Calls::default();
    let scheduled = |i: usize| {
        Duration::from_nanos(
            (i as u64 * period as u64 * 1_000_000_000) / w.foreground_rate.max(1) as u64,
        )
    };
    let deadline = start + Duration::from_millis(w.capacity_ms);
    while if paced {
        result.completed < limit
    } else {
        Instant::now() < deadline
    } {
        let n = result.completed;
        let arrival = if paced {
            start + scheduled(n)
        } else {
            Instant::now()
        };
        if let Some(wait) = arrival.checked_duration_since(Instant::now()) {
            thread::sleep(wait);
        }
        let key = mix(n as u64 ^ (lane as u64 * 0x9e37) ^ w.seed) as usize % ROWS;
        if foreground {
            map.insert(
                (key as u64).to_be_bytes(),
                &w.values[(n ^ key) % ROWS][..128],
            );
            result.bytes += 8 + 128;
        } else {
            let mut batch = map.batch();
            for j in 0..BATCH {
                let k = (key + j * 127) % ROWS;
                batch.insert((k as u64).to_be_bytes(), &w.values[(n ^ k) % ROWS]);
            }
            batch.commit().unwrap();
            result.bytes += BATCH * (8 + VALUE);
        }
        let ack = Instant::now();
        result.end_ns = ack.duration_since(start).as_nanos() as u64;
        result.completed += 1;
        if paced {
            finished.push(result.end_ns);
            if foreground {
                result
                    .latencies_ns
                    .push(ack.duration_since(arrival).as_nanos() as u64);
            }
        }
    }
    if paced {
        let debt = |cut: usize| {
            cut - finished.partition_point(|&ack| ack <= scheduled(cut - 1).as_nanos() as u64)
        };
        result.half_debt = debt(limit / 2);
        result.end_debt = debt(limit);
    }
    result
}

fn memory(ns: &Namespace, args: &[String]) {
    let hot = args[0] == "hot";
    assert!(hot || args[0] == "all");
    let w = Workload {
        seed: args[1].parse().unwrap(),
        foreground_rate: args[2].parse().unwrap(),
        foreground_ops: args[3].parse().unwrap(),
        capacity_ms: args[4].parse().unwrap(),
        values: corpus(args[1].parse().unwrap()),
    };
    assert!(w.foreground_ops >= 2 * RATIO && w.foreground_ops.is_multiple_of(RATIO));
    let foreground = MapxRaw::new_in(ns);
    let mut maps = worker_maps(&foreground, ns, hot);
    maps.push(foreground);
    for (lane, map) in maps.iter_mut().enumerate() {
        for chunk in (0..ROWS).step_by(128) {
            let mut batch = map.batch();
            for n in chunk..chunk + 128 {
                batch.insert((n as u64).to_be_bytes(), &w.values[(n ^ lane) % ROWS]);
            }
            batch.commit().unwrap();
        }
    }
    ns.flush();
    print!(
        "sample seed={} rate={} ops={} bulk_entries={} shards={} hot={} memory_post_preload_kib={}",
        w.seed,
        w.foreground_rate,
        w.foreground_ops,
        BATCH,
        SHARDS,
        hot,
        rss_kib("VmRSS:")
    );
    report_shape(ns, "before");
    let gate = Arc::new(Barrier::new(SHARDS + 2));
    let slots = std::sync::OnceLock::<Instant>::new();
    let mut runs = thread::scope(|scope| {
        let contexts: Vec<_> = maps
            .into_iter()
            .enumerate()
            .map(|(lane, map)| {
                let gate = Arc::clone(&gate);
                let slots = &slots;
                let w = &w;
                scope.spawn(move || {
                    gate.wait();
                    let start = *slots.wait();
                    if let Some(sleep) = start.checked_duration_since(Instant::now()) {
                        thread::sleep(sleep);
                    }
                    run_writer(map, lane, w, start)
                })
            })
            .collect();
        gate.wait();
        slots
            .set(Instant::now() + Duration::from_millis(10))
            .unwrap();
        contexts
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    let small = runs.pop().unwrap();
    let limit = small
        .end_ns
        .max(runs.iter().map(|r| r.end_ns).max().unwrap());
    let bytes = small.bytes + runs.iter().map(|r| r.bytes).sum::<usize>();
    let mut latencies = small.latencies_ns;
    latencies.sort_unstable();
    let p99 = if latencies.is_empty() {
        0
    } else {
        latencies[(99 * latencies.len()).div_ceil(100) - 1]
    };
    let bulk = runs.iter().map(|r| r.completed).sum::<usize>();
    let recorded_peak = rss_kib("VmHWM:");
    print!(
        " small={} bulk={} logical_bytes={} wall_ns={} p99_ns={} peak_kib={} small_half_debt={} small_end_debt={} bulk_half_debt={} bulk_end_debt={}",
        small.completed,
        bulk,
        bytes,
        limit,
        p99,
        recorded_peak,
        small.half_debt,
        small.end_debt,
        runs.iter().map(|r| r.half_debt).sum::<usize>(),
        runs.iter().map(|r| r.end_debt).sum::<usize>()
    );
    report_shape(ns, "after");
    println!();
    black_box(ns);
}

#[cfg(feature = "allocations")]
mod alloc;

fn main() {
    let args: Vec<_> = env::args().skip(1).collect();
    let root = env::var_os("VSDB_BASE_DIR").unwrap();
    vsdb_configure(VsdbOptions::new(root).with_mem_budget_mb(2048)).unwrap();
    let ns = Namespace::default_ns();
    match args[0].as_str() {
        "memory" => memory(&ns, &args[1..]),
        #[cfg(feature = "allocations")]
        "allocations" => alloc::run(&ns),
        _ => panic!("unknown case"),
    }
}
