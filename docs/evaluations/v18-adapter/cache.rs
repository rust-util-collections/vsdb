//! Supplemental pressure trial with an actual populated shared store and reads.
use super::{
    BATCH, Calls, Instant, MapxRaw, Namespace, ROWS, SHARDS, Workload, corpus, mix, report_shape,
    rss_kib, run_writer, worker_maps,
};
use std::{
    sync::{Arc, Barrier, OnceLock, mpsc},
    thread,
    time::Duration,
};
use vsdb_core::{MapxRawReader, NamespaceMemoryUsage};

const WARM_ROWS: usize = 32_768;

#[derive(Clone, Copy)]
struct Gauges {
    segmented: u64,
    overflow: u64,
    pins: u64,
    active: usize,
    immutable: usize,
    generations: usize,
}

impl Gauges {
    fn sample(g: &NamespaceMemoryUsage) -> Self {
        Self {
            segmented: g.pool.segmented_weighted_bytes,
            overflow: g.pool.overflow_weighted_bytes,
            pins: g.shards.iter().map(|s| s.l0_pin_bytes).sum(),
            active: g.shards.iter().map(|s| s.active_memtable_bytes).sum(),
            immutable: g.shards.iter().map(|s| s.immutable_memtable_bytes).sum(),
            generations: g.shards.iter().map(|s| s.immutable_memtable_count).sum(),
        }
    }
    fn merge(&mut self, other: Self) {
        self.segmented = self.segmented.max(other.segmented);
        self.overflow = self.overflow.max(other.overflow);
        self.pins = self.pins.max(other.pins);
        self.active = self.active.max(other.active);
        self.immutable = self.immutable.max(other.immutable);
        self.generations = self.generations.max(other.generations);
    }
}

fn members(ns: &Namespace, stage: &str) {
    let usage = ns.memory_usage();
    let rows: Vec<_> = usage
        .shards
        .iter()
        .map(|s| {
            format!(
                "{}:{}:{}:{}",
                s.active_memtable_bytes,
                s.immutable_memtable_count,
                s.immutable_memtable_bytes,
                s.l0_pin_bytes
            )
        })
        .collect();
    print!(" {stage}_members={}", rows.join(","));
}

fn read_pressure(readers: &[MapxRawReader], w: &Workload, start: Instant) -> (u64, usize) {
    let mut latency = Vec::with_capacity(w.foreground_ops);
    for i in 0..w.foreground_ops {
        let lane = i % SHARDS;
        // Bulk writers overwrite 0..ROWS. The higher rows remain a known
        // independent SST read-set throughout the same measured arrival trace.
        let key = ROWS + mix(i as u64 ^ w.seed) as usize % (WARM_ROWS - ROWS);
        let wanted = w.values[(key ^ lane) % ROWS][0];
        let at = start + Duration::from_nanos(i as u64 * 1_000_000_000 / w.foreground_rate as u64);
        if let Some(d) = at.checked_duration_since(Instant::now()) {
            thread::sleep(d);
        }
        let observed = readers[lane]
            .try_get_with((key as u64).to_be_bytes(), |bytes| {
                Ok((bytes.len(), bytes[0]))
            })
            .unwrap()
            .unwrap();
        assert_eq!(observed, (512, wanted));
        latency.push(Instant::now().duration_since(at).as_nanos() as u64);
    }
    latency.sort_unstable();
    (
        latency[(99 * latency.len()).div_ceil(100) - 1],
        latency.len(),
    )
}

pub(super) fn run(ns: &Namespace, args: &[String]) {
    let w = Workload {
        seed: args[0].parse().unwrap(),
        foreground_rate: args[1].parse().unwrap(),
        foreground_ops: 10_000,
        capacity_ms: 0,
        values: corpus(args[0].parse().unwrap()),
    };
    let foreground = MapxRaw::new_in(ns);
    let mut maps = worker_maps(&foreground, ns, false);
    maps.push(foreground);
    for (lane, map) in maps.iter_mut().enumerate() {
        let rows = if lane == SHARDS { ROWS } else { WARM_ROWS };
        for base in (0..rows).step_by(128) {
            let mut batch = map.batch();
            for key in base..base + 128 {
                batch.insert((key as u64).to_be_bytes(), &w.values[(key ^ lane) % ROWS]);
            }
            batch.commit().unwrap();
        }
    }
    ns.flush();
    let readers: Vec<_> = maps[..SHARDS].iter().map(MapxRaw::reader).collect();
    for _ in 0..2 {
        for (lane, reader) in readers.iter().enumerate() {
            for key in (0..WARM_ROWS).step_by(8) {
                let v = reader
                    .try_get_with((key as u64).to_be_bytes(), |b| Ok(b[0]))
                    .unwrap()
                    .unwrap();
                assert_eq!(v, w.values[(key ^ lane) % ROWS][0]);
            }
        }
        if ns.memory_usage().pool.segmented_weighted_bytes >= 128 << 20 {
            break;
        }
    }
    let initial = Gauges::sample(&ns.memory_usage());
    assert!(
        initial.segmented >= 128 << 20,
        "insufficient shared-store warmup, observed {}",
        initial.segmented
    );
    print!(
        "sample seed={} rate={} ops={} bulk_entries={} shards={} hot=false warm_rows_each={} warm_current_kib={} warm_peak_kib={}",
        w.seed,
        w.foreground_rate,
        w.foreground_ops,
        BATCH,
        SHARDS,
        WARM_ROWS,
        rss_kib("VmRSS:"),
        rss_kib("VmHWM:")
    );
    report_shape(ns, "before");
    members(ns, "before");
    let gate = Arc::new(Barrier::new(SHARDS + 3));
    let start = OnceLock::<Instant>::new();
    let wait = || {
        gate.wait();
        let start = *start.wait();
        if let Some(d) = start.checked_duration_since(Instant::now()) {
            thread::sleep(d);
        }
        start
    };
    let (mut writers, reads, peak, min_segmented, observations) = thread::scope(|scope| {
        let contexts: Vec<_> = maps
            .into_iter()
            .enumerate()
            .map(|(lane, map)| {
                let wait = &wait;
                let w = &w;
                scope.spawn(move || run_writer(map, lane, w, wait()))
            })
            .collect();
        let read = scope.spawn(|| read_pressure(&readers, &w, wait()));
        let (stop, done) = mpsc::channel();
        let samples = scope.spawn(move || {
            let mut peak = initial;
            let mut min_segmented = initial.segmented;
            let mut observations = 0;
            loop {
                let end = !matches!(
                    done.recv_timeout(Duration::from_millis(50)),
                    Err(mpsc::RecvTimeoutError::Timeout)
                );
                let current = Gauges::sample(&ns.memory_usage());
                peak.merge(current);
                min_segmented = min_segmented.min(current.segmented);
                observations += 1;
                if end {
                    break;
                }
            }
            (peak, min_segmented, observations)
        });
        gate.wait();
        start
            .set(Instant::now() + Duration::from_millis(10))
            .unwrap();
        let writers = contexts
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<Calls>>();
        let reads = read.join().unwrap();
        stop.send(()).unwrap();
        let (peak, min_segmented, count) = samples.join().unwrap();
        (writers, reads, peak, min_segmented, count)
    });
    let mut small = writers.pop().unwrap();
    small.latencies_ns.sort_unstable();
    print!(
        " small={} bulk={} reader={} small_p99_ns={} reader_p99_ns={} peak_kib={} small_half_debt={} small_end_debt={} bulk_half_debt={} bulk_end_debt={} sampled_segmented={} min_segmented={} sampled_overflow={} sampled_pins={} sampled_active={} sampled_immutable={} sampled_generations={} observations={}",
        small.completed,
        writers.iter().map(|r| r.completed).sum::<usize>(),
        reads.1,
        small.latencies_ns[9899],
        reads.0,
        rss_kib("VmHWM:"),
        small.half_debt,
        small.end_debt,
        writers.iter().map(|r| r.half_debt).sum::<usize>(),
        writers.iter().map(|r| r.end_debt).sum::<usize>(),
        peak.segmented,
        min_segmented,
        peak.overflow,
        peak.pins,
        peak.active,
        peak.immutable,
        peak.generations,
        observations
    );
    report_shape(ns, "after");
    members(ns, "after");
    println!();
}
