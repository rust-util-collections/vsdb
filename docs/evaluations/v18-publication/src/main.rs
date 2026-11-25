//! The same executable source builds against the v4 and v5 adapters.
use std::{
    env, fs,
    hint::black_box,
    thread,
    time::{Duration, Instant},
};
use vsdb::{DagMapRaw, MapxRaw, Namespace, VerMap, VsdbOptions, vsdb_configure};
use vsdb_core::NamespaceOpts;

fn mix(mut n: u64) -> u64 {
    n = (n ^ (n >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    n = (n ^ (n >> 27)).wrapping_mul(0x94d049bb133111eb);
    n ^ (n >> 31)
}

#[cfg(feature = "counters")]
fn submissions() -> u64 {
    mmdb::take_thread_write_profile().requests
}
#[cfg(not(feature = "counters"))]
fn submissions() -> u64 {
    0
}

fn perc(samples: &[u64], pct: usize) -> u64 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    sorted[(sorted.len() * pct).div_ceil(100) - 1]
}
fn rss_kib() -> u64 {
    fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}

fn publication(kind: &str, n: u64, seed: u64) {
    let ns = if kind == "dag16" {
        Namespace::default_ns()
    } else {
        Namespace::create_with(NamespaceOpts {
            shards: 1,
            ..Default::default()
        })
        .unwrap()
    };
    let mut dag = DagMapRaw::new_in(&ns, None);
    let mut m = VerMap::<u64, u64>::new_in(&ns);
    let branch = m.main_branch();
    let mut expected = [0u64; 128];
    m.write_batch(branch, (0..128).map(|k| (k, Some(0))))
        .unwrap();
    ns.flush();
    let mut times = Vec::with_capacity(n as usize);
    let mut last = None;
    submissions();
    let start = Instant::now();
    for i in 0..n {
        let a = (mix(i ^ seed) % 64) as usize;
        let b = a + 64;
        let clock = Instant::now();
        match kind {
            "dag" | "dag16" => last = Some(black_box(DagMapRaw::new(Some(&mut dag)))),
            "individual" => {
                for key in [a, b, a, b] {
                    let k = key as u64;
                    let old = m.get(branch, &k).unwrap().unwrap();
                    m.insert(branch, &k, &(old + 1)).unwrap();
                }
            }
            "batch" => {
                let edits = [
                    (a as u64, Some(expected[a] + 2)),
                    (b as u64, Some(expected[b] + 2)),
                ];
                m.write_batch(branch, edits).unwrap();
            }
            #[cfg(feature = "working-edit")]
            "overlay" => {
                m.edit_working(branch, 512, |edit| {
                    for key in [a, b, a, b] {
                        let k = key as u64;
                        let old = edit.get(&k)?.unwrap();
                        edit.insert(&k, &(old + 1))?;
                    }
                    Ok(())
                })
                .unwrap();
            }
            _ => panic!("unknown profile {kind}"),
        }
        times.push(clock.elapsed().as_nanos() as u64);
        expected[a] += 2;
        expected[b] += 2;
    }
    let elapsed = start.elapsed();
    if kind == "dag16" {
        let last_id = last.as_ref().unwrap().instance_id().map_id;
        print!("prefix_advance={} ", last_id - dag.instance_id().map_id);
    }
    let issued = submissions();
    if matches!(kind, "dag" | "dag16") {
        assert!(dag.child_id(last.as_ref().unwrap()).is_some());
        assert_eq!(dag.child_ids().len(), n as usize);
    } else {
        for (key, value) in expected.iter().enumerate() {
            assert_eq!(m.get(branch, &(key as u64)).unwrap(), Some(*value));
        }
    }
    println!(
        "mode={kind} ops={n} seed={seed} p99_ns={} p50_ns={} elapsed_ns={} submissions={issued} counted={} rss_kib={}",
        perc(&times, 99),
        perc(&times, 50),
        elapsed.as_nanos(),
        cfg!(feature = "counters"),
        rss_kib()
    );
}

fn debt(ns: &Namespace) -> u64 {
    ns.shard_properties("num-files-at-level0")
        .iter()
        .map(|n| n.as_deref().unwrap().parse::<u64>().unwrap())
        .sum()
}

fn mixed(n: usize, rate: u64, seed: u64, split: bool) {
    assert!(n > 0 && (1..=1_000_000_000).contains(&rate));
    let ns = Namespace::create_with(NamespaceOpts {
        shards: 2,
        ..Default::default()
    })
    .unwrap();
    let mut vm = VerMap::<u64, Vec<u8>>::new_in(&ns);
    let branch = vm.main_branch();
    let mut small = loop {
        let m = MapxRaw::new_in(&ns);
        if (m.instance_id().map_id % 2 != vm.instance_id().map_id % 2) == split {
            break m;
        }
    };
    let payload: Vec<_> = (0..4096)
        .map(|i| {
            (0..32)
                .flat_map(|j| mix(seed ^ (i * 32 + j)).to_le_bytes())
                .collect::<Vec<_>>()
        })
        .collect();
    vm.write_batch(
        branch,
        (0..4096).map(|k| (k, Some(payload[k as usize].clone()))),
    )
    .unwrap();
    let mut preload = small.batch();
    for i in 0..4096u64 {
        preload.insert(i.to_be_bytes(), &payload[i as usize][..128]);
    }
    preload.commit().unwrap();
    ns.flush();
    let before = debt(&ns);
    let period = Duration::from_nanos(1_000_000_000 / rate);
    let start = Instant::now() + Duration::from_millis(50);
    let batches = n.div_ceil(128);
    let (mut ack, mut service) = (Vec::with_capacity(n), Vec::with_capacity(n));
    let payload_ref = &payload;
    let (bulk_times, bulk_after) = thread::scope(|s| {
        let bulk = s.spawn(move || {
            let mut times = Vec::new();
            let mut check_key = 0;
            let mut check_index = 0;
            for j in 0..batches {
                let arrival = start + period * (128 * j) as u32;
                thread::sleep(arrival.saturating_duration_since(Instant::now()));
                let offset = mix(seed ^ j as u64) as usize;
                vm.write_batch(
                    branch,
                    (0..1024usize).map(|k| {
                        let slot = offset.wrapping_add(k) & 4095;
                        let v = slot.wrapping_add(j + 1) & 4095;
                        check_key = slot as u64;
                        check_index = v;
                        (check_key, Some(payload_ref[v].clone()))
                    }),
                )
                .unwrap();
                times.push(Instant::now());
            }
            let done = *times.last().unwrap();
            assert_eq!(
                vm.get(branch, &check_key).unwrap().unwrap(),
                payload_ref[check_index]
            );
            (times, done)
        });
        for j in 0..n {
            let arrival = start + period * j as u32;
            thread::sleep(arrival.saturating_duration_since(Instant::now()));
            let id = mix(seed ^ j as u64) as usize & 4095;
            let clock = Instant::now();
            small.insert((id as u64).to_be_bytes(), &payload[id][128..]);
            let done = Instant::now();
            ack.push(done);
            service.push((done - clock).as_nanos() as u64);
        }
        bulk.join().unwrap()
    });
    let end = bulk_after.max(*ack.last().unwrap());
    let backlog = |i: usize| {
        let cut = start + period * (i - 1) as u32;
        let scalar = i - ack.partition_point(|v| *v <= cut);
        let bulk = (i - 1) / 128 + 1 - bulk_times.partition_point(|v| *v <= cut);
        (scalar, bulk)
    };
    let (half, bulk_half) = backlog(n / 2);
    let (remaining, bulk_end) = backlog(n);
    let arrival: Vec<_> = ack
        .iter()
        .enumerate()
        .map(|(j, t)| (*t - (start + period * j as u32)).as_nanos() as u64)
        .collect();
    let after = debt(&ns);
    ns.try_sync_wal().unwrap();
    println!(
        "mode=mixed split={split} ops={n} seed={seed} rate={rate} p99_ns={} service_ns={} bulk_batches={batches} bulk_rows={} elapsed_ns={} pending_half={half} pending_end={remaining} bulk_half={bulk_half} bulk_end={bulk_end} before_l0={before} after_l0={after} rss_kib={}",
        perc(&arrival, 99),
        perc(&service, 99),
        batches * 1024,
        (end - start).as_nanos(),
        rss_kib()
    );
}

fn main() {
    let args: Vec<_> = env::args().collect();
    vsdb_configure(VsdbOptions::new(
        env::var_os("VSDB_BASE_DIR").expect("set an isolated VSDB_BASE_DIR"),
    ))
    .unwrap();
    let n = args[2].parse().unwrap();
    let seed = args[3].parse().unwrap();
    match args[1].as_str() {
        "mixed-hot" | "mixed-split" => mixed(
            n as usize,
            args[4].parse().unwrap(),
            seed,
            args[1] == "mixed-split",
        ),
        kind => publication(kind, n, seed),
    }
}
