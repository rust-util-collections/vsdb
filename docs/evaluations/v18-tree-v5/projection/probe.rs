//! Release-mode client for the standalone, immutable-root B+ tree experiment.
use std::{
    env,
    hint::black_box,
    ops::Bound::{Excluded, Included},
    path::PathBuf,
    time::Instant,
};
use vsdb::{PersistentBTree, VsdbOptions, vsdb_configure};

const ROWS: u64 = 32_768;
const WIDTH: u64 = 128;
const POINTS: usize = 250_000;
const SCANS: usize = 10_000;

fn value(n: u64) -> [u8; 32] {
    let mut v = [n as u8; 32];
    v[..8].copy_from_slice(&n.to_be_bytes());
    v
}

fn next(rng: &mut u64) -> u64 {
    *rng = rng.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = *rng;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

fn report(class: &str, mut nanos: Vec<u64>, start: Instant) {
    let wall_ns = start.elapsed().as_nanos();
    nanos.sort_unstable();
    let p99 = nanos[nanos.len() * 99 / 100 - 1];
    println!("{class} {} {p99} {wall_ns}", nanos.len());
}

fn main() {
    let args: Vec<_> = env::args().collect();
    let mut seed: u64 = args[2].parse().unwrap();
    vsdb_configure(VsdbOptions::new(PathBuf::from(&args[1])).with_mem_budget_mb(2048)).unwrap();
    let mut tree = PersistentBTree::new();
    let root =
        tree.bulk_load((0..ROWS).map(|n| ((2 * n).to_be_bytes().to_vec(), value(n).to_vec())));
    tree.namespace().flush();
    // Warm the same immutable root and the namespace block pool in all arms.
    for (n, (key, got)) in tree.iter(root).enumerate() {
        let n = n as u64;
        assert_eq!(key.as_slice(), (2 * n).to_be_bytes());
        assert_eq!(got.as_slice(), value(n));
    }
    let extended = args.get(3).is_some_and(|v| v == "extended");
    let samples = if extended { 1_000_000 } else { POINTS };
    for (class, hit, contains) in [
        ("hit", true, false),
        ("miss", false, false),
        ("contains-hit", true, true),
        ("contains-miss", false, true),
    ] {
        let mut nanos = Vec::with_capacity(samples);
        let start = Instant::now();
        for _ in 0..samples {
            let n = next(&mut seed) % ROWS;
            let key = (2 * n + u64::from(!hit)).to_be_bytes();
            let begin = Instant::now();
            if contains {
                let found = black_box(tree.contains_key(root, &key));
                nanos.push(begin.elapsed().as_nanos() as u64);
                assert_eq!(found, hit);
            } else {
                let got = black_box(tree.get(root, &key));
                nanos.push(begin.elapsed().as_nanos() as u64);
                if hit {
                    assert_eq!(got.unwrap().as_slice(), value(n));
                } else {
                    assert!(got.is_none());
                }
            }
        }
        report(class, nanos, start);
    }
    if extended {
        return;
    }
    let mut nanos = Vec::with_capacity(SCANS);
    let start = Instant::now();
    for _ in 0..SCANS {
        let first = next(&mut seed) % (ROWS - WIDTH + 1);
        let low = (2 * first).to_be_bytes();
        let high = (2 * (first + WIDTH)).to_be_bytes();
        let begin = Instant::now();
        let mut n = first;
        for (key, got) in tree.range(root, Included(&low), Excluded(&high)) {
            assert_eq!(key.as_slice(), (2 * n).to_be_bytes());
            assert_eq!(got.as_slice(), value(n));
            n += 1;
        }
        nanos.push(begin.elapsed().as_nanos() as u64);
        assert_eq!(n, first + WIDTH);
    }
    report("scan", nanos, start);
}
