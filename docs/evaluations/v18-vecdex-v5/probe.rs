//! Standalone VecDex query driver. See the adjacent reproduction instructions.
use std::{
    env, fs,
    hint::black_box,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};
use vsdb::{
    HnswConfig, InstanceId, L2, Namespace, NamespaceOpts, VecDex, VsdbOptions, vsdb_configure,
};
use vsdb_core::MapxRaw;

const DIM: usize = 96;
const NODES: u64 = 8192;
const K: usize = 5;
type Graph = VecDex<u64, L2>;

fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

// Half the clusters have nearby vector IDs. The other half interleave their rows.
fn vector(id: u64) -> Vec<f32> {
    let cluster = if id < NODES / 2 {
        id / 16
    } else {
        NODES / 32 + (id % (NODES / 32))
    };
    (0..DIM)
        .map(|d| {
            let center = mix(31 + cluster * DIM as u64 + d as u64) as u32;
            let offset = mix(900_000 + id * DIM as u64 + d as u64) as u32;
            (center as f32 + 0.08 * offset as f32) / u32::MAX as f32
        })
        .collect()
}

fn save_id(base: &Path, id: InstanceId) {
    fs::write(
        base.join("profile-handle"),
        postcard::to_allocvec(&id).unwrap(),
    )
    .unwrap();
}

fn graph_id(base: &Path) -> InstanceId {
    postcard::from_bytes(&fs::read(base.join("profile-handle")).unwrap()).unwrap()
}

// The profiling process owns this isolated graph. Use the public wire decoder
// for its raw prefix so both versions can transfer exactly the same rows.
fn graph_rows(id: InstanceId) -> MapxRaw {
    let mut frame = b"VSMAPX02".to_vec();
    frame.extend_from_slice(&id.map_id.to_le_bytes());
    frame.extend_from_slice(&id.ns.unwrap_or_default().to_le_bytes());
    postcard::from_bytes(&postcard::to_allocvec(&frame).unwrap()).unwrap()
}

fn graph(ns: &Namespace) -> Graph {
    Graph::new_in(
        ns,
        HnswConfig {
            dim: DIM,
            m: 16,
            m_max0: 32,
            ef_construction: 64,
            ef_search: 12,
        },
    )
    .unwrap()
}

fn namespace(mib: usize) -> Namespace {
    Namespace::create_with(NamespaceOpts {
        shards: 1,
        mem_budget_mb: Some(mib),
        ..Default::default()
    })
    .unwrap()
}

fn create(base: &Path, output: &Path) {
    vsdb_configure(VsdbOptions::new(base)).unwrap();
    let ns = namespace(512);
    let mut index = graph(&ns);
    for start in (0..NODES).step_by(64) {
        let rows: Vec<_> = (start..start + 64).map(|id| (id, vector(id))).collect();
        index.insert_batch(&rows).unwrap();
    }
    save_id(base, index.save_meta().unwrap());
    let raw = graph_rows(index.instance_id());
    let mut out = BufWriter::new(
        fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(output)
            .unwrap(),
    );
    for (key, value) in raw.iter() {
        for bytes in [&key, &value] {
            out.write_all(&(bytes.len() as u32).to_le_bytes()).unwrap();
            out.write_all(bytes).unwrap();
        }
    }
    out.flush().unwrap();
    ns.flush();
}

fn import(base: &Path, input: &Path, budget: usize) {
    vsdb_configure(VsdbOptions::new(base)).unwrap();
    let ns = namespace(budget);
    let index = graph(&ns);
    let id = index.save_meta().unwrap();
    let mut raw = graph_rows(id);
    let mut input = BufReader::new(fs::File::open(input).unwrap());
    let mut count = 0;
    loop {
        let mut batch = raw.batch();
        let mut eof = false;
        for _ in 0..256 {
            let mut size = [0; 4];
            let first = input.read(&mut size[..1]).unwrap();
            if first == 0 {
                eof = true;
                break;
            }
            input.read_exact(&mut size[1..]).unwrap();
            let mut key = vec![0; u32::from_le_bytes(size) as usize];
            input.read_exact(&mut key).unwrap();
            input.read_exact(&mut size).unwrap();
            let mut value = vec![0; u32::from_le_bytes(size) as usize];
            input.read_exact(&mut value).unwrap();
            batch.insert(&key, &value);
            count += 1;
        }
        batch.commit().unwrap();
        if eof {
            break;
        }
    }
    ns.flush();
    save_id(base, id);
    assert_eq!(index.len(), NODES);
    println!("imported_rows={count}");
}

fn read_bytes() -> u64 {
    fs::read_to_string("/proc/self/io")
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix("read_bytes: "))
        .unwrap()
        .parse()
        .unwrap()
}

fn evict(base: &Path) {
    let script = "import os,sys\nfrom pathlib import Path\nfiles=list(Path(sys.argv[1]).rglob('*.sst'))\nassert files\nfor path in files:\n with path.open('rb') as f: os.posix_fadvise(f.fileno(),0,0,os.POSIX_FADV_DONTNEED)";
    assert!(
        Command::new("python3")
            .args(["-c", script])
            .arg(base)
            .status()
            .unwrap()
            .success()
    );
}

fn property(ns: &Namespace, name: &str) -> u64 {
    let rows = ns.shard_properties(name);
    assert_eq!(rows.len(), 1);
    rows[0].as_ref().unwrap().parse().unwrap()
}

fn run(base: &Path, cold: bool, seed: u64, samples: usize) {
    vsdb_configure(VsdbOptions::read_only(base)).unwrap();
    let index = Graph::from_meta(graph_id(base)).unwrap();
    let ns = index.namespace();
    let queries: Vec<_> = (0..samples as u64)
        .map(|n| {
            let id = mix(0x2611_2026 + n + seed * 100_000) % NODES;
            (id, vector(id))
        })
        .collect();
    if !cold {
        let raw = graph_rows(index.instance_id());
        // Point lookups populate the normal engine cache, including the graph/link rows.
        for (key, _) in raw.iter() {
            black_box(raw.get(key).unwrap());
        }
    }
    let counters = || {
        (
            property(&ns, "stats.block_cache_hits") + property(&ns, "stats.block_cache_misses"),
            property(&ns, "stats.bytes_read"),
        )
    };
    let (before_blocks, before_logical) = counters();
    let mut before_io = read_bytes();
    let mut total_io = 0;
    let mut io_count = 0;
    let mut latencies = Vec::with_capacity(samples);
    let mut true_nearest = 0;
    let mut hash = 0u64;
    for (n, (id, q)) in queries.iter().enumerate() {
        if cold && n % 32 == 0 {
            evict(base);
            before_io = read_bytes();
        }
        let start = Instant::now();
        let hits = black_box(index.search(q, K).unwrap());
        latencies.push(start.elapsed().as_nanos() as u64);
        assert_eq!(hits.len(), K);
        true_nearest += usize::from(hits[0].0 == *id && hits[0].1 == 0.0);
        for (key, score) in hits {
            hash = hash.rotate_left(7).wrapping_mul(37) ^ key ^ u64::from(score.to_bits());
        }
        if cold {
            let after = read_bytes();
            let delta = after - before_io;
            total_io += delta;
            io_count += usize::from(delta >= 4096);
            before_io = after;
        }
    }
    if !cold {
        total_io = read_bytes() - before_io;
    }
    let (end_blocks, end_logical) = counters();
    latencies.sort_unstable();
    println!(
        "samples={samples} p50_ns={} p99_ns={} digest={hash:016x} nearest={true_nearest} blocks={} logical_bytes={} storage_bytes={total_io} io_positive={io_count}",
        latencies[(samples * 50).div_ceil(100) - 1],
        latencies[(samples * 99).div_ceil(100) - 1],
        end_blocks - before_blocks,
        end_logical - before_logical
    );
}

fn main() {
    let args: Vec<_> = env::args().collect();
    let base = PathBuf::from(&args[2]);
    match args[1].as_str() {
        "create" => create(&base, Path::new(&args[3])),
        "import" => import(&base, Path::new(&args[3]), args[4].parse().unwrap()),
        "run" => run(
            &base,
            match args[3].as_str() {
                "device" => true,
                "warm" => false,
                _ => panic!("unknown regime"),
            },
            args[4].parse().unwrap(),
            args[5].parse().unwrap(),
        ),
        other => panic!("unknown operation {other}"),
    }
}
