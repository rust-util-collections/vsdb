use vsdb::{Namespace, VsdbOptions, vsdb_configure};

fn main() {
    let dir = std::path::PathBuf::from(std::env::args_os().nth(1).unwrap());
    vsdb_configure(VsdbOptions::new(dir).with_mem_budget_mb(2048)).unwrap();
    let g = Namespace::default_ns().memory_usage();
    println!(
        "shards={} buffer_bytes={} immutable_limit={} segmented_capacity={} overflow_capacity={}",
        g.shards.len(),
        g.write_buffer_bytes_per_shard,
        g.immutable_limit_per_shard,
        g.pool.segmented_capacity_bytes,
        g.pool.overflow_capacity_bytes
    );
    assert_eq!(
        (
            g.shards.len(),
            g.write_buffer_bytes_per_shard,
            g.immutable_limit_per_shard
        ),
        (16, 16 << 20, 4)
    );
}
