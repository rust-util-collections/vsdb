mod units;

// Custom main (instead of `criterion_main!`): the data dir and legacy
// dynamic budget must be configured before the first engine touch.
fn main() {
    // Isolate from $HOME/.vsdb before the first engine touch.
    let dir = format!("/tmp/vsdb_bench_core_basic_{}", rand::random::<u128>());
    let mut opts = vsdb_core::VsdbOptions::new(&dir);
    if let Some(mb) = units::legacy_budget::budget_mb() {
        opts = opts.with_mem_budget_mb(mb);
    }
    vsdb_core::vsdb_configure(opts).unwrap();
    units::basic_mapx_raw::benches();
    units::batch_write::benches();
    criterion::Criterion::default()
        .configure_from_args()
        .final_summary();
}
