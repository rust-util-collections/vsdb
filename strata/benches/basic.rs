mod units;

// Custom main (instead of `criterion_main!`): the data dir and legacy
// dynamic budget must be configured before the first engine touch.
fn main() {
    // Isolate from $HOME/.vsdb before the first engine touch.
    let dir = format!("/tmp/vsdb_bench_basic_{}", rand::random::<u128>());
    let mut opts = vsdb::VsdbOptions::new(&dir);
    if let Some(mb) = units::legacy_budget::budget_mb() {
        opts = opts.with_mem_budget_mb(mb);
    }
    vsdb::vsdb_configure(opts).unwrap();
    units::basic_mapx::benches();
    units::basic_mapx_ord::benches();
    units::batch_vs_normal::benches();
    units::concurrent::benches();
    criterion::Criterion::default()
        .configure_from_args()
        .final_summary();
}
