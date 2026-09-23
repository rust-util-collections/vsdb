mod units;

// Custom main (instead of `criterion_main!`): the legacy dynamic
// budget must be exported before the first engine touch.
fn main() {
    units::legacy_budget::apply();
    // Isolate from $HOME/.vsdb before the first engine touch.
    let dir = format!("/tmp/vsdb_bench_basic_{}", rand::random::<u128>());
    vsdb::vsdb_set_base_dir(&dir).unwrap();
    units::basic_mapx::benches();
    units::basic_mapx_ord::benches();
    units::batch_vs_normal::benches();
    units::concurrent::benches();
    criterion::Criterion::default()
        .configure_from_args()
        .final_summary();
}
