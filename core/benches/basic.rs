mod units;

// Custom main (instead of `criterion_main!`): the legacy dynamic
// budget must be exported before the first engine touch.
fn main() {
    units::legacy_budget::apply();
    // Isolate from $HOME/.vsdb before the first engine touch.
    let dir = format!("/tmp/vsdb_bench_core_basic_{}", rand::random::<u128>());
    vsdb_core::vsdb_set_base_dir(&dir).unwrap();
    units::basic_mapx_raw::benches();
    units::batch_write::benches();
    criterion::Criterion::default()
        .configure_from_args()
        .final_summary();
}
