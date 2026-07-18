mod units;

// Custom main (instead of `criterion_main!`): the legacy dynamic
// budget must be exported before the first engine touch.
fn main() {
    units::legacy_budget::apply();
    units::basic_mapx::benches();
    units::basic_mapx_ord::benches();
    units::batch_vs_normal::benches();
    units::concurrent::benches();
    criterion::Criterion::default()
        .configure_from_args()
        .final_summary();
}
