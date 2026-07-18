mod units;

// Custom main (instead of `criterion_main!`): the legacy dynamic
// budget must be exported before the first engine touch.
fn main() {
    units::legacy_budget::apply();
    units::basic_mapx_raw::benches();
    units::batch_write::benches();
    criterion::Criterion::default()
        .configure_from_args()
        .final_summary();
}
