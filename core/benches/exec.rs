use criterion::criterion_main;

mod units;

criterion_main! {
    units::versioned_mapx_raw::benches,
    units::basic_mapx_raw::benches,
}
