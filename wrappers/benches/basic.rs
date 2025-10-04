#![allow(warnings)]

mod units;

criterion::criterion_main! {
    units::basic_mapx::benches,
    units::basic_mapx_ord::benches,
    units::batch_vs_normal::benches,
    units::concurrent::benches,
}
