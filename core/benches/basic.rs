#![allow(warnings)]

mod units;

criterion::criterion_main! {
    units::basic_mapx_raw::benches,
    units::batch_write::benches,
}
