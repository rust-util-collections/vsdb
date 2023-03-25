#![allow(warnings)]

#[cfg(not(feature = "vs"))]
criterion::criterion_main! {}

#[cfg(feature = "vs")]
mod units;

#[cfg(feature = "vs")]
criterion::criterion_main! {
    units::basic_mapx_raw::benches,
}
