#![allow(warnings)]

#[cfg(not(feature = "vs"))]
criterion::criterion_main! {}

#[cfg(feature = "vs")]
mod units;

#[cfg(feature = "vs")]
criterion::criterion_main! {
    units::versioned_multi_key_mapx_double_key::benches,
    units::versioned_multi_key_mapx_triple_key::benches,
    units::versioned_multi_key_mapx_raw::benches,

    units::versioned_mapx::benches,
    units::versioned_mapx_ord::benches,
    units::versioned_mapx_ord_rawkey::benches,
    units::versioned_vecx::benches,
}
