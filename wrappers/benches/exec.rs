use criterion::criterion_main;

mod units;

criterion_main! {
    units::versioned_multi_key_mapx_double_key::benches,
    units::versioned_multi_key_mapx_triple_key::benches,
    units::versioned_multi_key_mapx_raw::benches,

    units::versioned_mapx::benches,
    units::versioned_mapx_ord::benches,
    units::versioned_mapx_ord_rawkey::benches,
    units::versioned_vecx::benches,

    units::basic_multi_key_mapx_double_key::benches,
    units::basic_multi_key_mapx_raw::benches,
    units::basic_multi_key_mapx_rawkey::benches,
    units::basic_multi_key_mapx_triple_key::benches,

    units::basic_mapx::benches,
    units::basic_mapx_ord::benches,
    units::basic_mapx_ord_rawkey::benches,
    units::basic_mapx_ord_rawvalue::benches,
    units::basic_vecx::benches,
    units::basic_vecx_raw::benches,
}
