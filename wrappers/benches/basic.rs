#![allow(warnings)]

mod units;

criterion::criterion_main! {
    units::basic_multi_key_mapx_double_key::benches,
    units::basic_multi_key_mapx_raw::benches,
    units::basic_multi_key_mapx_rawkey::benches,
    units::basic_multi_key_mapx_triple_key::benches,

    units::basic_mapx::benches,
    units::basic_mapx_ord::benches,
    units::basic_mapx_ord_rawkey::benches,
    units::basic_mapx_ord_rawvalue::benches,
    // Vecx and VecxRaw have been removed - they relied on unreliable len() tracking
    // units::basic_vecx::benches,
    // units::basic_vecx_raw::benches,
}
