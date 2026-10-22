use parking_lot::Once;

/// Configures a process-unique base directory before the first engine use.
/// Integration test binaries run in separate processes, so one `Once` per
/// binary is sufficient even when several tests share this module.
pub fn setup() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let dir = std::env::temp_dir().join(format!(
            "vsdb_test_{}_{}",
            env!("CARGO_PKG_NAME"),
            rand::random::<u128>()
        ));
        vsdb_core::vsdb_configure(vsdb_core::VsdbOptions::new(dir)).unwrap();
    });
}
