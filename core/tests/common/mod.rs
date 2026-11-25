mod scratch;
use parking_lot::Once;

/// Configures a process-unique base directory before the first engine use.
/// Integration test binaries run in separate processes, so one `Once` per
/// binary is sufficient even when several tests share this module.
pub fn setup() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        vsdb_core::vsdb_configure(vsdb_core::VsdbOptions::new(scratch::database_dir()))
            .unwrap();
    });
}
