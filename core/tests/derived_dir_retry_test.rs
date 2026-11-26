//! A failed universe initialization keeps the configured selection, and path
//! getters recover once a later API opens the universe with it.
#[path = "common/scratch.rs"]
mod scratch;

use std::{fs, panic::catch_unwind};
use vsdb_core::{
    Namespace, VsdbError, VsdbOptions, vsdb_configure, vsdb_get_base_dir,
    vsdb_get_custom_dir, vsdb_get_system_dir,
};

#[test]
fn derived_dir_getters_retry_after_lock_contention() {
    let root = scratch::database_dir();
    let system = root.join("__SYSTEM__");
    fs::create_dir_all(&system).unwrap();
    // Another writer holds the universe lock during the first attempts.
    let other_writer = fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(system.join("LOCK"))
        .unwrap();
    other_writer.try_lock().unwrap();
    assert!(matches!(
        vsdb_configure(VsdbOptions::new(&root)),
        Err(VsdbError::Io(_))
    ));
    assert!(catch_unwind(vsdb_get_custom_dir).is_err());
    assert!(catch_unwind(vsdb_get_system_dir).is_err());
    // The selection stays in effect, so configuring again is refused.
    assert!(matches!(
        vsdb_configure(VsdbOptions::new(&root)),
        Err(VsdbError::BaseDirFrozen)
    ));
    assert_eq!(vsdb_get_base_dir(), root);

    drop(other_writer);
    assert!(Namespace::list().unwrap().is_empty());
    assert_eq!(vsdb_get_custom_dir(), root.join("__CUSTOM__"));
    assert_eq!(vsdb_get_system_dir(), root.join("__SYSTEM__"));
    assert!(root.join("__CUSTOM__").is_dir());
    fs::remove_dir_all(&root).unwrap();
}
