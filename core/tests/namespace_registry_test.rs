use std::{env, fs};
use vsdb_core::{
    MapxRaw, Namespace, NamespaceOpts, VsdbOptions, vsdb_configure, vsdb_get_base_dir,
};

#[test]
fn lost_registry_cannot_reuse_an_existing_derived_root() {
    let scratch = env::temp_dir().join(format!(
        "vsdb_ns_registry_{}_{}",
        std::process::id(),
        rand::random::<u128>()
    ));
    fs::create_dir_all(&scratch).unwrap();
    vsdb_configure(VsdbOptions::new(&scratch)).unwrap();

    let ns = Namespace::create_with(NamespaceOpts {
        shards: 2,
        ..Default::default()
    })
    .unwrap();
    let root = ns.path().to_path_buf();
    let mut map = MapxRaw::new_in(&ns);
    map.insert(b"key", b"value");
    ns.flush();
    drop(map);
    ns.close().unwrap();

    // Simulate a lost/reset registry while the derived root survives.
    let registry = vsdb_get_base_dir().join("__SYSTEM__/__namespaces__");
    fs::remove_file(&registry).unwrap();

    let error = Namespace::create_with(NamespaceOpts {
        shards: 4,
        ..Default::default()
    })
    .unwrap_err();
    assert!(
        error.to_string().contains("lifecycle"),
        "unexpected error: {error}"
    );
    assert!(
        root.join("__SYSTEM__/format_version").exists(),
        "the existing namespace root must not be removed"
    );

    fs::remove_dir_all(&scratch).unwrap();
}
