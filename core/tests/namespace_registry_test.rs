use std::{env, fs};
use vsdb_core::{
    MapxRaw, Namespace, NamespaceOpts, VsdbOptions, vsdb_configure, vsdb_get_base_dir,
};

#[test]
fn lost_registry_cannot_reuse_a_namespace_id_for_any_root() {
    let scratch = env::temp_dir().join(format!(
        "vsdb_ns_registry_{}_{}",
        std::process::id(),
        rand::random::<u128>()
    ));
    fs::create_dir_all(&scratch).unwrap();
    vsdb_configure(VsdbOptions::new(scratch.join("universe"))).unwrap();

    let ns = Namespace::create_with(NamespaceOpts {
        shards: 2,
        ..Default::default()
    })
    .unwrap();
    let id = ns.id();
    let root = ns.path().to_path_buf();
    let mut map = MapxRaw::new_in(&ns);
    map.insert(b"key", b"value");
    ns.flush();
    drop(map);
    ns.close().unwrap();

    // Simulate a lost/reset registry while the derived root survives.
    let registry = vsdb_get_base_dir().join("__SYSTEM__/__namespaces__");
    let lifecycle = vsdb_get_base_dir()
        .join("__SYSTEM__/__namespace_state__")
        .join(format!("{id:016x}"));
    let established = fs::read(&lifecycle).unwrap();
    fs::remove_file(&registry).unwrap();

    let explicit = scratch.join("explicit");
    for path in [None, Some(explicit.clone())] {
        let error = Namespace::create_with(NamespaceOpts {
            path,
            shards: 4,
            ..Default::default()
        })
        .unwrap_err();
        assert!(
            error.to_string().contains("lifecycle"),
            "unexpected error: {error}"
        );
        assert_eq!(fs::read(&lifecycle).unwrap(), established);
        assert!(!registry.exists(), "rejection must not publish a registry");
        assert!(!explicit.exists(), "rejection must not create a new root");
    }
    assert!(
        root.join("__SYSTEM__/format_version").exists(),
        "the existing namespace root must not be removed"
    );

    fs::remove_dir_all(&scratch).unwrap();
}
