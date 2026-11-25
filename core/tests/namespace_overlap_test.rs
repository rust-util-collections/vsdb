use std::{env, fs};
use vsdb_core::{Namespace, NamespaceOpts, VsdbOptions, vsdb_configure};

#[test]
fn relative_base_cannot_be_reused_as_a_namespace_root() {
    let scratch = env::temp_dir().join(format!(
        "vsdb_ns_overlap_{}_{}",
        std::process::id(),
        rand::random::<u128>()
    ));
    fs::create_dir_all(&scratch).unwrap();
    let original = env::current_dir().unwrap();
    env::set_current_dir(&scratch).unwrap();

    // The base does not exist yet and is spelled relatively; the explicit
    // root below resolves to the same directory in absolute form.
    vsdb_configure(VsdbOptions::new("vsdb_data")).unwrap();
    let candidate = scratch.join("vsdb_data");
    let error = Namespace::create_with(NamespaceOpts {
        path: Some(candidate.clone()),
        ..Default::default()
    })
    .unwrap_err();
    assert!(
        error.to_string().contains("overlaps the default base dir"),
        "unexpected error: {error}"
    );
    assert!(
        !candidate.join("mmdb").exists(),
        "a rejected create must not initialize an engine"
    );

    #[cfg(unix)]
    {
        let real_dir = scratch.join("real_ns");
        let link_dir = scratch.join("link_ns");
        fs::create_dir_all(&real_dir).unwrap();
        std::os::unix::fs::symlink(&real_dir, &link_dir).unwrap();
        let ns = Namespace::create_with(NamespaceOpts {
            path: Some(link_dir.clone()),
            shards: 1,
            mem_budget_mb: Some(16),
        })
        .unwrap();
        let ns_id = ns.id();
        assert!(real_dir.join("mmdb").is_dir());
        ns.close().unwrap();
        Namespace::destroy(ns_id).unwrap();
        assert!(fs::symlink_metadata(&link_dir).is_err());
        assert!(!real_dir.exists());
    }

    env::set_current_dir(original).unwrap();
    fs::remove_dir_all(&scratch).unwrap();
}
