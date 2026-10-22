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
        !candidate.exists(),
        "a rejected create must not write the root"
    );

    env::set_current_dir(original).unwrap();
    fs::remove_dir_all(&scratch).unwrap();
}
