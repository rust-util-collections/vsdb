use std::{env, fs, path::PathBuf, process::Command};
use vsdb_core::{MapxRaw, VsdbOptions, vsdb_configure, vsdb_get_base_dir};

#[test]
fn explicit_configuration_ignores_environment_default() {
    const CHILD_ROOT: &str = "VSDB_CONFIGURE_TEST_CHILD_ROOT";

    if let Some(root) = env::var_os(CHILD_ROOT) {
        let root = PathBuf::from(root);
        vsdb_configure(VsdbOptions::new(&root)).unwrap();
        assert_eq!(vsdb_get_base_dir(), root);
        let mut map = MapxRaw::new();
        map.insert(b"key", b"value");
        assert_eq!(map.get(b"key"), Some(b"value".to_vec()));
        return;
    }

    let scratch =
        env::temp_dir().join(format!("vsdb_configure_{}", rand::random::<u128>()));
    fs::create_dir_all(&scratch).unwrap();
    // A file as an ancestor fails even when the test runs as root.
    let blocker = scratch.join("file");
    fs::write(&blocker, b"not a directory").unwrap();

    let output = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "explicit_configuration_ignores_environment_default",
            "--nocapture",
        ])
        .env(CHILD_ROOT, scratch.join("chosen"))
        .env("VSDB_BASE_DIR", blocker.join("unused"))
        .output()
        .unwrap();

    fs::remove_dir_all(&scratch).unwrap();
    assert!(
        output.status.success(),
        "child failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
