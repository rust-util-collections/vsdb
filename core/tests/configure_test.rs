use std::{env, fs, path::PathBuf, process::Command};
use vsdb_core::{
    MapxRaw, Namespace, VsdbError, VsdbOptions, vsdb_configure, vsdb_get_base_dir,
    vsdb_get_custom_dir, vsdb_get_meta_dir, vsdb_get_system_dir,
};

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

#[test]
fn configure_racing_first_use_never_splits_bases() {
    const CHILD_CHOSEN: &str = "VSDB_CONFIGURE_RACE_CHOSEN";
    const CHILD_FIRST_USE: &str = "VSDB_CONFIGURE_RACE_FIRST_USE";

    if let Some(chosen) = env::var_os(CHILD_CHOSEN) {
        let chosen = PathBuf::from(chosen);
        let first_use = env::var(CHILD_FIRST_USE).unwrap();
        let start = std::sync::Barrier::new(2);
        std::thread::scope(|s| {
            let barrier = &start;
            let reader = s.spawn(move || {
                barrier.wait();
                match first_use.as_str() {
                    "meta" => vsdb_get_meta_dir().to_path_buf(),
                    "custom" => vsdb_get_custom_dir().to_path_buf(),
                    "engine" => {
                        let ns = Namespace::default_ns();
                        assert_eq!(ns.path(), vsdb_get_base_dir());
                        ns.meta_dir()
                    }
                    _ => unreachable!(),
                }
            });
            barrier.wait();
            let configured = vsdb_configure(VsdbOptions::new(&chosen));
            let materialized = reader.join().unwrap();
            let base = vsdb_get_base_dir();
            match configured {
                Ok(()) => assert_eq!(base, chosen),
                Err(VsdbError::BaseDirFrozen) => {
                    assert_eq!(
                        base,
                        PathBuf::from(env::var_os("VSDB_BASE_DIR").unwrap())
                    );
                }
                Err(error) => panic!("unexpected configuration error: {error}"),
            }
            assert!(materialized.starts_with(&base));
            assert_eq!(vsdb_get_system_dir(), base.join("__SYSTEM__"));
            assert_eq!(
                vsdb_get_meta_dir(),
                base.join("__SYSTEM__/__instance_meta__")
            );
            assert_eq!(vsdb_get_custom_dir(), base.join("__CUSTOM__"));
            assert!(matches!(
                vsdb_configure(VsdbOptions::new(chosen.join("again"))),
                Err(VsdbError::BaseDirFrozen)
            ));
        });
        return;
    }

    let scratch =
        env::temp_dir().join(format!("vsdb_configure_race_{}", rand::random::<u128>()));
    for first_use in ["meta", "custom", "engine"] {
        for round in 0..32 {
            let root = scratch.join(format!("{first_use}_{round}"));
            let output = Command::new(env::current_exe().unwrap())
                .args([
                    "--exact",
                    "configure_racing_first_use_never_splits_bases",
                    "--nocapture",
                ])
                .env(CHILD_CHOSEN, root.join("chosen"))
                .env(CHILD_FIRST_USE, first_use)
                .env("VSDB_BASE_DIR", root.join("default"))
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{first_use} round {round} failed:\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
    }
    fs::remove_dir_all(&scratch).unwrap();
}
