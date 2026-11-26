use std::{env, fs, path::PathBuf, process::Command};
use vsdb_core::{
    MapxRaw, Namespace, VsdbError, VsdbOptions, vsdb_configure, vsdb_get_base_dir,
    vsdb_get_custom_dir, vsdb_get_system_dir,
};

#[test]
fn relative_base_stays_with_the_selected_directory() {
    const MODE: &str = "VSDB_RELATIVE_BASE_TEST_MODE";
    if let Some(mode) = env::var_os(MODE) {
        if mode == "configured" {
            vsdb_configure(VsdbOptions::new("data")).unwrap();
        }
        let ns = Namespace::create().unwrap();
        env::set_current_dir("other").unwrap();
        assert!(
            Namespace::list()
                .unwrap()
                .iter()
                .any(|info| info.id == ns.id())
        );
        #[cfg(unix)]
        {
            fs::remove_dir(env::current_dir().unwrap()).unwrap();
            assert!(matches!(
                vsdb_configure(VsdbOptions::new("unused")),
                Err(VsdbError::BaseDirFrozen)
            ));
        }
        return;
    }

    let scratch =
        env::temp_dir().join(format!("vsdb_relative_{}", rand::random::<u128>()));
    for mode in ["configured", "environment"] {
        let cwd = scratch.join(mode);
        fs::create_dir_all(cwd.join("other")).unwrap();
        let output = Command::new(env::current_exe().unwrap())
            .args(["--exact", "relative_base_stays_with_the_selected_directory"])
            .env(MODE, mode)
            .env("VSDB_BASE_DIR", "data")
            .current_dir(cwd)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{mode}: {}",
            String::from_utf8_lossy(&output.stdout)
        );
    }
    fs::remove_dir_all(scratch).unwrap();
}

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
                    "base" => vsdb_get_base_dir(),
                    "meta" => vsdb_core::Namespace::default_ns().meta_dir(),
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
                vsdb_core::Namespace::default_ns().meta_dir(),
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
    for first_use in ["base", "meta", "custom", "engine"] {
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

#[test]
fn get_base_dir_freezes_subsequent_configure() {
    const CHILD_CHOSEN: &str = "VSDB_BASE_DIR_FREEZE_CHOSEN";

    if let Some(chosen) = env::var_os(CHILD_CHOSEN) {
        let initial = vsdb_get_base_dir();
        assert_eq!(
            initial,
            PathBuf::from(env::var_os("VSDB_BASE_DIR").unwrap())
        );
        assert!(matches!(
            vsdb_configure(VsdbOptions::new(chosen)),
            Err(VsdbError::BaseDirFrozen)
        ));
        assert_eq!(vsdb_get_base_dir(), initial);
        return;
    }

    let scratch =
        env::temp_dir().join(format!("vsdb_base_freeze_{}", rand::random::<u128>()));
    let output = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "get_base_dir_freezes_subsequent_configure",
            "--nocapture",
        ])
        .env(CHILD_CHOSEN, scratch.join("chosen"))
        .env("VSDB_BASE_DIR", scratch.join("default"))
        .output()
        .unwrap();
    let _ = fs::remove_dir_all(&scratch);
    assert!(
        output.status.success(),
        "child failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[cfg(unix)]
#[test]
fn non_utf8_environment_paths_select_requested_directories() {
    use std::{ffi::OsStr, os::unix::ffi::OsStrExt};

    const CHILD_CASE: &str = "VSDB_NON_UTF8_ENV_CASE";
    const CHILD_EXPECTED: &str = "VSDB_NON_UTF8_ENV_EXPECTED";

    if let Some(case) = env::var_os(CHILD_CASE) {
        let base = vsdb_get_base_dir();
        if case == "tmpdir" {
            let parent = PathBuf::from(env::var_os(CHILD_EXPECTED).unwrap());
            assert_eq!(base.parent(), Some(parent.as_path()));
            let mut map = MapxRaw::new();
            map.insert(b"k", b"v");
            assert_eq!(map.get(b"k"), Some(b"v".to_vec()));
            return;
        }
        let expected = PathBuf::from(env::var_os(CHILD_EXPECTED).unwrap());
        assert_eq!(base, expected);
        let mut map = MapxRaw::new();
        map.insert(b"k", b"v");
        assert_eq!(map.get(b"k"), Some(b"v".to_vec()));
        return;
    }

    let scratch =
        env::temp_dir().join(format!("vsdb_non_utf8_{}", rand::random::<u128>()));
    fs::create_dir_all(&scratch).unwrap();

    let non_utf8_base = scratch.join(OsStr::from_bytes(b"base-\xff-dir"));
    let utf8_home = scratch.join("home");
    fs::create_dir_all(&utf8_home).unwrap();
    let out = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "non_utf8_environment_paths_select_requested_directories",
            "--nocapture",
        ])
        .env(CHILD_CASE, "base")
        .env(CHILD_EXPECTED, &non_utf8_base)
        .env("VSDB_BASE_DIR", &non_utf8_base)
        .env("HOME", &utf8_home)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "non-UTF-8 VSDB_BASE_DIR failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        !utf8_home.join(".vsdb").exists(),
        "non-UTF-8 VSDB_BASE_DIR silently fell back to $HOME/.vsdb"
    );
    assert!(non_utf8_base.join("__SYSTEM__/universe_header").is_file());

    let non_utf8_home = scratch.join(OsStr::from_bytes(b"home-\xfe-dir"));
    fs::create_dir_all(&non_utf8_home).unwrap();
    let expected_home_db = non_utf8_home.join(".vsdb");
    let out = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "non_utf8_environment_paths_select_requested_directories",
            "--nocapture",
        ])
        .env(CHILD_CASE, "home")
        .env(CHILD_EXPECTED, &expected_home_db)
        .env_remove("VSDB_BASE_DIR")
        .env("HOME", &non_utf8_home)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "non-UTF-8 HOME failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert!(
        expected_home_db
            .join("__SYSTEM__/universe_header")
            .is_file()
    );

    let non_utf8_tmp = scratch.join(OsStr::from_bytes(b"tmp-\xfd-dir"));
    fs::create_dir_all(&non_utf8_tmp).unwrap();
    let out = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "non_utf8_environment_paths_select_requested_directories",
            "--nocapture",
        ])
        .env(CHILD_CASE, "tmpdir")
        .env(CHILD_EXPECTED, &non_utf8_tmp)
        .env_remove("VSDB_BASE_DIR")
        .env_remove("HOME")
        .env("TMPDIR", &non_utf8_tmp)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "non-UTF-8 TMPDIR failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    fs::remove_dir_all(&scratch).unwrap();
}

#[cfg(unix)]
#[test]
fn traverse_only_ancestor_does_not_block_durable_dir_creation() {
    use std::os::unix::fs::PermissionsExt;

    const CHILD_DB: &str = "VSDB_TRAVERSE_ONLY_ANCESTOR_DB";

    if let Some(db) = env::var_os(CHILD_DB) {
        let db = PathBuf::from(db);
        vsdb_configure(VsdbOptions::new(&db)).unwrap();
        let mut map = MapxRaw::new();
        map.insert(b"k", b"v");
        let id = map.save_meta().unwrap();
        assert_eq!(
            MapxRaw::from_meta(id).unwrap().get(b"k"),
            Some(b"v".to_vec())
        );
        let ns = Namespace::create().unwrap();
        let ns_id = ns.id();
        let mut ns_map = MapxRaw::new_in(&ns);
        ns_map.insert(b"nk", b"nv");
        let ns_map_id = ns_map.save_meta().unwrap();
        drop(ns_map);
        ns.close().unwrap();
        let reopened = MapxRaw::from_meta(ns_map_id).unwrap();
        assert_eq!(reopened.get(b"nk"), Some(b"nv".to_vec()));
        drop(reopened);
        Namespace::close_by_id(ns_id).unwrap();
        Namespace::destroy(ns_id).unwrap();
        return;
    }

    let scratch =
        env::temp_dir().join(format!("vsdb_traverse_only_{}", rand::random::<u128>()));
    let restricted = scratch.join("restricted");
    let user_home = restricted.join("user");
    fs::create_dir_all(&user_home).unwrap();
    fs::set_permissions(&restricted, fs::Permissions::from_mode(0o111)).unwrap();

    // Skip only when running with elevated DAC bypass (e.g. root), where
    // opening a 0o111 directory succeeds unconditionally.
    if fs::File::open(&restricted).is_err() {
        let db = user_home.join("vsdb_data");
        for attempt in 0..2 {
            let out = Command::new(env::current_exe().unwrap())
                .args([
                    "--exact",
                    "traverse_only_ancestor_does_not_block_durable_dir_creation",
                    "--nocapture",
                ])
                .env(CHILD_DB, &db)
                .env_remove("VSDB_BASE_DIR")
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "attempt {attempt} under traverse-only ancestor failed:\n{}\n{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr),
            );
        }
    }

    fs::set_permissions(&restricted, fs::Permissions::from_mode(0o755)).unwrap();
    fs::remove_dir_all(&scratch).unwrap();
}
