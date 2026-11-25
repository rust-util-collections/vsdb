use std::{fs, process::Command};
use vsdb_core::{Namespace, VsdbOptions, vsdb_configure, vsdb_flush};

#[test]
fn resumes_bootstrap_and_excludes_a_second_writer_before_opening_any_shard() {
    const PROBE: &str = "VSDB_V18_UNIVERSE_LOCK_PROBE";
    if let Some(dir) = std::env::var_os(PROBE) {
        let error = vsdb_configure(VsdbOptions::new(dir)).unwrap_err();
        assert!(matches!(error, vsdb_core::VsdbError::Io(_)), "{error}");
        return;
    }
    let root = std::env::temp_dir()
        .join(format!("vsdb-v18-bootstrap-{}", rand::random::<u128>()));
    let system = root.join("__SYSTEM__");
    fs::create_dir_all(&system).unwrap();
    // Crash after the registry commit and before the ceiling/header.
    fs::write(system.join("__namespaces__"), b"VSNREG02\x01\x00").unwrap();
    vsdb_configure(VsdbOptions::new(&root)).unwrap();
    assert_eq!(
        &fs::read(system.join("universe_header")).unwrap()[..8],
        b"VSUNI018"
    );
    assert!(system.join("__prefix_ceiling__").is_file());
    assert!(Namespace::list().unwrap().is_empty());
    vsdb_flush();
    assert!(!root.join("mmdb").exists());
    let result = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "resumes_bootstrap_and_excludes_a_second_writer_before_opening_any_shard",
        ])
        .env(PROBE, &root)
        .output()
        .unwrap();
    assert!(result.status.success(), "{result:?}");
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn read_only_universe_lock_covers_namespace_admin() {
    const CASE: &str = "VSDB_V18_READ_LOCK_CASE";
    const BASE: &str = "VSDB_V18_READ_LOCK_BASE";
    let dir = std::env::var_os(BASE)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir()
                .join(format!("vsdb-v18-read-lock-{}", rand::random::<u128>()))
        });
    let run = |case: &str| {
        let out = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "read_only_universe_lock_covers_namespace_admin",
                "--nocapture",
            ])
            .env(CASE, case)
            .env(BASE, &dir)
            .output()
            .unwrap();
        assert!(out.status.success(), "{case}: {out:?}");
    };
    match std::env::var(CASE).as_deref() {
        Ok("seed") => {
            vsdb_configure(VsdbOptions::new(&dir)).unwrap();
            Namespace::create().unwrap();
        }
        Ok("reader") => {
            vsdb_configure(VsdbOptions::read_only(&dir)).unwrap();
            // The global lock also protects a namespace before this
            // process has opened any of its mmdb shard lockfiles.
            assert_eq!(Namespace::list().unwrap().len(), 1);
            run("second_reader");
            run("writer");
        }
        Ok("second_reader") => {
            vsdb_configure(VsdbOptions::read_only(&dir)).unwrap();
        }
        Ok("writer") => {
            assert!(matches!(
                vsdb_configure(VsdbOptions::new(&dir)),
                Err(vsdb_core::VsdbError::Io(_))
            ));
        }
        _ => {
            run("seed");
            run("reader");
            let lock = dir.join("__SYSTEM__/LOCK");
            fs::remove_file(&lock).unwrap();
            run("second_reader");
            assert!(!lock.exists());
            fs::remove_dir_all(dir).unwrap();
        }
    }
}
