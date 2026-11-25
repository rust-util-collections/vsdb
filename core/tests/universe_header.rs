use std::{fs, path::PathBuf, process::Command, thread};
use vsdb_core::{MapxRaw, Namespace, VsdbError, VsdbOptions, vsdb_configure, vsdb_flush};

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

#[test]
fn read_only_open_reports_lock_contention_before_header_publish() {
    const PROBE: &str = "VSDB_V18_RO_CONTENTION_PROBE";
    if let Some(dir) = std::env::var_os(PROBE) {
        let err = vsdb_configure(VsdbOptions::read_only(dir)).unwrap_err();
        match err {
            vsdb_core::VsdbError::Io(source) => {
                assert_eq!(source.kind(), std::io::ErrorKind::WouldBlock);
            }
            other => panic!("expected WouldBlock Io error, got {other:?}"),
        }
        return;
    }

    let root = std::env::temp_dir()
        .join(format!("vsdb-v18-ro-contention-{}", rand::random::<u128>()));
    let system = root.join("__SYSTEM__");
    fs::create_dir_all(&system).unwrap();
    let lock_file = fs::File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(system.join("LOCK"))
        .unwrap();
    lock_file.try_lock().unwrap();

    let out = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "read_only_open_reports_lock_contention_before_header_publish",
            "--nocapture",
        ])
        .env(PROBE, &root)
        .output()
        .unwrap();
    drop(lock_file);
    fs::remove_dir_all(&root).unwrap();
    assert!(out.status.success(), "{out:?}");
}

#[test]
fn empty_map_metadata_reports_allocator_errors_and_retries() {
    const PROBE: &str = "VSDB_V18_PREFIX_IO_PROBE";
    if let Some(base) = std::env::var_os(PROBE) {
        let base = PathBuf::from(base);
        vsdb_configure(VsdbOptions::new(&base)).unwrap();
        let map = MapxRaw::new();
        let token = base.join("saved-token");
        if !token.exists() {
            fs::write(token, map.save_meta().unwrap().to_string()).unwrap();
            return;
        }
        let saved_id: vsdb_core::InstanceId =
            fs::read_to_string(token).unwrap().parse().unwrap();
        let ceiling = base.join("__SYSTEM__/__prefix_ceiling__");
        let fail_read = || {
            let saved = fs::read(&ceiling).unwrap();
            fs::remove_file(&ceiling).unwrap();
            fs::create_dir(&ceiling).unwrap();
            saved
        };
        let restore = |saved| {
            fs::remove_dir(&ceiling).unwrap();
            fs::write(&ceiling, saved).unwrap();
        };
        let saved = fail_read();
        assert!(matches!(
            MapxRaw::from_meta(saved_id),
            Err(VsdbError::Io(_))
        ));
        assert!(matches!(map.save_meta(), Err(VsdbError::Io(_))));
        assert!(postcard::to_allocvec(&map).is_err());
        restore(saved);
        let id = map.save_meta().unwrap();
        assert_eq!(MapxRaw::from_meta(id).unwrap().instance_id(), id);
        assert_eq!(
            MapxRaw::from_meta(saved_id).unwrap().instance_id(),
            saved_id
        );

        // The next thread needs the last allocator window. A failed refill
        // must leave that window available for a retry on another thread.
        let saved = fail_read();
        let allocate = || MapxRaw::new().try_instance_id();
        assert!(matches!(
            thread::spawn(allocate).join().unwrap(),
            Err(VsdbError::Io(_))
        ));
        restore(saved);
        assert_eq!(
            thread::spawn(allocate).join().unwrap().unwrap().map_id,
            u64::MAX - 2
        );
        return;
    }
    let root =
        std::env::temp_dir().join(format!("vsdb-prefix-io-{}", rand::random::<u128>()));
    let run = || {
        let out = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "empty_map_metadata_reports_allocator_errors_and_retries",
                "--nocapture",
            ])
            .env(PROBE, &root)
            .output()
            .unwrap();
        assert!(out.status.success(), "{out:?}");
    };
    run();
    fs::write(
        root.join("__SYSTEM__/__prefix_ceiling__"),
        (u64::MAX - 8194).to_le_bytes(),
    )
    .unwrap();
    run();
    fs::remove_dir_all(root).unwrap();
}
