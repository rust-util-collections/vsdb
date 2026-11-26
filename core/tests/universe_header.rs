use std::{fs, path::PathBuf, process::Command, thread};
use vsdb_core::{
    MapxRaw, Namespace, NamespaceOpts, VsdbError, VsdbOptions, vsdb_configure,
    vsdb_flush,
};

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
    // Crash after the downgrade marker and registry, before the ceiling/header.
    fs::write(system.join("format_version"), b"18").unwrap();
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
        .map(PathBuf::from)
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
fn legacy_writer_marker_covers_the_universe_and_placed_engines() {
    const PHASE: &str = "VSDB_V18_FORMAT_MARKER_PHASE";
    const BASE: &str = "VSDB_V18_FORMAT_MARKER_BASE";
    const MARKER: &str = "__SYSTEM__/format_version";
    let scratch = std::env::var_os(BASE)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::temp_dir()
                .join(format!("vsdb-format-marker-{}", rand::random::<u128>()))
        });
    let base = scratch.join("base");
    let placed = scratch.join("placed");
    match std::env::var(PHASE).as_deref() {
        Ok("create") => {
            vsdb_configure(VsdbOptions::new(&base)).unwrap();
            assert!(!base.join("mmdb").exists());
            assert_eq!(fs::read(base.join(MARKER)).unwrap(), b"18");
            let ns = Namespace::create_with(NamespaceOpts {
                path: Some(placed.clone()),
                ..Default::default()
            })
            .unwrap();
            assert_eq!(ns.id(), 1);
            assert_eq!(fs::read(placed.join(MARKER)).unwrap(), b"18");
            Namespace::open(0).unwrap();
            assert_eq!(fs::read(base.join(MARKER)).unwrap(), b"18");
        }
        Ok("read") => {
            vsdb_configure(VsdbOptions::read_only(&base)).unwrap();
            Namespace::open(0).unwrap();
            Namespace::open(1).unwrap();
            assert!(!base.join(MARKER).exists());
            assert!(!placed.join(MARKER).exists());
        }
        Ok("backfill") => {
            vsdb_configure(VsdbOptions::new(&base)).unwrap();
            Namespace::open(1).unwrap();
            assert_eq!(fs::read(placed.join(MARKER)).unwrap(), b"18");
            assert_eq!(fs::read(base.join(MARKER)).unwrap(), b"18");
        }
        Ok("relocate") => {
            vsdb_configure(VsdbOptions::new(&base)).unwrap();
            let new_root = scratch.join("moved");
            fs::rename(placed, &new_root).unwrap();
            fs::write(new_root.join(MARKER), b"19").unwrap();
            assert!(Namespace::relocate(1, &new_root).is_err());
            fs::write(new_root.join(MARKER), b"18").unwrap();
            Namespace::relocate(1, &new_root).unwrap();
            Namespace::open(1).unwrap();
        }
        Ok("reject") => {
            let before = fs::read(base.join(MARKER)).unwrap();
            assert!(vsdb_configure(VsdbOptions::new(&base)).is_err());
            assert_eq!(fs::read(base.join(MARKER)).unwrap(), before);
        }
        _ => {
            let run = |phase| {
                let out = Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "legacy_writer_marker_covers_the_universe_and_placed_engines",
                        "--nocapture",
                    ])
                    .env(PHASE, phase)
                    .env(BASE, &scratch)
                    .output()
                    .unwrap();
                assert!(out.status.success(), "{phase}: {out:?}");
            };
            run("create");
            fs::remove_file(base.join(MARKER)).unwrap();
            fs::remove_file(placed.join(MARKER)).unwrap();
            run("read");
            run("backfill");
            run("relocate");
            for incompatible in [b"16", b"19"] {
                fs::write(base.join(MARKER), incompatible).unwrap();
                run("reject");
            }
            fs::remove_dir_all(scratch).unwrap();
        }
    }
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

#[test]
fn required_engine_marker_loss_reports_corrupt_in_both_open_modes() {
    const ROOT: &str = "VSDB_MARKER_CLASSIFICATION_ROOT";
    const PHASE: &str = "VSDB_MARKER_CLASSIFICATION_PHASE";
    if let Some(base) = std::env::var_os(ROOT) {
        let phase = std::env::var(PHASE).unwrap();
        let base = PathBuf::from(base);
        if phase == "create" {
            vsdb_configure(VsdbOptions::new(&base)).unwrap();
            Namespace::open(0).unwrap();
            assert_eq!(
                Namespace::create_with(NamespaceOpts {
                    shards: 1,
                    ..Default::default()
                })
                .unwrap()
                .id(),
                1
            );
            vsdb_flush();
            return;
        }
        let (mode, which) = phase.split_once('-').unwrap();
        let options = if mode == "read" {
            VsdbOptions::read_only(&base)
        } else {
            VsdbOptions::new(&base)
        };
        let id = u64::from(which == "named");
        let result = vsdb_configure(options).and_then(|_| Namespace::open(id).map(drop));
        let error = result.unwrap_err();
        assert!(
            matches!(error, VsdbError::Corrupt { .. }),
            "{phase}: {error}"
        );
        assert!(error.to_string().contains("__SYSTEM__"), "{error}");
        return;
    }
    let dir = std::env::temp_dir()
        .join(format!("vsdb-required-markers-{}", rand::random::<u128>()));
    let run = |phase: &str| {
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "required_engine_marker_loss_reports_corrupt_in_both_open_modes",
            ])
            .env(ROOT, &dir)
            .env(PHASE, phase)
            .output()
            .unwrap();
        assert!(
            child.status.success(),
            "{phase}: {} {}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr)
        );
    };
    run("create");
    for (system, which) in [
        (dir.join("__SYSTEM__"), "default"),
        (
            dir.join("__NAMESPACES__/0000000000000001/__SYSTEM__"),
            "named",
        ),
    ] {
        let ready = system.join(".shards_ready");
        let init = system.join("__initializing__");
        if init.exists() {
            fs::remove_file(&init).unwrap();
        }
        fs::remove_file(&ready).unwrap();
        for mode in ["read", "write"] {
            run(&format!("{mode}-{which}"));
            assert!(!ready.exists() && !init.exists());
        }
        fs::write(&ready, b"18").unwrap();
    }
    fs::remove_dir_all(dir).unwrap();
}
