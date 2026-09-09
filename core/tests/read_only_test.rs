#![cfg(unix)]

use std::{
    collections::BTreeMap,
    env, fs,
    os::unix::fs::PermissionsExt,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    process::Command,
};
use vsdb_core::{
    InstanceId, MapxRaw, Namespace, OpenMode, VsdbError, VsdbOptions, vsdb_configure,
    vsdb_flush, vsdb_get_custom_dir, vsdb_get_meta_dir, vsdb_get_system_dir,
    vsdb_ns_destroy, vsdb_ns_relocate, vsdb_open_mode,
};

const HELPER_BASE: &str = "VSDB_READ_ONLY_HELPER_BASE";

fn helper_base() -> Option<PathBuf> {
    env::var_os(HELPER_BASE).map(PathBuf::from)
}

fn run_helper(name: &str, base: &Path) {
    let output = Command::new(env::current_exe().unwrap())
        .args(["--ignored", "--exact", name, "--nocapture"])
        .env(HELPER_BASE, base)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "helper {name} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn tree_contents(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        let mut entries: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                visit(root, &path, out);
            } else {
                out.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }

    let mut out = BTreeMap::new();
    visit(root, root, &mut out);
    out
}

fn make_tree_read_only(root: &Path) {
    fn visit(path: &Path) {
        if path.is_dir() {
            for entry in fs::read_dir(path).unwrap() {
                visit(&entry.unwrap().path());
            }
            fs::set_permissions(path, fs::Permissions::from_mode(0o555)).unwrap();
        } else {
            fs::set_permissions(path, fs::Permissions::from_mode(0o444)).unwrap();
        }
    }
    visit(root);
}

fn make_tree_writable(root: &Path) {
    fn visit(path: &Path) {
        if path.is_dir() {
            fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
            for entry in fs::read_dir(path).unwrap() {
                visit(&entry.unwrap().path());
            }
        } else {
            fs::set_permissions(path, fs::Permissions::from_mode(0o644)).unwrap();
        }
    }
    visit(root);
}

#[test]
fn read_only_universe_reads_without_changing_store() {
    let base =
        env::temp_dir().join(format!("vsdb_read_only_{}", rand::random::<u128>()));
    let missing = env::temp_dir()
        .join(format!("vsdb_read_only_missing_{}", rand::random::<u128>()));
    run_helper("read_only_missing_dataset_helper", &missing);
    assert!(!missing.exists());

    run_helper("read_only_writer_helper", &base);

    let before = tree_contents(&base);
    make_tree_read_only(&base);
    run_helper("read_only_reader_helper", &base);
    let after = tree_contents(&base);
    make_tree_writable(&base);

    assert_eq!(before, after);
    fs::remove_dir_all(base).unwrap();
}

#[test]
#[ignore]
fn read_only_missing_dataset_helper() {
    let Some(base) = helper_base() else {
        return;
    };
    vsdb_configure(VsdbOptions::read_only(&base)).unwrap();

    assert_eq!(vsdb_get_custom_dir(), base.join("__CUSTOM__"));
    assert_eq!(vsdb_get_system_dir(), base.join("__SYSTEM__"));
    assert_eq!(
        vsdb_get_meta_dir(),
        base.join("__SYSTEM__/__instance_meta__")
    );
    assert!(!base.exists());

    let open = catch_unwind(Namespace::default_ns);
    assert!(open.is_err());
    assert!(!base.exists());
}

#[test]
#[ignore]
fn read_only_writer_helper() {
    let Some(base) = helper_base() else {
        return;
    };
    vsdb_configure(VsdbOptions::new(&base)).unwrap();

    let mut default_map = MapxRaw::new();
    default_map.insert(b"default-key", b"default-value");
    let default_id = default_map.save_meta().unwrap();

    let ns = Namespace::create().unwrap();
    let mut namespaced_map = MapxRaw::new_in(&ns);
    namespaced_map.insert(b"namespace-key", b"namespace-value");
    let namespaced_id = namespaced_map.save_meta().unwrap();

    let state = postcard::to_allocvec(&(default_id, namespaced_id)).unwrap();
    fs::write(vsdb_get_custom_dir().join("read-only-test-state"), state).unwrap();
    vsdb_flush();
    // Leave a committed value only in the WAL. Read-only MMDB recovery must
    // expose it from memory without flushing an SST or rotating the WAL.
    default_map.insert(b"wal-key", b"wal-value");
}

#[test]
#[ignore]
fn read_only_reader_helper() {
    let Some(base) = helper_base() else {
        return;
    };
    vsdb_configure(VsdbOptions::read_only(&base)).unwrap();
    assert_eq!(OpenMode::ReadOnly, vsdb_open_mode());
    assert!(matches!(
        vsdb_configure(VsdbOptions::new(base.join("other"))),
        Err(VsdbError::BaseDirFrozen)
    ));
    assert_eq!(OpenMode::ReadOnly, vsdb_open_mode());

    let state = fs::read(vsdb_get_custom_dir().join("read-only-test-state")).unwrap();
    let (default_id, namespaced_id): (InstanceId, InstanceId) =
        postcard::from_bytes(&state).unwrap();

    // Restore the non-default handle first: prefix validation must read the
    // global ceiling without forcing creation/opening of a writable default DB.
    let namespaced_map = MapxRaw::from_meta(namespaced_id).unwrap();
    assert!(namespaced_map.namespace().is_read_only());
    assert_eq!(
        Some(b"namespace-value".to_vec()),
        namespaced_map.get(b"namespace-key")
    );

    let mut default_map = MapxRaw::from_meta(default_id).unwrap();
    assert!(default_map.namespace().is_read_only());
    assert_eq!(
        Some(b"default-value".to_vec()),
        default_map.get(b"default-key")
    );
    assert_eq!(Some(b"wal-value".to_vec()), default_map.get(b"wal-key"));

    namespaced_map.lazy_delete(b"namespace-key");
    namespaced_map.sync_wal();
    default_map.sync_wal();
    vsdb_flush();

    assert!(matches!(
        default_map.save_meta(),
        Err(VsdbError::ReadOnly { .. })
    ));
    assert!(matches!(
        Namespace::create(),
        Err(VsdbError::ReadOnly { .. })
    ));
    assert!(matches!(
        vsdb_ns_destroy(namespaced_id.ns.unwrap()),
        Err(VsdbError::ReadOnly { .. })
    ));
    assert!(matches!(
        vsdb_ns_relocate(namespaced_id.ns.unwrap(), base.join("relocated")),
        Err(VsdbError::ReadOnly { .. })
    ));
    assert!(matches!(
        default_map.clone_in(&default_map.namespace()),
        Err(VsdbError::ReadOnly { .. })
    ));

    {
        let mut batch = default_map.batch_entry();
        batch.insert(b"blocked-batch-key", b"blocked-batch-value");
        assert!(matches!(batch.commit(), Err(VsdbError::ReadOnly { .. })));
    }
    assert_eq!(None, default_map.get(b"blocked-batch-key"));

    let insert = catch_unwind(AssertUnwindSafe(|| {
        default_map.insert(b"blocked-key", b"blocked-value");
    }));
    assert!(insert.is_err());
    assert_eq!(None, default_map.get(b"blocked-key"));

    let create_map = catch_unwind(MapxRaw::new);
    assert!(create_map.is_err());
}
