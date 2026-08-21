#![cfg(unix)]

use std::{
    collections::BTreeMap,
    env, fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};
use vsdb::{
    InstanceId, Mapx, MptCalc, OpenMode, VerMap, VerMapWithProof, VsdbError,
    VsdbOptions, vsdb_configure, vsdb_flush, vsdb_get_base_dir, vsdb_open_mode,
};

const HELPER_BASE: &str = "VSDB_STRATA_READ_ONLY_HELPER_BASE";

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

fn set_tree_mode(root: &Path, dir_mode: u32, file_mode: u32) {
    fn visit(path: &Path, dir_mode: u32, file_mode: u32) {
        if path.is_dir() {
            for entry in fs::read_dir(path).unwrap() {
                visit(&entry.unwrap().path(), dir_mode, file_mode);
            }
            fs::set_permissions(path, fs::Permissions::from_mode(dir_mode)).unwrap();
        } else {
            fs::set_permissions(path, fs::Permissions::from_mode(file_mode)).unwrap();
        }
    }
    visit(root, dir_mode, file_mode);
}

#[test]
fn typed_and_versioned_reads_leave_store_unchanged() {
    let base = env::temp_dir()
        .join(format!("vsdb_strata_read_only_{}", rand::random::<u128>()));
    run_helper("read_only_writer_helper", &base);

    let before = tree_contents(&base);
    set_tree_mode(&base, 0o555, 0o444);
    run_helper("read_only_reader_helper", &base);
    let after = tree_contents(&base);
    set_tree_mode(&base, 0o755, 0o644);

    assert_eq!(before, after);
    fs::remove_dir_all(base).unwrap();
}

#[test]
#[ignore]
fn read_only_writer_helper() {
    let Some(base) = helper_base() else {
        return;
    };
    vsdb_configure(VsdbOptions::new(&base)).unwrap();

    let mut map = Mapx::<u64, String>::new();
    map.insert(&7, &"typed-value".to_owned());
    let map_id = map.save_meta().unwrap();

    let mut versioned = VerMap::<u64, String>::new();
    let main = versioned.main_branch();
    versioned
        .insert(main, &11, &"versioned-value".to_owned())
        .unwrap();
    versioned.commit(main).unwrap();
    let versioned_id = versioned.save_meta().unwrap();

    let expected_root = {
        let mut proof = VerMapWithProof::<u64, String, MptCalc>::from_map(versioned);
        proof.merkle_root(main).unwrap()
    };
    // Force the reader to rebuild the disposable trie cache. In read-only
    // mode the rebuild is in-memory only and must not recreate this file.
    let cache_path = vsdb_get_base_dir()
        .join("__SYSTEM__")
        .join(format!("mpt_cache_{}.bin", versioned_id.map_id));
    if cache_path.exists() {
        fs::remove_file(cache_path).unwrap();
    }

    let state = postcard::to_allocvec(&(map_id, versioned_id, expected_root)).unwrap();
    fs::write(base.join("read-only-strata-state"), state).unwrap();
    vsdb_flush();
}

#[test]
#[ignore]
fn read_only_reader_helper() {
    let Some(base) = helper_base() else {
        return;
    };
    vsdb_configure(VsdbOptions::read_only(&base)).unwrap();
    assert_eq!(OpenMode::ReadOnly, vsdb_open_mode());

    let state = fs::read(base.join("read-only-strata-state")).unwrap();
    let (map_id, versioned_id, expected_root): (InstanceId, InstanceId, Vec<u8>) =
        postcard::from_bytes(&state).unwrap();

    let map = Mapx::<u64, String>::from_meta(map_id).unwrap();
    assert!(map.namespace().is_read_only());
    assert_eq!(Some("typed-value".to_owned()), map.get(&7));
    assert!(matches!(map.save_meta(), Err(VsdbError::ReadOnly { .. })));

    let mut standalone_trie = MptCalc::new();
    assert!(matches!(
        standalone_trie.save_cache(&base, 1, 1),
        Err(VsdbError::ReadOnly { .. })
    ));

    let mut versioned = VerMap::<u64, String>::from_meta(versioned_id).unwrap();
    let main = versioned.branch_id("main").unwrap();
    assert_eq!(
        Some("versioned-value".to_owned()),
        versioned.get(main, &11).unwrap()
    );
    assert!(matches!(
        versioned.insert(main, &12, &"blocked".to_owned()),
        Err(VsdbError::ReadOnly { .. })
    ));

    let mut proof = VerMapWithProof::<u64, String, MptCalc>::from_map(versioned);
    assert_eq!(expected_root, proof.merkle_root(main).unwrap());
}
