use std::{env, fs, path::PathBuf, process::Command, sync::Barrier};
use vsdb_core::{MapxRaw, common::atomic_write_file};

mod common;

#[test]
fn atomic_write_file_preserves_existing_staging_files() {
    const CHILD_ROOT: &str = "VSDB_ATOMIC_WRITE_TEST_ROOT";
    if let Some(root) = env::var_os(CHILD_ROOT) {
        let root = PathBuf::from(root);
        let path = root.join("meta.bin");
        // The isolated process starts its staging sequence at zero. Simulate
        // a file left by a previous process whose pid has since been reused.
        let stale = root.join(format!("meta.bin.{}.0.tmp", std::process::id()));
        fs::write(&stale, b"old staging data").unwrap();
        atomic_write_file(&path, b"replacement").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"replacement");
        assert_eq!(fs::read(&stale).unwrap(), b"old staging data");
        assert_eq!(fs::read_dir(root).unwrap().count(), 2);
        return;
    }

    let root =
        env::temp_dir().join(format!("vsdb_atomic_write_{}", rand::random::<u128>()));
    fs::create_dir_all(&root).unwrap();
    let output = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "atomic_write_file_preserves_existing_staging_files",
            "--nocapture",
        ])
        .env(CHILD_ROOT, &root)
        .output()
        .unwrap();
    fs::remove_dir_all(root).unwrap();
    assert!(
        output.status.success(),
        "child failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
fn concurrent_save_meta_remains_restorable() {
    common::setup();
    let mut map = MapxRaw::new();
    map.insert(b"key", b"value");
    let id = map.instance_id();
    let start = Barrier::new(8);
    std::thread::scope(|s| {
        for _ in 0..8 {
            s.spawn(|| {
                start.wait();
                for _ in 0..32 {
                    assert_eq!(map.save_meta().unwrap(), id);
                    let restored = MapxRaw::from_meta(id).unwrap();
                    assert_eq!(restored.instance_id(), id);
                    assert_eq!(
                        restored.get(b"key").as_deref(),
                        Some(b"value".as_slice())
                    );
                }
            });
        }
    });
    assert_eq!(
        MapxRaw::from_meta(id).unwrap().get(b"key"),
        Some(b"value".to_vec())
    );
}
