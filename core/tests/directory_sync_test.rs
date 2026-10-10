#![cfg(target_os = "linux")]

use std::{env, fs, path::PathBuf, process::Command};
use vsdb_core::{MapxRaw, VsdbOptions, vsdb_configure};

/// Exercise the real parent-directory fsync, rather than a simulated crash.
/// An EIO there must prevent success, including when retrying an existing
/// directory created by the failed attempt.
#[test]
fn directory_sync_failure_prevents_metadata_publication() {
    const CHILD_ROOT: &str = "VSDB_DIRECTORY_SYNC_CHILD";
    if let Some(root) = env::var_os(CHILD_ROOT) {
        let root = PathBuf::from(root);
        vsdb_configure(VsdbOptions::new(&root)).unwrap();
        let mut map = MapxRaw::new();
        map.insert(b"key", b"value");
        let id = map.instance_id();
        let path = map.namespace().meta_path(id.map_id);
        let arm = root.join("arm");
        fs::write(&arm, b"1").unwrap();
        assert!(
            map.save_meta().is_err(),
            "parent directory sync was skipped"
        );
        assert!(
            !path.exists(),
            "metadata published before its directory link"
        );
        // The directory now exists, but the failed parent fsync must still
        // be retried before a later call may publish metadata.
        assert!(
            map.save_meta().is_err(),
            "retry skipped directory synchronization"
        );
        fs::remove_file(arm).unwrap();
        assert_eq!(map.save_meta().unwrap(), id);
        assert_eq!(
            MapxRaw::from_meta(id).unwrap().get(b"key"),
            Some(b"value".to_vec())
        );
        return;
    }

    let root =
        env::temp_dir().join(format!("vsdb_directory_sync_{}", rand::random::<u128>()));
    fs::create_dir_all(&root).unwrap();
    let source = root.join("gate.c");
    let shim = root.join("gate.so");
    fs::write(
        &source,
        r#"
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
int fsync(int fd) {
    int (*real_fsync)(int) = dlsym(RTLD_NEXT, "fsync");
    const char *target = getenv("VSDB_DIRECTORY_SYNC_TARGET");
    const char *arm = getenv("VSDB_DIRECTORY_SYNC_ARM");
    if (target && arm && access(arm, F_OK) == 0) {
        char link[64], path[4096];
        snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
        ssize_t len = readlink(link, path, sizeof(path) - 1);
        if (len >= 0) {
            path[len] = 0;
            if (strcmp(path, target) == 0) { errno = EIO; return -1; }
        }
    }
    return real_fsync(fd);
}
"#,
    )
    .unwrap();
    let compiled = Command::new("cc")
        .args(["-shared", "-fPIC"])
        .arg(&source)
        .arg("-o")
        .arg(&shim)
        .arg("-ldl")
        .output()
        .unwrap();
    assert!(
        compiled.status.success(),
        "failed to build fsync interposer: {}",
        String::from_utf8_lossy(&compiled.stderr),
    );
    let data = root.join("data");
    let output = Command::new(env::current_exe().unwrap())
        .args([
            "--exact",
            "directory_sync_failure_prevents_metadata_publication",
            "--nocapture",
        ])
        .env(CHILD_ROOT, &data)
        .env("LD_PRELOAD", &shim)
        .env("VSDB_DIRECTORY_SYNC_TARGET", data.join("__SYSTEM__"))
        .env("VSDB_DIRECTORY_SYNC_ARM", data.join("arm"))
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
