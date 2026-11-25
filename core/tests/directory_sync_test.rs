#![cfg(target_os = "linux")]

use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    thread,
};
use vsdb_core::{
    InstanceId, MapxRaw, Namespace, VsdbError, VsdbOptions, vsdb_configure,
};

const EIO: i32 = 5;

fn prefix_directory_sync_retries(root: &Path) {
    let map = MapxRaw::new();
    let path = root.join("__SYSTEM__/__prefix_ceiling__");
    let ceiling = || u64::from_le_bytes(fs::read(&path).unwrap().try_into().unwrap());
    let arm = root.join("arm");
    let start = ceiling();
    fs::write(&arm, b"1").unwrap();
    let fails = |r: vsdb_core::Result<InstanceId>| {
        assert!(matches!(r, Err(VsdbError::Io(e)) if e.raw_os_error() == Some(EIO)));
    };
    fails(map.try_instance_id());
    let visible = ceiling();
    assert!(visible > start);
    fails(map.try_instance_id());
    assert_eq!(ceiling(), visible);
    fs::remove_file(&arm).unwrap();
    assert_eq!(map.try_instance_id().unwrap().map_id, start);

    // Each new thread must claim a window. The first failed refill leaves
    // the renamed ceiling visible. Neither attempt may publish that window.
    fs::write(&arm, b"1").unwrap();
    let refill = || {
        thread::spawn(|| MapxRaw::new().try_instance_id())
            .join()
            .unwrap()
    };
    fails(refill());
    assert!(ceiling() > visible);
    fails(refill());
    fs::remove_file(arm).unwrap();
    assert_eq!(refill().unwrap().map_id, visible);
}

fn marker_directory_sync_retries(root: &Path) {
    let ns = Namespace::create().unwrap();
    let id = ns.id();
    let marker = ns.system_dir().join("format_version");
    ns.close().unwrap();
    fs::remove_file(&marker).unwrap();
    let arm = root.join("arm");
    fs::write(&arm, b"1").unwrap();
    for _ in 0..2 {
        assert!(
            matches!(Namespace::open(id), Err(VsdbError::Io(e)) if e.raw_os_error() == Some(EIO))
        );
        assert_eq!(fs::read(&marker).unwrap(), b"18");
    }
    fs::remove_file(arm).unwrap();
    Namespace::open(id).unwrap();
}

/// Exercise the real parent-directory fsync, rather than a simulated crash.
/// An EIO there must prevent success, including when retrying an existing
/// directory created by the failed attempt.
#[test]
fn directory_sync_failure_prevents_metadata_publication() {
    const CHILD_ROOT: &str = "VSDB_DIRECTORY_SYNC_CHILD";
    if let Some(root) = env::var_os(CHILD_ROOT) {
        let root = PathBuf::from(root);
        vsdb_configure(VsdbOptions::new(&root)).unwrap();
        match env::var("VSDB_DIRECTORY_SYNC_CASE").unwrap().as_str() {
            "prefix" => return prefix_directory_sync_retries(&root),
            "marker" => return marker_directory_sync_retries(&root),
            "metadata" => {}
            case => panic!("unknown directory sync case: {case}"),
        }
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
    for case in ["metadata", "prefix", "marker"] {
        let data = root.join(case);
        let target = if case == "marker" {
            data.join("__NAMESPACES__/0000000000000001/__SYSTEM__")
        } else {
            data.join("__SYSTEM__")
        };
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "directory_sync_failure_prevents_metadata_publication",
                "--nocapture",
            ])
            .env(CHILD_ROOT, &data)
            .env("LD_PRELOAD", &shim)
            .env("VSDB_DIRECTORY_SYNC_TARGET", target)
            .env("VSDB_DIRECTORY_SYNC_ARM", data.join("arm"))
            .env("VSDB_DIRECTORY_SYNC_CASE", case)
            .output()
            .unwrap();
        assert!(output.status.success(), "{case} failed: {output:?}");
    }
    fs::remove_dir_all(root).unwrap();
}
