//! Destroying a namespace whose explicit root was provisioned in advance.
#![cfg(unix)]

#[path = "common/scratch.rs"]
mod scratch;

use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf};
use vsdb_core::{MapxRaw, Namespace, NamespaceOpts, VsdbOptions, vsdb_configure};

/// Makes the provisioned parent writable again even if an assertion fails,
/// so the scratch directory can be removed.
struct Writable(PathBuf);

impl Drop for Writable {
    fn drop(&mut self) {
        let _ = fs::set_permissions(&self.0, fs::Permissions::from_mode(0o755));
    }
}

#[test]
fn destroy_keeps_an_adopted_root_it_cannot_remove() {
    let scratch = scratch::database_dir();
    let parent = scratch.join("provisioned");
    let root = parent.join("ns_root");
    fs::create_dir_all(&root).unwrap();
    vsdb_configure(VsdbOptions::new(scratch.join("universe"))).unwrap();
    fs::set_permissions(&parent, fs::Permissions::from_mode(0o555)).unwrap();
    let restore = Writable(parent.clone());

    // Skip when running with DAC bypass (e.g. root), where restricted parent
    // modes do not block writes or reads.
    if fs::create_dir(parent.join("probe")).is_err() {
        for mode in [0o555, 0o111, 0o311] {
            fs::set_permissions(&parent, fs::Permissions::from_mode(mode)).unwrap();
            let ns = Namespace::create_with(NamespaceOpts {
                path: Some(root.clone()),
                shards: 1,
                mem_budget_mb: Some(16),
            })
            .unwrap();
            let id = ns.id();
            let mut map = MapxRaw::new_in(&ns);
            map.insert(b"k", b"v");
            drop(map);
            ns.close().unwrap();

            Namespace::destroy(id).unwrap();
            assert!(!Namespace::list().unwrap().iter().any(|info| info.id == id));
            assert!(root.is_dir(), "mode {mode:#o} removed an unfenceable root");
            assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
        }

        // An already-removed root under a traverse-only parent must still be
        // unregisterable when the process cannot open the parent for fsync.
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o755)).unwrap();
        let ns = Namespace::create_with(NamespaceOpts {
            path: Some(root.clone()),
            shards: 1,
            mem_budget_mb: Some(16),
        })
        .unwrap();
        let missing_id = ns.id();
        ns.close().unwrap();
        fs::remove_dir_all(&root).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o111)).unwrap();
        if fs::File::open(&parent).is_err() {
            Namespace::destroy(missing_id).unwrap();
            assert!(
                !Namespace::list()
                    .unwrap()
                    .iter()
                    .any(|info| info.id == missing_id)
            );
        }
    }

    drop(restore);
    fs::remove_dir_all(&scratch).unwrap();
}
