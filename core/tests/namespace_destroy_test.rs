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

    // Skip when running with DAC bypass (e.g. root), where the parent stays
    // writable regardless of its mode.
    if fs::create_dir(parent.join("probe")).is_err() {
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
        assert!(root.is_dir());
        assert_eq!(fs::read_dir(&root).unwrap().count(), 0);

        // The kept root can be adopted again.
        let again = Namespace::create_with(NamespaceOpts {
            path: Some(root.clone()),
            shards: 1,
            mem_budget_mb: Some(16),
        })
        .unwrap();
        let again_id = again.id();
        again.close().unwrap();
        Namespace::destroy(again_id).unwrap();
    }

    drop(restore);
    fs::remove_dir_all(&scratch).unwrap();
}
