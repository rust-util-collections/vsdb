//! Atomic publication across related raw map prefixes.
#[path = "common/scratch.rs"]
mod scratch;

use std::{fs, path::Path, process::Command};
use vsdb_core::{
    InstanceId, MapxRaw, Namespace, NamespaceOpts, VsdbError, VsdbOptions,
    vsdb_configure,
};

fn setup() {
    static INIT: parking_lot::Once = parking_lot::Once::new();
    INIT.call_once(|| {
        vsdb_configure(VsdbOptions::new(scratch::database_dir())).unwrap();
    });
}

#[test]
fn rejected_placement_does_not_modify_either_map() {
    setup();
    let ns = Namespace::create_with(NamespaceOpts {
        shards: 2,
        ..Default::default()
    })
    .unwrap();
    let mut a = MapxRaw::new_in(&ns);
    a.insert(b"old", b"preserved");
    let mut b = loop {
        let candidate = MapxRaw::new_in(&ns);
        if !a.is_colocated_with(&candidate) {
            break candidate;
        }
    };
    let mut alias = MapxRaw::from_meta(a.save_meta().unwrap()).unwrap();
    a.insert_colocated(&mut alias, &[(b"same", b"first")], &[(b"same", b"last")])
        .unwrap();
    assert_eq!(a.get(b"same").as_deref(), Some(b"last".as_slice()));
    let mut other = MapxRaw::new_in(&Namespace::create().unwrap());
    for target in [&mut b, &mut other] {
        assert!(matches!(
            a.insert_colocated(target, &[(b"old", b"replaced")], &[(b"x", b"y")]),
            Err(VsdbError::InvalidConfig { .. })
        ));
        assert_eq!(a.get(b"old").as_deref(), Some(b"preserved".as_slice()));
        assert!(!target.contains_key(b"x"));
    }
}

#[test]
fn colocated_write_publishes_puts_and_removals_together() {
    setup();
    let ns = Namespace::create_with(NamespaceOpts {
        shards: 2,
        ..Default::default()
    })
    .unwrap();
    let mut a = MapxRaw::new_in(&ns);
    a.insert(b"gone", b"old");
    a.insert(b"kept", b"old");
    let mut b = a.new_colocated();
    let mut c = a.new_colocated();
    let mut alias = MapxRaw::from_meta(a.save_meta().unwrap()).unwrap();
    let reader = b.reader();
    let before = reader.read_view();
    MapxRaw::write_colocated(&mut [
        (&mut a, &[(b"gone", None), (b"kept", Some(b"first"))]),
        (&mut b, &[(b"owner", Some(b"a"))]),
        (&mut c, &[(b"empty", Some(b""))]),
        (&mut alias, &[(b"kept", Some(b"last"))]),
    ])
    .unwrap();
    assert!(!before.contains_key(b"owner"));
    assert_eq!(a.get(b"gone"), None);
    assert_eq!(a.get(b"kept").as_deref(), Some(b"last".as_slice()));
    assert_eq!(b.get(b"owner").as_deref(), Some(b"a".as_slice()));
    assert_eq!(c.get(b"empty"), Some(Vec::new()));
    MapxRaw::write_colocated(&mut []).unwrap();

    let mut far = loop {
        let candidate = MapxRaw::new_in(&ns);
        if !a.is_colocated_with(&candidate) {
            break candidate;
        }
    };
    let mut foreign = MapxRaw::new_in(&Namespace::create().unwrap());
    for target in [&mut far, &mut foreign] {
        assert!(matches!(
            MapxRaw::write_colocated(&mut [
                (&mut a, &[(b"kept", None)]),
                (target, &[(b"x", Some(b"y"))]),
            ]),
            Err(VsdbError::InvalidConfig { .. })
        ));
        assert_eq!(a.get(b"kept").as_deref(), Some(b"last".as_slice()));
        assert!(!target.contains_key(b"x"));
    }
}

#[test]
fn paired_prefixes_survive_fence_and_process_exit() {
    const VAR: &str = "VSDB_TWO_PREFIX_PHASE";
    if let Ok(phase) = std::env::var(VAR) {
        let base = std::env::var_os("VSDB_BASE_DIR").unwrap();
        let meta = Path::new(&base).join("two-prefix-ids");
        vsdb_configure(VsdbOptions::new(base)).unwrap();
        if phase == "write" {
            let mut parent = MapxRaw::new();
            let mut child = parent.new_colocated();
            let ids = (parent.save_meta().unwrap(), child.save_meta().unwrap());
            fs::write(meta, postcard::to_allocvec(&ids).unwrap()).unwrap();
            let reader = child.reader();
            let before = reader.read_view();
            parent
                .insert_colocated(
                    &mut child,
                    &[
                        (b"next", b"2"),
                        (b"edge1", b"initial"),
                        (b"edge1", b"child"),
                    ],
                    &[(b"owner", b"parent"), (b"empty", b"")],
                )
                .unwrap();
            assert!(!before.contains_key(b"owner"));
            parent.try_sync_wal().unwrap();
            std::process::exit(0);
        }
        let ids: (InstanceId, InstanceId) =
            postcard::from_bytes(&fs::read(meta).unwrap()).unwrap();
        let parent = MapxRaw::from_meta(ids.0).unwrap();
        let child = MapxRaw::from_meta(ids.1).unwrap();
        assert_eq!(parent.get(b"next").as_deref(), Some(b"2".as_slice()));
        assert_eq!(parent.get(b"edge1").as_deref(), Some(b"child".as_slice()));
        assert_eq!(child.get(b"owner").as_deref(), Some(b"parent".as_slice()));
        assert_eq!(child.get(b"empty"), Some(Vec::new()));
        return;
    }
    let base = scratch::database_dir();
    for phase in ["write", "read"] {
        let s = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "paired_prefixes_survive_fence_and_process_exit"])
            .env(VAR, phase)
            .env("VSDB_BASE_DIR", &base)
            .status()
            .unwrap();
        assert!(s.success(), "{phase}: {s}");
    }
    fs::remove_dir_all(base).unwrap();
}
