//! Frozen v17.0.21 data, including an Orphan<CommitId> and a typed handle
//! embedded in another collection's value in a different namespace.
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};
use vsdb::{CommitId, InstanceId, Mapx, MapxOrd, Orphan, VsdbOptions, vsdb_configure};
use vsdb_migrate::{basic_handle, migrate_basic, migrate_universe, rewrite_values};

type Outer = Mapx<String, MapxOrd<u64, String>>;
type Fixture = (InstanceId, InstanceId, InstanceId, Vec<(String, Vec<u8>)>);

#[test]
fn migrate_existing_universe_and_nested_values() {
    let (outer, child, token, files): Fixture =
        postcard::from_bytes(include_bytes!("fixtures/basic-v17.postcard")).unwrap();
    if let Some(root) = std::env::var_os("VSDB_MIGRATION_PROBE") {
        assert!(vsdb_configure(VsdbOptions::new(PathBuf::from(root))).is_err());
        return;
    }
    let root = std::env::temp_dir().join(format!("vsdb-v17-basic-{}", rand::random::<u128>()));
    for (name, bytes) in files {
        let path = root.join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }
    let probe = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "migrate_existing_universe_and_nested_values"])
        .env("VSDB_MIGRATION_PROBE", &root)
        .output()
        .unwrap();
    assert!(probe.status.success(), "{probe:?}");
    // Recover a missing ceiling using the old envelopes while they are still
    // identifiable. No v17 prefix scan is needed inside the v18 allocator.
    fs::remove_file(root.join("__SYSTEM__/__prefix_ceiling__")).unwrap();
    migrate_universe(&root).unwrap();
    let hdr = fs::read(root.join("__SYSTEM__/universe_header")).unwrap();
    assert_eq!(&hdr[..8], b"VSUNI018");
    migrate_universe(&root).unwrap();
    assert_eq!(
        hdr,
        fs::read(root.join("__SYSTEM__/universe_header")).unwrap()
    );
    vsdb_configure(VsdbOptions::new(&root)).unwrap();
    assert!(Outer::from_meta(outer).is_err());
    assert!(Orphan::<CommitId>::from_meta(token).is_err());
    let map = migrate_basic::<Outer>(outer).unwrap();
    let encoded = vsdb_migrate::raw_map(outer)
        .unwrap()
        .iter()
        .next()
        .unwrap()
        .1;
    assert!(postcard::from_bytes::<MapxOrd<u64, String>>(&encoded).is_err());
    let convert = |_: &[u8], bytes: &[u8]| {
        if postcard::from_bytes::<MapxOrd<u64, String>>(bytes).is_ok() {
            Ok(None)
        } else {
            basic_handle::<MapxOrd<u64, String>>(bytes).map(Some)
        }
    };
    assert_eq!(rewrite_values(outer, convert).unwrap(), 1);
    assert_eq!(rewrite_values(outer, convert).unwrap(), 0);
    let converted = basic_handle::<MapxOrd<u64, String>>(&encoded).unwrap();
    assert_eq!(
        basic_handle::<MapxOrd<u64, String>>(&converted).unwrap(),
        converted
    );
    assert!(basic_handle::<MapxOrd<u64, u64>>(&converted).is_err());
    let embedded = map.get("child").unwrap();
    assert_eq!(embedded.instance_id(), child);
    let mut v17 = b"VSMAPX01".to_vec();
    v17.extend(child.map_id.to_le_bytes());
    if let Some(ns) = child.ns {
        v17.extend(ns.to_le_bytes())
    };
    let raw = vsdb_migrate::raw_handle(&postcard::to_allocvec(&v17).unwrap()).unwrap();
    assert_eq!(vsdb_migrate::raw_handle(&raw).unwrap(), raw);
    assert_eq!(
        postcard::from_bytes::<vsdb::MapxRaw>(&raw)
            .unwrap()
            .instance_id(),
        child
    );
    assert_eq!(embedded.get(&1000).as_deref(), Some("nested"));
    assert_eq!(
        migrate_basic::<Orphan<CommitId>>(token)
            .unwrap()
            .get_value()
            .raw(),
        99
    );
    assert_eq!(migrate_basic::<Outer>(outer).unwrap().instance_id(), outer);
    assert!(
        vsdb::MapxRaw::new().instance_id().map_id
            > child.map_id.max(token.map_id).max(outer.map_id)
    );
    fs::remove_dir_all(Path::new(&root)).unwrap();
}

#[test]
fn upgrade_preserves_reserved_ids_and_rejects_reset_registry() {
    let (_, _, _, files): Fixture =
        postcard::from_bytes(include_bytes!("fixtures/basic-v17.postcard")).unwrap();
    let root =
        std::env::temp_dir().join(format!("vsdb-migrate-universe-{}", rand::random::<u128>()));
    for (name, bytes) in files {
        let path = root.join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }
    let registry = root.join("__SYSTEM__/__namespaces__");
    let original = fs::read(&registry).unwrap();
    fs::write(&registry, [1, 0]).unwrap(); // reset next_id despite surviving namespace 1
    assert!(migrate_universe(&root).is_err());
    assert!(!root.join("__SYSTEM__/universe_header").exists());
    fs::write(&registry, [2, 0]).unwrap(); // allocator intact, namespace entry missing
    assert!(migrate_universe(&root).is_err());
    fs::write(&registry, original).unwrap();

    // The v17 writer only understands shard locks. Holding one excludes
    // metadata conversion even with an intact prefix ceiling.
    let lock = fs::File::options()
        .read(true)
        .write(true)
        .open(root.join("mmdb/shard_04/LOCK"))
        .unwrap();
    lock.try_lock().unwrap();
    assert!(migrate_universe(&root).is_err());
    assert!(!root.join("__SYSTEM__/upgrade_v18").exists());
    drop(lock);

    let ceiling = root.join("__SYSTEM__/__prefix_ceiling__");
    let old = u64::from_le_bytes(fs::read(&ceiling).unwrap().try_into().unwrap());
    fs::write(&ceiling, 1u64.to_le_bytes()).unwrap();
    assert!(migrate_universe(&root).is_err());
    let historic = old + 12345;
    let db = mmdb::DB::open(
        mmdb::DbOptions {
            prefix_len: 8,
            ..Default::default()
        },
        root.join("mmdb/shard_00"),
    )
    .unwrap();
    db.put(&[0], &historic.to_le_bytes()).unwrap();
    db.close().unwrap();
    fs::remove_file(&ceiling).unwrap();
    migrate_universe(&root).unwrap();
    assert_eq!(fs::read(&ceiling).unwrap(), historic.to_le_bytes());

    // A save_instance_meta payload may be opaque; the hex filename still
    // reserves an empty map's identity.
    let filename_id = historic + 100;
    let path = root
        .join("__SYSTEM__/__instance_meta__")
        .join(format!("{filename_id:016x}"));
    fs::write(path, b"opaque").unwrap();
    fs::remove_file(&ceiling).unwrap();
    migrate_universe(&root).unwrap();
    assert_eq!(fs::read(&ceiling).unwrap(), (filename_id + 1).to_le_bytes());
    fs::write(&registry, b"VSNREG02bad").unwrap();
    assert!(migrate_universe(&root).is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn interrupted_namespace_and_default_engine_initializers_resume_in_v18() {
    if let Some(root) = std::env::var_os("VSDB_PENDING_MIGRATION_PROBE") {
        vsdb_configure(VsdbOptions::new(PathBuf::from(root))).unwrap();
        assert_eq!(vsdb::Namespace::default_ns().id(), 0);
        assert_eq!(vsdb::Namespace::open(1).unwrap().id(), 1);
        return;
    }
    let root =
        std::env::temp_dir().join(format!("vsdb-pending-migration-{}", rand::random::<u128>()));
    let child = root.join("__NAMESPACES__/0000000000000001");
    let system = root.join("__SYSTEM__");
    fs::create_dir_all(system.join("__namespace_state__")).unwrap();
    fs::create_dir_all(child.join("__SYSTEM__")).unwrap();
    fs::create_dir_all(child.join("mmdb/shard_00")).unwrap(); // no CURRENT yet
    fs::write(system.join("__namespace_state__/0000000000000001"), b"P").unwrap();
    let record = (
        1u64,
        Option::<String>::None,
        2u32,
        Option::<u64>::None,
        123u64,
    );
    fs::write(
        system.join("__namespaces__"),
        postcard::to_allocvec(&(2u64, vec![record])).unwrap(),
    )
    .unwrap();
    fs::write(system.join("__initializing__"), b"1").unwrap();
    let sentinel = child.join("__SYSTEM__/__initializing__");
    fs::write(&sentinel, b"wrong engine").unwrap();
    assert!(migrate_universe(&root).is_err());
    assert!(!system.join("upgrade_v18").exists());
    fs::write(&sentinel, b"1").unwrap();
    migrate_universe(&root).unwrap();
    let header = fs::read(system.join("universe_header")).unwrap();
    for (path, ns, shards) in [(&root, 0u64, 16u32), (&child, 1, 2)] {
        assert!(!path.join("__SYSTEM__/.shards_ready").exists());
        let id = fs::read(path.join("__SYSTEM__/__initializing__")).unwrap();
        assert_eq!(&id[..8], b"VSENG018");
        assert_eq!(&id[8..24], &header[12..]);
        assert_eq!(id[24..32], ns.to_le_bytes());
        assert_eq!(id[32..], shards.to_le_bytes());
    }
    // Simulate termination after publishing the v18 registry and bound
    // sentinels but before the final universe header rename.
    fs::rename(system.join("universe_header"), system.join("upgrade_v18")).unwrap();
    migrate_universe(&root).unwrap();
    assert_eq!(fs::read(system.join("universe_header")).unwrap(), header);
    let probe = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "interrupted_namespace_and_default_engine_initializers_resume_in_v18",
        ])
        .env("VSDB_PENDING_MIGRATION_PROBE", &root)
        .output()
        .unwrap();
    assert!(probe.status.success(), "{probe:?}");
    fs::remove_dir_all(root).unwrap();
}
