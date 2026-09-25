use std::{env, fs, path::Path, process::Command};
use vsdb::{
    InstanceId, Mapx, MapxOrd, MapxOrdRawKey, Namespace, NamespaceOpts, Orphan,
    VsdbOptions, vsdb_configure, vsdb_core::MapxRaw,
};

fn phase(dir: &Path, action: &str) {
    vsdb_configure(if action == "seed" {
        VsdbOptions::new(dir)
    } else {
        VsdbOptions::read_only(dir)
    })
    .unwrap();
    if action == "seed" {
        let ns = Namespace::create_with(NamespaceOpts {
            path: None,
            shards: 1,
            mem_budget_mb: Some(64),
        })
        .unwrap();
        let mut raw = MapxRaw::new_in(&ns);
        let mut plain = Mapx::<u64, u64>::new_in(&ns);
        let mut ordered = MapxOrd::<u64, u64>::new_in(&ns);
        let mut raw_keys = MapxOrdRawKey::<u64>::new_in(&ns);
        let mut scalar = Orphan::new_in(&ns, 0u64);
        let ids = [
            raw.save_meta().unwrap(),
            plain.save_meta().unwrap(),
            ordered.save_meta().unwrap(),
            raw_keys.save_meta().unwrap(),
            scalar.save_meta().unwrap(),
        ];
        fs::write(
            dir.join("wal-handles"),
            postcard::to_allocvec(&ids).unwrap(),
        )
        .unwrap();
        let before = ns.shard_properties("stats.flushes_completed");
        for n in 0u64..16 {
            raw.insert(n.to_be_bytes(), n.to_be_bytes());
            plain.insert(&n, &n);
            ordered.insert(&n, &n);
            raw_keys.insert(n.to_be_bytes(), &n);
            scalar.set_value(&n);
            raw.try_sync_wal().unwrap();
            plain.try_sync_wal().unwrap();
            ordered.try_sync_wal().unwrap();
            raw_keys.try_sync_wal().unwrap();
            scalar.try_sync_wal().unwrap();
        }
        // The namespace form also covers writes not individually fenced.
        scalar.set_value(&99);
        ns.try_sync_wal().unwrap();
        assert_eq!(ns.shard_properties("stats.flushes_completed"), before);
        // Test explicit fences, with no destructor or shutdown flush.
        std::process::exit(0);
    }
    let ids: [InstanceId; 5] =
        postcard::from_bytes(&fs::read(dir.join("wal-handles")).unwrap()).unwrap();
    let raw = MapxRaw::from_meta(ids[0]).unwrap();
    let plain = Mapx::<u64, u64>::from_meta(ids[1]).unwrap();
    let ordered = MapxOrd::<u64, u64>::from_meta(ids[2]).unwrap();
    let raw_keys = MapxOrdRawKey::<u64>::from_meta(ids[3]).unwrap();
    let scalar = Orphan::<u64>::from_meta(ids[4]).unwrap();
    for n in 0u64..16 {
        assert_eq!(raw.get(n.to_be_bytes()).unwrap(), n.to_be_bytes());
        assert_eq!(plain.get(&n), Some(n));
        assert_eq!(ordered.get(&n), Some(n));
        assert_eq!(raw_keys.get(n.to_be_bytes()), Some(n));
    }
    assert_eq!(scalar.get_value(), 99);
    raw.try_sync_wal().unwrap();
    plain.try_sync_wal().unwrap();
    ordered.try_sync_wal().unwrap();
    raw_keys.try_sync_wal().unwrap();
    scalar.try_sync_wal().unwrap();
    scalar.namespace().try_sync_wal().unwrap();
}

#[test]
fn typed_wal_fences_survive_process_exit_without_table_flush() {
    if let Ok(action) = env::var("VSDB_TYPED_WAL_PHASE") {
        phase(
            Path::new(&env::var_os("VSDB_TYPED_WAL_DIR").unwrap()),
            &action,
        );
        return;
    }
    let dir = env::temp_dir().join(format!("vsdb-typed-wal-{}", rand::random::<u128>()));
    for action in ["seed", "read"] {
        let result = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "typed_wal_fences_survive_process_exit_without_table_flush",
                "--nocapture",
            ])
            .env("VSDB_TYPED_WAL_PHASE", action)
            .env("VSDB_TYPED_WAL_DIR", &dir)
            .output()
            .unwrap();
        assert!(result.status.success(), "{action}: {result:?}");
    }
    fs::remove_dir_all(dir).unwrap();
}
