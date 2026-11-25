use std::{borrow::Cow, ops::Bound};
use vsdb_core::{
    MAX_GROUPED_READ_KEYS, MapxRaw, Namespace, NamespaceOpts, StorageErrorKind,
    VsdbError,
};

mod common;

#[test]
fn pins_and_bounded_visit_preserve_snapshot_and_slots() {
    common::setup();
    let mut map = MapxRaw::new();
    let large_key = vec![b'x'; 513];
    let mut setup = map.batch();
    setup.insert(b"", b"");
    setup.insert(&large_key, b"kept");
    setup.insert(b"removed", b"old");
    setup.commit_sync_no_slowdown().unwrap();

    let reader = map.reader();
    let view = reader.read_view();
    let mut replace = map.batch_wiped();
    replace.insert(&large_key, b"replaced");
    replace.commit_no_slowdown().unwrap();
    map.namespace().flush();
    let len = |bytes: &[u8]| Ok(bytes.len());
    assert_eq!(view.try_get_with(&large_key, len).unwrap(), Some(4));
    assert_eq!(reader.try_get_with(&large_key, len).unwrap(), Some(8));
    assert_eq!(map.try_get_with(b"", len).unwrap(), None);
    assert_eq!(view.try_get_with(b"", len).unwrap(), Some(0));
    assert!(view.try_contains_key(b"removed").unwrap());
    assert!(!map.try_contains_key(b"removed").unwrap());

    let keys: &[&[u8]] = &[&large_key, b"missing", b"", &large_key];
    let mut seen = Vec::new();
    view.try_visit_many(keys, |slot, value| {
        seen.push((slot, value.map(<[u8]>::to_vec)));
        Ok(())
    })
    .unwrap();
    assert_eq!(
        seen,
        vec![
            (0, Some(b"kept".to_vec())),
            (1, None),
            (2, Some(Vec::new())),
            (3, Some(b"kept".to_vec()))
        ]
    );
    let mut seen = 0;
    let failure = view.try_visit_many(keys, |slot, _| {
        seen += 1;
        if slot == 1 {
            return Err(VsdbError::Other {
                detail: "decoder stopped".into(),
            });
        }
        Ok(())
    });
    assert!(matches!(failure, Err(VsdbError::Other { .. })));
    assert_eq!(seen, 2);
    view.try_visit_many(&[], |_, _| panic!("no slots")).unwrap();
    let err = view
        .try_visit_many(&[b"".as_slice(); MAX_GROUPED_READ_KEYS + 1], |_, _| {
            panic!("oversized request should fail")
        })
        .unwrap_err();
    assert!(matches!(
        err,
        VsdbError::Storage {
            kind: StorageErrorKind::InvalidArgument,
            ..
        }
    ));
}

#[test]
fn borrowed_cursor_bounds_are_shared_between_forward_and_reverse_steps() {
    common::setup();
    let mut map = MapxRaw::new();
    for key in [b"a".as_slice(), b"a\0", b"a\0\0", b"b"] {
        map.insert(key, key);
    }
    let view = map.read_view();
    let bounds = (
        Bound::Excluded(Cow::Borrowed(b"a".as_slice())),
        Bound::Included(Cow::Borrowed(b"b".as_slice())),
    );
    let mut cursor = view.try_range(bounds).unwrap();
    assert_eq!(
        cursor.try_step_prev().unwrap(),
        Some((b"b".as_slice(), b"b".as_slice()))
    );
    assert_eq!(
        cursor.try_step_next().unwrap(),
        Some((b"a\0".as_slice(), b"a\0".as_slice()))
    );
    assert_eq!(
        cursor.try_step_prev().unwrap(),
        Some((b"a\0\0".as_slice(), b"a\0\0".as_slice()))
    );
    assert_eq!(cursor.try_step_next().unwrap(), None);
    let mut cursor = map.try_range(..).unwrap();
    assert_eq!(cursor.try_step_next().unwrap().unwrap().0, b"a");
}

#[test]
fn namespace_reports_one_pool_and_each_members_own_memtables() {
    common::setup();
    let ns = Namespace::create_with(NamespaceOpts {
        shards: 2,
        mem_budget_mb: Some(32),
        ..Default::default()
    })
    .unwrap();
    let first = ns.memory_usage();
    assert_eq!(first.shards.len(), 2);
    assert_eq!(first.pool.segmented_capacity_bytes, 4 << 20);
    assert_eq!(first.pool.overflow_capacity_bytes, 4 << 20);
    assert_eq!(first.write_buffer_bytes_per_shard, 768 << 10);
    assert_eq!(first.immutable_limit_per_shard, 4);
    let mut map = MapxRaw::new_in(&ns);
    map.insert(b"test", vec![1; 128 << 10]);
    let before_flush = ns.memory_usage();
    let shard = map.instance_id().map_id as usize % 2;
    assert!(
        before_flush.shards[shard].active_memtable_bytes
            > first.shards[shard].active_memtable_bytes
    );
    assert_eq!(before_flush.shards[1 - shard], first.shards[1 - shard]);
    ns.flush();
    let after = ns.memory_usage();
    assert_eq!(after.shards[shard].immutable_memtable_count, 0);
    assert_eq!(after.shards[shard].immutable_memtable_bytes, 0);
    assert!(
        after.shards[shard].active_memtable_bytes
            < before_flush.shards[shard].active_memtable_bytes
    );
}

#[test]
fn fence_and_geometry_child() {
    use std::io::Write;
    let Ok(mode) = std::env::var("VSDB_V5_ADAPTER_CHILD") else {
        return;
    };
    let root = std::env::var_os("VSDB_V5_ADAPTER_ROOT").unwrap();
    let read_only = mode == "read" || mode == "reject-ro";
    let opts = if read_only {
        vsdb_core::VsdbOptions::read_only(&root)
    } else {
        vsdb_core::VsdbOptions::new(&root)
    };
    if mode.starts_with("reject-") {
        let error = vsdb_core::vsdb_configure(opts).unwrap_err();
        assert!(matches!(error, VsdbError::Corrupt { .. }), "{error}");
        return;
    }
    vsdb_core::vsdb_configure(opts).unwrap();
    if mode == "read" {
        let saved = std::env::var("VSDB_V5_ADAPTER_ID")
            .unwrap()
            .parse::<vsdb_core::InstanceId>()
            .unwrap();
        let map = MapxRaw::from_meta(saved).unwrap();
        for i in [0u64, 511, 1023, 1535] {
            assert_eq!(map.get(i.to_be_bytes()).unwrap(), vec![i as u8; 8192]);
        }
        assert_eq!(map.get(b"residual"), Some(b"after rotation".to_vec()));
        map.try_sync_wal().unwrap();
        return;
    }
    let mut map = MapxRaw::new();
    let id = map.save_meta().unwrap();
    let ns = map.namespace();
    let shard = id.map_id as usize % 16;
    for i in 0..1536u64 {
        map.insert(i.to_be_bytes(), vec![i as u8; 8192]);
    }
    let start = std::time::Instant::now();
    while ns.shard_properties("stats.flushes_completed")[shard].as_deref() == Some("0") {
        assert!(
            start.elapsed() < std::time::Duration::from_secs(30),
            "automatic rotation/flush did not finish"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    map.insert(b"residual", b"after rotation");
    map.try_sync_wal().unwrap();
    println!("FENCED {id}");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn acknowledged_native_fence_survives_kill_and_wrong_last_shard_leaves_source_intact() {
    use std::{
        collections::BTreeMap,
        fs,
        io::{BufRead, BufReader},
        path::{Path, PathBuf},
        process::{Command, Stdio},
    };
    fn command(root: &Path, mode: &str) -> Command {
        let mut cmd = Command::new(std::env::current_exe().unwrap());
        cmd.args(["--exact", "fence_and_geometry_child", "--nocapture"])
            .env("VSDB_V5_ADAPTER_CHILD", mode)
            .env("VSDB_V5_ADAPTER_ROOT", root);
        cmd
    }
    fn inventory(dir: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for item in fs::read_dir(dir).unwrap() {
            let path = item.unwrap().path();
            if path.is_dir() {
                inventory(&path, files);
            } else {
                files.insert(path.clone(), fs::read(path).unwrap());
            }
        }
    }
    let root =
        std::env::temp_dir().join(format!("vsdb-v5-fence-{}", rand::random::<u128>()));
    let mut child = command(&root, "write")
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let id = BufReader::new(child.stdout.take().unwrap())
        .lines()
        .map(|row| row.unwrap())
        .find_map(|row| row.strip_prefix("FENCED ").map(str::to_owned))
        .expect("native WAL fence acknowledgment");
    child.kill().unwrap();
    child.wait().unwrap();
    let out = command(&root, "read")
        .env("VSDB_V5_ADAPTER_ID", id)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let last = root.join("mmdb/shard_15");
    for (levels, prefix) in [(4, 8), (7, 0)] {
        fs::remove_dir_all(&last).unwrap();
        mmdb::DB::open(
            mmdb::DbOptions::default()
                .with_num_levels(levels)
                .with_prefix_len(prefix),
            &last,
        )
        .unwrap()
        .close()
        .unwrap();
        let mut before = BTreeMap::new();
        inventory(&root, &mut before);
        for mode in ["reject-ro", "reject-rw"] {
            let out = command(&root, mode).output().unwrap();
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
            let mut after = BTreeMap::new();
            inventory(&root, &mut after);
            assert_eq!(
                before.keys().collect::<Vec<_>>(),
                after.keys().collect::<Vec<_>>()
            );
            for (path, old) in &before {
                assert!(old == &after[path], "changed {}", path.display());
            }
        }
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn named_budget_floor_is_preserved_through_reopen() {
    common::setup();
    for mb in [0, 1, 7, 8] {
        let ns = Namespace::create_with(NamespaceOpts {
            shards: 2,
            mem_budget_mb: Some(mb),
            ..Default::default()
        })
        .unwrap();
        let id = ns.id();
        let check = |ns: &Namespace| {
            assert_eq!(ns.mem_budget_bytes(), 8 << 20);
            let usage = ns.memory_usage();
            assert_eq!(usage.shards.len(), 2);
            assert_eq!(usage.pool.segmented_capacity_bytes, 1 << 20);
            assert_eq!(usage.pool.overflow_capacity_bytes, 1 << 20);
        };
        check(&ns);
        ns.close().unwrap();
        let reopened = Namespace::open(id).unwrap();
        check(&reopened);
        reopened.close().unwrap();
    }
}
