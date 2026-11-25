use super::*;
use crate::StorageErrorKind;
use std::collections::BTreeMap;

fn wal_files(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fs::read_dir(path)
        .unwrap()
        .map(|file| file.unwrap().path())
        .filter(|file| file.extension().is_some_and(|ext| ext == "wal"))
        .map(|file| {
            let contents = fs::read(&file).unwrap();
            (file, contents)
        })
        .collect()
}

#[test]
fn native_shard_fence_and_admission_preserve_wal_and_sequence() {
    let dir = std::env::temp_dir()
        .join(format!("vsdb-v5-admission-{}", rand::random::<u128>()));
    let options = DbOptions::default()
        .with_prefix_len(PREFIX_SIZE)
        .with_l0_compaction_trigger(usize::MAX)
        .with_l0_slowdown_trigger(3)
        .with_l0_stop_trigger(3);
    let db = DB::open(options.clone(), &dir).unwrap();
    let engine = MmDB {
        dbs: vec![db].into_boxed_slice(),
        read_only: false,
        mem_budget: 8 << 20,
    };
    let prefix = 42u64.to_le_bytes();
    let db = engine.shard(&prefix);

    for key in [b"a", b"b", b"c"] {
        let mut batch = engine.batch_begin(Ok(prefix));
        batch.insert(key, key);
        batch.commit().unwrap();
        db.flush().unwrap();
    }
    assert_eq!(db.get_property("num-files-at-level0").as_deref(), Some("3"));
    let sequence = db.snapshot().sequence();
    let logs = wal_files(&dir);
    assert!(!logs.is_empty());
    for sync in [false, true] {
        let mut batch = engine.batch_begin(Ok(prefix));
        batch.insert(b"rejected", b"value");
        let err = batch.commit_no_slowdown(sync).unwrap_err();
        assert!(
            matches!(&err, VsdbError::Storage { kind: StorageErrorKind::Busy, source }
            if source.downcast_ref::<mmdb::Error>().is_some_and(|e| e.kind() == mmdb::ErrorKind::Busy))
        );
    }
    assert_eq!(db.snapshot().sequence(), sequence);
    assert_eq!(wal_files(&dir), logs);
    assert_eq!(engine.get(prefix, b"rejected"), None);

    db.compact().unwrap();
    let mut retry = engine.batch_begin(Ok(prefix));
    retry.insert(b"rejected", b"accepted");
    retry.commit_no_slowdown(false).unwrap();
    let accepted = db.snapshot().sequence();
    assert_eq!(accepted, sequence + 1);
    let logs = wal_files(&dir);
    engine.try_sync_wal(prefix).unwrap();
    engine.try_sync_all_wals().unwrap();
    assert_eq!(db.snapshot().sequence(), accepted);
    assert_eq!(wal_files(&dir), logs);

    let mut malformed = engine.batch_begin(Ok(prefix));
    malformed.remove(&vec![0; mmdb::MAX_USER_KEY_SIZE]);
    let error = malformed.commit_no_slowdown(false).unwrap_err();
    assert!(matches!(
        error,
        VsdbError::Storage {
            kind: StorageErrorKind::InvalidArgument,
            ..
        }
    ));
    assert_eq!(db.snapshot().sequence(), accepted);
    assert_eq!(wal_files(&dir), logs);
    engine.close().unwrap();
    {
        let reopened = DB::open(options, &dir).unwrap();
        for key in [b"a", b"b", b"c"] {
            assert_eq!(
                reopened
                    .get(&make_full_key(&prefix, key))
                    .unwrap()
                    .as_deref(),
                Some(key.as_slice())
            );
        }
        assert_eq!(
            reopened
                .get(&make_full_key(&prefix, b"rejected"))
                .unwrap()
                .as_deref(),
            Some(b"accepted".as_slice())
        );
    }
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn namespace_budget_counts_all_five_possible_generations_and_both_stores() {
    let normal = EngineSizing::from_budget_mb(2048);
    assert_eq!(normal.write_buffer_size(16), 6 << 20);
    assert_eq!(normal.write_buffer_size(1), 96 << 20);
    assert_eq!(normal.pool_store_capacity(), 256 << 20);
    let small = EngineSizing::from_budget_mb(1);
    assert_eq!(small.pool_store_capacity(), 1 << 20);
    assert_eq!(small.write_buffer_size(1), 384 << 10);
    assert_eq!(small.write_buffer_size(16), 64 << 10);
    assert_eq!(
        EngineSizing::from_budget_mb(usize::MAX).write_buffer_size(1),
        512 << 20
    );
}
